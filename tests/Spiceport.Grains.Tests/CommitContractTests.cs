using System.Text;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Step-4 gates for the Commit wire contract (docs/graph-sharded-datastore.md section 3): relationship
/// writes/deletes/counters are DECLARATIVE <see cref="CommitRequest"/>s executed inside the sequencer
/// (<see cref="IDatastoreGrain.Commit"/> — one hop, no client retry), while the lambda
/// <c>GrainBackedDatastore.ReadWriteTx</c> survives as the ExpectedHead-CAS compatibility path. Every
/// rejection crosses the grain boundary as STRUCTURED REPLY DATA and is rethrown by the client as the
/// exact typed exception the write surface has always thrown, so the gRPC status mappings
/// (FailedPrecondition / AlreadyExists / Aborted) are pinned unchanged. Driven THROUGH the real grain
/// mesh (in-process Orleans TestingHost), the same way the data-plane suite drives its writes.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class CommitContractTests
{
    private const string ViewerSchema = """
        definition user {}

        definition document {
            relation viewer: user
            permission view = viewer
        }
        """;

    private static RelationshipUpdateWire Update(RelationshipUpdateOpWire op, string res, string subj) =>
        new(op, new RelationshipWire("document", res, "viewer", "user", subj, CoreConstants.Ellipsis, null, null, null));

    private static RelationshipUpdateWire Touch(string res, string subj) =>
        Update(RelationshipUpdateOpWire.Touch, res, subj);

    private static RelationshipUpdateWire Create(string res, string subj) =>
        Update(RelationshipUpdateOpWire.Create, res, subj);

    /// <summary>A filter over document#viewer rows, optionally narrowed to one resource id.</summary>
    private static RelationshipsFilterWire ViewerFilter(string? resourceId = null) => new(
        "document", null, resourceId is null ? null : new List<string> { resourceId }, "viewer",
        null, null, null);

    private static IDatastoreGrain Sequencer(MeshTestCluster cluster) =>
        cluster.GrainFactory.GetGrain<IDatastoreGrain>(IDatastoreGrain.Key);

    /// <summary>Decodes a reply token back to the grain-minted revision (nanos).</summary>
    private static async Task<long> RevisionOf(MeshTestCluster cluster, string token)
    {
        var parser = await cluster.Datastore.GetRevisionParser(CancellationToken.None);
        var decoded = ZedTokens.DecodeRevision(new ZedToken(token), parser);
        Assert.Equal(ZedTokenStatus.Valid, decoded.Status);
        return ((TimestampRevision)decoded.Revision).TimestampNanosSinceEpoch;
    }

    /// <summary>Fully-consistent read of the live document#viewer rows for one resource.</summary>
    private static async Task<List<RelationshipWire>> ReadViewers(MeshTestCluster cluster, string resourceId)
    {
        var rows = new List<RelationshipWire>();
        await foreach (var item in cluster.RelationshipReads.ReadRelationships(
            new ReadRelationshipsArgs(ViewerFilter(resourceId), null, null, ConsistencyWire.FullyConsistent)))
            rows.Add(item.Relationship);
        return rows;
    }

    // ---- 1. MustMatch preconditions ride the declarative commit. ----

    [Fact]
    public async Task MustMatch_precondition_satisfied_commits_the_write()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerSchema);
        await cluster.Relationships.WriteRelationships(
            new WriteRelationshipsArgs(new List<RelationshipUpdateWire> { Touch("seed", "alice") }));

        var reply = await cluster.Relationships.WriteRelationships(new WriteRelationshipsArgs(
            new List<RelationshipUpdateWire> { Touch("guarded", "bob") },
            new List<PreconditionWire>
            {
                new(PreconditionOpWire.MustMatch, ViewerFilter("seed")),
            }));

        Assert.False(string.IsNullOrEmpty(reply.WrittenAtToken));
        Assert.Single(await ReadViewers(cluster, "guarded"));
    }

    [Fact]
    public async Task MustMatch_precondition_failure_throws_the_typed_exception_and_nothing_commits()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerSchema);
        await cluster.Relationships.WriteRelationships(
            new WriteRelationshipsArgs(new List<RelationshipUpdateWire> { Touch("seed", "alice") }));

        // Two preconditions: the first passes, the SECOND fails — pins that the failing index (not just
        // "a failure") survives the reply-data round trip into the rethrown exception.
        var ex = await Assert.ThrowsAsync<PreconditionFailedException>(() =>
            cluster.Relationships.WriteRelationships(new WriteRelationshipsArgs(
                new List<RelationshipUpdateWire> { Touch("guarded", "bob") },
                new List<PreconditionWire>
                {
                    new(PreconditionOpWire.MustMatch, ViewerFilter("seed")),
                    new(PreconditionOpWire.MustMatch, ViewerFilter("missing")),
                })));

        Assert.Equal(PreconditionFailureKind.MustMatchFoundNone, ex.Kind);
        Assert.Equal(1, ex.PreconditionIndex);

        // Atomic rejection: the guarded update never landed.
        Assert.Empty(await ReadViewers(cluster, "guarded"));
    }

    // ---- 2. MustNotMatch preconditions. ----

    [Fact]
    public async Task MustNotMatch_precondition_satisfied_commits_the_write()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerSchema);

        var reply = await cluster.Relationships.WriteRelationships(new WriteRelationshipsArgs(
            new List<RelationshipUpdateWire> { Touch("guarded", "bob") },
            new List<PreconditionWire>
            {
                new(PreconditionOpWire.MustNotMatch, ViewerFilter("missing")),
            }));

        Assert.False(string.IsNullOrEmpty(reply.WrittenAtToken));
        Assert.Single(await ReadViewers(cluster, "guarded"));
    }

    [Fact]
    public async Task MustNotMatch_precondition_failure_throws_the_typed_exception_and_nothing_commits()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerSchema);
        await cluster.Relationships.WriteRelationships(
            new WriteRelationshipsArgs(new List<RelationshipUpdateWire> { Touch("seed", "alice") }));

        var ex = await Assert.ThrowsAsync<PreconditionFailedException>(() =>
            cluster.Relationships.WriteRelationships(new WriteRelationshipsArgs(
                new List<RelationshipUpdateWire> { Touch("guarded", "bob") },
                new List<PreconditionWire>
                {
                    new(PreconditionOpWire.MustNotMatch, ViewerFilter("seed")),
                })));

        Assert.Equal(PreconditionFailureKind.MustNotMatchFoundOne, ex.Kind);
        Assert.Equal(0, ex.PreconditionIndex);
        Assert.Empty(await ReadViewers(cluster, "guarded"));
    }

    // ---- 3. Duplicate CREATE is a typed conflict; the first create survives. ----

    [Fact]
    public async Task Duplicate_create_throws_the_typed_conflict_and_first_create_survives()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerSchema);

        var first = await cluster.Relationships.WriteRelationships(
            new WriteRelationshipsArgs(new List<RelationshipUpdateWire> { Create("readme", "alice") }));
        Assert.False(string.IsNullOrEmpty(first.WrittenAtToken));

        var ex = await Assert.ThrowsAsync<WriteConflictException>(() =>
            cluster.Relationships.WriteRelationships(
                new WriteRelationshipsArgs(new List<RelationshipUpdateWire> { Create("readme", "alice") })));
        Assert.Equal(WriteConflictKind.CreateExisting, ex.Kind);

        // The first create landed and is untouched by the rejected duplicate.
        var rows = await ReadViewers(cluster, "readme");
        Assert.Single(rows);
        Assert.Equal("alice", rows[0].SubjectId);

        // A Touch of the same tuple afterwards is the documented upsert escape hatch and still works.
        var touched = await cluster.Relationships.WriteRelationships(
            new WriteRelationshipsArgs(new List<RelationshipUpdateWire> { Touch("readme", "alice") }));
        Assert.False(string.IsNullOrEmpty(touched.WrittenAtToken));
        Assert.Single(await ReadViewers(cluster, "readme"));
    }

    // ---- 4. DeleteByFilter with a limit reports count/reached-limit and stops exactly at the limit. ----

    [Fact]
    public async Task Delete_with_limit_reports_reached_and_leaves_the_remainder()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerSchema);
        await cluster.Relationships.WriteRelationships(new WriteRelationshipsArgs(
            Enumerable.Range(1, 5).Select(i => Touch("bulk", $"user{i}")).ToList()));

        var limited = await cluster.Relationships.DeleteRelationships(
            new DeleteRelationshipsArgs(ViewerFilter("bulk"), OptionalLimit: 3));
        Assert.Equal(3UL, limited.DeletedCount);
        Assert.True(limited.ReachedLimit);

        // Exactly 2 remain, observed via a snapshot reader pinned at the delete's own token revision.
        var deleteRevision = await RevisionOf(cluster, limited.DeletedAtToken);
        var reader = cluster.Datastore.SnapshotReader(new TimestampRevision(deleteRevision));
        var remaining = new List<Relationship>();
        await foreach (var rel in reader.QueryRelationships(
            new RelationshipsFilter { OptionalResourceType = "document", OptionalResourceIds = new[] { "bulk" } }))
            remaining.Add(rel);
        Assert.Equal(2, remaining.Count);

        var rest = await cluster.Relationships.DeleteRelationships(
            new DeleteRelationshipsArgs(ViewerFilter("bulk"), OptionalLimit: null));
        Assert.Equal(2UL, rest.DeletedCount);
        Assert.False(rest.ReachedLimit);
        Assert.Empty(await ReadViewers(cluster, "bulk"));
    }

    // ---- 5. ExpectedHead CAS: the lambda compatibility path retries a lost race and loses no update. ----

    /// <summary>
    /// The Commit-path equivalent of <c>GrainBackedDatastoreWriteBaseTests.RacingWrites_*</c> (which pins
    /// the retry/call-count mechanics against a counting fake): here two concurrent ReadWriteTx lambdas
    /// race THROUGH the real sequencer grain, each doing a read-modify-write (create a row whose id is the
    /// number of rows it currently sees). A lost update would make both writers see the same count — the
    /// second Create would then conflict (or a row would silently vanish) — so both committing with rows
    /// "s0" AND "s1" present proves the ExpectedHead CAS rejected the stale commit and the loser re-ran
    /// its whole lambda against the fresh base.
    /// </summary>
    [Fact]
    public async Task Racing_ReadWriteTx_lambdas_both_commit_and_the_state_reflects_both()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerSchema);
        var ds = cluster.Datastore;

        var filter = new RelationshipsFilter
        {
            OptionalResourceType = "document",
            OptionalResourceIds = new[] { "tally" },
        };

        async Task<IRevision> WriteNext() => await ds.ReadWriteTx(async tx =>
        {
            var count = 0;
            await foreach (var _ in tx.QueryRelationships(filter))
                count++;
            await tx.WriteRelationships(new[]
            {
                new RelationshipUpdate(
                    Relationship.Create(
                        new ObjectAndRelation("document", "tally", "viewer"),
                        new ObjectAndRelation("user", $"s{count}", CoreConstants.Ellipsis)),
                    UpdateOperation.Create),
            });
        });

        var first = WriteNext();
        var second = WriteNext();
        var revisions = await Task.WhenAll(first, second);

        Assert.NotEqual(
            ((TimestampRevision)revisions[0]).TimestampNanosSinceEpoch,
            ((TimestampRevision)revisions[1]).TimestampNanosSinceEpoch);

        var subjects = (await ReadViewers(cluster, "tally")).Select(r => r.SubjectId).ToHashSet();
        Assert.Equal(new HashSet<string> { "s0", "s1" }, subjects);
    }

    // ---- 6. ExpectedSchemaHash: schema-write serializability at the grain, convergence at the RPC. ----

    [Fact]
    public async Task Stale_ExpectedSchemaHash_is_rejected_as_SchemaHashMoved_and_normal_WriteSchema_converges()
    {
        const string SchemaA = ViewerSchema;
        const string SchemaB = ViewerSchema + "\ndefinition folder {}";
        const string SchemaC = ViewerSchema + "\ndefinition team {}";

        await using var cluster = await MeshTestCluster.CreateAsync(SchemaA);

        // Store schema A and pin the stored-schema hash the sequencer's gate compares.
        await cluster.WriteSchema(SchemaA);
        var hashA = (await cluster.Datastore.HeadRevision(CancellationToken.None)).SchemaHash;
        Assert.False(string.IsNullOrEmpty(hashA));

        // Schema C lands through the RPC — the hash at head has now moved past A's.
        await cluster.WriteSchema(SchemaC);
        var hashC = (await cluster.Datastore.HeadRevision(CancellationToken.None)).SchemaHash;
        Assert.NotEqual(hashA, hashC);

        // A commit carrying B validated against A's hash (the simulated in-flight WriteSchema B) must be
        // rejected as SchemaHashMoved at the grain — as reply data, with nothing applied.
        var reply = await Sequencer(cluster).Commit(new CommitRequest(
            Array.Empty<CommitPreconditionWire>(),
            Array.Empty<RelationshipUpdateWire>(),
            DeleteByFilter: null,
            SchemaBytes: Encoding.UTF8.GetBytes(SchemaB),
            ExpectedSchemaHash: hashA,
            CounterChanges: Array.Empty<CounterDeltaWire>(),
            ExpectedHead: null));

        Assert.Null(reply.Revision);
        Assert.Equal(CommitFailureKind.SchemaHashMoved, reply.Failure!.Kind);
        Assert.Equal(hashC, (await cluster.Datastore.HeadRevision(CancellationToken.None)).SchemaHash);

        // Driven normally, the RPC's validate-and-commit loop re-pins a fresh hash and converges.
        var written = await cluster.WriteSchema(SchemaB);
        Assert.False(string.IsNullOrEmpty(written.WrittenAtToken));
        Assert.Equal(SchemaB, cluster.SchemaProvider.Current.SourceText);
    }

    // ---- 7. Serialized declarative commits need no retry. ----

    [Fact]
    public async Task Parallel_declarative_writes_all_commit_with_strictly_increasing_revisions()
    {
        const int Writers = 20;
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerSchema);

        var preHead = ((TimestampRevision)(await cluster.Datastore.HeadRevision(CancellationToken.None)).Revision)
            .TimestampNanosSinceEpoch;

        // Distinct tuples in parallel through the RPC path. A declarative commit executes inside the
        // single-threaded sequencer, so every one must succeed first try — a SerializationException here
        // (or any other throw) fails the Task.WhenAll and the test.
        var replies = await Task.WhenAll(Enumerable.Range(0, Writers).Select(i =>
            cluster.Relationships.WriteRelationships(
                new WriteRelationshipsArgs(new List<RelationshipUpdateWire> { Touch($"doc{i}", "alice") }))));

        var tokenRevisions = new HashSet<long>();
        foreach (var reply in replies)
            tokenRevisions.Add(await RevisionOf(cluster, reply.WrittenAtToken));
        Assert.Equal(Writers, tokenRevisions.Count);

        // The log is the authority on the commit order: exactly one event per write, strictly increasing,
        // and carrying exactly the revisions the reply tokens minted.
        var segment = await Sequencer(cluster).ReadFrom(preHead, -1);
        Assert.Equal(Writers, segment.Events.Count);
        for (var i = 1; i < segment.Events.Count; i++)
            Assert.True(
                segment.Events[i].Revision > segment.Events[i - 1].Revision,
                $"log revisions not strictly increasing at index {i}");
        Assert.Equal(tokenRevisions, segment.Events.Select(e => e.Revision).ToHashSet());
    }

    // ---- 8. Watch continuity over the new write path. ----

    [Fact]
    public async Task Watch_tails_declarative_commits_with_their_minted_revisions()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerSchema);
        var ds = cluster.Datastore;

        var preWrite = (await ds.HeadRevision(CancellationToken.None)).Revision;

        var expected = new List<long>();
        foreach (var res in new[] { "a", "b", "c" })
        {
            var reply = await cluster.Relationships.WriteRelationships(
                new WriteRelationshipsArgs(new List<RelationshipUpdateWire> { Touch(res, "alice") }));
            expected.Add(await RevisionOf(cluster, reply.WrittenAtToken));
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var changes = new List<RevisionChange>();
        try
        {
            await foreach (var change in ds.Watch(
                preWrite, new WatchOptions(WatchContent.Relationships), cts.Token))
            {
                changes.Add(change);
                if (changes.Count >= 3)
                {
                    await cts.CancelAsync();
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Cancellation racing the final yield is fine; the assertions below are the gate.
        }

        Assert.Equal(3, changes.Count);
        Assert.Equal(new[] { "a", "b", "c" }, changes.Select(c => c.RelationshipChanges.Single().Relationship.Resource.ObjectId));
        Assert.Equal(expected, changes.Select(c => ((TimestampRevision)c.Revision).TimestampNanosSinceEpoch));
    }

    // ---- 9. Counter guards: typed rejections and the happy path. ----

    [Fact]
    public async Task Register_counter_twice_throws_the_typed_already_registered_exception()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerSchema);

        await cluster.Relationships.RegisterRelationshipCounter(
            new RegisterCounterArgs("viewers", ViewerFilter()));

        var ex = await Assert.ThrowsAsync<CounterOperationException>(() =>
            cluster.Relationships.RegisterRelationshipCounter(
                new RegisterCounterArgs("viewers", ViewerFilter())));
        Assert.Equal(CounterErrorKind.AlreadyRegistered, ex.Kind);
        Assert.Contains("viewers", ex.Message);
    }

    [Fact]
    public async Task Unregister_missing_counter_throws_the_typed_not_registered_exception()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerSchema);

        var ex = await Assert.ThrowsAsync<CounterOperationException>(() =>
            cluster.Relationships.UnregisterRelationshipCounter(new UnregisterCounterArgs("missing")));
        Assert.Equal(CounterErrorKind.NotRegistered, ex.Kind);
        Assert.Contains("missing", ex.Message);
    }

    /// <summary>The lossless full-filter form of <see cref="ViewerFilter"/> for direct grain commits.</summary>
    private static FullRelationshipsFilterWire FullViewerFilter(string resourceId) => new(
        "document", new List<string> { resourceId }, null, "viewer", null, null, 0);

    /// <summary>A direct-grain <see cref="CommitRequest"/> with everything defaulted to empty/absent.</summary>
    private static CommitRequest DirectCommit(
        IReadOnlyList<CommitPreconditionWire>? preconditions = null,
        IReadOnlyList<RelationshipUpdateWire>? updates = null,
        DeleteByFilterWire? deleteByFilter = null,
        byte[]? schemaBytes = null,
        string? expectedSchemaHash = null,
        long? expectedHead = null) => new(
            preconditions ?? Array.Empty<CommitPreconditionWire>(),
            updates ?? Array.Empty<RelationshipUpdateWire>(),
            deleteByFilter,
            schemaBytes,
            expectedSchemaHash,
            Array.Empty<CounterDeltaWire>(),
            expectedHead);

    // ---- 10. Preconditions evaluate against the PRE-mutation snapshot. ----

    [Fact]
    public async Task Preconditions_evaluate_before_the_commits_own_mutations()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerSchema);
        var sequencer = Sequencer(cluster);

        // x currently has NO viewers: a MustNotMatch(viewers-of-x) riding the very commit that touches a
        // viewer of x must SUCCEED — the precondition is evaluated before any mutation, so the commit's
        // own update cannot trip it.
        var reply = await sequencer.Commit(DirectCommit(
            preconditions: new[] { new CommitPreconditionWire(FullViewerFilter("x"), MustMatch: false) },
            updates: new[] { Touch("x", "bob") }));
        Assert.Null(reply.Failure);
        Assert.NotNull(reply.Revision);
        Assert.Single(await ReadViewers(cluster, "x"));

        // The inverse: a MustMatch that only the commit's OWN update would satisfy must FAIL — the
        // pre-mutation evaluation never sees the staged update, so nothing commits.
        var inverse = await sequencer.Commit(DirectCommit(
            preconditions: new[] { new CommitPreconditionWire(FullViewerFilter("y"), MustMatch: true) },
            updates: new[] { Touch("y", "carol") }));
        Assert.Null(inverse.Revision);
        Assert.Equal(CommitFailureKind.PreconditionFailed, inverse.Failure!.Kind);
        Assert.Empty(await ReadViewers(cluster, "y"));
    }

    // ---- 11. Failed commits leave the head and the log untouched. ----

    [Fact]
    public async Task Failed_commits_leave_the_head_and_the_log_untouched()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerSchema);
        var sequencer = Sequencer(cluster);
        var headBefore = (await sequencer.GetHead()).Head;

        // A PreconditionFailed commit: rejected, and neither the head nor the log moved.
        var rejected = await sequencer.Commit(DirectCommit(
            preconditions: new[] { new CommitPreconditionWire(FullViewerFilter("missing"), MustMatch: true) },
            updates: new[] { Touch("x", "bob") }));
        Assert.Equal(CommitFailureKind.PreconditionFailed, rejected.Failure!.Kind);
        Assert.Equal(headBefore, (await sequencer.GetHead()).Head);
        Assert.Empty((await sequencer.ReadFrom(headBefore, -1)).Events);

        // A HeadMoved commit (stale ExpectedHead): same guarantee.
        var stale = await sequencer.Commit(DirectCommit(
            updates: new[] { Touch("x", "bob") },
            expectedHead: headBefore - 1));
        Assert.Equal(CommitFailureKind.HeadMoved, stale.Failure!.Kind);
        Assert.Equal(headBefore, (await sequencer.GetHead()).Head);
        Assert.Empty((await sequencer.ReadFrom(headBefore, -1)).Events);
    }

    // ---- 12. Updates stage BEFORE DeleteByFilter runs (the declared contract order). ----

    [Fact]
    public async Task Updates_stage_before_the_delete_by_filter_runs()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerSchema);
        var sequencer = Sequencer(cluster);

        // Empty store: the commit touches a viewer of x AND deletes viewers-of-x. Contract order stages
        // the touch first, so the delete sees (and removes) it: DeletedCount is 1 and the final state has
        // no viewers of x. A swapped order would delete nothing and leave the touched row live.
        var reply = await sequencer.Commit(DirectCommit(
            updates: new[] { Touch("x", "alice") },
            deleteByFilter: new DeleteByFilterWire(FullViewerFilter("x"), Limit: null)));
        Assert.Null(reply.Failure);
        Assert.NotNull(reply.Revision);
        Assert.Equal(1UL, reply.DeletedCount);
        Assert.False(reply.ReachedLimit);
        Assert.Empty(await ReadViewers(cluster, "x"));
    }

    // ---- 13. The empty ExpectedSchemaHash is a SEED-WINDOW claim, rejected once a schema is stored. ----

    [Fact]
    public async Task Empty_expected_schema_hash_is_rejected_once_a_schema_hash_exists()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerSchema);
        var sequencer = Sequencer(cluster);

        // Store a schema so the hash at head is non-null (the seed window is over).
        await cluster.WriteSchema(ViewerSchema);
        var storedHash = (await cluster.Datastore.HeadRevision(CancellationToken.None)).SchemaHash;
        Assert.False(string.IsNullOrEmpty(storedHash));

        // An empty ExpectedSchemaHash claims the pre-first-schema seed window; with a real hash stored it
        // must be rejected as SchemaHashMoved with nothing applied.
        var reply = await sequencer.Commit(DirectCommit(
            schemaBytes: Encoding.UTF8.GetBytes(ViewerSchema + "\ndefinition intruder {}"),
            expectedSchemaHash: string.Empty));
        Assert.Null(reply.Revision);
        Assert.Equal(CommitFailureKind.SchemaHashMoved, reply.Failure!.Kind);
        Assert.Equal(storedHash, (await cluster.Datastore.HeadRevision(CancellationToken.None)).SchemaHash);
    }

    // ---- 14. Deterministic ExpectedHead CAS (the racing test above covers the concurrent shape). ----

    [Fact]
    public async Task Stale_ExpectedHead_is_rejected_as_HeadMoved_with_the_head_unchanged()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerSchema);
        var sequencer = Sequencer(cluster);
        var head = (await sequencer.GetHead()).Head;

        var reply = await sequencer.Commit(DirectCommit(
            updates: new[] { Touch("cas", "alice") },
            expectedHead: head - 1));

        Assert.Null(reply.Revision);
        Assert.Equal(CommitFailureKind.HeadMoved, reply.Failure!.Kind);
        Assert.Equal(head, (await sequencer.GetHead()).Head);
        Assert.Empty(await ReadViewers(cluster, "cas"));
    }

    // ---- 15. Failed commits never notify watchers; successful ones do. ----

    /// <summary>Records every <see cref="IDatastoreWatcher.HeadAdvanced"/> push it receives.</summary>
    private sealed class RecordingWatcher : IDatastoreWatcher
    {
        private readonly object _lock = new();
        private readonly List<long> _heads = new();

        public Task HeadAdvanced(long head)
        {
            lock (_lock)
                _heads.Add(head);
            return Task.CompletedTask;
        }

        public IReadOnlyList<long> Heads()
        {
            lock (_lock)
                return _heads.ToArray();
        }
    }

    [Fact]
    public async Task Failed_commit_never_notifies_watchers_and_a_successful_commit_does()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerSchema);
        var sequencer = Sequencer(cluster);

        // Register a watcher observer exactly the way LogWatchHub does (CreateObjectReference +
        // SubscribeWatch); the subscribe reply pins the pre-failure head.
        var watcher = new RecordingWatcher();
        var reference = cluster.GrainFactory.CreateObjectReference<IDatastoreWatcher>(watcher);
        var preFailureHead = (await sequencer.SubscribeWatch(reference)).Head;

        var rejected = await sequencer.Commit(DirectCommit(
            preconditions: new[] { new CommitPreconditionWire(FullViewerFilter("missing"), MustMatch: true) },
            updates: new[] { Touch("x", "bob") }));
        Assert.Equal(CommitFailureKind.PreconditionFailed, rejected.Failure!.Kind);

        // HeadAdvanced is [OneWay], so give the (phantom) push a short deterministic settle window and
        // require that NO notification past the pre-failure head ever arrives.
        var settleDeadline = DateTime.UtcNow + TimeSpan.FromMilliseconds(500);
        while (DateTime.UtcNow < settleDeadline)
        {
            Assert.DoesNotContain(watcher.Heads(), h => h > preFailureHead);
            await Task.Delay(25);
        }
        Assert.DoesNotContain(watcher.Heads(), h => h > preFailureHead);

        // Positive control in the same registration: a successful commit DOES notify, with exactly the
        // minted revision.
        var committed = await sequencer.Commit(DirectCommit(updates: new[] { Touch("notify", "alice") }));
        Assert.Null(committed.Failure);
        var minted = committed.Revision!.Value;

        var notifyDeadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (!watcher.Heads().Contains(minted) && DateTime.UtcNow < notifyDeadline)
            await Task.Delay(25);
        Assert.Contains(minted, watcher.Heads());
    }

    [Fact]
    public async Task Counter_register_count_unregister_happy_path()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerSchema);

        await cluster.Relationships.RegisterRelationshipCounter(
            new RegisterCounterArgs("viewers", ViewerFilter()));
        await cluster.Relationships.WriteRelationships(new WriteRelationshipsArgs(
            new List<RelationshipUpdateWire> { Touch("a", "alice"), Touch("b", "bob") }));

        var counted = await cluster.Relationships.CountRelationships(new CountRelationshipsArgs("viewers"));
        Assert.Equal(2UL, counted.Count);
        Assert.False(string.IsNullOrEmpty(counted.ReadAtToken));

        await cluster.Relationships.UnregisterRelationshipCounter(new UnregisterCounterArgs("viewers"));

        // The unregister is live: counting again is the typed not-registered failure.
        var ex = await Assert.ThrowsAsync<CounterOperationException>(() =>
            cluster.Relationships.CountRelationships(new CountRelationshipsArgs("viewers")));
        Assert.Equal(CounterErrorKind.NotRegistered, ex.Kind);
    }
}
