using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Orleans.EventSourcing;
using Orleans.Runtime;
using Orleans.Storage;
using Spiceport.Conformance.Tests;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Closing gates for the THIN-SEQUENCER flush protocol (docs/graph-sharded-datastore.md §6): the
/// <see cref="DatastoreGrain"/> journals only the small <see cref="DatastoreMetaState"/>, keeps a dirty
/// buffer of touched shard keys, and every <c>FlushInterval</c> (64) events writes the dirty keys'
/// version-qualified <c>shard/{flushVersion}/{dir}/{type}/{id}</c> rows, writes a <c>meta/{version}</c>
/// row carrying the key->rowVersion index maps, and trims the log.
/// These tests pin: flush-boundary integrity (reads at head through the sharded reader AND the scan seam
/// stay exact after the buffer has been flushed and the log trimmed), the memory ceiling itself (the
/// journaled state type carries NO relationship-row collections), the clean-key relabel rule (a stored
/// row's content is complete unless an event touched its key — <c>ShardFoldLemmaTests</c>), lazy row-GC
/// (floor enforced on every serve path while stored rows compact only at their next dirty+flush), the
/// partial write base (Commit candidate-key resolution pulls in keys named only by precondition filters,
/// forward AND reverse), and recovery fidelity across a genuine deactivation (head/schema/counters/rows/
/// Watch tail all rebuilt from head+meta+shard rows+log tail).
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class ThinSequencerTests
{
    private const string Schema = """
        definition user {}

        caveat is_active(level int) {
          level > 0
        }

        definition doc {
          relation viewer: user | user with is_active
        }
        """;

    private static Relationship Row(string docId, string userId) => Relationship.Create(
        new ObjectAndRelation("doc", docId, "viewer"),
        new ObjectAndRelation("user", userId, CoreConstants.Ellipsis));

    private static Task<IRevision> Write(IDatastore ds, Relationship rel, UpdateOperation op = UpdateOperation.Create) =>
        ds.ReadWriteTx(tx => tx.WriteRelationships([new RelationshipUpdate(rel, op)]));

    private static IDatastoreGrain Sequencer(MeshTestCluster cluster) =>
        cluster.GrainFactory.GetGrain<IDatastoreGrain>(IDatastoreGrain.Key);

    private static long Nanos(IRevision revision) => ((TimestampRevision)revision).TimestampNanosSinceEpoch;

    private static async Task<List<string>> Collect(IAsyncEnumerable<Relationship> source)
    {
        var rows = new List<string>();
        await foreach (var rel in source)
            rows.Add(Canonical(rel));
        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    private static string Canonical(Relationship rel) =>
        $"{rel.Resource.ObjectType}:{rel.Resource.ObjectId}#{rel.Resource.Relation}" +
        $"@{rel.Subject.ObjectType}:{rel.Subject.ObjectId}#{rel.Subject.Relation}";

    /// <summary>Reads one raw storage row of the sequencer through the SAME keyed provider the grain uses.</summary>
    private static async Task<GrainState<T>> ReadRow<T>(MeshTestCluster cluster, string stateName)
    {
        var storage = cluster.Services.GetRequiredKeyedService<IGrainStorage>("datastore");
        var grainId = ((GrainReference)Sequencer(cluster)).GrainId;
        var entry = new GrainState<T>();
        await storage.ReadStateAsync(stateName, grainId, entry);
        return entry;
    }

    // ---- (a) Flush-boundary integrity: dirty-buffer flush + log trim + meta write. ----

    /// <summary>
    /// More than <c>FlushInterval</c> (64) commits across several keys — creates, a delete, a touch, and
    /// a post-flush key — then, WITHOUT any restart, reads at head through BOTH the shard mesh
    /// (<see cref="ShardedGraphReader"/>) and the scan seam (<see cref="ISnapshotScanner"/>) must equal
    /// the independently-tracked expectation. The storage-level probes then pin that the flush protocol
    /// actually ran: the head row's <c>SnapshotVersion</c> sits on the 64-boundary, the
    /// <c>meta/{version}</c> row exists, the seed <c>meta/0</c> row and every log entry at or below the
    /// flush boundary are trimmed, and the post-flush log tail is still present.
    /// </summary>
    [Fact]
    public async Task Reads_stay_exact_after_the_dirty_buffer_flush_trims_the_log()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var ds = cluster.Datastore;

        // The live-set model the reads must reproduce, keyed by resource id.
        var expected = new Dictionary<string, SortedSet<string>>(StringComparer.Ordinal);
        void Add(string res, string subj)
        {
            if (!expected.TryGetValue(res, out var set))
                expected[res] = set = new SortedSet<string>(StringComparer.Ordinal);
            set.Add(subj);
        }

        // 67 creates across five keys (commits 1..67) — comfortably past the 64-event flush boundary.
        for (var i = 0; i < 67; i++)
        {
            var res = $"k{i % 5}";
            var subj = $"u{i}";
            await Write(ds, Row(res, subj));
            Add(res, subj);
        }

        // Commit 68: delete a PRE-flush row (its tombstone must land on the already-flushed key).
        await Write(ds, Row("k0", "u0"), UpdateOperation.Delete);
        expected["k0"].Remove("u0");

        // Commit 69: touch an existing pre-flush row (replace-in-place; the live set is unchanged).
        await Write(ds, Row("k1", "u1"), UpdateOperation.Touch);

        // Commit 70: a key that never existed before the flush (dirty-buffer-only at read time).
        await Write(ds, Row("k9", "fresh"));
        Add("k9", "fresh");

        var head = (await ds.HeadRevision()).Revision;

        // Scan seam: the whole live set in one broad scan.
        var scanner = cluster.Services.GetRequiredService<ISnapshotScanner>();
        var expectedAll = expected
            .SelectMany(kv => kv.Value.Select(subj => Canonical(Row(kv.Key, subj))))
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();
        var scanned = await Collect(scanner.Scan(
            new RelationshipsFilter { OptionalResourceType = "doc" }, head, CancellationToken.None));
        Assert.Equal(expectedAll, scanned);

        // Shard mesh: per-key forward reads, plus reverse probes for the deleted/touched/fresh subjects.
        IGraphReader sharded = new ShardedGraphReader(cluster.GrainFactory, Nanos(head));
        foreach (var (res, subjects) in expected)
        {
            var viaShard = await Collect(sharded.QueryRelationships(new RelationshipsFilter
            {
                OptionalResourceType = "doc",
                OptionalResourceIds = [res],
            }));
            Assert.Equal(subjects.Select(s => Canonical(Row(res, s))).ToList(), viaShard);
        }
        Assert.Empty(await Collect(sharded.ReverseQueryRelationships(
            new SubjectsFilter("user", OptionalSubjectIds: ["u0"]))));
        Assert.Equal(
            [Canonical(Row("k1", "u1"))],
            await Collect(sharded.ReverseQueryRelationships(new SubjectsFilter("user", OptionalSubjectIds: ["u1"]))));
        Assert.Equal(
            [Canonical(Row("k9", "fresh"))],
            await Collect(sharded.ReverseQueryRelationships(new SubjectsFilter("user", OptionalSubjectIds: ["fresh"]))));

        // Storage-level: the flush protocol really ran. 70 commits => LogVersion 70, flush boundary 64.
        var headRow = await ReadRow<LogHeadEntry>(cluster, "head");
        Assert.True(headRow.RecordExists);
        var logVersion = headRow.State!.LogVersion;
        var flushVersion = headRow.State.SnapshotVersion;
        Assert.True(logVersion >= 70, $"expected at least the 70 test commits, saw LogVersion {logVersion}");
        Assert.Equal(logVersion - logVersion % 64, flushVersion); // the meta row is the latest 64-boundary
        Assert.True(flushVersion >= 64, "no flush happened — the gate exercised nothing");

        var metaRow = await ReadRow<DatastoreMetaEntry>(cluster, $"meta/{flushVersion}");
        Assert.True(metaRow.RecordExists, "the flush's meta/{version} row is missing");
        Assert.Equal(flushVersion, metaRow.State!.FlushedThroughLogVersion);

        // Log trim: entries at or below the boundary cleared; the post-flush tail retained; the seed
        // meta/0 row cleared once superseded.
        Assert.False((await ReadRow<LogEvent>(cluster, "log/1")).RecordExists, "log/1 survived compaction");
        Assert.False(
            (await ReadRow<LogEvent>(cluster, $"log/{flushVersion}")).RecordExists,
            "the flush-boundary log entry survived compaction");
        Assert.True(
            (await ReadRow<LogEvent>(cluster, $"log/{flushVersion + 1}")).RecordExists,
            "the first post-flush log entry is missing — the tail was over-trimmed");
        Assert.False((await ReadRow<DatastoreMetaEntry>(cluster, "meta/0")).RecordExists, "meta/0 survived compaction");
    }

    // ---- (b) The memory ceiling: no relationship rows in the journaled state; the relabel rule. ----

    /// <summary>
    /// The tripwire for the ceiling this rework removed: the sequencer's <c>JournaledGrain</c> state
    /// type must be the slim <see cref="DatastoreMetaHolder"/>/<see cref="DatastoreMetaState"/> pair and
    /// must not reach — anywhere in its public shape — a <see cref="StoredRelationshipWire"/> or any
    /// collection of them. A regression that re-materializes the whole fold in grain state cannot pass.
    /// </summary>
    [Fact]
    public void Journaled_state_carries_no_relationship_rows_anywhere_in_its_shape()
    {
        // Locate JournaledGrain<TState, TEvent> in DatastoreGrain's base chain and take TState.
        Type? baseType = typeof(DatastoreGrain);
        while (baseType is not null &&
               (!baseType.IsGenericType || baseType.GetGenericTypeDefinition() != typeof(JournaledGrain<,>)))
            baseType = baseType.BaseType;
        Assert.NotNull(baseType);
        var stateType = baseType!.GetGenericArguments()[0];

        // The slim meta-state pair exists and IS the journaled state.
        Assert.Equal(typeof(DatastoreMetaHolder), stateType);
        Assert.Equal(
            typeof(DatastoreMetaState),
            stateType.GetProperty(nameof(DatastoreMetaHolder.Value))!.PropertyType);

        // Walk the whole public shape: every property type and every generic type argument, transitively.
        var visited = new HashSet<Type>();
        var queue = new Queue<Type>();
        queue.Enqueue(stateType);
        while (queue.Count > 0)
        {
            var type = queue.Dequeue();
            if (!visited.Add(type))
                continue;

            Assert.False(
                type == typeof(StoredRelationshipWire),
                $"the journaled state reaches StoredRelationshipWire (via {type})");
            Assert.False(
                typeof(IEnumerable<StoredRelationshipWire>).IsAssignableFrom(type),
                $"the journaled state reaches a collection of StoredRelationshipWire ({type})");

            if (type.IsGenericType)
            {
                foreach (var argument in type.GetGenericArguments())
                    queue.Enqueue(argument);
            }

            if (type.Namespace?.StartsWith("Spiceport", StringComparison.Ordinal) == true)
            {
                foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                    queue.Enqueue(property.PropertyType);

                // Private instance fields too: a row collection smuggled into the journaled state
                // behind a private field (or a record's compiler-generated backing field) reaches the
                // same memory ceiling as a public property and must trip the wire just the same.
                foreach (var field in type.GetFields(
                             BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
                    queue.Enqueue(field.FieldType);
            }
        }
    }

    /// <summary>
    /// The behavioral half of the memory bound — the CLEAN-KEY RELABEL RULE the whole design rests on
    /// (pinned as a fold lemma by <c>ShardFoldLemmaTests</c>, here proven live): seed a real corpus
    /// file, push the sequencer across a flush boundary with commits that never touch the probed
    /// resource's forward key, keep committing past the flush — and <c>ReadShard</c> for that clean key
    /// must answer with <c>AppliedRevision == the CURRENT head</c> (its stored row relabeled, no per-key
    /// tail replay) while its rows still exactly match the snapshot reader.
    /// </summary>
    [Fact]
    public async Task Clean_key_ReadShard_serves_its_stored_row_relabeled_to_the_current_head()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "TestData", "multipleops.yaml");
        Assert.True(File.Exists(path), $"Linked corpus file missing from output: {path}");
        var file = ValidationFileLoader.LoadFromFile(path);
        Assert.True(file.Relationships.Count > 0);

        await using var cluster = await MeshTestCluster.CreateAsync(file.SchemaText);
        var updates = file.Relationships
            .Select(r => new RelationshipUpdate(r, UpdateOperation.Create))
            .ToList();
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(updates));

        var probe = file.Relationships[0];

        // 64 filler commits cross the flush boundary (clearing the dirty buffer), then 3 more advance
        // the head PAST the flush revision — so at read time the probed key's stored row is stamped
        // strictly below head and only the relabel rule can make the reply's watermark reach it. The
        // fillers reuse the corpus's own resource type/relation/subject (schema-valid) under fresh
        // resource ids, so the probed FORWARD key is never touched again after the seed commit.
        for (var i = 0; i < 67; i++)
        {
            var filler = Relationship.Create(
                probe.Resource with { ObjectId = $"thin-seq-filler-{i}" },
                probe.Subject);
            await Write(cluster.Datastore, filler);
        }

        var head = await Sequencer(cluster).GetHead();
        var shard = await Sequencer(cluster).ReadShard(
            GraphShardKeyWire.ForResource(probe.Resource.ObjectType, probe.Resource.ObjectId));

        Assert.Equal(head.Head, shard.AppliedRevision); // the relabel rule, live
        Assert.Equal(head.GcFloor, shard.GcFloor);

        // And the relabeled content is exact: it equals the snapshot reader's rows for the same key.
        var viaReader = await Collect(cluster.Datastore
            .SnapshotReader(new TimestampRevision(head.Head))
            .QueryRelationships(new RelationshipsFilter
            {
                OptionalResourceType = probe.Resource.ObjectType,
                OptionalResourceIds = [probe.Resource.ObjectId],
            }));
        var viaShard = shard.Rows
            .Where(r => r.DeletedRevision is null)
            .Select(r => Canonical(WireConvert.ToRelationship(r.Relationship)))
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();
        Assert.NotEmpty(viaReader);
        Assert.Equal(viaReader, viaShard);
    }

    // ---- (c) Lazy row-GC: floor on every serve path; physical compaction at the next dirty+flush. ----

    /// <summary>
    /// Lazy-GC correctness end to end. A key ("cold") accumulates a dead row, is flushed (so its stored
    /// row physically retains the tombstone), and is never re-dirtied while a GC advances the floor past
    /// the tombstone: the CLEAN key must still serve exactly right at head (the serve-path
    /// <c>CollectBelow</c> hides the lazily-retained dead row) and a below-floor pin must be rejected.
    /// The key is then re-dirtied and pushed through another flush (which physically compacts its row),
    /// the whole cluster's activations are dropped, and the reactivated sequencer — now recovering over
    /// the compacted row — must still serve the same answers and still reject the below-floor pin
    /// (compaction itself is internal; the restart makes its correctness observable).
    /// </summary>
    [Fact]
    public async Task Lazy_gc_serves_clean_keys_exactly_and_compaction_survives_recovery()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema, gcWindow: TimeSpan.Zero);
        var ds = cluster.Datastore;
        var grain = Sequencer(cluster);

        var rev1 = await ds.ReadWriteTx(tx => tx.WriteRelationships(
        [
            new RelationshipUpdate(Row("cold", "carol"), UpdateOperation.Create),
            new RelationshipUpdate(Row("cold", "dead"), UpdateOperation.Create),
            new RelationshipUpdate(Row("x", "alice"), UpdateOperation.Create),
        ]));
        await Write(ds, Row("cold", "dead"), UpdateOperation.Delete);

        // Cross the flush boundary on OTHER keys: "cold"'s stored row (carol live + dead's tombstone)
        // is durable and its dirty entry cleared — from here on "cold" is the clean key under test.
        for (var i = 0; i < 64; i++)
            await Write(ds, Row($"f{i}", "filler"));

        // Dirty a DIFFERENT key, then GC: Window=Zero makes the floor land at the current head, past
        // the tombstone — but "cold" is never dirtied, so its stored row keeps the dead row lazily.
        await Write(ds, Row("x", "alice"), UpdateOperation.Delete);
        var floor = await grain.RunGc();
        Assert.NotNull(floor);
        Assert.True(floor > Nanos(rev1));

        // The clean key serves exactly at head: the serve path re-applies the floor, so the lazily
        // retained dead row is invisible, and the reply is relabeled to the current head.
        var head = await grain.GetHead();
        var shard = await grain.ReadShard(GraphShardKeyWire.ForResource("doc", "cold"));
        Assert.Equal(head.Head, shard.AppliedRevision);
        Assert.Equal("carol", Assert.Single(shard.Rows).Relationship.SubjectId);

        // Below-floor pins are rejected outright — through the snapshot reader AND the shard mesh.
        await Assert.ThrowsAsync<RevisionNotFoundException>(async () =>
        {
            await foreach (var _ in ds.SnapshotReader(rev1).QueryRelationships(new RelationshipsFilter()))
            {
            }
        });
        var coldShardGrain = cluster.GrainFactory.GetGrain<IGraphShardGrain>(
            GraphShardGrainKey.Build(GraphShardKeyWire.ForResource("doc", "cold")));
        await Assert.ThrowsAsync<RevisionNotFoundException>(
            () => coldShardGrain.RowsAt(Nanos(rev1), CancellationToken.None));

        // Re-dirty "cold" and push another flush past it: this flush is where its stored row
        // physically compacts (the tombstone is finally dropped from storage).
        await Write(ds, Row("cold", "late"));
        for (var i = 0; i < 64; i++)
            await Write(ds, Row($"g{i}", "filler"));
        var headBeforeRestart = await grain.GetHead();

        // Drop EVERY activation (grain state survives only in the storage provider), then re-read:
        // recovery over the compacted row must serve the same answers.
        var management = cluster.GrainFactory.GetGrain<IManagementGrain>(0);
        await management.ForceActivationCollection(TimeSpan.Zero);
        var stats = await management.GetDetailedGrainStatistics();
        Assert.DoesNotContain(stats, s =>
            s.GrainType.ToString()!.Contains("datastoregrain", StringComparison.OrdinalIgnoreCase));

        var headAfter = await grain.GetHead();
        Assert.Equal(headBeforeRestart.Head, headAfter.Head); // negative control: lost state re-seeds a new head

        var shardAfter = await grain.ReadShard(GraphShardKeyWire.ForResource("doc", "cold"));
        Assert.Equal(headAfter.Head, shardAfter.AppliedRevision);
        Assert.Equal(
            new[] { "carol", "late" },
            shardAfter.Rows.Where(r => r.DeletedRevision is null)
                .Select(r => r.Relationship.SubjectId).OrderBy(s => s, StringComparer.Ordinal).ToArray());

        IGraphReader sharded = new ShardedGraphReader(cluster.GrainFactory, headAfter.Head);
        Assert.Equal(
            new[] { Canonical(Row("cold", "carol")), Canonical(Row("cold", "late")) },
            await Collect(sharded.QueryRelationships(new RelationshipsFilter
            {
                OptionalResourceType = "doc",
                OptionalResourceIds = ["cold"],
            })));

        // The below-floor rejection also survives recovery.
        await Assert.ThrowsAsync<RevisionNotFoundException>(async () =>
        {
            await foreach (var _ in ds.SnapshotReader(rev1).QueryRelationships(new RelationshipsFilter()))
            {
            }
        });
    }

    // ---- (d) Partial-base sufficiency: candidate-key resolution pulls in filter-only keys. ----

    private static RelationshipUpdateWire TouchWire(string docId, string userId) => new(
        RelationshipUpdateOpWire.Touch,
        WireConvert.ToWire(Row(docId, userId)));

    private static CommitRequest Guarded(CommitPreconditionWire precondition, RelationshipUpdateWire update) => new(
        // Concrete List<T>s, not collection expressions: the compiler-synthesized single-element list
        // type a collection expression produces has no Orleans serialization codec.
        new List<CommitPreconditionWire> { precondition },
        new List<RelationshipUpdateWire> { update },
        DeleteByFilter: null, SchemaBytes: null,
        ExpectedSchemaHash: null, CounterChanges: Array.Empty<CounterDeltaWire>(), ExpectedHead: null);

    private static async Task<bool> Exists(IDatastore ds, string docId)
    {
        var head = (await ds.HeadRevision()).Revision;
        await foreach (var _ in ds.SnapshotReader(head).QueryRelationships(new RelationshipsFilter
        {
            OptionalResourceType = "doc",
            OptionalResourceIds = [docId],
        }))
            return true;
        return false;
    }

    /// <summary>
    /// A declarative Commit whose precondition filter names a resource NOT otherwise touched by the
    /// commit — and whose key is CLEAN (flushed to storage, not in the dirty buffer) — so the partial
    /// write base is sufficient only if candidate-key resolution pulls that key in from its stored row.
    /// Both outcomes are pinned: MustMatch satisfied commits the guarded update; MustMatch against a
    /// missing resource rejects with <see cref="CommitFailureKind.PreconditionFailed"/> and applies
    /// nothing.
    /// </summary>
    [Fact]
    public async Task Commit_precondition_on_an_untouched_clean_resource_key_behaves_exactly()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var ds = cluster.Datastore;
        var grain = Sequencer(cluster);

        await Write(ds, Row("a", "alice"));

        // Flush "a" out of the dirty buffer so the precondition can only be answered from its stored
        // shard row (the partial-base storage path, not the in-memory overlay).
        for (var i = 0; i < 64; i++)
            await Write(ds, Row($"pf{i}", "filler"));

        // Concrete List<string>s inside anything that rides a CommitRequest (see Guarded's note).
        var onA = WireConvert.ToFullFilter(new RelationshipsFilter
        {
            OptionalResourceType = "doc",
            OptionalResourceIds = new List<string> { "a" },
        });
        var satisfied = await grain.Commit(Guarded(new CommitPreconditionWire(onA, MustMatch: true), TouchWire("b", "bob")));
        Assert.Null(satisfied.Failure);
        Assert.NotNull(satisfied.Revision);
        Assert.True(await Exists(ds, "b"));

        var onMissing = WireConvert.ToFullFilter(new RelationshipsFilter
        {
            OptionalResourceType = "doc",
            OptionalResourceIds = new List<string> { "nonexistent" },
        });
        var violated = await grain.Commit(Guarded(new CommitPreconditionWire(onMissing, MustMatch: true), TouchWire("c", "carol")));
        Assert.Null(violated.Revision);
        Assert.Equal(CommitFailureKind.PreconditionFailed, violated.Failure!.Kind);
        Assert.False(await Exists(ds, "c")); // atomic rejection: nothing applied
    }

    /// <summary>
    /// The reverse-index candidate path: a precondition filter constraining ONLY subjects must resolve
    /// its candidates through the REVERSE key index (the updated resource's forward key alone cannot
    /// answer it). Satisfied (an existing subject) commits; violated (a subject that matches nothing)
    /// rejects with <see cref="CommitFailureKind.PreconditionFailed"/> and applies nothing.
    /// </summary>
    [Fact]
    public async Task Commit_subject_only_precondition_resolves_through_the_reverse_index_both_ways()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var ds = cluster.Datastore;
        var grain = Sequencer(cluster);

        await Write(ds, Row("a", "alice"));

        // Flush so user:alice's reverse key lives only in its stored row at commit time.
        for (var i = 0; i < 64; i++)
            await Write(ds, Row($"sf{i}", "filler"));

        // Concrete List<T>s inside anything that rides a CommitRequest (see Guarded's note).
        var subjectAlice = WireConvert.ToFullFilter(new RelationshipsFilter
        {
            OptionalSubjectsSelectors = new List<SubjectsSelector>
            {
                new("user", OptionalSubjectIds: new List<string> { "alice" }),
            },
        });
        var satisfied = await grain.Commit(
            Guarded(new CommitPreconditionWire(subjectAlice, MustMatch: true), TouchWire("b", "bob")));
        Assert.Null(satisfied.Failure);
        Assert.NotNull(satisfied.Revision);
        Assert.True(await Exists(ds, "b"));

        var subjectNobody = WireConvert.ToFullFilter(new RelationshipsFilter
        {
            OptionalSubjectsSelectors = new List<SubjectsSelector>
            {
                new("user", OptionalSubjectIds: new List<string> { "nobody" }),
            },
        });
        var violated = await grain.Commit(
            Guarded(new CommitPreconditionWire(subjectNobody, MustMatch: true), TouchWire("d", "dave")));
        Assert.Null(violated.Revision);
        Assert.Equal(CommitFailureKind.PreconditionFailed, violated.Failure!.Kind);
        Assert.False(await Exists(ds, "d"));
    }

    // ---- (e) Restart fidelity across a genuine deactivation. ----

    private static readonly DateTimeOffset Expiry = new(2031, 6, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>
    /// The recovery gate over the full mixed load: relationships (plain, caveated-with-context,
    /// expiring), a stored-schema write, a counter, a delete, and a REAL GC event, all crossing a flush
    /// boundary — then every activation in the cluster is force-collected (the in-process equivalent of
    /// a restart under the shared in-memory storage provider) and, after reactivation,
    /// head/schema-hash/floor/counters must be identical, sharded reads at head must equal the
    /// pre-deactivation captures, and a Watch resumed from a pre-deactivation cursor must replay exactly
    /// the remaining committed events from the rebuilt log tail. The head-equality assertion doubles as
    /// the negative control: lost state would re-seed a fresh, different head with nothing in it.
    /// </summary>
    [Fact]
    public async Task Deactivation_recovery_preserves_head_schema_counters_reads_and_watch_tail()
    {
        // A realistic (1h) GC window: RunGc genuinely appends a GC event with a floor at now-1h (far
        // below every revision this test mints), so the mixed load contains a real GC without ever
        // invalidating the Watch cursor captured below.
        await using var cluster = await MeshTestCluster.CreateAsync(Schema, gcWindow: TimeSpan.FromHours(1));
        var ds = cluster.Datastore;
        var grain = Sequencer(cluster);

        var caveatContext =
            System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object?>>("""{"region":"eu","level":7}""")!;
        var schemaBytes = System.Text.Encoding.UTF8.GetBytes(Schema);

        await ds.ReadWriteTx(async tx =>
        {
            await tx.WriteStoredSchema(schemaBytes);
            await tx.WriteRelationships(
            [
                new RelationshipUpdate(Row("plain", "alice"), UpdateOperation.Create),
                new RelationshipUpdate(
                    Relationship.Create(
                        new ObjectAndRelation("doc", "caveated", "viewer"),
                        new ObjectAndRelation("user", "bob", CoreConstants.Ellipsis),
                        caveat: new ContextualizedCaveat("is_active", caveatContext)),
                    UpdateOperation.Create),
                new RelationshipUpdate(
                    Relationship.Create(
                        new ObjectAndRelation("doc", "expiring", "viewer"),
                        new ObjectAndRelation("user", "carol", CoreConstants.Ellipsis),
                        expiration: Expiry),
                    UpdateOperation.Create),
            ]);
            await tx.WriteCounter("doc_viewers", new RelationshipsFilter
            {
                OptionalResourceType = "doc",
                OptionalResourceRelation = "viewer",
            });
        });

        // Cross the flush boundary, capturing doc:m0's create revision for the row-level gate below.
        IRevision? m0CreateRev = null;
        for (var i = 0; i < 66; i++)
        {
            var rev = await Write(ds, Row($"m{i}", $"w{i}"));
            if (i == 0)
                m0CreateRev = rev;
        }

        // A delete and a real GC event. The delete lands ABOVE the 64-event flush boundary, making
        // doc:m0's forward key TAIL-RESIDENT: recovery must seed it from its flushed row (the create)
        // and replay the tail's delete on top.
        var m0DeleteRev = await Write(ds, Row("m0", "w0"), UpdateOperation.Delete);
        var floor = await grain.RunGc();
        Assert.NotNull(floor);

        // The Watch cursor, then the two events it must replay after recovery.
        var cursor = (await ds.HeadRevision()).Revision;
        await Write(ds, Row("post-1", "zoe"));
        await Write(ds, Row("post-2", "zed"));

        // Pre-deactivation captures.
        var headBefore = await ds.HeadRevision();
        var floorBefore = (await grain.GetHead()).GcFloor;
        var readerBefore = ds.SnapshotReader(headBefore.Revision);
        var liveBefore = await Collect(readerBefore.QueryRelationships(new RelationshipsFilter { OptionalResourceType = "doc" }));
        var countBefore = await readerBefore.CountRelationships("doc_viewers");
        IGraphReader shardedBefore = new ShardedGraphReader(cluster.GrainFactory, Nanos(headBefore.Revision));
        var shardCaptureBefore = await Collect(shardedBefore.QueryRelationships(new RelationshipsFilter
        {
            OptionalResourceType = "doc",
            OptionalResourceIds = ["plain", "caveated", "expiring", "m0", "post-1"],
        }));

        // Drop every activation; prove the sequencer's is gone before re-reading.
        var management = cluster.GrainFactory.GetGrain<IManagementGrain>(0);
        await management.ForceActivationCollection(TimeSpan.Zero);
        var stats = await management.GetDetailedGrainStatistics();
        Assert.DoesNotContain(stats, s =>
            s.GrainType.ToString()!.Contains("datastoregrain", StringComparison.OrdinalIgnoreCase));

        // Head, schema hash, floor, counters: identical after recovery.
        var headAfter = await ds.HeadRevision();
        Assert.Equal(Nanos(headBefore.Revision), Nanos(headAfter.Revision));
        Assert.Equal(headBefore.SchemaHash, headAfter.SchemaHash);
        Assert.NotNull(headAfter.SchemaHash); // the stored-schema write is what made it non-null
        Assert.Equal(floorBefore, (await grain.GetHead()).GcFloor);

        var readerAfter = ds.SnapshotReader(headAfter.Revision);
        Assert.Equal(liveBefore, await Collect(readerAfter.QueryRelationships(new RelationshipsFilter { OptionalResourceType = "doc" })));
        Assert.Equal(countBefore, await readerAfter.CountRelationships("doc_viewers"));
        var counterFilter = await readerAfter.ReadCounterFilter("doc_viewers");
        Assert.Equal("doc", counterFilter!.OptionalResourceType);
        Assert.Equal("viewer", counterFilter.OptionalResourceRelation);
        var schemaAfter = await readerAfter.ReadStoredSchema();
        Assert.True(schemaBytes.SequenceEqual(schemaAfter!));

        // Payload fidelity through recovery: caveat context and expiration byte-exact.
        var rels = new List<Relationship>();
        await foreach (var rel in readerAfter.QueryRelationships(new RelationshipsFilter
        {
            OptionalResourceType = "doc",
            OptionalResourceIds = ["caveated", "expiring"],
        }))
            rels.Add(rel);
        var caveated = rels.Single(r => r.Resource.ObjectId == "caveated");
        Assert.Equal("is_active", caveated.OptionalCaveat!.CaveatName);
        Assert.Equal(
            "\"eu\"",
            ((System.Text.Json.JsonElement)caveated.OptionalCaveat.Context!["region"]!).GetRawText());
        Assert.Equal(Expiry, rels.Single(r => r.Resource.ObjectId == "expiring").OptionalExpiration);

        // RAW ROW-LEVEL GATE for a TAIL-RESIDENT key: doc:m0's last touch (the delete) sits above the
        // flush boundary, so its recovered state is a flushed-row seed plus a tail replay. Assert the
        // EXACT row multiset (payload identity + MVCC stamps) equals a fresh ShardFold fold of the
        // key's touching log events from empty — the visibility-filtered reads above cannot see a
        // recovery double-fold (re-applying tail events over a row that already contains them leaves a
        // same-revision duplicate or a same-revision live+tombstone pair, both invisible at head).
        var m0Key = GraphShardKeyWire.ForResource("doc", "m0");
        var m0Events = new[]
        {
            new LogEvent(
                Nanos(m0CreateRev!),
                new List<RelationshipUpdateWire> { new(RelationshipUpdateOpWire.Touch, WireConvert.ToWire(Row("m0", "w0"))) },
                SchemaChange: null, Array.Empty<CounterDeltaWire>(), GcFloor: null),
            new LogEvent(
                Nanos(m0DeleteRev),
                new List<RelationshipUpdateWire> { new(RelationshipUpdateOpWire.Delete, WireConvert.ToWire(Row("m0", "w0"))) },
                SchemaChange: null, Array.Empty<CounterDeltaWire>(), GcFloor: null),
        };
        var expectedM0 = m0Events.Aggregate(GraphShardState.Empty, (state, ev) => ShardFold.ApplyEvent(state, ev, m0Key));
        static List<string> RowMultiset(IEnumerable<StoredRelationshipWire> rows) => rows
            .Select(r => $"{r.Relationship.ResourceType}:{r.Relationship.ResourceId}#{r.Relationship.ResourceRelation}"
                + $"@{r.Relationship.SubjectType}:{r.Relationship.SubjectId}#{r.Relationship.SubjectRelation}"
                + $"|created={r.CreatedRevision}|deleted={r.DeletedRevision?.ToString() ?? "live"}")
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();
        var m0Shard = await grain.ReadShard(m0Key);
        Assert.Equal(RowMultiset(expectedM0.Rows), RowMultiset(m0Shard.Rows));
        // Explicitly: no two rows share (identity, CreatedRevision) — the same-revision
        // tombstone+live duplicate shape a crashed in-place flush used to be able to leave behind.
        Assert.Equal(
            m0Shard.Rows.Count,
            m0Shard.Rows.Select(r =>
                    $"{r.Relationship.ResourceType}:{r.Relationship.ResourceId}#{r.Relationship.ResourceRelation}"
                    + $"@{r.Relationship.SubjectType}:{r.Relationship.SubjectId}#{r.Relationship.SubjectRelation}"
                    + $"|{r.CreatedRevision}")
                .Distinct(StringComparer.Ordinal)
                .Count());

        // Sharded reads at head equal the pre-deactivation capture (m0 was deleted; post-1 present).
        IGraphReader shardedAfter = new ShardedGraphReader(cluster.GrainFactory, Nanos(headAfter.Revision));
        Assert.Equal(shardCaptureBefore, await Collect(shardedAfter.QueryRelationships(new RelationshipsFilter
        {
            OptionalResourceType = "doc",
            OptionalResourceIds = ["plain", "caveated", "expiring", "m0", "post-1"],
        })));

        // Watch resumes from the pre-deactivation cursor with exactly the two remaining events, in
        // order — served from the reactivated grain's rebuilt in-memory log tail.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var replayed = new List<RevisionChange>();
        await foreach (var change in ds.Watch(cursor, new WatchOptions(WatchContent.Relationships), cts.Token))
        {
            replayed.Add(change);
            if (replayed.Count == 2)
                break;
        }
        Assert.Equal("post-1", Assert.Single(replayed[0].RelationshipChanges).Relationship.Resource.ObjectId);
        Assert.Equal("post-2", Assert.Single(replayed[1].RelationshipChanges).Relationship.Resource.ObjectId);
        Assert.True(Nanos(replayed[0].Revision) < Nanos(replayed[1].Revision));
        Assert.True(Nanos(replayed[0].Revision) > Nanos(cursor));
    }

    // ---- (f) Log-row ETag hygiene: a retried append overwrites its own crashed attempt's orphan. ----

    /// <summary>
    /// The Phase 0 hygiene rider (docs/scalability-program.md section 2): a crashed append attempt can
    /// leave an ORPHAN log row — the <c>log/{version}</c> entry written but the head never advanced, so
    /// the durable log version still names the row's slot for the next append. Log-row writes must be
    /// the same ETag-tolerant read-then-write the versioned shard/meta rows use, so the retried append
    /// OVERWRITES the orphan instead of ETag-clashing against it (a fresh empty-ETag wrapper would be
    /// rejected by every provider that enforces optimistic concurrency, MemoryStorage included). Plant
    /// the orphan directly through the grain's own storage provider at the next log version, then
    /// commit through the grain: the commit must succeed and the row must hold the NEW event.
    /// </summary>
    [Fact]
    public async Task Commit_overwrites_an_orphan_log_row_left_by_a_crashed_append_attempt()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var ds = cluster.Datastore;

        // A baseline commit so the grain is active and the durable head row exists.
        await Write(ds, Row("k0", "u0"));

        var headRow = await ReadRow<LogHeadEntry>(cluster, "head");
        Assert.True(headRow.RecordExists, "no durable head after the baseline commit");
        var nextVersion = headRow.State!.LogVersion + 1;

        // Plant the orphan exactly where a crashed append attempt leaves it: the next log version's
        // row written (with its own storage ETag minted), the head untouched.
        var storage = cluster.Services.GetRequiredKeyedService<IGrainStorage>("datastore");
        var grainId = ((GrainReference)Sequencer(cluster)).GrainId;
        var orphan = new LogEvent(
            long.MaxValue, Array.Empty<RelationshipUpdateWire>(), SchemaChange: null,
            Array.Empty<CounterDeltaWire>(), GcFloor: null);
        await storage.WriteStateAsync($"log/{nextVersion}", grainId, new GrainState<LogEvent>(orphan));

        // The retried append must succeed (no ETag clash) ...
        var revision = await Write(ds, Row("k1", "u1"));

        // ... and the slot must hold the NEW commit's event, not the orphan's content.
        var logRow = await ReadRow<LogEvent>(cluster, $"log/{nextVersion}");
        Assert.True(logRow.RecordExists, $"log/{nextVersion} is missing after the retried append");
        Assert.Equal(Nanos(revision), logRow.State!.Revision);
    }
}
