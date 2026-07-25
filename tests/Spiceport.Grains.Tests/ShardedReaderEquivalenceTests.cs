using System.Text.Json;
using System.Text.Json.Nodes;
using Spiceport.Conformance.Tests;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// THE fold-equivalence gate for the sharded read path (<c>docs/graph-sharded-datastore.md</c> §7):
/// every read answered by both the SEQUENCER-SNAPSHOT reader
/// (<see cref="GrainBackedDatastore.SnapshotReader"/> — one full-state fetch of the singleton grain's
/// fold, the independent oracle now that the per-silo whole-graph projection is gone) and the
/// <see cref="ShardedGraphReader"/> over the <c>IGraphShardGrain</c> mesh must agree EXACTLY. Shards
/// serve data, not candidates, so the applicable instrument is full read-for-read equivalence — not
/// candidates-plus-Check-confirmation. The time-travel and catch-up passes here are also where the
/// per-shard closed-timestamp gate (watermark covers the pinned revision before serving) stays pinned.
/// </summary>
/// <remarks>
/// The oracle side deliberately does not touch the shard mesh: the sequencer-snapshot reader folds the
/// whole state via <see cref="MvccSnapshotReader"/>, while every shard grain hydrates and catches up on
/// demand against the very same datastore grain log. Rows are compared as multisets of canonical
/// strings (the <c>GrainBackedDatastoreFidelityTests</c> idiom) because <see cref="Relationship"/>
/// record equality compares the caveat-context dictionary by REFERENCE — record equality would report
/// false negatives for equal rows.
/// </remarks>
[Collection(MeshClusterCollection.Name)]
public class ShardedReaderEquivalenceTests
{
    /// <summary>
    /// The representative subset the mesh conformance gate runs, plus <c>relexpiration.yaml</c>: its
    /// already-expired row is stored live in MVCC and must be skipped at query time by BOTH readers,
    /// exercising the sharded reader's caller-side expiry filter.
    /// </summary>
    public static IEnumerable<object[]> EquivalenceFiles() =>
    [
        ["multipleops.yaml"],          // set-ops (union / intersection / exclusion)
        ["teamwitharrow.yaml"],        // arrow (tuple-to-userset)
        ["simplewildcard.yaml"],       // wildcard ('*' subject ids get their own reverse shard)
        ["indirectnestedgroups.yaml"], // indirect / nested group
        ["simplerecursive.yaml"],      // recursive
        ["basiccaveat.yaml"],          // caveat (caveat context must round-trip the shard boundary)
        ["caveatlr.yaml"],             // caveat (left/right ordering)
        ["caveatip.yaml"],             // caveat (typed/nested context values round-trip the shard boundary)
        ["relexpiration.yaml"],        // expiration (expired row skipped at query time)
    ];

    [Theory]
    [MemberData(nameof(EquivalenceFiles))]
    public async Task Sharded_Reader_Agrees_With_Sequencer_Snapshot_Reader(string fileName)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "TestData", fileName);
        Assert.True(File.Exists(path), $"Linked corpus file missing from output: {path}");

        var file = ValidationFileLoader.LoadFromFile(path);
        Assert.True(file.Relationships.Count > 0, $"{fileName}: gate needs a non-empty relationships block");

        await using var cluster = await MeshTestCluster.CreateAsync(file.SchemaText);
        await Seed(cluster.Datastore, file.Relationships);

        var head = await cluster.Datastore.HeadRevision();
        IGraphReader sequencer = cluster.Datastore.SnapshotReader(head.Revision);
        IGraphReader sharded = new ShardedGraphReader(cluster.GrainFactory, Nanos(head.Revision));

        var failures = new List<string>();

        // (a) Forward: EVERY distinct (resourceType, resourceId), EVERY relation on it, plus the bare
        // type+id filter with no relation constraint.
        var resources = file.Relationships
            .Select(r => (r.Resource.ObjectType, r.Resource.ObjectId))
            .Distinct()
            .ToList();
        foreach (var (type, id) in resources)
        {
            var relations = file.Relationships
                .Where(r => r.Resource.ObjectType == type && r.Resource.ObjectId == id)
                .Select(r => r.Resource.Relation)
                .Distinct()
                .Append(null)
                .ToList();
            foreach (string? relation in relations)
            {
                var filter = new RelationshipsFilter
                {
                    OptionalResourceType = type,
                    OptionalResourceIds = [id],
                    OptionalResourceRelation = relation,
                };
                CompareSorted(
                    await Collect(sequencer.QueryRelationships(filter)),
                    await Collect(sharded.QueryRelationships(filter)),
                    $"forward {type}:{id}#{relation ?? "<any>"}",
                    failures);
            }
        }

        // (b) Reverse, unsorted: EVERY distinct (subjectType, subjectId) — wildcard '*' ids included.
        var subjects = file.Relationships
            .Select(r => (r.Subject.ObjectType, r.Subject.ObjectId))
            .Distinct()
            .ToList();
        foreach (var (type, id) in subjects)
        {
            var filter = new SubjectsFilter(type, OptionalSubjectIds: [id]);
            CompareSorted(
                await Collect(sequencer.ReverseQueryRelationships(filter)),
                await Collect(sharded.ReverseQueryRelationships(filter)),
                $"reverse {type}:{id}",
                failures);
        }

        // (c) Sorted equivalence + keyset resume for three representative subjects: the BySubject
        // sequences must match IN ORDER, and resuming after the middle element of the sequencer reader's
        // sequence must yield identical remainders (the LookupResources cursor contract).
        foreach (var (type, id) in Representatives(subjects))
        {
            var filter = new SubjectsFilter(type, OptionalSubjectIds: [id]);
            var sorted = new ReverseQueryOptions(ReverseQuerySort.BySubject);

            var expectedRows = await Materialize(sequencer.ReverseQueryRelationships(filter, sorted));
            var actualRows = await Materialize(sharded.ReverseQueryRelationships(filter, sorted));
            CompareOrdered(
                expectedRows.Select(Canonical).ToList(),
                actualRows.Select(Canonical).ToList(),
                $"sorted reverse {type}:{id}",
                failures);

            if (expectedRows.Count == 0)
                continue;

            var resume = new ReverseQueryOptions(
                ReverseQuerySort.BySubject, After: expectedRows[expectedRows.Count / 2].Reference);
            CompareOrdered(
                (await Materialize(sequencer.ReverseQueryRelationships(filter, resume))).Select(Canonical).ToList(),
                (await Materialize(sharded.ReverseQueryRelationships(filter, resume))).Select(Canonical).ToList(),
                $"keyset-resumed reverse {type}:{id}",
                failures);

            // Synthetic keyset: an After that exists in NO row, constructed lexically strictly between
            // the first two rows. The resource relation is the LAST BySubject tiebreaker and no real
            // relation contains NUL, so appending "\0" yields a key strictly greater than row 0 and
            // strictly less than any strictly-greater row — both readers must resume from row 1.
            if (expectedRows.Count >= 2)
            {
                var first = expectedRows[0].Reference;
                var synthetic = first with
                {
                    Resource = first.Resource with { Relation = first.Resource.Relation + "\0" },
                };
                Assert.True(
                    ReverseQueryOptions.CompareBySubject(first, synthetic) < 0
                        && ReverseQueryOptions.CompareBySubject(synthetic, expectedRows[1].Reference) < 0,
                    $"synthetic keyset for {type}:{id} is not strictly between the first two rows");

                var syntheticResume = new ReverseQueryOptions(ReverseQuerySort.BySubject, After: synthetic);
                var sequencerRemainder =
                    (await Materialize(sequencer.ReverseQueryRelationships(filter, syntheticResume))).Select(Canonical).ToList();
                CompareOrdered(
                    sequencerRemainder,
                    (await Materialize(sharded.ReverseQueryRelationships(filter, syntheticResume))).Select(Canonical).ToList(),
                    $"synthetic keyset-resumed reverse {type}:{id}",
                    failures);
                // Pin what the resume means, not just that the readers agree: everything after row 0.
                CompareOrdered(
                    expectedRows.Skip(1).Select(Canonical).ToList(),
                    sequencerRemainder,
                    $"synthetic keyset remainder {type}:{id}",
                    failures);
            }
        }

        Assert.True(
            failures.Count == 0,
            $"{fileName}: {failures.Count} read(s) diverged between the sequencer snapshot and the shard mesh:\n{string.Join('\n', failures)}");

        // (d) Absent keys: a never-written resource and subject activate cold, empty shards — both
        // readers must return nothing (not throw, not invent rows).
        var absentForward = new RelationshipsFilter
        {
            OptionalResourceType = resources[0].ObjectType,
            OptionalResourceIds = ["spiceport-never-written"],
        };
        Assert.Empty(await Collect(sequencer.QueryRelationships(absentForward)));
        Assert.Empty(await Collect(sharded.QueryRelationships(absentForward)));

        var absentReverse = new SubjectsFilter(
            subjects[0].ObjectType, OptionalSubjectIds: ["spiceport-never-written"]);
        Assert.Empty(await Collect(sequencer.ReverseQueryRelationships(absentReverse)));
        Assert.Empty(await Collect(sharded.ReverseQueryRelationships(absentReverse)));

        // (e) Shape guards: scan-shaped filters must be REJECTED by the sharded reader — silently
        // serving a partial answer for a scan is the failure mode the narrow seam exists to prevent.
        await Assert.ThrowsAsync<NotSupportedException>(() => Drain(sharded.QueryRelationships(
            new RelationshipsFilter
            {
                OptionalResourceType = resources[0].ObjectType,
                OptionalResourceIds = [resources[0].ObjectId],
                OptionalResourceIdPrefix = "p",
            })));
        await Assert.ThrowsAsync<NotSupportedException>(() => Drain(sharded.QueryRelationships(
            new RelationshipsFilter { OptionalResourceType = resources[0].ObjectType })));
        await Assert.ThrowsAsync<NotSupportedException>(() => Drain(sharded.ReverseQueryRelationships(
            new SubjectsFilter(subjects[0].ObjectType))));

        // (f) Write-after-hydrate freshness: the shards above are hydrated at the OLD head, so a new
        // commit exercises catch-up-on-demand — the per-shard watermark is the closed-timestamp gate.
        await AssertFreshnessAfterWrite(cluster, file.Relationships, fileName);
    }

    /// <summary>
    /// Writes one NEW relationship touching an already-read resource shard and an already-read subject
    /// shard, asserts both readers at the new head agree and include it, then deletes it and asserts
    /// both agree on exclusion (tombstone visibility through the shard grain's fold). The new row
    /// reuses the first relationship's resource object and subject under a probe RELATION name: shard
    /// keys are (type, id) with no relation segment, so both shards are exactly the ones pass one
    /// hydrated, while the six-tuple is guaranteed unwritten in any corpus file.
    /// </summary>
    private static async Task AssertFreshnessAfterWrite(
        MeshTestCluster cluster,
        IReadOnlyList<Relationship> existing,
        string fileName)
    {
        var fresh = Relationship.Create(
            existing[0].Resource with { Relation = "shard_gate_probe" },
            existing[0].Subject);
        Assert.DoesNotContain(Identity(fresh), existing.Select(Identity));

        // Time-travel pinning needs the heads AROUND the probe write: R0 before it, R1 after the
        // write, R2 after the delete.
        var r0 = (await cluster.Datastore.HeadRevision()).Revision;

        var r1 = await cluster.Datastore.ReadWriteTx(tx =>
            tx.WriteRelationships([new RelationshipUpdate(fresh, UpdateOperation.Create)]));
        await AssertBothReadersAgreeOn(cluster, fresh, expectPresent: true, fileName);

        var r2 = await cluster.Datastore.ReadWriteTx(tx =>
            tx.WriteRelationships([new RelationshipUpdate(fresh, UpdateOperation.Delete)]));
        await AssertBothReadersAgreeOn(cluster, fresh, expectPresent: false, fileName);

        // Time travel: the shard watermark now sits at (or past) R2, so a reader pinned at the OLD
        // revisions distinguishes VisibleAt(pinned) from live-at-watermark — a shard that served its
        // current live rows would report the probe absent at R1 (already deleted at the watermark)
        // and could not flip presence across R0/R1/R2.
        await AssertBothReadersAgreeOn(cluster, fresh, expectPresent: false, fileName, pinned: r0);
        await AssertBothReadersAgreeOn(cluster, fresh, expectPresent: true, fileName, pinned: r1);
        await AssertBothReadersAgreeOn(cluster, fresh, expectPresent: false, fileName, pinned: r2);
    }

    private static async Task AssertBothReadersAgreeOn(
        MeshTestCluster cluster, Relationship rel, bool expectPresent, string fileName, IRevision? pinned = null)
    {
        var revision = pinned ?? (await cluster.Datastore.HeadRevision()).Revision;
        IGraphReader sequencer = cluster.Datastore.SnapshotReader(revision);
        IGraphReader sharded = new ShardedGraphReader(cluster.GrainFactory, Nanos(revision));

        var forward = new RelationshipsFilter
        {
            OptionalResourceType = rel.Resource.ObjectType,
            OptionalResourceIds = [rel.Resource.ObjectId],
            OptionalResourceRelation = rel.Resource.Relation,
        };
        var reverse = new SubjectsFilter(rel.Subject.ObjectType, OptionalSubjectIds: [rel.Subject.ObjectId]);

        var failures = new List<string>();
        var sequencerForward = await Collect(sequencer.QueryRelationships(forward));
        CompareSorted(sequencerForward, await Collect(sharded.QueryRelationships(forward)),
            $"freshness forward {Identity(rel)}", failures);
        var sequencerReverse = await Collect(sequencer.ReverseQueryRelationships(reverse));
        CompareSorted(sequencerReverse, await Collect(sharded.ReverseQueryRelationships(reverse)),
            $"freshness reverse {Identity(rel)}", failures);
        Assert.True(
            failures.Count == 0,
            $"{fileName}: freshness pass (expectPresent={expectPresent}, pinned={(pinned is null ? "head" : Nanos(revision).ToString())}) diverged:\n{string.Join('\n', failures)}");

        // Both agree — pin what they agree ON against the write just committed.
        Assert.Equal(expectPresent, sequencerForward.Contains(Canonical(rel)));
        Assert.Equal(expectPresent, sequencerReverse.Contains(Canonical(rel)));
    }

    /// <summary>
    /// The LookupResourcesEngine frontier shape: one reverse query carrying SEVERAL subject ids plus
    /// the wildcard — the sharded reader must fan out to one shard per id (the wildcard gets its own
    /// reverse shard), merge, and still match the sequencer-snapshot reader both as a multiset (unsorted) and as the
    /// BySubject global sort with a keyset resume that crosses a subject boundary.
    /// </summary>
    [Theory]
    [InlineData("simplewildcard.yaml", "test/user", new[] { "concreteguy", "anotheruser", "*" }, true)]
    [InlineData("teamwitharrow.yaml", "test/user", new[] { "jake", "jimmy", "*" }, false)]
    public async Task Multi_Id_With_Wildcard_Reverse_Agrees_And_Resumes_Across_Subject_Boundary(
        string fileName, string subjectType, string[] subjectIds, bool expectWildcardRow)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "TestData", fileName);
        var file = ValidationFileLoader.LoadFromFile(path);

        await using var cluster = await MeshTestCluster.CreateAsync(file.SchemaText);
        await Seed(cluster.Datastore, file.Relationships);

        var head = await cluster.Datastore.HeadRevision();
        IGraphReader sequencer = cluster.Datastore.SnapshotReader(head.Revision);
        IGraphReader sharded = new ShardedGraphReader(cluster.GrainFactory, Nanos(head.Revision));

        var filter = new SubjectsFilter(subjectType, OptionalSubjectIds: subjectIds);
        var failures = new List<string>();

        // Unsorted: multiset equality against the sequencer-snapshot reader.
        CompareSorted(
            await Collect(sequencer.ReverseQueryRelationships(filter)),
            await Collect(sharded.ReverseQueryRelationships(filter)),
            $"multi-id reverse [{string.Join(",", subjectIds)}]",
            failures);

        // Sorted: the merged cross-shard sequence must BE the global BySubject order.
        var sorted = new ReverseQueryOptions(ReverseQuerySort.BySubject);
        var expectedRows = await Materialize(sequencer.ReverseQueryRelationships(filter, sorted));
        var actualRows = await Materialize(sharded.ReverseQueryRelationships(filter, sorted));
        CompareOrdered(
            expectedRows.Select(Canonical).ToList(),
            actualRows.Select(Canonical).ToList(),
            $"multi-id sorted reverse [{string.Join(",", subjectIds)}]",
            failures);

        if (expectWildcardRow)
        {
            // The wildcard SHARD must have contributed — a '*' row can only come from it.
            Assert.Contains(actualRows, r => r.Subject.ObjectId == "*");
        }

        // Keyset resume ACROSS a subject boundary: After = the last row of the FIRST subject's block
        // in the sequencer reader's sorted sequence, so the remainder starts inside another shard's rows.
        var firstSubject = expectedRows[0].Subject;
        var boundary = expectedRows
            .Last(r => r.Subject.ObjectType == firstSubject.ObjectType && r.Subject.ObjectId == firstSubject.ObjectId)
            .Reference;
        Assert.Contains(expectedRows, r => r.Subject.ObjectId != firstSubject.ObjectId);

        var resume = new ReverseQueryOptions(ReverseQuerySort.BySubject, After: boundary);
        var sequencerRemainder =
            (await Materialize(sequencer.ReverseQueryRelationships(filter, resume))).Select(Canonical).ToList();
        CompareOrdered(
            sequencerRemainder,
            (await Materialize(sharded.ReverseQueryRelationships(filter, resume))).Select(Canonical).ToList(),
            $"multi-id boundary-crossing resume [{string.Join(",", subjectIds)}]",
            failures);
        // The remainder is exactly the later subjects' rows — the resume genuinely crossed the boundary.
        CompareOrdered(
            expectedRows.Where(r => r.Subject.ObjectId != firstSubject.ObjectId).Select(Canonical).ToList(),
            sequencerRemainder,
            $"multi-id boundary remainder [{string.Join(",", subjectIds)}]",
            failures);

        Assert.True(
            failures.Count == 0,
            $"{fileName}: multi-id/wildcard pass diverged:\n{string.Join('\n', failures)}");
    }

    /// <summary>
    /// Narrowing-filter passes: the shard holds ALL rows of its subject slice, so a
    /// <see cref="SubjectsFilter"/> whose RelationFilter / resource constraints EXCLUDE some of those
    /// rows makes the caller-side <c>Matches</c> re-filter load-bearing — a reader that returned the
    /// slice unfiltered would show the excluded rows. Each case asserts equality with the sequencer-snapshot reader
    /// AND that the filter really excluded something (fewer rows than the unfiltered slice).
    /// </summary>
    public static IEnumerable<object[]> NarrowingFilterCases()
    {
        // teamwitharrow: test/team:support_engineers appears as a subject with TWO subject relations
        // (repository maintainer @team#member; team parent @team#...) across TWO resource types.
        yield return
        [
            "teamwitharrow.yaml",
            new SubjectsFilter(
                "test/team", OptionalSubjectIds: ["support_engineers"],
                RelationFilter: new SubjectRelationFilter(NonEllipsisRelation: "member")),
            1, // only the repository maintainer row; the ellipsis parent row is excluded
        ];
        yield return
        [
            "teamwitharrow.yaml",
            new SubjectsFilter(
                "test/team", OptionalSubjectIds: ["support_engineers"],
                OptionalResourceType: "test/team"),
            1, // only the parent row; the repository maintainer row is excluded
        ];
        yield return
        [
            "teamwitharrow.yaml",
            new SubjectsFilter(
                "test/team", OptionalSubjectIds: ["support_engineers"],
                OptionalResourceRelation: "maintainer"),
            1, // only the repository maintainer row
        ];
        // indirectnestedgroups: user:tom appears under THREE resource relations across two types
        // (document viewer, group direct_member, group intern).
        yield return
        [
            "indirectnestedgroups.yaml",
            new SubjectsFilter(
                "user", OptionalSubjectIds: ["tom"],
                OptionalResourceType: "group", OptionalResourceRelation: "intern"),
            1, // the two other tom rows are excluded
        ];
    }

    [Theory]
    [MemberData(nameof(NarrowingFilterCases))]
    public async Task Narrowing_Reverse_Filters_Exclude_Rows_The_Shard_Holds(
        string fileName, SubjectsFilter filter, int expectedCount)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "TestData", fileName);
        var file = ValidationFileLoader.LoadFromFile(path);

        await using var cluster = await MeshTestCluster.CreateAsync(file.SchemaText);
        await Seed(cluster.Datastore, file.Relationships);

        var head = await cluster.Datastore.HeadRevision();
        IGraphReader sequencer = cluster.Datastore.SnapshotReader(head.Revision);
        IGraphReader sharded = new ShardedGraphReader(cluster.GrainFactory, Nanos(head.Revision));

        var failures = new List<string>();
        var narrowed = await Collect(sequencer.ReverseQueryRelationships(filter));
        CompareSorted(
            narrowed.ToList(),
            await Collect(sharded.ReverseQueryRelationships(filter)),
            $"narrowed reverse {filter}",
            failures);
        Assert.True(failures.Count == 0, $"{fileName}: narrowing pass diverged:\n{string.Join('\n', failures)}");

        // The filter is genuinely narrowing: the subject's whole slice holds MORE rows than survive it.
        var unfiltered = await Collect(sharded.ReverseQueryRelationships(
            new SubjectsFilter(filter.SubjectType, OptionalSubjectIds: filter.OptionalSubjectIds)));
        Assert.Equal(expectedCount, narrowed.Count);
        Assert.True(
            unfiltered.Count > narrowed.Count,
            $"case is not narrowing: slice has {unfiltered.Count} rows, filter kept {narrowed.Count}");
    }

    /// <summary>
    /// Forces the catch-up loop through a FULL 256-event page (<c>Events.Count == BatchSize</c>, loop
    /// again) rather than the single short-page drain every other gate happens to exercise: hydrate a
    /// shard, advance the log by 300 commits on other resources plus one on this shard's key, then
    /// demand the new head.
    /// </summary>
    [Fact]
    public async Task Shard_Catch_Up_Pages_Through_A_Full_Log_Batch()
    {
        const string schema = """
            definition user {}

            definition doc {
              relation viewer: user
            }
            """;
        static Relationship Row(string docId, string userId) => Relationship.Create(
            new ObjectAndRelation("doc", docId, "viewer"),
            new ObjectAndRelation("user", userId, CoreConstants.Ellipsis));
        static Task Write(IDatastore ds, Relationship rel) => ds.ReadWriteTx(tx =>
            tx.WriteRelationships([new RelationshipUpdate(rel, UpdateOperation.Create)]));

        await using var cluster = await MeshTestCluster.CreateAsync(schema);
        await Write(cluster.Datastore, Row("x", "seed"));

        // Hydrate X's forward shard at the current head via a direct grain call.
        var shard = cluster.GrainFactory.GetGrain<IGraphShardGrain>(
            GraphShardGrainKey.Build(GraphShardKeyWire.ForResource("doc", "x")));
        var headBefore = await cluster.Datastore.HeadRevision();
        var hydrated = await shard.RowsAt(Nanos(headBefore.Revision), CancellationToken.None);
        Assert.Single(hydrated.Rows);

        // 300 single-row commits touching OTHER resources: more than one full ReadFrom page (256),
        // then one more commit on X itself so the catch-up has a matching row to fold at the tail.
        for (var i = 0; i < 300; i++)
            await Write(cluster.Datastore, Row($"other-{i}", "carol"));
        await Write(cluster.Datastore, Row("x", "late"));

        var head = await cluster.Datastore.HeadRevision();
        var reply = await shard.RowsAt(Nanos(head.Revision), CancellationToken.None);
        var actual = reply.Rows.Select(WireConvert.ToRelationship).Select(Canonical).ToList();

        Assert.Contains(Canonical(Row("x", "late")), actual);

        var expected = await Collect(cluster.Datastore.SnapshotReader(head.Revision).QueryRelationships(
            new RelationshipsFilter { OptionalResourceType = "doc", OptionalResourceIds = ["x"] }));
        var failures = new List<string>();
        CompareSorted(expected, actual, "post-paging rows of doc:x", failures);
        Assert.True(failures.Count == 0, $"paged catch-up diverged:\n{string.Join('\n', failures)}");
    }

    /// <summary>
    /// GC through the grain: with <see cref="DatastoreGcOptions.Window"/> = Zero the floor a
    /// <c>RunGc</c> computes is <c>min(head, now) == head</c>, so it lands ABOVE the captured
    /// pre-delete revision Rd. A cold shard must hydrate and serve correctly at head, a below-floor
    /// pin must be rejected with <see cref="RevisionNotFoundException"/> ACROSS the grain boundary
    /// (the surrogate round-trip), and the sharded reader at head must agree with the sequencer-snapshot reader.
    /// </summary>
    [Fact]
    public async Task Gc_Floor_Is_Enforced_Through_The_Shard_Grain()
    {
        const string schema = """
            definition user {}

            definition doc {
              relation viewer: user
            }
            """;
        static Relationship Row(string docId, string userId) => Relationship.Create(
            new ObjectAndRelation("doc", docId, "viewer"),
            new ObjectAndRelation("user", userId, CoreConstants.Ellipsis));

        await using var cluster = await MeshTestCluster.CreateAsync(schema, gcWindow: TimeSpan.Zero);

        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(
        [
            new RelationshipUpdate(Row("x", "alice"), UpdateOperation.Create),
            new RelationshipUpdate(Row("cold", "carol"), UpdateOperation.Create),
        ]));
        var rd = await cluster.Datastore.ReadWriteTx(tx =>
            tx.WriteRelationships([new RelationshipUpdate(Row("x", "bob"), UpdateOperation.Create)]));
        await cluster.Datastore.ReadWriteTx(tx =>
            tx.WriteRelationships([new RelationshipUpdate(Row("x", "bob"), UpdateOperation.Delete)]));

        var floor = await cluster.GrainFactory.GetGrain<IDatastoreGrain>(IDatastoreGrain.Key).RunGc();
        Assert.NotNull(floor);
        Assert.True(floor > Nanos(rd), $"Window=Zero GC floor {floor} must land past Rd {Nanos(rd)}");

        var head = await cluster.Datastore.HeadRevision();

        // (a) A COLD shard — written before GC, first ever read now — hydrates from the post-GC
        // snapshot and serves correctly at head.
        var coldShard = cluster.GrainFactory.GetGrain<IGraphShardGrain>(
            GraphShardGrainKey.Build(GraphShardKeyWire.ForResource("doc", "cold")));
        var coldRows = await coldShard.RowsAt(Nanos(head.Revision), CancellationToken.None);
        Assert.Equal("carol", Assert.Single(coldRows.Rows).SubjectId);

        // (b) A below-floor pin is rejected with the DOMAIN exception type across the grain boundary —
        // the RevisionNotFoundSurrogate must round-trip it, not surface a serialization failure.
        var xShard = cluster.GrainFactory.GetGrain<IGraphShardGrain>(
            GraphShardGrainKey.Build(GraphShardKeyWire.ForResource("doc", "x")));
        await Assert.ThrowsAsync<RevisionNotFoundException>(
            () => xShard.RowsAt(Nanos(rd), CancellationToken.None));

        // (c) At head, the sharded reader still agrees with the sequencer-snapshot reader.
        IGraphReader sequencer = cluster.Datastore.SnapshotReader(head.Revision);
        IGraphReader sharded = new ShardedGraphReader(cluster.GrainFactory, Nanos(head.Revision));
        var failures = new List<string>();
        foreach (var id in new[] { "x", "cold" })
        {
            var filter = new RelationshipsFilter { OptionalResourceType = "doc", OptionalResourceIds = [id] };
            CompareSorted(
                await Collect(sequencer.QueryRelationships(filter)),
                await Collect(sharded.QueryRelationships(filter)),
                $"post-GC forward doc:{id}",
                failures);
        }
        foreach (var subjectId in new[] { "alice", "carol", "bob" })
        {
            var filter = new SubjectsFilter("user", OptionalSubjectIds: [subjectId]);
            CompareSorted(
                await Collect(sequencer.ReverseQueryRelationships(filter)),
                await Collect(sharded.ReverseQueryRelationships(filter)),
                $"post-GC reverse user:{subjectId}",
                failures);
        }
        Assert.True(failures.Count == 0, $"post-GC head reads diverged:\n{string.Join('\n', failures)}");
    }

    /// <summary>
    /// The WARM-shard counterpart of <see cref="Gc_Floor_Is_Enforced_Through_The_Shard_Grain"/>: a shard
    /// hydrated BEFORE GC must, without deactivating, (a) serve post-GC-correct rows at the new head by
    /// folding the <c>GcApplied</c> event through its tail catch-up, and (b) reject a below-floor pin
    /// with <see cref="RevisionNotFoundException"/> from that same warm activation. The window is small
    /// but NONZERO: with <see cref="TimeSpan.Zero"/> the singleton's per-commit tail trim always empties
    /// the retained log, so a warm shard could only re-bootstrap — the GC event would never reach it
    /// through the catch-up fold this gate exists to pin. The shard hydrates at the post-delete head, so
    /// the GC floor (<c>min(head, now - window)</c>, after the delay) lands EXACTLY on its watermark:
    /// the catch-up then serves the GcApplied event from the retained tail rather than re-hydrating.
    /// </summary>
    [Fact]
    public async Task Warm_Shard_Folds_GcApplied_And_Rejects_Below_Floor_Without_Reactivation()
    {
        const string schema = """
            definition user {}

            definition doc {
              relation viewer: user
            }
            """;
        static Relationship Row(string docId, string userId) => Relationship.Create(
            new ObjectAndRelation("doc", docId, "viewer"),
            new ObjectAndRelation("user", userId, CoreConstants.Ellipsis));

        var gcWindow = TimeSpan.FromMilliseconds(200);
        await using var cluster = await MeshTestCluster.CreateAsync(schema, gcWindow: gcWindow);

        await cluster.Datastore.ReadWriteTx(tx =>
            tx.WriteRelationships([new RelationshipUpdate(Row("x", "alice"), UpdateOperation.Create)]));
        var rd = await cluster.Datastore.ReadWriteTx(tx =>
            tx.WriteRelationships([new RelationshipUpdate(Row("x", "bob"), UpdateOperation.Create)]));
        var r2 = await cluster.Datastore.ReadWriteTx(tx =>
            tx.WriteRelationships([new RelationshipUpdate(Row("x", "bob"), UpdateOperation.Delete)]));

        // Hydrate the shard WARM at the post-delete head: its state carries bob's full MVCC history and
        // no GC floor yet, and its watermark sits exactly at r2.
        var shard = cluster.GrainFactory.GetGrain<IGraphShardGrain>(
            GraphShardGrainKey.Build(GraphShardKeyWire.ForResource("doc", "x")));
        var preGc = await shard.RowsAt(Nanos(r2), CancellationToken.None);
        Assert.Equal("alice", Assert.Single(preGc.Rows).SubjectId);

        // Let wall-clock pass the retention window, then GC: the floor becomes min(head, now - window)
        // = head = r2 — past the delete (rd's row history is collected) and exactly at the warm shard's
        // watermark, which is the premise that forces the catch-up to FOLD the GcApplied event from the
        // retained tail (a floor above the watermark would force a re-bootstrap instead).
        await Task.Delay(gcWindow + TimeSpan.FromMilliseconds(100));
        var floor = await cluster.GrainFactory.GetGrain<IDatastoreGrain>(IDatastoreGrain.Key).RunGc();
        Assert.NotNull(floor);
        Assert.True(floor > Nanos(rd), $"GC floor {floor} must land past Rd {Nanos(rd)}");
        Assert.Equal(Nanos(r2), floor.Value);

        var head = await cluster.Datastore.HeadRevision();

        // (a) WITHOUT deactivating: the warm shard catches up by folding the GcApplied event and serves
        // post-GC-correct rows at the new head, agreeing with the sequencer-snapshot reader.
        var atHead = await shard.RowsAt(Nanos(head.Revision), CancellationToken.None);
        Assert.Equal("alice", Assert.Single(atHead.Rows).SubjectId);

        var expected = await Collect(cluster.Datastore.SnapshotReader(head.Revision).QueryRelationships(
            new RelationshipsFilter { OptionalResourceType = "doc", OptionalResourceIds = ["x"] }));
        var actual = atHead.Rows.Select(WireConvert.ToRelationship).Select(Canonical).ToList();
        var failures = new List<string>();
        CompareSorted(expected, actual, "post-GC warm rows of doc:x", failures);
        Assert.True(failures.Count == 0, $"warm post-GC read diverged:\n{string.Join('\n', failures)}");

        // (b) The SAME warm activation — its state now carries the folded floor — rejects a below-floor
        // pin with the domain exception across the grain boundary.
        await Assert.ThrowsAsync<RevisionNotFoundException>(
            () => shard.RowsAt(Nanos(rd), CancellationToken.None));
    }

    // --- helpers ---

    private static async Task Seed(IDatastore datastore, IReadOnlyList<Relationship> relationships)
    {
        var updates = relationships
            .Select(r => new RelationshipUpdate(r, UpdateOperation.Create))
            .ToList();
        await datastore.ReadWriteTx(tx => tx.WriteRelationships(updates));
    }

    /// <summary>First, middle and last of the distinct subjects in a stable order (deduplicated).</summary>
    private static IEnumerable<(string ObjectType, string ObjectId)> Representatives(
        IReadOnlyList<(string ObjectType, string ObjectId)> subjects)
    {
        var ordered = subjects
            .OrderBy(s => s.ObjectType, StringComparer.Ordinal)
            .ThenBy(s => s.ObjectId, StringComparer.Ordinal)
            .ToList();
        return new[] { ordered[0], ordered[ordered.Count / 2], ordered[^1] }.Distinct();
    }

    private static void CompareSorted(List<string> expected, List<string> actual, string label, List<string> failures)
    {
        expected.Sort(StringComparer.Ordinal);
        actual.Sort(StringComparer.Ordinal);
        CompareOrdered(expected, actual, label, failures);
    }

    private static void CompareOrdered(List<string> expected, List<string> actual, string label, List<string> failures)
    {
        if (!expected.SequenceEqual(actual, StringComparer.Ordinal))
        {
            failures.Add(
                $"  {label}:\n    sequencer:  [{string.Join(", ", expected)}]\n    sharded:    [{string.Join(", ", actual)}]");
        }
    }

    private static async Task<List<string>> Collect(IAsyncEnumerable<Relationship> source) =>
        (await Materialize(source)).Select(Canonical).ToList();

    private static async Task<List<Relationship>> Materialize(IAsyncEnumerable<Relationship> source)
    {
        var rows = new List<Relationship>();
        await foreach (var rel in source)
            rows.Add(rel);
        return rows;
    }

    private static async Task Drain(IAsyncEnumerable<Relationship> source)
    {
        await foreach (var _ in source)
        {
        }
    }

    private static long Nanos(IRevision revision) => revision switch
    {
        TimestampRevision t => t.TimestampNanosSinceEpoch,
        _ => throw new InvalidOperationException($"unexpected revision type: {revision.GetType().Name}"),
    };

    // Canonical string forms (caveat context compared via its serialized values, matching the
    // GrainBackedDatastoreFidelityTests idiom, since record equality compares the dictionary by
    // reference).

    private static string Canonical(Relationship rel)
    {
        var caveat = rel.OptionalCaveat is { } cav ? $"[{cav.CaveatName}:{ContextString(cav.Context)}]" : "";
        var exp = rel.OptionalExpiration is { } e ? $"@exp={e:O}" : "";
        return $"{Identity(rel)}{caveat}{exp}";
    }

    private static string Identity(Relationship rel) =>
        $"{rel.Resource.ObjectType}:{rel.Resource.ObjectId}#{rel.Resource.Relation}" +
        $"@{rel.Subject.ObjectType}:{rel.Subject.ObjectId}#{rel.Subject.Relation}";

    private static string ContextString(IReadOnlyDictionary<string, object?>? ctx) =>
        ctx is null || ctx.Count == 0
            ? ""
            : string.Join(",", ctx
                .OrderBy(kv => kv.Key, StringComparer.Ordinal)
                .Select(kv => $"{kv.Key}={CanonicalJson(JsonSerializer.SerializeToNode(kv.Value))}"));

    // Context VALUES compare structurally: re-serialized as JSON with object keys sorted at every
    // depth. kv.Value?.ToString() would render nested list/map values by their runtime ToString (or a
    // JsonElement's raw insertion-ordered text), under which two differently-corrupted nested values
    // could canonicalize equal — the exact false negative this gate must not admit.
    private static string CanonicalJson(JsonNode? node) => node switch
    {
        null => "null",
        JsonObject obj => "{" + string.Join(",", obj
            .OrderBy(p => p.Key, StringComparer.Ordinal)
            .Select(p => $"\"{p.Key}\":{CanonicalJson(p.Value)}")) + "}",
        JsonArray arr => "[" + string.Join(",", arr.Select(CanonicalJson)) + "]",
        _ => node.ToJsonString(),
    };
}
