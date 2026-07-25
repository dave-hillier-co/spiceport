using Microsoft.Extensions.DependencyInjection;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Gates for the storage-direct scan seam (<see cref="ISnapshotScanner"/>,
/// <c>docs/graph-sharded-datastore.md</c> §3) now that it is the ONLY broad-read path: the
/// ReadRelationships-shaped scan shapes served through the DI scanner must agree exactly with the same
/// filters through the sequencer-snapshot reader (<see cref="GrainBackedDatastore.SnapshotReader"/>) at
/// the same pinned revision; the on-demand counter lifecycle driven through the data-plane RPC path must
/// agree with both scanner and reader counts (including MVCC time travel across the unregister
/// tombstone); and a bulk export resumed from a mid-stream cursor must reproduce the uninterrupted run's
/// remainder with byte-identical resume tokens — the reconnect contract a client's cursor depends on.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class SnapshotScannerTests
{
    private const string Schema = """
        definition user {}

        definition folder {
          relation viewer: user
        }

        definition doc {
          relation viewer: user | user:*
          relation editor: user
        }
        """;

    /// <summary>
    /// Fixed rows exercising each scan shape: two resource types, a shared <c>plan-</c> id prefix, a
    /// wildcard subject row, and one subject (<c>alice</c>) spanning both types.
    /// </summary>
    private static readonly IReadOnlyList<Relationship> Rows =
    [
        Row("doc", "plan-a", "viewer", "user", "alice"),
        Row("doc", "plan-b", "viewer", "user", "bob"),
        Row("doc", "plan-a", "editor", "user", "carol"),
        Row("doc", "spec", "viewer", "user", "*"),
        Row("doc", "spec", "editor", "user", "alice"),
        Row("folder", "root", "viewer", "user", "alice"),
    ];

    /// <summary>
    /// The three ReadRelationships scan shapes — broad (type only), resource-id prefix, and
    /// subject-selector (no resource constraint at all) — through the DI
    /// <see cref="ISnapshotScanner"/> must return exactly the rows the sequencer-snapshot reader
    /// returns for the same filter at the same revision (multiset compare on canonical strings).
    /// Each shape's expected count is pinned so a filter that silently matched nothing (or
    /// everything) cannot pass vacuously.
    /// </summary>
    [Fact]
    public async Task Scanner_Agrees_With_The_Sequencer_Snapshot_Reader_Across_Scan_Shapes()
    {
        await using var cluster = await ClusterWithRows();
        var scanner = cluster.Services.GetRequiredService<ISnapshotScanner>();
        var head = (await cluster.Datastore.HeadRevision()).Revision;
        var reader = cluster.Datastore.SnapshotReader(head);

        var shapes = new (string Label, RelationshipsFilter Filter, int ExpectedCount)[]
        {
            ("broad type-only", new RelationshipsFilter { OptionalResourceType = "doc" }, 5),
            ("resource-id prefix", new RelationshipsFilter
            {
                OptionalResourceType = "doc",
                OptionalResourceIdPrefix = "plan-",
            }, 3),
            ("subject-selector", new RelationshipsFilter
            {
                OptionalSubjectsSelectors = [new SubjectsSelector("user", OptionalSubjectIds: ["alice"])],
            }, 3),
        };

        var failures = new List<string>();
        foreach (var (label, filter, expectedCount) in shapes)
        {
            var viaReader = await Collect(reader.QueryRelationships(filter));
            var viaScanner = await Collect(scanner.Scan(filter, head, CancellationToken.None));
            Assert.Equal(expectedCount, viaReader.Count);
            CompareSorted(viaReader, viaScanner, label, failures);
        }

        Assert.True(
            failures.Count == 0,
            $"{failures.Count} scan shape(s) diverged between the scanner and the snapshot reader:\n{string.Join('\n', failures)}");
    }

    /// <summary>
    /// A Scan whose filter carries a subject selector WITH a relation filter that EXCLUDES rows present
    /// in the state: <c>group:eng</c> appears as a subject both with the non-ellipsis relation
    /// <c>member</c> (the doc viewer row) and with the ellipsis relation (the folder owner row), so a
    /// scanner that ignored the selector's relation constraint would return both. Counts are pinned
    /// (1 narrowed, 2 unfiltered), not just reader agreement, so a filter that silently matched nothing
    /// or everything cannot pass vacuously.
    /// </summary>
    [Fact]
    public async Task Scan_With_Subject_Relation_Filter_Excludes_Rows_The_State_Holds()
    {
        const string GroupSchema = """
            definition user {}

            definition group {
              relation member: user
            }

            definition folder {
              relation owner: group
            }

            definition doc {
              relation viewer: user | group#member
            }
            """;
        var rows = new[]
        {
            Relationship.Create(
                new ObjectAndRelation("doc", "plan", "viewer"),
                new ObjectAndRelation("group", "eng", "member")),
            Relationship.Create(
                new ObjectAndRelation("folder", "root", "owner"),
                new ObjectAndRelation("group", "eng", CoreConstants.Ellipsis)),
            Relationship.Create(
                new ObjectAndRelation("group", "eng", "member"),
                new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis)),
        };

        await using var cluster = await MeshTestCluster.CreateAsync(GroupSchema);
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(
            rows.Select(r => new RelationshipUpdate(r, UpdateOperation.Create)).ToList()));
        var scanner = cluster.Services.GetRequiredService<ISnapshotScanner>();
        var head = (await cluster.Datastore.HeadRevision()).Revision;
        var reader = cluster.Datastore.SnapshotReader(head);

        var narrowed = new RelationshipsFilter
        {
            OptionalSubjectsSelectors =
            [
                new SubjectsSelector(
                    "group", OptionalSubjectIds: ["eng"],
                    RelationFilter: new SubjectRelationFilter(NonEllipsisRelation: "member")),
            ],
        };

        var viaReader = await Collect(reader.QueryRelationships(narrowed));
        var viaScanner = await Collect(scanner.Scan(narrowed, head, CancellationToken.None));
        var failures = new List<string>();
        CompareSorted(viaReader, viaScanner, "subject selector with relation filter", failures);
        Assert.True(failures.Count == 0, $"narrowed scan diverged:\n{string.Join('\n', failures)}");
        Assert.Equal("doc:plan#viewer@group:eng#member", Assert.Single(viaScanner));

        // The relation filter genuinely narrowed: the same selector WITHOUT it also matches the
        // ellipsis-relation folder row the state holds.
        var unfiltered = await Collect(scanner.Scan(
            new RelationshipsFilter
            {
                OptionalSubjectsSelectors = [new SubjectsSelector("group", OptionalSubjectIds: ["eng"])],
            },
            head, CancellationToken.None));
        Assert.Equal(2, unfiltered.Count);
    }

    /// <summary>
    /// The on-demand counter lifecycle through the data-plane RPC path (register, count, unregister on
    /// <see cref="Abstractions.IRelationshipsGrain"/>) must agree with the same counter read through the
    /// scanner and the snapshot reader at the same revisions: identical counts before and after a
    /// matching write; after unregister, the RPC count fails NotRegistered, the scanner rejects the
    /// counter at the new head — and still serves it at the pre-unregister head (the MVCC tombstone is
    /// revision-visible, not destructive).
    /// </summary>
    [Fact]
    public async Task Counter_Lifecycle_Through_The_Rpc_Path_Agrees_With_Scanner_And_Reader()
    {
        await using var cluster = await ClusterWithRows();
        var scanner = cluster.Services.GetRequiredService<ISnapshotScanner>();

        await cluster.Relationships.RegisterRelationshipCounter(new RegisterCounterArgs(
            "doc_viewers",
            new RelationshipsFilterWire("doc", null, null, "viewer", null, null, null)));

        var first = await cluster.Relationships.CountRelationships(new CountRelationshipsArgs("doc_viewers"));
        Assert.Equal(3UL, first.Count); // plan-a, plan-b and the wildcard spec viewer rows

        var head1 = (await cluster.Datastore.HeadRevision()).Revision;
        Assert.Equal(first.Count, await scanner.CountRelationships("doc_viewers", head1, CancellationToken.None));
        Assert.Equal(first.Count, await cluster.Datastore.SnapshotReader(head1).CountRelationships("doc_viewers"));
        Assert.NotNull(await scanner.ReadCounterFilter("doc_viewers", head1, CancellationToken.None));
        Assert.Contains("doc_viewers", await CollectCounterNames(scanner, head1));

        // A write matching the counter filter moves the RPC, scanner and reader counts identically.
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(
            [new RelationshipUpdate(Row("doc", "plan-c", "viewer", "user", "dave"), UpdateOperation.Create)]));
        var second = await cluster.Relationships.CountRelationships(new CountRelationshipsArgs("doc_viewers"));
        Assert.Equal(first.Count + 1, second.Count);

        var head2 = (await cluster.Datastore.HeadRevision()).Revision;
        Assert.Equal(second.Count, await scanner.CountRelationships("doc_viewers", head2, CancellationToken.None));
        Assert.Equal(second.Count, await cluster.Datastore.SnapshotReader(head2).CountRelationships("doc_viewers"));

        // Unregister through the RPC path: the RPC count now fails with the typed NotRegistered error
        // across the grain boundary, and the scanner agrees at the new head.
        await cluster.Relationships.UnregisterRelationshipCounter(new UnregisterCounterArgs("doc_viewers"));
        var ex = await Assert.ThrowsAsync<CounterOperationException>(() =>
            cluster.Relationships.CountRelationships(new CountRelationshipsArgs("doc_viewers")));
        Assert.Equal(CounterErrorKind.NotRegistered, ex.Kind);

        var head3 = (await cluster.Datastore.HeadRevision()).Revision;
        await Assert.ThrowsAsync<CounterNotRegisteredException>(() =>
            scanner.CountRelationships("doc_viewers", head3, CancellationToken.None));
        Assert.Null(await scanner.ReadCounterFilter("doc_viewers", head3, CancellationToken.None));
        Assert.DoesNotContain("doc_viewers", await CollectCounterNames(scanner, head3));

        // Time travel: at the pre-unregister head the counter still counts — the tombstone is a
        // revision-visible MVCC fact, not a destructive delete.
        Assert.Equal(second.Count, await scanner.CountRelationships("doc_viewers", head2, CancellationToken.None));
    }

    /// <summary>
    /// Bulk-export reconnect contract at the <see cref="RelationshipReads"/> level: resuming from the
    /// cursor of a mid-stream item must yield exactly the uninterrupted run's remainder — the same rows
    /// in the same order AND byte-identical resume tokens for every remaining item (the cursor encodes
    /// the pinned revision plus the last tuple, so both runs mint identical tokens). A row committed
    /// AFTER the export pinned its snapshot must not leak into the resumed remainder.
    /// </summary>
    [Fact]
    public async Task Bulk_Export_Resumed_Mid_Stream_Yields_Byte_Identical_Remainder_Tokens()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(
            Enumerable.Range(0, 30)
                .Select(i => new RelationshipUpdate(
                    Row("doc", $"d{i:D3}", "viewer", "user", "alice"), UpdateOperation.Create))
                .ToList()));

        var args = new BulkExportRelationshipsArgs(
            new RelationshipsFilterWire("doc", null, null, null, null, null, null),
            Limit: 1000,
            Cursor: null);

        var full = await CollectItems(cluster.RelationshipReads.BulkExportRelationships(args));
        Assert.Equal(30, full.Count);

        // A later matching write must be invisible to the resumed run (its cursor pins the revision).
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(
            [new RelationshipUpdate(Row("doc", "d999", "viewer", "user", "zzz"), UpdateOperation.Create)]));

        var mid = full.Count / 2;
        var resumed = await CollectItems(cluster.RelationshipReads.BulkExportRelationships(
            args with { Cursor = full[mid].ResumeCursor }));

        var expectedTail = full.Skip(mid + 1).ToList();
        Assert.Equal(expectedTail.Count, resumed.Count);
        Assert.Equal(expectedTail.Select(i => Canonical(i.Relationship)), resumed.Select(i => Canonical(i.Relationship)));
        Assert.Equal(expectedTail.Select(i => i.ResumeCursor), resumed.Select(i => i.ResumeCursor));
        Assert.DoesNotContain(resumed, i => i.Relationship.ResourceId == "d999");
    }

    // --- helpers ---

    private static Relationship Row(
        string resourceType, string resourceId, string relation, string subjectType, string subjectId) =>
        Relationship.Create(
            new ObjectAndRelation(resourceType, resourceId, relation),
            new ObjectAndRelation(subjectType, subjectId, CoreConstants.Ellipsis));

    private static async Task<MeshTestCluster> ClusterWithRows()
    {
        var cluster = await MeshTestCluster.CreateAsync(Schema);
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(
            Rows.Select(r => new RelationshipUpdate(r, UpdateOperation.Create)).ToList()));
        return cluster;
    }

    private static async Task<List<string>> CollectCounterNames(ISnapshotScanner scanner, IRevision revision)
    {
        var names = new List<string>();
        await foreach (var counter in scanner.LookupCounters(revision, CancellationToken.None))
            names.Add(counter.Name);
        return names;
    }

    private static async Task<List<string>> Collect(IAsyncEnumerable<Relationship> source)
    {
        var rows = new List<string>();
        await foreach (var rel in source)
            rows.Add(Canonical(rel));
        return rows;
    }

    private static async Task<List<RelationshipStreamItem>> CollectItems(
        IAsyncEnumerable<RelationshipStreamItem> source)
    {
        var items = new List<RelationshipStreamItem>();
        await foreach (var item in source)
            items.Add(item);
        return items;
    }

    private static void CompareSorted(List<string> expected, List<string> actual, string label, List<string> failures)
    {
        expected.Sort(StringComparer.Ordinal);
        actual.Sort(StringComparer.Ordinal);
        if (!expected.SequenceEqual(actual, StringComparer.Ordinal))
        {
            failures.Add(
                $"  {label}:\n    reader:  [{string.Join(", ", expected)}]\n    scanner: [{string.Join(", ", actual)}]");
        }
    }

    // Canonical string forms: this file's fixture rows carry no caveat context, so identity plus the
    // caveat name and expiration suffices (no nested-context canonicalization needed here — see
    // ShardedReaderEquivalenceTests for the full treatment).

    private static string Canonical(Relationship rel) =>
        $"{rel.Resource.ObjectType}:{rel.Resource.ObjectId}#{rel.Resource.Relation}" +
        $"@{rel.Subject.ObjectType}:{rel.Subject.ObjectId}#{rel.Subject.Relation}" +
        (rel.OptionalCaveat is { } cav ? $"[{cav.CaveatName}]" : "") +
        (rel.OptionalExpiration is { } exp ? $"@exp={exp:O}" : "");

    private static string Canonical(RelationshipWire rel) =>
        $"{rel.ResourceType}:{rel.ResourceId}#{rel.ResourceRelation}" +
        $"@{rel.SubjectType}:{rel.SubjectId}#{rel.SubjectRelation}" +
        (rel.CaveatName is { Length: > 0 } cav ? $"[{cav}]" : "") +
        (rel.Expiration is { } exp ? $"@exp={exp:O}" : "");
}
