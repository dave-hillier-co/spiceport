using Microsoft.Extensions.DependencyInjection;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Exercises the three reverse / tree ops through the REAL Orleans grain mesh: the
/// <see cref="IReverseOpsStreamGrain"/> resolved from the in-process <see cref="TestCluster"/>'s grain
/// factory, running ExpandPermissionTree, LookupSubjects and LookupResources against the silo's
/// datastore snapshot.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class ReverseOpsMeshTests
{
    private const string SchemaText = """
        definition user {}

        definition document {
            relation viewer: user
            relation editor: user
            permission view = viewer + editor
        }
        """;

    private static async Task SeedAsync(IDatastore datastore, params (string res, string rel, string subj)[] tuples)
    {
        var updates = tuples
            .Select(t => new RelationshipUpdate(
                Relationship.Create(
                    new ObjectAndRelation("document", t.res, t.rel),
                    new ObjectAndRelation("user", t.subj, CoreConstants.Ellipsis)),
                UpdateOperation.Touch))
            .ToList();
        await datastore.ReadWriteTx(tx => tx.WriteRelationships(updates));
    }

    // ExpandPermissionTree is unary with no follow-up MoveNext, so it reuses the well-known Expand key.
    private static IReverseOpsStreamGrain Grain(MeshTestCluster cluster) =>
        cluster.GrainFactory.GetGrain<IReverseOpsStreamGrain>(IReverseOpsStreamGrain.ExpandKey);

    // A FRESH Guid per enumeration: native IAsyncEnumerable streaming pins the enumerator to one activation,
    // so every stream (and every resume) must resolve its own brand-new grain key.
    private static IReverseOpsStreamGrain StreamGrain(MeshTestCluster cluster) =>
        cluster.GrainFactory.GetGrain<IReverseOpsStreamGrain>(Guid.NewGuid());

    private static async Task<List<T>> Collect<T>(IAsyncEnumerable<T> stream)
    {
        var list = new List<T>();
        await foreach (var item in stream)
            list.Add(item);
        return list;
    }

    private static async Task<List<T>> TakeN<T>(IAsyncEnumerable<T> stream, int n)
    {
        var list = new List<T>();
        await foreach (var item in stream)
        {
            list.Add(item);
            if (list.Count >= n)
                break; // stopping the await foreach stops the upstream engine walk (backpressure).
        }
        return list;
    }

    [Fact]
    public async Task ExpandPermissionTree_Union_Returns_SetOp_With_Both_Operands()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(SchemaText);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"), ("readme", "editor", "bob"));

        var reply = await Grain(cluster).ExpandPermissionTree(
            new ExpandTreeArgs("document", "readme", "view", ExpandModeWire.Shallow));

        var root = reply.Root;
        Assert.False(root.IsLeaf);
        Assert.Equal(SetOpWire.Union, root.Operation);
        Assert.Equal("view", root.ExpandedRelation);

        // The union's leaves carry alice (viewer) and bob (editor).
        var subjects = root.Children
            .Where(c => c.IsLeaf)
            .SelectMany(c => c.Subjects)
            .Select(s => s.SubjectId)
            .ToHashSet();
        Assert.Contains("alice", subjects);
        Assert.Contains("bob", subjects);
    }

    [Fact]
    public async Task LookupSubjects_Returns_All_Holders_Of_Permission()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(SchemaText);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"), ("readme", "editor", "bob"));

        var items = await Collect(StreamGrain(cluster).StreamLookupSubjects(new LookupSubjectsArgs(
            "document", "readme", "view", "user", CoreConstants.Ellipsis,
            Context: null, Limit: null, Cursor: null)));

        var ids = items.Select(s => s.Subject.SubjectId).ToHashSet();
        Assert.Equal(new HashSet<string> { "alice", "bob" }, ids);
        Assert.All(items, s => Assert.False(s.Subject.Permissionship.IsCaveated));
    }

    [Fact]
    public async Task LookupSubjects_Honors_Limit_And_Resumes_Via_Cursor()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(SchemaText);
        await SeedAsync(cluster.Datastore,
            ("readme", "viewer", "alice"),
            ("readme", "viewer", "bob"),
            ("readme", "viewer", "carol"));

        // Take 2 from a fresh stream, then resume from the 2nd item's cursor on a FRESH grain activation.
        var page1 = await TakeN(StreamGrain(cluster).StreamLookupSubjects(new LookupSubjectsArgs(
            "document", "readme", "view", "user", CoreConstants.Ellipsis,
            Context: null, Limit: 2, Cursor: null)), 2);
        Assert.Equal(2, page1.Count);
        Assert.False(string.IsNullOrEmpty(page1[^1].ResumeCursor));

        var page2 = await Collect(StreamGrain(cluster).StreamLookupSubjects(new LookupSubjectsArgs(
            "document", "readme", "view", "user", CoreConstants.Ellipsis,
            Context: null, Limit: 2, Cursor: page1[^1].ResumeCursor)));
        Assert.Single(page2);

        var all = page1.Concat(page2).Select(s => s.Subject.SubjectId).ToHashSet();
        Assert.Equal(new HashSet<string> { "alice", "bob", "carol" }, all);
    }

    [Fact]
    public async Task LookupSubjects_Resume_Through_The_SubjectFrontier_Memo_Union_Equals_Unlimited_No_Duplicates()
    {
        // The consumer cursor contract must be unchanged now that StreamLookupSubjects consults the
        // SubjectFrontierGrain memo: a limited first page, resumed via ITS OWN cursor on a FRESH
        // Guid-keyed stream grain instance (a brand-new activation, unrelated to whichever
        // SubjectFrontierGrain activation served either call), must union with no duplicates to the
        // unlimited result, and the resume token itself must still be the SAME opaque
        // ReverseOpsCursorCodec.EncodeSubjectId(lastSubjectId) shape (a plain last-subject-id token,
        // unaffected by which frontier source produced the item).
        await using var cluster = await MeshTestCluster.CreateAsync(SchemaText);
        await SeedAsync(cluster.Datastore,
            ("readme", "viewer", "alice"),
            ("readme", "viewer", "bob"),
            ("readme", "viewer", "carol"),
            ("readme", "viewer", "dave"),
            ("readme", "viewer", "erin"));

        var unlimited = await Collect(StreamGrain(cluster).StreamLookupSubjects(new LookupSubjectsArgs(
            "document", "readme", "view", "user", CoreConstants.Ellipsis,
            Context: null, Limit: null, Cursor: null)));
        var unlimitedIds = unlimited.Select(s => s.Subject.SubjectId).ToList();

        var page1 = await TakeN(StreamGrain(cluster).StreamLookupSubjects(new LookupSubjectsArgs(
            "document", "readme", "view", "user", CoreConstants.Ellipsis,
            Context: null, Limit: 2, Cursor: null)), 2);
        Assert.Equal(2, page1.Count);
        var resumeToken = page1[^1].ResumeCursor;
        Assert.False(string.IsNullOrEmpty(resumeToken));
        // The cursor is still exactly ReverseOpsCursorCodec's plain last-subject-id token — unchanged
        // format/constant.
        Assert.Equal(
            ReverseOpsCursorCodec.EncodeSubjectId(page1[^1].Subject.SubjectId), resumeToken);

        var page2 = await Collect(StreamGrain(cluster).StreamLookupSubjects(new LookupSubjectsArgs(
            "document", "readme", "view", "user", CoreConstants.Ellipsis,
            Context: null, Limit: null, Cursor: resumeToken)));

        var resumedIds = page1.Concat(page2).Select(s => s.Subject.SubjectId).ToList();

        // No duplicates and exact union with the unlimited result (order-independent: both walks share
        // the same underlying engine order, but comparing as sets is the contract that matters here).
        Assert.Equal(resumedIds.Count, resumedIds.Distinct().Count());
        Assert.Equal(unlimitedIds.ToHashSet(), resumedIds.ToHashSet());
    }

    [Fact]
    public async Task LookupResources_Returns_All_Reachable_Resources()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(SchemaText);
        await SeedAsync(cluster.Datastore,
            ("readme", "viewer", "alice"),
            ("design", "editor", "alice"),
            ("secret", "viewer", "bob"));

        var items = await Collect(StreamGrain(cluster).StreamLookupResources(new LookupResourcesArgs(
            "document", "view", "user", "alice", CoreConstants.Ellipsis,
            Context: null, Limit: null, Cursor: null)));

        var ids = items.Select(r => r.ResourceId).ToHashSet();
        Assert.Equal(new HashSet<string> { "readme", "design" }, ids);
        Assert.All(items, r => Assert.False(r.Permissionship.IsCaveated));
    }

    [Fact]
    public async Task LookupResources_Honors_Limit_And_Resumes_Via_Cursor()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(SchemaText);
        await SeedAsync(cluster.Datastore,
            ("d1", "viewer", "alice"),
            ("d2", "viewer", "alice"),
            ("d3", "viewer", "alice"));

        // Take 2 from a fresh stream (a limited walk emits a per-item cursor), then resume on a FRESH grain.
        var page1 = await TakeN(StreamGrain(cluster).StreamLookupResources(new LookupResourcesArgs(
            "document", "view", "user", "alice", CoreConstants.Ellipsis,
            Context: null, Limit: 2, Cursor: null)), 2);
        Assert.Equal(2, page1.Count);
        Assert.False(string.IsNullOrEmpty(page1[^1].AfterResultCursor));

        var page2 = await Collect(StreamGrain(cluster).StreamLookupResources(new LookupResourcesArgs(
            "document", "view", "user", "alice", CoreConstants.Ellipsis,
            Context: null, Limit: 2, Cursor: page1[^1].AfterResultCursor)));

        var all = page1.Concat(page2).Select(r => r.ResourceId).ToHashSet();
        Assert.Equal(new HashSet<string> { "d1", "d2", "d3" }, all);
    }

    private const string NestedSchemaText = """
        definition user {}

        definition group {
            relation member: user | group#member
        }

        definition document {
            relation viewer: user | group#member
            permission view = viewer
        }
        """;

    [Fact]
    public async Task LookupResources_PagesMultiLevelCursor_Through_Grain_Codec()
    {
        // alice -> g1#member -> g2#member -> {g3a,g3b}#member -> document#viewer. Paging one result at a
        // time forces the opaque page cursor (a multi-section, keyset-bearing token) to round-trip through
        // the grain's codec on every page. The concatenation must equal the unpaged set with no drops/dupes.
        await using var cluster = await MeshTestCluster.CreateAsync(NestedSchemaText);
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(
        [
            Member("g1", new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis)),
            Member("g2", new ObjectAndRelation("group", "g1", "member")),
            Member("g3a", new ObjectAndRelation("group", "g2", "member")),
            Member("g3b", new ObjectAndRelation("group", "g2", "member")),
            Viewer("doc_a", new ObjectAndRelation("group", "g3a", "member")),
            Viewer("doc_z", new ObjectAndRelation("group", "g3a", "member")),
            Viewer("doc_b", new ObjectAndRelation("group", "g3b", "member")),
            Viewer("doc_m", new ObjectAndRelation("group", "g3b", "member")),
        ]));
        var unpaged = await Collect(StreamGrain(cluster).StreamLookupResources(new LookupResourcesArgs(
            "document", "view", "user", "alice", CoreConstants.Ellipsis,
            Context: null, Limit: null, Cursor: null)));
        var expected = unpaged.Select(r => r.ResourceId)
            .OrderBy(x => x, StringComparer.Ordinal).ToArray();
        Assert.Equal(["doc_a", "doc_b", "doc_m", "doc_z"], expected);

        // Resume ONE result at a time, each on a FRESH grain activation, through the opaque multi-section
        // cursor codec. The concatenation must equal the unpaged set with no drops/dupes.
        var paged = new List<string>();
        string? cursor = null;
        while (paged.Count <= unpaged.Count)
        {
            var page = await TakeN(StreamGrain(cluster).StreamLookupResources(new LookupResourcesArgs(
                "document", "view", "user", "alice", CoreConstants.Ellipsis,
                Context: null, Limit: 1, Cursor: cursor)), 1);
            if (page.Count == 0)
                break;
            paged.Add(page[0].ResourceId);
            cursor = page[0].AfterResultCursor;
            if (string.IsNullOrEmpty(cursor))
                break;
        }

        Assert.Equal(expected, paged.OrderBy(x => x, StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public async Task StreamLookupResources_PreCancelledToken_Throws_Without_Hanging()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(SchemaText);
        await SeedAsync(cluster.Datastore,
            ("d1", "viewer", "alice"), ("d2", "viewer", "alice"), ("d3", "viewer", "alice"));

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // A pre-cancelled token must surface as OperationCanceledException promptly (bounded so a regression
        // that ignored the token would fail the test rather than hang CI).
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => Drain(cts.Token).WaitAsync(TimeSpan.FromSeconds(10)));

        async Task Drain(CancellationToken ct)
        {
            await foreach (var _ in StreamGrain(cluster).StreamLookupResources(new LookupResourcesArgs(
                "document", "view", "user", "alice", CoreConstants.Ellipsis,
                Context: null, Limit: null, Cursor: null), ct))
            {
            }
        }
    }

    [Fact]
    public async Task StreamLookupResources_CancelMidStream_StopsWithoutHanging()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(SchemaText);
        var tuples = Enumerable.Range(0, 50)
            .Select(i => ($"d{i:D3}", "viewer", "alice"))
            .ToArray();
        await SeedAsync(cluster.Datastore, tuples);

        using var cts = new CancellationTokenSource();
        var seen = 0;

        // Cancelling after the first item must stop the enumeration without hanging (bounded outer wait).
        await Run().WaitAsync(TimeSpan.FromSeconds(10));
        Assert.True(seen >= 1);

        async Task Run()
        {
            try
            {
                await foreach (var _ in StreamGrain(cluster).StreamLookupResources(new LookupResourcesArgs(
                    "document", "view", "user", "alice", CoreConstants.Ellipsis,
                    Context: null, Limit: null, Cursor: null), cts.Token))
                {
                    seen++;
                    if (seen == 1)
                        cts.Cancel();
                }
            }
            catch (OperationCanceledException)
            {
                // Expected when the cancellation is observed before the stream naturally drains.
            }
        }
    }

    [Fact]
    public async Task StreamLookupResources_CrossesSiloBoundary_MatchesEngineOracle()
    {
        // A genuine multi-silo cluster: the client grain factory resolves a Guid-keyed stream grain that may
        // activate on any silo, so the enumeration crosses the grain boundary. The full streamed set must
        // equal the engine's own (index-off) LookupResources over the same pinned snapshot.
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(NestedSchemaText, siloCount: 3);
        Assert.True(cluster.SiloCount >= 2);
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(
        [
            Member("g1", new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis)),
            Member("g2", new ObjectAndRelation("group", "g1", "member")),
            Viewer("doc_a", new ObjectAndRelation("group", "g2", "member")),
            Viewer("doc_b", new ObjectAndRelation("group", "g1", "member")),
        ]));

        var streamed = new SortedSet<string>(StringComparer.Ordinal);
        await foreach (var r in StreamGrain(cluster).StreamLookupResources(new LookupResourcesArgs(
            "document", "view", "user", "alice", CoreConstants.Ellipsis,
            Context: null, Limit: null, Cursor: null)))
            streamed.Add(r.ResourceId);

        // Engine oracle over the same snapshot the mesh pinned.
        var schema = cluster.Services.GetRequiredService<ISchemaProvider>().Current;
        var rev = await cluster.Datastore.OptimizedRevision();
        var reader = cluster.Datastore.SnapshotReader(rev.Revision);
        var engine = new LookupResourcesEngine(schema.Namespaces, schema.Caveats);
        var oracle = new SortedSet<string>(StringComparer.Ordinal);
        await foreach (var f in engine.LookupResources(
            reader, "user", "alice", CoreConstants.Ellipsis, "document", "view", index: null))
            oracle.Add(f.ResourceId);

        Assert.NotEmpty(streamed);
        Assert.Equal(oracle, streamed);
        Assert.Contains("doc_a", streamed);
        Assert.Contains("doc_b", streamed);
    }

    private static RelationshipUpdate Member(string group, ObjectAndRelation subject) =>
        new(Relationship.Create(new ObjectAndRelation("group", group, "member"), subject), UpdateOperation.Touch);

    private static RelationshipUpdate Viewer(string document, ObjectAndRelation subject) =>
        new(Relationship.Create(new ObjectAndRelation("document", document, "viewer"), subject), UpdateOperation.Touch);
}
