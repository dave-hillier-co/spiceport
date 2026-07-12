using Microsoft.Extensions.DependencyInjection;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Mesh-level gates for <see cref="MembershipWalkGrain"/>: warm-activation reuse, correctness over a
/// genuine data cycle end-to-end, the depth-exhaustion/incomplete-reply contract (and that
/// <see cref="ReverseOpsStreamGrain.StreamLookupResources"/> falls back to the live traversal rather than
/// trusting a partial candidate set), and that <see cref="MembershipIndexOptions.Enabled"/>=false still
/// produces correct results via the unaccelerated live path.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class MembershipWalkGrainTests
{
    private const string NestedSchema = """
        definition user {}

        definition group {
            relation member: user | group#member
        }

        definition document {
            relation viewer: user | group#member
            relation editor: user
            permission view = viewer + editor
        }
        """;

    private static Relationship Rel(string rt, string rid, string rel, ObjectAndRelation subject) =>
        Relationship.Create(new ObjectAndRelation(rt, rid, rel), subject);

    private static ObjectAndRelation Onr(string type, string id, string relation = CoreConstants.Ellipsis) =>
        new(type, id, relation);

    private static async Task Seed(MeshTestCluster cluster, params Relationship[] rels) =>
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(
            rels.Select(r => new RelationshipUpdate(r, UpdateOperation.Create)).ToList()));

    private static async Task<SortedSet<string>> GrainResources(
        MeshTestCluster cluster, string subjectType, string subjectId, string subjectRelation,
        string resourceType, string permission)
    {
        var ids = new SortedSet<string>(StringComparer.Ordinal);
        await foreach (var r in cluster.GrainFactory.GetGrain<IReverseOpsStreamGrain>(Guid.NewGuid())
            .StreamLookupResources(new LookupResourcesArgs(
                resourceType, permission, subjectType, subjectId, subjectRelation, null, null, null)))
            ids.Add(r.ResourceId);
        return ids;
    }

    private static async Task<SortedSet<string>> EngineResources(
        MeshTestCluster cluster, string subjectType, string subjectId, string subjectRelation,
        string resourceType, string permission)
    {
        var schema = cluster.Services.GetRequiredService<ISchemaProvider>().Current;
        var rev = await cluster.Datastore.OptimizedRevision();
        var reader = cluster.Datastore.SnapshotReader(rev.Revision);
        var engine = new LookupResourcesEngine(schema.Namespaces, schema.Caveats);

        var ids = new SortedSet<string>(StringComparer.Ordinal);
        await foreach (var f in engine.LookupResources(
            reader, subjectType, subjectId, subjectRelation, resourceType, permission, coveredCandidateIds: null))
            ids.Add(f.ResourceId);
        return ids;
    }

    [Fact]
    public async Task WarmActivation_ServesTheSecondIdenticalLookup_Identically()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(NestedSchema);
        await Seed(cluster,
            Rel("group", "g1", "member", Onr("user", "alice")),
            Rel("document", "d1", "viewer", Onr("group", "g1", "member")));

        var first = await GrainResources(cluster, "user", "alice", CoreConstants.Ellipsis, "document", "view");
        var second = await GrainResources(cluster, "user", "alice", CoreConstants.Ellipsis, "document", "view");

        Assert.Equal(first, second);
        Assert.Contains("d1", first);
    }

    [Fact]
    public async Task CyclicMembershipData_LookupTerminatesAndIsCorrect()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(NestedSchema);
        // group a member of b, b member of a: a genuine cycle in the stored data.
        await Seed(cluster,
            Rel("group", "a", "member", Onr("group", "b", "member")),
            Rel("group", "b", "member", Onr("group", "a", "member")),
            Rel("group", "a", "member", Onr("user", "alice")),
            Rel("document", "d1", "viewer", Onr("group", "b", "member")));

        var task = GrainResources(cluster, "user", "alice", CoreConstants.Ellipsis, "document", "view");
        var completed = await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(30)));
        Assert.Same(task, completed);

        var grainResult = await task;
        var engineResult = await EngineResources(cluster, "user", "alice", CoreConstants.Ellipsis, "document", "view");
        Assert.Equal(engineResult, grainResult);
        Assert.Contains("d1", grainResult);
    }

    [Fact]
    public async Task CyclicMembershipData_CutsOnTheBackEdge_NotOnDepthExhaustion()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(NestedSchema);
        await Seed(cluster,
            Rel("group", "a", "member", Onr("group", "b", "member")),
            Rel("group", "b", "member", Onr("group", "a", "member")),
            Rel("group", "a", "member", Onr("user", "alice")));

        var head = await cluster.Datastore.HeadRevision();
        var schemaHash = cluster.SchemaProvider.Current.SchemaHash;
        var key = MembershipWalkKey.Build("user", "alice", CoreConstants.Ellipsis, head.Revision.ToString(), schemaHash);
        var grain = cluster.GrainFactory.GetGrain<IMembershipWalkGrain>(key);

        using var ct = new CancellationTokenSource();
        var reply = await grain.GetContainingSet(
            new MembershipWalkArgs(Path: [], DepthRemaining: CheckEngine.DefaultMaxDepth), ct.Token);

        // The a<->b cycle must terminate via the exact path-list back-edge cut — CycleCut, with plenty of
        // depth budget left — never by burning the whole budget down to an Incomplete reply (which would
        // silently disable the fast path for all cyclic data). Both groups still appear as candidates.
        Assert.True(reply.CycleCut);
        Assert.False(reply.Incomplete);
        Assert.Contains(reply.Nodes, n => n is { Type: "group", Id: "a", Relation: "member" });
        Assert.Contains(reply.Nodes, n => n is { Type: "group", Id: "b", Relation: "member" });
    }

    [Fact]
    public async Task DepthExhaustion_UnitLevel_ReportsIncomplete()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(NestedSchema);
        await Seed(cluster,
            Rel("group", "g1", "member", Onr("user", "alice")),
            Rel("group", "g2", "member", Onr("group", "g1", "member")));

        var head = await cluster.Datastore.HeadRevision();
        var schemaHash = cluster.SchemaProvider.Current.SchemaHash;
        var key = MembershipWalkKey.Build("user", "alice", CoreConstants.Ellipsis, head.Revision.ToString(), schemaHash);
        var grain = cluster.GrainFactory.GetGrain<IMembershipWalkGrain>(key);

        using var ct = new CancellationTokenSource();
        // Budget exhausted before it can recurse past alice's own direct parent (g1): the walk still returns
        // g1 as a direct-parent node, but marks the reply incomplete because it never explored beyond it.
        var reply = await grain.GetContainingSet(new MembershipWalkArgs(Path: [], DepthRemaining: 0), ct.Token);

        Assert.True(reply.Incomplete);
        Assert.Contains(reply.Nodes, n => n is { Type: "group", Id: "g1", Relation: "member" });
        Assert.DoesNotContain(reply.Nodes, n => n is { Type: "group", Id: "g2", Relation: "member" });
    }

    [Fact]
    public async Task DepthExhaustion_EndToEnd_FallsBackToLiveAndStaysCorrect()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(NestedSchema);

        // A chain longer than CheckEngine.DefaultMaxDepth (both the walk grain's root budget AND the live
        // LookupResourcesEngine's own recursion cap — a genuine, documented depth limit on both paths, not
        // an accelerator-only shortfall). "shallow" sits well inside the budget on a separate branch;
        // "deep" sits past it. The point of this test is that the accelerated path must not DIVERGE from
        // live once the walk grain reports Incomplete: it must fall back rather than trust a partial
        // candidate set — proven by grain and live agreeing on BOTH the reachable and the capped resource.
        const int chainLength = CheckEngine.DefaultMaxDepth + 10;
        var rels = new List<Relationship>
        {
            Rel("group", "g0", "member", Onr("user", "alice")),
            Rel("group", "s0", "member", Onr("user", "alice")),
            Rel("document", "shallow", "viewer", Onr("group", "s0", "member")),
        };
        for (var i = 1; i < chainLength; i++)
            rels.Add(Rel("group", $"g{i}", "member", Onr("group", $"g{i - 1}", "member")));
        rels.Add(Rel("document", "deep", "viewer", Onr("group", $"g{chainLength - 1}", "member")));
        await Seed(cluster, rels.ToArray());

        var grainResult = await GrainResources(cluster, "user", "alice", CoreConstants.Ellipsis, "document", "view");
        var engineResult = await EngineResources(cluster, "user", "alice", CoreConstants.Ellipsis, "document", "view");

        Assert.Equal(engineResult, grainResult);
        Assert.Contains("shallow", grainResult);
        Assert.DoesNotContain("deep", grainResult); // beyond the documented depth cap on both paths
    }

    [Fact]
    public async Task Disabled_MembershipIndex_StillProducesCorrectResults()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(NestedSchema, useMembershipIndex: false);
        await Seed(cluster,
            Rel("group", "g1", "member", Onr("user", "alice")),
            Rel("group", "g2", "member", Onr("group", "g1", "member")),
            Rel("document", "d1", "viewer", Onr("group", "g2", "member")));

        var grainResult = await GrainResources(cluster, "user", "alice", CoreConstants.Ellipsis, "document", "view");
        var engineResult = await EngineResources(cluster, "user", "alice", CoreConstants.Ellipsis, "document", "view");

        Assert.Equal(engineResult, grainResult);
        Assert.Contains("d1", grainResult);
    }
}
