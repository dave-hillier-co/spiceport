using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Exercises the three reverse / tree ops through the REAL Orleans grain mesh: the stateless-worker
/// <see cref="IReverseOpsGrain"/> resolved from the in-process <see cref="TestCluster"/>'s grain
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

    private static IReverseOpsGrain Grain(MeshTestCluster cluster) =>
        cluster.GrainFactory.GetGrain<IReverseOpsGrain>(IReverseOpsGrain.Key);

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

        var reply = await Grain(cluster).LookupSubjects(new LookupSubjectsArgs(
            "document", "readme", "view", "user", CoreConstants.Ellipsis,
            Context: null, Limit: null, Cursor: null));

        var ids = reply.Subjects.Select(s => s.SubjectId).ToHashSet();
        Assert.Equal(new HashSet<string> { "alice", "bob" }, ids);
        Assert.All(reply.Subjects, s => Assert.False(s.Permissionship.IsCaveated));
    }

    [Fact]
    public async Task LookupSubjects_Honors_Limit_And_Resumes_Via_Cursor()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(SchemaText);
        await SeedAsync(cluster.Datastore,
            ("readme", "viewer", "alice"),
            ("readme", "viewer", "bob"),
            ("readme", "viewer", "carol"));

        var grain = Grain(cluster);

        var page1 = await grain.LookupSubjects(new LookupSubjectsArgs(
            "document", "readme", "view", "user", CoreConstants.Ellipsis,
            Context: null, Limit: 2, Cursor: null));
        Assert.Equal(2, page1.Subjects.Count);
        Assert.False(string.IsNullOrEmpty(page1.Cursor));

        var page2 = await grain.LookupSubjects(new LookupSubjectsArgs(
            "document", "readme", "view", "user", CoreConstants.Ellipsis,
            Context: null, Limit: 2, Cursor: page1.Cursor));

        var all = page1.Subjects.Concat(page2.Subjects).Select(s => s.SubjectId).ToHashSet();
        Assert.Equal(new HashSet<string> { "alice", "bob", "carol" }, all);
        Assert.True(string.IsNullOrEmpty(page2.Cursor));
    }

    [Fact]
    public async Task LookupResources_Returns_All_Reachable_Resources()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(SchemaText);
        await SeedAsync(cluster.Datastore,
            ("readme", "viewer", "alice"),
            ("design", "editor", "alice"),
            ("secret", "viewer", "bob"));

        var reply = await Grain(cluster).LookupResources(new LookupResourcesArgs(
            "document", "view", "user", "alice", CoreConstants.Ellipsis,
            Context: null, Limit: null, Cursor: null));

        var ids = reply.Resources.Select(r => r.ResourceId).ToHashSet();
        Assert.Equal(new HashSet<string> { "readme", "design" }, ids);
        Assert.All(reply.Resources, r => Assert.False(r.Permissionship.IsCaveated));
    }

    [Fact]
    public async Task LookupResources_Honors_Limit_And_Resumes_Via_Cursor()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(SchemaText);
        await SeedAsync(cluster.Datastore,
            ("d1", "viewer", "alice"),
            ("d2", "viewer", "alice"),
            ("d3", "viewer", "alice"));

        var grain = Grain(cluster);

        var page1 = await grain.LookupResources(new LookupResourcesArgs(
            "document", "view", "user", "alice", CoreConstants.Ellipsis,
            Context: null, Limit: 2, Cursor: null));
        Assert.Equal(2, page1.Resources.Count);
        Assert.False(string.IsNullOrEmpty(page1.Cursor));

        var page2 = await grain.LookupResources(new LookupResourcesArgs(
            "document", "view", "user", "alice", CoreConstants.Ellipsis,
            Context: null, Limit: 2, Cursor: page1.Cursor));

        var all = page1.Resources.Concat(page2.Resources).Select(r => r.ResourceId).ToHashSet();
        Assert.Equal(new HashSet<string> { "d1", "d2", "d3" }, all);
    }
}
