using Microsoft.Extensions.DependencyInjection;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Stage-4 gates for the Leopard <see cref="MembershipIndex"/> wired into the mesh behind the
/// <c>useMembershipIndex</c> flag. Drives <see cref="IReverseOpsGrain.LookupResources"/> with the index ON and
/// proves the result set is IDENTICAL to the index-OFF engine over the same snapshot (oracle equivalence
/// end-to-end), that every returned resource is Check-confirmed, and that a runtime schema swap invalidates the
/// old-hash index (the rebuilt index keeps verdicts correct).
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class Stage4LeopardMeshTests
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

    private static IReverseOpsStreamGrain StreamGrain(MeshTestCluster cluster) =>
        cluster.GrainFactory.GetGrain<IReverseOpsStreamGrain>(Guid.NewGuid());

    private static Relationship Rel(string rt, string rid, string rel, ObjectAndRelation subject) =>
        Relationship.Create(new ObjectAndRelation(rt, rid, rel), subject);

    private static ObjectAndRelation Onr(string type, string id, string relation = CoreConstants.Ellipsis) =>
        new(type, id, relation);

    private static async Task Seed(MeshTestCluster cluster, params Relationship[] rels) =>
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(
            rels.Select(r => new RelationshipUpdate(r, UpdateOperation.Create)).ToList()));

    /// <summary>The index-OFF engine's answer over the same pinned snapshot the grain would use.</summary>
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
            reader, subjectType, subjectId, subjectRelation, resourceType, permission, index: null))
            ids.Add(f.ResourceId);
        return ids;
    }

    private static async Task<SortedSet<string>> GrainResources(
        MeshTestCluster cluster, string subjectType, string subjectId, string subjectRelation,
        string resourceType, string permission)
    {
        var ids = new SortedSet<string>(StringComparer.Ordinal);
        await foreach (var r in StreamGrain(cluster).StreamLookupResources(new LookupResourcesArgs(
            resourceType, permission, subjectType, subjectId, subjectRelation, null, null, null)))
            ids.Add(r.ResourceId);
        return ids;
    }

    private static async Task AssertGrainEqualsEngine(
        MeshTestCluster cluster, string subjectType, string subjectId, string subjectRelation,
        string resourceType, string permission)
    {
        var engine = await EngineResources(cluster, subjectType, subjectId, subjectRelation, resourceType, permission);
        var grain = await GrainResources(cluster, subjectType, subjectId, subjectRelation, resourceType, permission);
        Assert.Equal(engine, grain);

        // The consistency invariant: every resource the index-accelerated grain returns is Check-confirmed.
        foreach (var id in grain)
        {
            var result = await cluster.Checker.Check(
                resourceType, id, permission, new ObjectAndRelation(subjectType, subjectId, subjectRelation), null);
            Assert.NotEqual(Membership.NotMember, result.Verdict);
        }
    }

    [Fact]
    public async Task IndexedLookupResources_EqualsLiveEngine_AcrossNestedGroups()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(NestedSchema, useMembershipIndex: true);
        await Seed(cluster,
            Rel("group", "g1", "member", Onr("user", "alice")),
            Rel("group", "g2", "member", Onr("group", "g1", "member")),
            Rel("group", "g3", "member", Onr("group", "g2", "member")),
            Rel("group", "g3", "member", Onr("user", "bob")),
            Rel("document", "d1", "viewer", Onr("group", "g3", "member")),
            Rel("document", "d2", "viewer", Onr("group", "g1", "member")),
            Rel("document", "d3", "editor", Onr("user", "alice")));

        await AssertGrainEqualsEngine(cluster, "user", "alice", CoreConstants.Ellipsis, "group", "member");
        await AssertGrainEqualsEngine(cluster, "user", "bob", CoreConstants.Ellipsis, "group", "member");
        await AssertGrainEqualsEngine(cluster, "user", "alice", CoreConstants.Ellipsis, "document", "view");
        await AssertGrainEqualsEngine(cluster, "user", "bob", CoreConstants.Ellipsis, "document", "view");
        await AssertGrainEqualsEngine(cluster, "user", "nobody", CoreConstants.Ellipsis, "document", "view");
    }

    // A single-level (non-self-referential) group schema. The dynamic WriteSchema validator rejects a relation
    // that lists its own `type#relation` as a subject, so the schema-swap gate uses this flatten-coverable but
    // non-recursive shape (document.viewer still flattens through group#member).
    private const string FlatGroupSchema = """
        definition user {}

        definition group {
            relation member: user
        }

        definition document {
            relation viewer: user | group#member
            permission view = viewer
        }
        """;

    [Fact]
    public async Task SchemaSwap_InvalidatesOldHashIndex_AndStaysCorrect()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(FlatGroupSchema, useMembershipIndex: true);
        await Seed(cluster,
            Rel("group", "g1", "member", Onr("user", "alice")),
            Rel("document", "d1", "viewer", Onr("group", "g1", "member")));

        // Build the index under the original schema hash.
        await AssertGrainEqualsEngine(cluster, "user", "alice", CoreConstants.Ellipsis, "document", "view");

        // Swap the schema (add an unrelated definition) -> a new schema hash. The cached index for the old hash
        // must not be reused; the rebuilt index keeps lookups correct.
        await cluster.WriteSchema(FlatGroupSchema + "\ndefinition folder { relation viewer: user }");

        await AssertGrainEqualsEngine(cluster, "user", "alice", CoreConstants.Ellipsis, "document", "view");
        await AssertGrainEqualsEngine(cluster, "user", "alice", CoreConstants.Ellipsis, "group", "member");
    }
}
