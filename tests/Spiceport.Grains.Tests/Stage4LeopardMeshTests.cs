using Microsoft.Extensions.DependencyInjection;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Stage-4 gates for the Leopard membership-walk grain mesh (<see cref="MembershipWalkGrain"/>) wired into
/// the mesh behind the <c>useMembershipWalk</c> flag. Drives
/// <see cref="IReverseOpsStreamGrain.StreamLookupResources"/> with the accelerator ON and proves the result
/// set is IDENTICAL to the accelerator-OFF engine over the same snapshot (oracle equivalence end-to-end),
/// that every returned resource is Check-confirmed, that a runtime schema swap rotates the walk-grain
/// keyspace (a new schema hash addresses disjoint activations rather than requiring cache invalidation), and
/// that a delete immediately excludes the detached subtree at the post-delete revision (the retired per-silo
/// replica's weak spot — this walk's trivial case, by construction).
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
            reader, subjectType, subjectId, subjectRelation, resourceType, permission, coveredCandidateIds: null))
            ids.Add(f.ResourceId);
        return ids;
    }

    private static async Task<SortedSet<string>> GrainResources(
        MeshTestCluster cluster, string subjectType, string subjectId, string subjectRelation,
        string resourceType, string permission, ConsistencyWire? consistency = null)
    {
        var ids = new SortedSet<string>(StringComparer.Ordinal);
        await foreach (var r in StreamGrain(cluster).StreamLookupResources(new LookupResourcesArgs(
            resourceType, permission, subjectType, subjectId, subjectRelation, null, null, null, consistency)))
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
        await using var cluster = await MeshTestCluster.CreateAsync(NestedSchema, useMembershipWalk: true);
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
        await using var cluster = await MeshTestCluster.CreateAsync(FlatGroupSchema, useMembershipWalk: true);
        await Seed(cluster,
            Rel("group", "g1", "member", Onr("user", "alice")),
            Rel("document", "d1", "viewer", Onr("group", "g1", "member")));

        // Warm the walk-grain mesh under the original schema hash.
        await AssertGrainEqualsEngine(cluster, "user", "alice", CoreConstants.Ellipsis, "document", "view");

        // Swap the schema (add an unrelated definition) -> a new schema hash. Because the schema hash is a
        // segment of IMembershipWalkGrain's key (see MembershipWalkKey), this proves KEY ROTATION rather
        // than any cache-invalidation logic: a walk request after the swap simply addresses a disjoint set
        // of grain activations under the new hash — there is nothing stale to invalidate — and lookups stay
        // correct under the new schema's coverage.
        await cluster.WriteSchema(FlatGroupSchema + "\ndefinition folder { relation viewer: user }");

        await AssertGrainEqualsEngine(cluster, "user", "alice", CoreConstants.Ellipsis, "document", "view");
        await AssertGrainEqualsEngine(cluster, "user", "alice", CoreConstants.Ellipsis, "group", "member");
    }

    // A single-level (non-self-referential) group schema, a distinct instance from FlatGroupSchema above so
    // this file's tests stay independent.
    private const string DeleteHeavySchema = """
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
    public async Task DeleteHeavy_DetachedSubtree_IsExcludedImmediatelyAtThePostDeleteRevision()
    {
        // The retired per-silo replica's weak spot: a delete folded into a shared, revision-approximate
        // cache could still serve a stale membership for a request pinned exactly at the post-delete
        // revision. A walk over a reader pinned to that EXACT revision has no such window — this is its
        // trivial case, not a special one.
        await using var cluster = await MeshTestCluster.CreateAsync(DeleteHeavySchema, useMembershipWalk: true);
        await Seed(cluster,
            Rel("group", "g1", "member", Onr("user", "alice")),
            Rel("group", "g2", "member", Onr("group", "g1", "member")),
            Rel("group", "g3", "member", Onr("group", "g2", "member")),
            Rel("document", "d1", "viewer", Onr("group", "g3", "member")));

        // Before the delete: alice reaches d1 through the nested chain. Fully-consistent reads throughout
        // this test so each request pins EXACTLY head — a MinimizeLatency read would legitimately serve the
        // quantized optimized revision, which can still predate the delete (correct consistency semantics,
        // but not what this gate is proving).
        var before = await GrainResources(
            cluster, "user", "alice", CoreConstants.Ellipsis, "document", "view", ConsistencyWire.FullyConsistent);
        Assert.Contains("d1", before);

        // Sever the middle edge (g2 no longer contains g1): the whole g1 subtree detaches from g3/d1.
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships([
            new RelationshipUpdate(
                Rel("group", "g2", "member", Onr("group", "g1", "member")), UpdateOperation.Delete),
        ]));

        // At the post-delete revision, d1 (and any resource reachable only through the severed subtree)
        // must be excluded immediately — no fold lag, because the walk reads a fresh pinned snapshot.
        var after = await GrainResources(
            cluster, "user", "alice", CoreConstants.Ellipsis, "document", "view", ConsistencyWire.FullyConsistent);
        Assert.DoesNotContain("d1", after);

        // alice is still a member of g1 itself (that edge was untouched), but g1 is no longer reachable from
        // g3, so the group-membership walk reflects the severed edge too.
        var groups = await GrainResources(
            cluster, "user", "alice", CoreConstants.Ellipsis, "group", "member", ConsistencyWire.FullyConsistent);
        Assert.Contains("g1", groups);
        Assert.DoesNotContain("g3", groups);

        // And the head-pinned engine agrees with the head-pinned grain result (walked == live at head).
        var head = await cluster.Datastore.HeadRevision();
        var schema = cluster.SchemaProvider.Current;
        var engine = new LookupResourcesEngine(schema.Namespaces, schema.Caveats);
        var liveAtHead = new SortedSet<string>(StringComparer.Ordinal);
        await foreach (var f in engine.LookupResources(
            cluster.Datastore.SnapshotReader(head.Revision), "user", "alice", CoreConstants.Ellipsis,
            "document", "view", coveredCandidateIds: null))
            liveAtHead.Add(f.ResourceId);
        Assert.Equal(liveAtHead, after);
    }
}
