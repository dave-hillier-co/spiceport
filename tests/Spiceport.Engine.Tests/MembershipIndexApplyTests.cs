using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Schema;

namespace Spiceport.Engine.Tests;

/// <summary>
/// Fold-equivalence gates for <see cref="MembershipIndex.Apply"/>: folding the relationship deltas committed
/// in <c>(R1..R2]</c> into an index built at <c>R1</c> must produce candidate output identical to a fresh
/// full <see cref="MembershipIndex.Build"/> at <c>R2</c> — the invariant documented on <c>Apply</c>. Driven
/// directly against a <see cref="ReferenceDatastore"/> (no Orleans), mirroring <c>Stage4MembershipIndexTests</c>.
/// </summary>
public class MembershipIndexApplyTests
{
    private const string NestedSchema = """
        definition user {}

        definition group {
            relation member: user | group#member
        }

        definition document {
            relation viewer: user | group#member
            relation editor: user
            relation banned: user

            permission view = viewer + editor
            permission edit = editor & viewer
        }
        """;

    private const string WildcardSchema = """
        definition user {}

        definition group {
            relation member: user:* | user | group#member
        }

        definition document {
            relation viewer: group#member
            permission view = viewer
        }
        """;

    private const string CaveatSchema = """
        caveat over_age(age int, min_age int) { age >= min_age }

        definition user {}

        definition group {
            relation member: user | group#member
        }

        definition document {
            relation viewer: user | group#member with over_age
            permission view = viewer
        }
        """;

    private static ObjectAndRelation Onr(string type, string id, string relation = CoreConstants.Ellipsis) =>
        new(type, id, relation);

    private static Relationship Rel(string rt, string rid, string rel, ObjectAndRelation subject) =>
        Relationship.Create(new ObjectAndRelation(rt, rid, rel), subject);

    private static Relationship Caveated(string rt, string rid, string rel, ObjectAndRelation subject, string caveat,
        IReadOnlyDictionary<string, object?>? ctx = null) =>
        Relationship.Create(new ObjectAndRelation(rt, rid, rel), subject, new ContextualizedCaveat(caveat, ctx));

    /// <summary>
    /// Asserts <see cref="MembershipIndex.TryCoveredResources"/> returns identical output from both indexes
    /// for the given (subject, target) shape — the equivalence the fold invariant promises.
    /// </summary>
    private static void AssertSameCandidates(
        MembershipIndex a, MembershipIndex b,
        string subjectType, string subjectId, string subjectRelation, string resourceType, string permission)
    {
        var okA = a.TryCoveredResources(subjectType, subjectId, subjectRelation, resourceType, permission, out var idsA);
        var okB = b.TryCoveredResources(subjectType, subjectId, subjectRelation, resourceType, permission, out var idsB);
        Assert.Equal(okB, okA);
        Assert.Equal(idsB, idsA);
    }

    [Fact]
    public async Task NestedGroupChain_ApplyAcrossCreatesAndDeletes_EqualsFreshBuildAtR2()
    {
        var compiled = SchemaCompiler.CompileSchema(NestedSchema);
        var store = new ReferenceDatastore();

        var r1 = await store.ReadWriteTx(tx => tx.WriteRelationships(new List<RelationshipUpdate>
        {
            new(Rel("group", "g1", "member", Onr("user", "alice")), UpdateOperation.Create),
            new(Rel("document", "d1", "viewer", Onr("group", "g1", "member")), UpdateOperation.Create),
            new(Rel("group", "g1", "member", Onr("user", "carol")), UpdateOperation.Create),
        }));

        var index1 = await MembershipIndex.Build(compiled.Namespaces, store.SnapshotReader(r1), "hash");

        // (R1..R2]: extend the chain (g2 -> g1), add a second document reached only through the new edge,
        // and delete carol's original membership entirely (a delete-heavy shape).
        var deltas = new List<RelationshipUpdate>
        {
            new(Rel("group", "g2", "member", Onr("group", "g1", "member")), UpdateOperation.Create),
            new(Rel("document", "d2", "viewer", Onr("group", "g2", "member")), UpdateOperation.Create),
            new(Rel("group", "g1", "member", Onr("user", "carol")), UpdateOperation.Delete),
        };
        var r2 = await store.ReadWriteTx(tx => tx.WriteRelationships(deltas));

        var applied = index1.Apply(deltas);
        var fresh = await MembershipIndex.Build(compiled.Namespaces, store.SnapshotReader(r2), "hash");

        AssertSameCandidates(applied, fresh, "user", "alice", CoreConstants.Ellipsis, "document", "view");
        AssertSameCandidates(applied, fresh, "user", "alice", CoreConstants.Ellipsis, "group", "member");
        AssertSameCandidates(applied, fresh, "user", "carol", CoreConstants.Ellipsis, "document", "view");
        AssertSameCandidates(applied, fresh, "user", "carol", CoreConstants.Ellipsis, "group", "member");
        AssertSameCandidates(applied, fresh, "group", "g1", "member", "group", "member");

        // The new edge is genuinely reflected (not just "equivalent to an unrelated fresh build").
        Assert.True(applied.TryCoveredResources("user", "alice", CoreConstants.Ellipsis, "document", "view", out var aliceDocs));
        Assert.Equal(new[] { "d1", "d2" }, aliceDocs);
        Assert.True(applied.TryCoveredResources("user", "carol", CoreConstants.Ellipsis, "document", "view", out var carolDocs));
        Assert.Empty(carolDocs);
    }

    [Fact]
    public async Task WildcardUsersetEdge_AddedAfterR1_ApplyEqualsFreshBuildAtR2()
    {
        var compiled = SchemaCompiler.CompileSchema(WildcardSchema);
        var store = new ReferenceDatastore();

        var r1 = await store.ReadWriteTx(tx => tx.WriteRelationships(new List<RelationshipUpdate>
        {
            new(Rel("group", "everyone", "member", Onr("user", CoreConstants.PublicWildcard)), UpdateOperation.Create),
        }));
        var index1 = await MembershipIndex.Build(compiled.Namespaces, store.SnapshotReader(r1), "hash");

        var deltas = new List<RelationshipUpdate>
        {
            new(Rel("document", "d1", "viewer", Onr("group", "everyone", "member")), UpdateOperation.Create),
        };
        var r2 = await store.ReadWriteTx(tx => tx.WriteRelationships(deltas));

        var applied = index1.Apply(deltas);
        var fresh = await MembershipIndex.Build(compiled.Namespaces, store.SnapshotReader(r2), "hash");

        AssertSameCandidates(applied, fresh, "user", "alice", CoreConstants.Ellipsis, "document", "view");
        Assert.True(applied.TryCoveredResources("user", "alice", CoreConstants.Ellipsis, "document", "view", out var ids));
        Assert.Equal(new[] { "d1" }, ids);
    }

    [Fact]
    public async Task IntersectionPermission_FirstOperandSeededByDelta_ApplyEqualsFreshBuildAtR2()
    {
        var compiled = SchemaCompiler.CompileSchema(NestedSchema);
        var store = new ReferenceDatastore();

        // R1: alice can view d1, but is not yet an editor, so `edit = editor & viewer` is not a candidate.
        var r1 = await store.ReadWriteTx(tx => tx.WriteRelationships(new List<RelationshipUpdate>
        {
            new(Rel("document", "d1", "viewer", Onr("user", "alice")), UpdateOperation.Create),
        }));
        var index1 = await MembershipIndex.Build(compiled.Namespaces, store.SnapshotReader(r1), "hash");

        Assert.True(index1.TryCoveredResources("user", "alice", CoreConstants.Ellipsis, "document", "edit", out var before));
        Assert.Empty(before);

        // (R1..R2]: alice becomes an editor too — the first (positive) operand of the intersection, which
        // alone seeds candidates.
        var deltas = new List<RelationshipUpdate>
        {
            new(Rel("document", "d1", "editor", Onr("user", "alice")), UpdateOperation.Create),
        };
        var r2 = await store.ReadWriteTx(tx => tx.WriteRelationships(deltas));

        var applied = index1.Apply(deltas);
        var fresh = await MembershipIndex.Build(compiled.Namespaces, store.SnapshotReader(r2), "hash");

        AssertSameCandidates(applied, fresh, "user", "alice", CoreConstants.Ellipsis, "document", "edit");
        Assert.True(applied.TryCoveredResources("user", "alice", CoreConstants.Ellipsis, "document", "edit", out var after));
        Assert.Equal(new[] { "d1" }, after);
    }

    [Fact]
    public async Task TouchOnlyCaveatChange_IsANoOp_ApplyEqualsFreshBuildAtR2()
    {
        var compiled = SchemaCompiler.CompileSchema(CaveatSchema);
        var store = new ReferenceDatastore();

        var r1 = await store.ReadWriteTx(tx => tx.WriteRelationships(new List<RelationshipUpdate>
        {
            new(Rel("group", "g1", "member", Onr("user", "alice")), UpdateOperation.Create),
            new(Rel("document", "d1", "viewer", Onr("group", "g1", "member")), UpdateOperation.Create),
        }));
        var index1 = await MembershipIndex.Build(compiled.Namespaces, store.SnapshotReader(r1), "hash");

        // (R1..R2]: re-touch the SAME edge, only adding a caveat — the index deliberately ignores caveats,
        // so this must be a no-op for the fold (and for a fresh build, since the edge is unchanged as a row).
        var deltas = new List<RelationshipUpdate>
        {
            new(Caveated("document", "d1", "viewer", Onr("group", "g1", "member"), "over_age",
                new Dictionary<string, object?> { ["min_age"] = 18 }), UpdateOperation.Touch),
        };
        var r2 = await store.ReadWriteTx(tx => tx.WriteRelationships(deltas));

        var applied = index1.Apply(deltas);
        var fresh = await MembershipIndex.Build(compiled.Namespaces, store.SnapshotReader(r2), "hash");

        AssertSameCandidates(applied, fresh, "user", "alice", CoreConstants.Ellipsis, "document", "view");
        Assert.True(applied.TryCoveredResources("user", "alice", CoreConstants.Ellipsis, "document", "view", out var ids));
        Assert.Equal(new[] { "d1" }, ids);
    }

    [Fact]
    public async Task UpdatesOutsideScanSet_AreNoOps()
    {
        var compiled = SchemaCompiler.CompileSchema(NestedSchema);
        var store = new ReferenceDatastore();

        var r1 = await store.ReadWriteTx(tx => tx.WriteRelationships(new List<RelationshipUpdate>
        {
            new(Rel("document", "d1", "viewer", Onr("user", "alice")), UpdateOperation.Create),
        }));
        var index1 = await MembershipIndex.Build(compiled.Namespaces, store.SnapshotReader(r1), "hash");

        // `document.banned` is not in the scan set of any coverable target in NestedSchema (no permission here
        // routes through it), so this update must not perturb the index at all.
        var deltas = new List<RelationshipUpdate>
        {
            new(Rel("document", "d1", "banned", Onr("user", "mallory")), UpdateOperation.Create),
        };

        var applied = index1.Apply(deltas);

        AssertSameCandidates(index1, applied, "user", "alice", CoreConstants.Ellipsis, "document", "view");
        AssertSameCandidates(index1, applied, "user", "mallory", CoreConstants.Ellipsis, "document", "view");
    }

    [Fact]
    public async Task DeleteOfNonexistentEdge_IsANoOp()
    {
        var compiled = SchemaCompiler.CompileSchema(NestedSchema);
        var store = new ReferenceDatastore();

        var r1 = await store.ReadWriteTx(tx => tx.WriteRelationships(new List<RelationshipUpdate>
        {
            new(Rel("document", "d1", "viewer", Onr("user", "alice")), UpdateOperation.Create),
        }));
        var index1 = await MembershipIndex.Build(compiled.Namespaces, store.SnapshotReader(r1), "hash");

        // bob was never added as a viewer of d1 — deleting that (never-existed) edge must be a no-op.
        var deltas = new List<RelationshipUpdate>
        {
            new(Rel("document", "d1", "viewer", Onr("user", "bob")), UpdateOperation.Delete),
        };

        var applied = index1.Apply(deltas);

        AssertSameCandidates(index1, applied, "user", "alice", CoreConstants.Ellipsis, "document", "view");
        AssertSameCandidates(index1, applied, "user", "bob", CoreConstants.Ellipsis, "document", "view");
    }
}
