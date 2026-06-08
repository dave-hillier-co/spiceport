using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Datastore.Memory;
using Spiceport.Engine;
using Spiceport.Schema;

namespace Spiceport.Engine.Tests;

public class CheckEngineTests
{
    private const string Schema = """
        definition user {}

        definition group {
            relation member: user | group#member
        }

        definition document {
            relation viewer: user | group#member
            relation editor: user
            relation banned: user
            relation parent: document

            permission view = viewer + editor
            permission edit_only = editor & viewer
            permission allowed_view = viewer - banned
            permission inherited_view = viewer + parent->view
        }
        """;

    private const string WildcardSchema = """
        definition user {}

        definition document {
            relation viewer: user | user:*
            permission view = viewer
        }
        """;

    private static CheckEngine BuildEngine(string schemaText) =>
        new(SchemaCompiler.Compile(schemaText));

    private static CheckEngine BuildEngine(string schemaText, int maxDepth) =>
        new(SchemaCompiler.Compile(schemaText), maxDepth);

    private static async Task<(InMemoryDatastore Store, IRevision Rev)> Seed(params Relationship[] rels)
    {
        var store = new InMemoryDatastore();
        var rev = await store.ReadWriteTx(async tx =>
        {
            var updates = rels.Select(r => new RelationshipUpdate(r, UpdateOperation.Create)).ToList();
            await tx.WriteRelationships(updates);
        });
        return (store, rev);
    }

    private static ObjectAndRelation Onr(string type, string id, string relation = CoreConstants.Ellipsis) =>
        new(type, id, relation);

    private static Relationship Tuple(string resType, string resId, string resRel, ObjectAndRelation subject) =>
        Relationship.Create(new ObjectAndRelation(resType, resId, resRel), subject);

    [Fact]
    public async Task DirectTuple_Member()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "viewer", Onr("user", "alice")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        var result = await engine.Check(reader, "document", "doc1", "view", Onr("user", "alice"));

        Assert.Equal(Membership.Member, result.Verdict);
    }

    [Fact]
    public async Task DirectTuple_NotMember()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "viewer", Onr("user", "alice")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        var result = await engine.Check(reader, "document", "doc1", "view", Onr("user", "bob"));

        Assert.Equal(Membership.NotMember, result.Verdict);
    }

    [Fact]
    public async Task Union_EitherOperandGrants()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "editor", Onr("user", "alice")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        // view = viewer + editor ; alice is only editor.
        var result = await engine.Check(reader, "document", "doc1", "view", Onr("user", "alice"));

        Assert.Equal(Membership.Member, result.Verdict);
    }

    [Fact]
    public async Task Intersection_RequiresBoth()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "editor", Onr("user", "alice")),
            Tuple("document", "doc1", "viewer", Onr("user", "alice")),
            Tuple("document", "doc1", "editor", Onr("user", "bob")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        // edit_only = editor & viewer.
        var alice = await engine.Check(reader, "document", "doc1", "edit_only", Onr("user", "alice"));
        var bob = await engine.Check(reader, "document", "doc1", "edit_only", Onr("user", "bob"));

        Assert.Equal(Membership.Member, alice.Verdict);
        Assert.Equal(Membership.NotMember, bob.Verdict);
    }

    [Fact]
    public async Task Exclusion_SubtractsBanned()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "viewer", Onr("user", "alice")),
            Tuple("document", "doc1", "viewer", Onr("user", "bob")),
            Tuple("document", "doc1", "banned", Onr("user", "bob")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        // allowed_view = viewer - banned.
        var alice = await engine.Check(reader, "document", "doc1", "allowed_view", Onr("user", "alice"));
        var bob = await engine.Check(reader, "document", "doc1", "allowed_view", Onr("user", "bob"));

        Assert.Equal(Membership.Member, alice.Verdict);
        Assert.Equal(Membership.NotMember, bob.Verdict);
    }

    [Fact]
    public async Task SubjectRelationWalking_ThroughGroupMembership()
    {
        var (store, rev) = await Seed(
            // alice is a member of group:eng
            Tuple("group", "eng", "member", Onr("user", "alice")),
            // group:eng#member is a viewer of doc1
            Tuple("document", "doc1", "viewer", Onr("group", "eng", "member")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        var result = await engine.Check(reader, "document", "doc1", "view", Onr("user", "alice"));

        Assert.Equal(Membership.Member, result.Verdict);
    }

    [Fact]
    public async Task NestedSubjectRelationWalking_GroupInGroup()
    {
        var (store, rev) = await Seed(
            // alice in group:inner
            Tuple("group", "inner", "member", Onr("user", "alice")),
            // group:inner#member in group:outer
            Tuple("group", "outer", "member", Onr("group", "inner", "member")),
            // group:outer#member views doc1
            Tuple("document", "doc1", "viewer", Onr("group", "outer", "member")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        var result = await engine.Check(reader, "document", "doc1", "view", Onr("user", "alice"));

        Assert.Equal(Membership.Member, result.Verdict);
    }

    [Fact]
    public async Task Arrow_InheritedViewThroughParent()
    {
        var (store, rev) = await Seed(
            // doc1's parent is folderDoc
            Tuple("document", "doc1", "parent", Onr("document", "folderDoc")),
            // alice views the parent
            Tuple("document", "folderDoc", "viewer", Onr("user", "alice")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        // inherited_view = viewer + parent->view ; alice is not a direct viewer of doc1.
        var result = await engine.Check(reader, "document", "doc1", "inherited_view", Onr("user", "alice"));

        Assert.Equal(Membership.Member, result.Verdict);
    }

    [Fact]
    public async Task Arrow_NoParentTuple_NotMember()
    {
        var (store, rev) = await Seed(
            Tuple("document", "folderDoc", "viewer", Onr("user", "alice")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        var result = await engine.Check(reader, "document", "doc1", "inherited_view", Onr("user", "alice"));

        Assert.Equal(Membership.NotMember, result.Verdict);
    }

    [Fact]
    public async Task Wildcard_PublicAccessGrantsAnyUser()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "viewer", Onr("user", CoreConstants.PublicWildcard)));
        var engine = BuildEngine(WildcardSchema);
        var reader = store.SnapshotReader(rev);

        var alice = await engine.Check(reader, "document", "doc1", "view", Onr("user", "alice"));
        var bob = await engine.Check(reader, "document", "doc1", "view", Onr("user", "bob"));

        Assert.Equal(Membership.Member, alice.Verdict);
        Assert.Equal(Membership.Member, bob.Verdict);
    }

    [Fact]
    public async Task Wildcard_DoesNotGrantOtherSubjectType()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "viewer", Onr("user", CoreConstants.PublicWildcard)));
        var engine = BuildEngine(WildcardSchema);
        var reader = store.SnapshotReader(rev);

        // A group subject must not be granted by a user:* wildcard.
        var result = await engine.Check(reader, "document", "doc1", "view", Onr("group", "eng", "member"));

        Assert.Equal(Membership.NotMember, result.Verdict);
    }

    [Fact]
    public async Task SameKeyCycle_ThrowsMaxDepthExceeded_NotConfidentNotMember()
    {
        // group:a#member -> group:b#member -> group:a#member (a same-key cycle), nobody real inside.
        // Correctness now rests SOLELY on the depth budget (SpiceDB's dispatch.CheckDepth): a genuine
        // cycle consumes depth until it errors, rather than the bloom silently cutting to a confident
        // (and wrong) NotMember. This matches SpiceDB, which raises MaxDepthExceededError here. The check
        // must TERMINATE (no infinite loop) and surface an error, never a definitive verdict.
        var (store, rev) = await Seed(
            Tuple("group", "a", "member", Onr("group", "b", "member")),
            Tuple("group", "b", "member", Onr("group", "a", "member")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        await Assert.ThrowsAsync<MaxDepthExceededException>(
            () => engine.Check(reader, "group", "a", "member", Onr("user", "alice")));
    }

    [Fact]
    public async Task DeepLinearChain_WithinBudget_ResolvesMember_NoFalseTermination()
    {
        // A linear chain of 49 hops (under maxDepth 50) must resolve Member: removing the bloom cut must
        // not introduce a false termination on a legitimately deep, acyclic graph.
        var rels = new List<Relationship>();
        for (var i = 0; i < 48; i++)
            rels.Add(Tuple("group", $"g{i}", "member", Onr("group", $"g{i + 1}", "member")));
        rels.Add(Tuple("group", "g48", "member", Onr("user", "x")));

        var (store, rev) = await Seed([.. rels]);
        var engine = BuildEngine(Schema, maxDepth: 50);
        var reader = store.SnapshotReader(rev);

        var result = await engine.Check(reader, "group", "g0", "member", Onr("user", "x"));
        Assert.Equal(Membership.Member, result.Verdict);
    }

    [Fact]
    public async Task DeepLinearChain_ExceedingBudget_ThrowsMaxDepthExceeded()
    {
        // A chain deeper than maxDepth(50) must error, not silently deny — the deepest production-realistic
        // case the bloom false-positive class used to corrupt.
        var rels = new List<Relationship>();
        for (var i = 0; i < 60; i++)
            rels.Add(Tuple("group", $"g{i}", "member", Onr("group", $"g{i + 1}", "member")));
        rels.Add(Tuple("group", "g60", "member", Onr("user", "x")));

        var (store, rev) = await Seed([.. rels]);
        var engine = BuildEngine(Schema, maxDepth: 50);
        var reader = store.SnapshotReader(rev);

        await Assert.ThrowsAsync<MaxDepthExceededException>(
            () => engine.Check(reader, "group", "g0", "member", Onr("user", "x")));
    }

    [Fact]
    public async Task DepthExhaustion_ThrowsMaxDepthExceeded_NotSilentNotMember()
    {
        // A linear chain group:g0#member -> group:g1#member -> ... -> group:g5#member <- user:alice.
        // With a max depth small enough to run out before reaching alice, SpiceDB raises a max-depth
        // error rather than returning a confident "not a member" (which would mask the misconfiguration
        // AND let alice — a genuine member — be reported as denied).
        var rels = new List<Relationship>();
        for (var i = 0; i < 5; i++)
            rels.Add(Tuple("group", $"g{i}", "member", Onr("group", $"g{i + 1}", "member")));
        rels.Add(Tuple("group", "g5", "member", Onr("user", "alice")));

        var (store, rev) = await Seed([.. rels]);
        var engine = BuildEngine(Schema, maxDepth: 3);
        var reader = store.SnapshotReader(rev);

        await Assert.ThrowsAsync<MaxDepthExceededException>(
            () => engine.Check(reader, "group", "g0", "member", Onr("user", "alice")));
    }

    [Fact]
    public async Task DepthWithinBudget_StillResolves()
    {
        // The same chain resolves to Member when the depth budget is ample, proving the error above is
        // depth exhaustion and not a structural failure.
        var rels = new List<Relationship>();
        for (var i = 0; i < 5; i++)
            rels.Add(Tuple("group", $"g{i}", "member", Onr("group", $"g{i + 1}", "member")));
        rels.Add(Tuple("group", "g5", "member", Onr("user", "alice")));

        var (store, rev) = await Seed([.. rels]);
        var engine = BuildEngine(Schema, maxDepth: 50);
        var reader = store.SnapshotReader(rev);

        var result = await engine.Check(reader, "group", "g0", "member", Onr("user", "alice"));
        Assert.Equal(Membership.Member, result.Verdict);
    }

    [Fact]
    public async Task BaseRelationCheck_DirectMember()
    {
        var (store, rev) = await Seed(
            Tuple("group", "eng", "member", Onr("user", "alice")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        // Check the base relation directly (not a permission).
        var result = await engine.Check(reader, "group", "eng", "member", Onr("user", "alice"));

        Assert.Equal(Membership.Member, result.Verdict);
    }
}
