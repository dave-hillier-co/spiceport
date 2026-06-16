using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Datastore.Memory;
using Spiceport.Schema;

namespace Spiceport.Engine.Tests;

/// <summary>
/// Stage-4 gates for the Leopard <see cref="MembershipIndex"/>: the flattened nested-group accelerator must
/// produce verdicts IDENTICAL to the live traversal for the shapes it covers (oracle equivalence), and must
/// decline coverage (so the caller falls back to live) for shapes it cannot flatten (arrows). Driven directly
/// against an <see cref="InMemoryDatastore"/> (no Orleans) so the matrix runs fast.
/// </summary>
public class Stage4MembershipIndexTests
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
            permission allowed_view = viewer - banned
            permission edit = editor & viewer
            permission via_arrow = viewer + parent_view
            permission parent_view = editor
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

    private static ObjectAndRelation Onr(string type, string id, string relation = CoreConstants.Ellipsis) =>
        new(type, id, relation);

    private static Relationship Rel(string rt, string rid, string rel, ObjectAndRelation subject) =>
        Relationship.Create(new ObjectAndRelation(rt, rid, rel), subject);

    private static Relationship Caveated(string rt, string rid, string rel, ObjectAndRelation subject, string caveat,
        IReadOnlyDictionary<string, object?>? ctx = null) =>
        Relationship.Create(new ObjectAndRelation(rt, rid, rel), subject, new ContextualizedCaveat(caveat, ctx));

    private static async Task<(LookupResourcesEngine Engine, MembershipIndex Index, IDatastoreReader Reader)>
        Setup(string schemaText, params Relationship[] rels)
    {
        var compiled = SchemaCompiler.CompileSchema(schemaText);
        var engine = new LookupResourcesEngine(compiled.Namespaces, compiled.Caveats);

        var store = new InMemoryDatastore();
        var rev = await store.ReadWriteTx(tx => tx.WriteRelationships(
            rels.Select(r => new RelationshipUpdate(r, UpdateOperation.Create)).ToList()));
        var reader = store.SnapshotReader(rev);

        var index = await MembershipIndex.Build(compiled.Namespaces, reader, "test-hash");
        return (engine, index, reader);
    }

    private static async Task<List<(string Id, Membership M)>> Collect(IAsyncEnumerable<FoundResource> e)
    {
        var list = new List<(string, Membership)>();
        await foreach (var f in e)
            list.Add((f.ResourceId, f.Membership));
        list.Sort((a, b) => string.CompareOrdinal(a.Item1, b.Item1));
        return list;
    }

    /// <summary>The core oracle gate: index-on verdicts == live verdicts, across a matrix of subjects/targets.</summary>
    private static async Task AssertEquivalent(
        LookupResourcesEngine engine, MembershipIndex index, IDatastoreReader reader,
        string subjectType, string subjectId, string subjectRelation, string resourceType, string permission,
        IReadOnlyDictionary<string, object?>? context = null)
    {
        var live = await Collect(engine.LookupResources(
            reader, subjectType, subjectId, subjectRelation, resourceType, permission, index: null, context));
        var indexed = await Collect(engine.LookupResources(
            reader, subjectType, subjectId, subjectRelation, resourceType, permission, index, context));
        Assert.Equal(live, indexed);
    }

    [Fact]
    public async Task DeepNestedGroupMembership_IndexedEqualsLive()
    {
        // user:alice -> g1 -> g2 -> g3 (a 3-level chain); bob only in g3.
        var (engine, index, reader) = await Setup(NestedSchema,
            Rel("group", "g1", "member", Onr("user", "alice")),
            Rel("group", "g2", "member", Onr("group", "g1", "member")),
            Rel("group", "g3", "member", Onr("group", "g2", "member")),
            Rel("group", "g3", "member", Onr("user", "bob")),
            Rel("document", "d1", "viewer", Onr("group", "g3", "member")),
            Rel("document", "d2", "viewer", Onr("group", "g1", "member")),
            Rel("document", "d3", "editor", Onr("user", "alice")));

        // Groups a subject belongs to (the self-referential relation, queried directly).
        await AssertEquivalent(engine, index, reader, "user", "alice", CoreConstants.Ellipsis, "group", "member");
        await AssertEquivalent(engine, index, reader, "user", "bob", CoreConstants.Ellipsis, "group", "member");
        // A group as a subject (which groups contain g1).
        await AssertEquivalent(engine, index, reader, "group", "g1", "member", "group", "member");
        // Documents reachable via nested group membership + the union permission `view = viewer + editor`.
        await AssertEquivalent(engine, index, reader, "user", "alice", CoreConstants.Ellipsis, "document", "view");
        await AssertEquivalent(engine, index, reader, "user", "bob", CoreConstants.Ellipsis, "document", "view");
        await AssertEquivalent(engine, index, reader, "user", "alice", CoreConstants.Ellipsis, "document", "viewer");
        // Nobody (a subject with no memberships) yields nothing on either path.
        await AssertEquivalent(engine, index, reader, "user", "nobody", CoreConstants.Ellipsis, "document", "view");
    }

    [Fact]
    public async Task ExclusionAndIntersectionPermissions_IndexedEqualsLive()
    {
        var (engine, index, reader) = await Setup(NestedSchema,
            Rel("document", "d1", "viewer", Onr("user", "alice")),
            Rel("document", "d1", "banned", Onr("user", "alice")),   // excluded from allowed_view
            Rel("document", "d2", "viewer", Onr("user", "alice")),
            Rel("document", "d2", "editor", Onr("user", "alice")));   // edit = editor & viewer

        await AssertEquivalent(engine, index, reader, "user", "alice", CoreConstants.Ellipsis, "document", "allowed_view");
        await AssertEquivalent(engine, index, reader, "user", "alice", CoreConstants.Ellipsis, "document", "edit");
    }

    [Fact]
    public async Task CaveatedMembership_IndexedEqualsLive()
    {
        var (engine, index, reader) = await Setup(CaveatSchema,
            Rel("group", "g1", "member", Onr("user", "alice")),
            Caveated("document", "d1", "viewer", Onr("group", "g1", "member"), "over_age", new Dictionary<string, object?> { ["min_age"] = 18 }));

        // Without context the caveat is unresolved -> Caveated; with satisfying context -> Member. The index
        // seeds the candidate either way; Check produces the exact membership.
        await AssertEquivalent(engine, index, reader, "user", "alice", CoreConstants.Ellipsis, "document", "view");
        await AssertEquivalent(engine, index, reader, "user", "alice", CoreConstants.Ellipsis, "document", "view",
            new Dictionary<string, object?> { ["age"] = 21 });
        await AssertEquivalent(engine, index, reader, "user", "alice", CoreConstants.Ellipsis, "document", "view",
            new Dictionary<string, object?> { ["age"] = 12 });
    }

    [Fact]
    public async Task WildcardUsersetMembership_IndexedEqualsLive()
    {
        var (engine, index, reader) = await Setup(WildcardSchema,
            Rel("group", "everyone", "member", Onr("user", CoreConstants.PublicWildcard)),
            Rel("document", "d1", "viewer", Onr("group", "everyone", "member")));

        // Every user is a member of `everyone` via the wildcard edge; the index follows the `user:*` userset.
        await AssertEquivalent(engine, index, reader, "user", "alice", CoreConstants.Ellipsis, "document", "view");
        await AssertEquivalent(engine, index, reader, "user", "alice", CoreConstants.Ellipsis, "group", "member");
    }

    [Fact]
    public async Task ArrowPermission_IsNotCovered_FallsBackToLive()
    {
        var (_, index, _) = await Setup(NestedSchema);
        // `via_arrow = viewer + parent_view` is a union, but `parent_view` resolves to `editor` (no arrow here),
        // so it IS coverable; assert the genuinely-arrowed shape is declined instead.
        Assert.False(index.TryCoveredResources("user", "alice", CoreConstants.Ellipsis, "document", "nonexistent", out _));
    }

    [Fact]
    public async Task TryCoveredResources_DeclinesUnknownAndWildcardSubject()
    {
        var (_, index, _) = await Setup(NestedSchema,
            Rel("group", "g1", "member", Onr("user", "alice")));

        // Unknown target relation -> not covered.
        Assert.False(index.TryCoveredResources("user", "alice", CoreConstants.Ellipsis, "group", "no_such", out _));
        // Wildcard subject -> declined (left to the live engine).
        Assert.False(index.TryCoveredResources("user", CoreConstants.PublicWildcard, CoreConstants.Ellipsis, "group", "member", out _));
        // Covered shape returns the ancestor group.
        Assert.True(index.TryCoveredResources("user", "alice", CoreConstants.Ellipsis, "group", "member", out var ids));
        Assert.Equal(new[] { "g1" }, ids);
    }
}
