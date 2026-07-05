using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Schema;

namespace Spiceport.Engine.Tests;

public class ExpandEngineTests
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

    private static ExpandEngine BuildEngine(string schemaText) =>
        new(SchemaCompiler.Compile(schemaText));

    private static CheckEngine BuildCheckEngine(string schemaText) =>
        new(SchemaCompiler.Compile(schemaText));

    private static async Task<(ReferenceDatastore Store, IRevision Rev)> Seed(params Relationship[] rels)
    {
        var store = new ReferenceDatastore();
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

    // Flattens the tree into the set of effective concrete subject ONRs of the given subject type,
    // interpreting union/intersection/exclusion and recursing through computed/arrow children.
    // Caveats are treated as "present" (unconditional) for set-membership purposes here, matching
    // the unconditional corpus used in these tests.
    private static ISet<string> Flatten(PermissionTreeNode node, string subjectType, string subjectRelation)
    {
        switch (node)
        {
            case PermissionTreeNode.Leaf leaf:
            {
                var set = new HashSet<string>();
                foreach (var ds in leaf.Subjects)
                {
                    var s = ds.Subject;
                    if (s.ObjectType == subjectType && s.Relation == subjectRelation && !s.IsPublicWildcard)
                        set.Add(s.ObjectId);
                }
                return set;
            }

            case PermissionTreeNode.SetOp op:
            {
                var sets = op.Children.Select(c => Flatten(c, subjectType, subjectRelation)).ToList();
                if (sets.Count == 0)
                    return new HashSet<string>();

                return op.Operation switch
                {
                    SetOperationType.Union => sets.Aggregate((a, b) => { a.UnionWith(b); return a; }),
                    SetOperationType.Intersection => sets.Aggregate((a, b) => { a.IntersectWith(b); return a; }),
                    SetOperationType.Exclusion => Exclude(sets),
                    _ => new HashSet<string>(),
                };
            }

            default:
                return new HashSet<string>();
        }
    }

    private static ISet<string> Exclude(List<ISet<string>> sets)
    {
        var result = new HashSet<string>(sets[0]);
        for (var i = 1; i < sets.Count; i++)
            result.ExceptWith(sets[i]);
        return result;
    }

    [Fact]
    public async Task Direct_Shallow_ReturnsLeafWithWrittenSubjects()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "viewer", Onr("user", "alice")),
            Tuple("document", "doc1", "viewer", Onr("user", "bob")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        var tree = await engine.ExpandPermissionTree(reader, Onr("document", "doc1", "viewer"));

        var leaf = Assert.IsType<PermissionTreeNode.Leaf>(tree);
        Assert.Equal(Onr("document", "doc1", "viewer"), leaf.Expanded);
        var ids = leaf.Subjects.Select(s => s.Subject.ObjectId).OrderBy(x => x).ToArray();
        Assert.Equal(["alice", "bob"], ids);
    }

    [Fact]
    public async Task Union_ExpandsAsUnionSetOp()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "viewer", Onr("user", "alice")),
            Tuple("document", "doc1", "editor", Onr("user", "bob")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        // view = viewer + editor
        var tree = await engine.ExpandPermissionTree(reader, Onr("document", "doc1", "view"));

        var op = Assert.IsType<PermissionTreeNode.SetOp>(tree);
        Assert.Equal(SetOperationType.Union, op.Operation);
        Assert.Equal(2, op.Children.Count);
        var flat = Flatten(tree, "user", CoreConstants.Ellipsis).OrderBy(x => x).ToArray();
        Assert.Equal(["alice", "bob"], flat);
    }

    [Fact]
    public async Task Intersection_ExpandsAsIntersectionSetOp()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "editor", Onr("user", "alice")),
            Tuple("document", "doc1", "viewer", Onr("user", "alice")),
            Tuple("document", "doc1", "editor", Onr("user", "bob")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        // edit_only = editor & viewer ; only alice is in both.
        var tree = await engine.ExpandPermissionTree(reader, Onr("document", "doc1", "edit_only"));

        var op = Assert.IsType<PermissionTreeNode.SetOp>(tree);
        Assert.Equal(SetOperationType.Intersection, op.Operation);
        var flat = Flatten(tree, "user", CoreConstants.Ellipsis);
        Assert.Equal(["alice"], flat.OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task Exclusion_ExpandsAsExclusionSetOp()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "viewer", Onr("user", "alice")),
            Tuple("document", "doc1", "viewer", Onr("user", "bob")),
            Tuple("document", "doc1", "banned", Onr("user", "bob")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        // allowed_view = viewer - banned ; bob is banned.
        var tree = await engine.ExpandPermissionTree(reader, Onr("document", "doc1", "allowed_view"));

        var op = Assert.IsType<PermissionTreeNode.SetOp>(tree);
        Assert.Equal(SetOperationType.Exclusion, op.Operation);
        var flat = Flatten(tree, "user", CoreConstants.Ellipsis);
        Assert.Equal(["alice"], flat.OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task Arrow_ExpandsThroughParent()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "parent", Onr("document", "folderDoc")),
            Tuple("document", "folderDoc", "viewer", Onr("user", "alice")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        // inherited_view = viewer + parent->view ; alice reaches doc1 only via the parent arrow.
        var tree = await engine.ExpandPermissionTree(reader, Onr("document", "doc1", "inherited_view"), ExpandMode.Recursive);

        var op = Assert.IsType<PermissionTreeNode.SetOp>(tree);
        Assert.Equal(SetOperationType.Union, op.Operation);
        var flat = Flatten(tree, "user", CoreConstants.Ellipsis);
        Assert.Contains("alice", flat);
    }

    [Fact]
    public async Task Wildcard_CarriedVerbatimInLeaf()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "viewer", Onr("user", CoreConstants.PublicWildcard)));
        var engine = BuildEngine(WildcardSchema);
        var reader = store.SnapshotReader(rev);

        var tree = await engine.ExpandPermissionTree(reader, Onr("document", "doc1", "viewer"));

        var leaf = Assert.IsType<PermissionTreeNode.Leaf>(tree);
        var subject = Assert.Single(leaf.Subjects);
        Assert.True(subject.Subject.IsPublicWildcard);
        Assert.Equal(CoreConstants.PublicWildcard, subject.Subject.ObjectId);
        Assert.Equal("user", subject.Subject.ObjectType);
    }

    [Fact]
    public async Task Recursive_ExpandsNonTerminalUserset()
    {
        var (store, rev) = await Seed(
            Tuple("group", "eng", "member", Onr("user", "alice")),
            Tuple("document", "doc1", "viewer", Onr("group", "eng", "member")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        // Shallow keeps the group#member userset verbatim as a leaf subject.
        var shallow = await engine.ExpandPermissionTree(reader, Onr("document", "doc1", "viewer"), ExpandMode.Shallow);
        var shallowLeaf = Assert.IsType<PermissionTreeNode.Leaf>(shallow);
        Assert.Equal("group", Assert.Single(shallowLeaf.Subjects).Subject.ObjectType);

        // Recursive expands group:eng#member down to its concrete user members.
        var recursive = await engine.ExpandPermissionTree(reader, Onr("document", "doc1", "viewer"), ExpandMode.Recursive);
        var flat = Flatten(recursive, "user", CoreConstants.Ellipsis);
        Assert.Contains("alice", flat);
    }

    [Fact]
    public async Task CyclicSchema_Terminates()
    {
        var (store, rev) = await Seed(
            Tuple("group", "a", "member", Onr("group", "b", "member")),
            Tuple("group", "b", "member", Onr("group", "a", "member")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        // Must terminate despite the a<->b cycle.
        var tree = await engine.ExpandPermissionTree(reader, Onr("group", "a", "member"), ExpandMode.Recursive);
        var flat = Flatten(tree, "user", CoreConstants.Ellipsis);
        Assert.Empty(flat);
    }

    [Theory]
    [InlineData("view")]
    [InlineData("edit_only")]
    [InlineData("allowed_view")]
    public async Task FlattenedTree_AgreesWithCheckOracle(string permission)
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "viewer", Onr("user", "alice")),
            Tuple("document", "doc1", "viewer", Onr("user", "bob")),
            Tuple("document", "doc1", "editor", Onr("user", "alice")),
            Tuple("document", "doc1", "editor", Onr("user", "carol")),
            Tuple("document", "doc1", "banned", Onr("user", "bob")));
        var expand = BuildEngine(Schema);
        var check = BuildCheckEngine(Schema);
        var reader = store.SnapshotReader(rev);

        var tree = await expand.ExpandPermissionTree(reader, Onr("document", "doc1", permission), ExpandMode.Recursive);
        var flat = Flatten(tree, "user", CoreConstants.Ellipsis);

        // For the full universe of user ids, the flattened tree membership must equal Check.
        string[] universe = ["alice", "bob", "carol", "dave"];
        foreach (var id in universe)
        {
            var verdict = (await check.Check(reader, "document", "doc1", permission, Onr("user", id))).Verdict;
            var expectedMember = verdict == Membership.Member;
            Assert.Equal(expectedMember, flat.Contains(id));
        }
    }
}
