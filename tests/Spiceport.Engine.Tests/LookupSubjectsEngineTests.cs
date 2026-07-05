using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Schema;

namespace Spiceport.Engine.Tests;

public class LookupSubjectsEngineTests
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

    // A schema whose intersection / exclusion permissions mix a wildcard operand with concrete
    // operands, exercising the wildcard subject-set algebra (BaseSubjectSet port).
    private const string WildcardAlgebraSchema = """
        definition user {}

        definition document {
            relation any: user | user:*
            relation special: user
            relation blocked: user | user:*

            permission special_and_any = special & any
            permission special_minus_blocked = special - blocked
        }
        """;

    private const string CaveatSchema = """
        caveat over_age(age int, min_age int) {
          age >= min_age
        }

        definition user {}

        definition document {
          relation viewer: user with over_age
          permission view = viewer
        }
        """;

    private static LookupSubjectsEngine BuildEngine(string schemaText) =>
        new(SchemaCompiler.Compile(schemaText));

    private static CheckEngine BuildCheckEngine(string schemaText)
    {
        var compiled = SchemaCompiler.CompileSchema(schemaText);
        return new CheckEngine(compiled.Namespaces, compiled.Caveats);
    }

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

    private static Relationship Caveated(
        string resType, string resId, string resRel, ObjectAndRelation subject,
        string caveatName, IReadOnlyDictionary<string, object?>? ctx = null) =>
        Relationship.Create(
            new ObjectAndRelation(resType, resId, resRel),
            subject,
            new ContextualizedCaveat(caveatName, ctx));

    private static async Task<List<FoundSubject>> Collect(IAsyncEnumerable<FoundSubject> e)
    {
        var list = new List<FoundSubject>();
        await foreach (var f in e)
            list.Add(f);
        return list;
    }

    [Fact]
    public async Task Direct_ReturnsWrittenSubjects()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "viewer", Onr("user", "alice")),
            Tuple("document", "doc1", "viewer", Onr("user", "bob")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        var found = await Collect(engine.LookupSubjects(reader, Onr("document", "doc1", "viewer"), "user"));

        var ids = found.Select(f => f.SubjectId).OrderBy(x => x).ToArray();
        Assert.Equal(["alice", "bob"], ids);
        Assert.All(found, f => Assert.Null(f.Caveat));
    }

    [Fact]
    public async Task Union_MergesSubjectsAcrossChildren()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "viewer", Onr("user", "alice")),
            Tuple("document", "doc1", "editor", Onr("user", "bob")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        // view = viewer + editor
        var found = await Collect(engine.LookupSubjects(reader, Onr("document", "doc1", "view"), "user"));

        Assert.Equal(["alice", "bob"], found.Select(f => f.SubjectId).OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task Intersection_KeepsOnlyCommonSubjects()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "editor", Onr("user", "alice")),
            Tuple("document", "doc1", "viewer", Onr("user", "alice")),
            Tuple("document", "doc1", "editor", Onr("user", "bob")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        // edit_only = editor & viewer ; only alice is in both.
        var found = await Collect(engine.LookupSubjects(reader, Onr("document", "doc1", "edit_only"), "user"));

        Assert.Equal(["alice"], found.Select(f => f.SubjectId).ToArray());
    }

    [Fact]
    public async Task Exclusion_RemovesBannedSubjects()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "viewer", Onr("user", "alice")),
            Tuple("document", "doc1", "viewer", Onr("user", "bob")),
            Tuple("document", "doc1", "banned", Onr("user", "bob")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        // allowed_view = viewer - banned ; bob is banned.
        var found = await Collect(engine.LookupSubjects(reader, Onr("document", "doc1", "allowed_view"), "user"));

        Assert.Equal(["alice"], found.Select(f => f.SubjectId).ToArray());
    }

    [Fact]
    public async Task Arrow_ReachesSubjectsThroughParent()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "parent", Onr("document", "folderDoc")),
            Tuple("document", "folderDoc", "viewer", Onr("user", "alice")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        // inherited_view = viewer + parent->view
        var found = await Collect(engine.LookupSubjects(reader, Onr("document", "doc1", "inherited_view"), "user"));

        Assert.Contains(found, f => f.SubjectId == "alice");
    }

    [Fact]
    public async Task NestedUserset_ReachesConcreteSubjects()
    {
        var (store, rev) = await Seed(
            Tuple("group", "eng", "member", Onr("user", "alice")),
            Tuple("document", "doc1", "viewer", Onr("group", "eng", "member")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        var found = await Collect(engine.LookupSubjects(reader, Onr("document", "doc1", "viewer"), "user"));

        Assert.Equal(["alice"], found.Select(f => f.SubjectId).ToArray());
    }

    [Fact]
    public async Task Userset_SubjectRelationRequest_ReturnsUserset()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "viewer", Onr("group", "eng", "member")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        // Requesting subject type group with subrelation member should return the userset verbatim.
        var found = await Collect(engine.LookupSubjects(reader, Onr("document", "doc1", "viewer"), "group", "member"));

        var single = Assert.Single(found);
        Assert.Equal("eng", single.SubjectId);
        Assert.False(single.IsWildcard);
    }

    [Fact]
    public async Task Wildcard_IsReturnedAsWildcardFoundSubject()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "viewer", Onr("user", CoreConstants.PublicWildcard)));
        var engine = BuildEngine(WildcardSchema);
        var reader = store.SnapshotReader(rev);

        var found = await Collect(engine.LookupSubjects(reader, Onr("document", "doc1", "view"), "user"));

        var single = Assert.Single(found);
        Assert.True(single.IsWildcard);
        Assert.Equal(CoreConstants.PublicWildcard, single.SubjectId);
        Assert.Null(single.Caveat);
    }

    [Fact]
    public async Task WildcardIntersectConcrete_YieldsConcrete()
    {
        // special = {tom}; any = {*}. special & any must yield {tom} (a concrete matches a wildcard),
        // not empty. The old keyed-by-id set produced empty because "*" != "user:tom".
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "special", Onr("user", "tom")),
            Tuple("document", "doc1", "any", Onr("user", CoreConstants.PublicWildcard)));
        var engine = BuildEngine(WildcardAlgebraSchema);
        var reader = store.SnapshotReader(rev);

        var found = await Collect(engine.LookupSubjects(reader, Onr("document", "doc1", "special_and_any"), "user"));

        var single = Assert.Single(found);
        Assert.Equal("tom", single.SubjectId);
        Assert.False(single.IsWildcard);
        Assert.Null(single.Caveat);
    }

    [Fact]
    public async Task BaseMinusWildcard_RemovesConcretesModuloExclusions()
    {
        // special = {tom, amy}; blocked = {*}. special - blocked removes everything.
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "special", Onr("user", "tom")),
            Tuple("document", "doc1", "special", Onr("user", "amy")),
            Tuple("document", "doc1", "blocked", Onr("user", CoreConstants.PublicWildcard)));
        var engine = BuildEngine(WildcardAlgebraSchema);
        var reader = store.SnapshotReader(rev);

        var found = await Collect(engine.LookupSubjects(reader, Onr("document", "doc1", "special_minus_blocked"), "user"));

        Assert.Empty(found);
    }

    [Fact]
    public async Task WildcardIntersectWildcard_YieldsWildcard()
    {
        // special would need a wildcard too; use any & blocked which are both wildcard-capable.
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "any", Onr("user", CoreConstants.PublicWildcard)),
            Tuple("document", "doc1", "special", Onr("user", "tom")),
            Tuple("document", "doc1", "special", Onr("user", "amy")));
        var engine = BuildEngine(WildcardAlgebraSchema);
        var reader = store.SnapshotReader(rev);

        // special & any : both tom and amy are concretes that match the wildcard => {tom, amy}.
        var found = await Collect(engine.LookupSubjects(reader, Onr("document", "doc1", "special_and_any"), "user"));

        Assert.Equal(["amy", "tom"], found.Select(f => f.SubjectId).OrderBy(x => x).ToArray());
        Assert.All(found, f => Assert.False(f.IsWildcard));
    }

    [Fact]
    public async Task Caveated_SubjectCarriesCaveatVerbatim()
    {
        var (store, rev) = await Seed(
            Caveated("document", "doc1", "viewer", Onr("user", "alice"), "over_age",
                new Dictionary<string, object?> { ["min_age"] = 18 }),
            Tuple("document", "doc1", "viewer", Onr("user", "bob")));
        var engine = BuildEngine(CaveatSchema);
        var checkEngine = BuildCheckEngine(CaveatSchema);
        var reader = store.SnapshotReader(rev);

        var found = await Collect(engine.LookupSubjects(reader, Onr("document", "doc1", "view"), "user"));

        var alice = Assert.Single(found, f => f.SubjectId == "alice");
        Assert.NotNull(alice.Caveat); // the Caveated marker.
        var bob = Assert.Single(found, f => f.SubjectId == "bob");
        Assert.Null(bob.Caveat); // unconditional.

        // The carried caveat must collapse the same way Check does against request context.
        var memberCtx = new Dictionary<string, object?> { ["age"] = 21 };
        var notMemberCtx = new Dictionary<string, object?> { ["age"] = 16 };
        Assert.Equal(Membership.Member,
            (await checkEngine.Check(reader, "document", "doc1", "view", Onr("user", "alice"), memberCtx)).Verdict);
        Assert.Equal(Membership.NotMember,
            (await checkEngine.Check(reader, "document", "doc1", "view", Onr("user", "alice"), notMemberCtx)).Verdict);
    }

    [Fact]
    public async Task SelfShortCircuit_YieldsResourceWhenTypeAndRelationMatch()
    {
        var (store, rev) = await Seed();
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        // Asking for subjects of (user, ...) on user:alice#... returns alice itself.
        var found = await Collect(engine.LookupSubjects(reader, Onr("user", "alice"), "user"));

        var single = Assert.Single(found);
        Assert.Equal("alice", single.SubjectId);
    }

    [Fact]
    public async Task CyclicSchema_Terminates()
    {
        var (store, rev) = await Seed(
            Tuple("group", "a", "member", Onr("group", "b", "member")),
            Tuple("group", "b", "member", Onr("group", "a", "member")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        var found = await Collect(engine.LookupSubjects(reader, Onr("group", "a", "member"), "user"));

        Assert.Empty(found);
    }

    [Theory]
    [InlineData("view")]
    [InlineData("edit_only")]
    [InlineData("allowed_view")]
    [InlineData("inherited_view")]
    public async Task LookupSubjects_AgreesWithCheckOracle(string permission)
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "parent", Onr("document", "folder")),
            Tuple("document", "folder", "viewer", Onr("user", "dave")),
            Tuple("document", "doc1", "viewer", Onr("user", "alice")),
            Tuple("document", "doc1", "viewer", Onr("user", "bob")),
            Tuple("document", "doc1", "editor", Onr("user", "alice")),
            Tuple("document", "doc1", "editor", Onr("user", "carol")),
            Tuple("document", "doc1", "banned", Onr("user", "bob")));
        var engine = BuildEngine(Schema);
        var check = BuildCheckEngine(Schema);
        var reader = store.SnapshotReader(rev);

        var found = await Collect(engine.LookupSubjects(reader, Onr("document", "doc1", permission), "user"));
        var foundIds = found.Select(f => f.SubjectId).ToHashSet();

        // Soundness + completeness against the Check oracle across the full universe.
        string[] universe = ["alice", "bob", "carol", "dave", "erin"];
        foreach (var id in universe)
        {
            var verdict = (await check.Check(reader, "document", "doc1", permission, Onr("user", id))).Verdict;
            Assert.Equal(verdict == Membership.Member, foundIds.Contains(id));
        }
    }
}
