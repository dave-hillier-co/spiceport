using System.Collections.Immutable;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Schema;

namespace Spiceport.Engine.Tests;

/// <summary>
/// Unit gates for <see cref="MembershipWalk"/>'s one-step primitive (<see cref="MembershipWalk.DirectParents"/>)
/// and in-process driver (<see cref="MembershipWalk.LocalClosure"/>): scan-set/subject filtering, wildcard
/// seeding, the reflexive rule, and — critically — completeness/termination over a genuine DATA cycle (group
/// a member of b, b member of a).
/// </summary>
public class MembershipWalkTests
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

    private const string WildcardSchema = """
        definition user {}

        definition group {
            relation member: user:* | user | group#member
        }
        """;

    private static ObjectAndRelation Onr(string type, string id, string relation = CoreConstants.Ellipsis) =>
        new(type, id, relation);

    private static Relationship Rel(string rt, string rid, string rel, ObjectAndRelation subject) =>
        Relationship.Create(new ObjectAndRelation(rt, rid, rel), subject);

    private static async Task<(MembershipCoverage Coverage, IDatastoreReader Reader)> Setup(
        string schemaText, params Relationship[] rels)
    {
        var compiled = SchemaCompiler.CompileSchema(schemaText);
        var coverage = MembershipCoverage.Build(compiled.Namespaces);

        var store = new ReferenceDatastore();
        var rev = await store.ReadWriteTx(tx => tx.WriteRelationships(
            rels.Select(r => new RelationshipUpdate(r, UpdateOperation.Create)).ToList()));
        var reader = store.SnapshotReader(rev);

        return (coverage, reader);
    }

    [Fact]
    public async Task DirectParents_ReturnsOnlyScanSetRelations()
    {
        var (coverage, reader) = await Setup(NestedSchema,
            Rel("group", "g1", "member", Onr("user", "alice")),
            Rel("document", "d1", "editor", Onr("user", "alice"))); // "editor" is a scanned relation too (view's yield)

        var parents = await MembershipWalk.DirectParents(
            reader, coverage, new MembershipWalk.SubjectKey("user", "alice", CoreConstants.Ellipsis));

        Assert.Contains(parents, p => p is { Type: "group", Id: "g1", Relation: "member" });
        Assert.Contains(parents, p => p is { Type: "document", Id: "d1", Relation: "editor" });
    }

    [Fact]
    public async Task DirectParents_FiltersBySubjectRelation_ExactMatch()
    {
        // alice is both a direct user AND (via a different row) a userset subject id coincidentally shaped
        // like a group's own relation — DirectParents must key strictly on (type, id, relation).
        var (coverage, reader) = await Setup(NestedSchema,
            Rel("group", "g1", "member", Onr("user", "alice", CoreConstants.Ellipsis)),
            Rel("group", "g2", "member", Onr("group", "alice", "member")));

        var asUser = await MembershipWalk.DirectParents(
            reader, coverage, new MembershipWalk.SubjectKey("user", "alice", CoreConstants.Ellipsis));
        Assert.Single(asUser);
        Assert.Equal("g1", asUser[0].Id);

        var asGroupMember = await MembershipWalk.DirectParents(
            reader, coverage, new MembershipWalk.SubjectKey("group", "alice", "member"));
        Assert.Single(asGroupMember);
        Assert.Equal("g2", asGroupMember[0].Id);
    }

    [Fact]
    public async Task LocalClosure_FollowsWildcardEdge()
    {
        var (coverage, reader) = await Setup(WildcardSchema,
            Rel("group", "everyone", "member", Onr("user", CoreConstants.PublicWildcard)));

        var nodes = await MembershipWalk.LocalClosure(
            reader, coverage, new MembershipWalk.SubjectKey("user", "alice", CoreConstants.Ellipsis));

        Assert.Contains(nodes, n => n is { Type: "group", Id: "everyone", Relation: "member" });
    }

    [Fact]
    public async Task LocalClosure_ReflexiveRule_SubjectSelfMembership_IsAppliedByCaller()
    {
        // LocalClosure itself walks edges only; the reflexive self-membership candidate is added by
        // MembershipWalk.ToCoveredCandidates (mirrored by ReverseOpsSupport.AcquireCoveredCandidates), not
        // by the walk itself. Assert the composed contract here.
        var (coverage, reader) = await Setup(NestedSchema);
        Assert.True(coverage.TryGetYields("group", "member", out var yields));

        var nodes = await MembershipWalk.LocalClosure(
            reader, coverage, new MembershipWalk.SubjectKey("group", "g1", "member"));
        var candidates = MembershipWalk.ToCoveredCandidates(nodes, yields, "group", "group", "g1");

        Assert.Contains("g1", candidates); // reflexive: a group#member userset is a member of itself
    }

    [Fact]
    public async Task LocalClosure_TerminatesAndIsComplete_OverADataCycle()
    {
        // group a member of b, b member of a: a genuine cycle in the stored data.
        var (coverage, reader) = await Setup(NestedSchema,
            Rel("group", "a", "member", Onr("group", "b", "member")),
            Rel("group", "b", "member", Onr("group", "a", "member")),
            Rel("document", "d1", "viewer", Onr("group", "a", "member")));

        var task = MembershipWalk.LocalClosure(
            reader, coverage, new MembershipWalk.SubjectKey("group", "a", "member"));
        var completed = await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(5)));
        Assert.Same(task, completed); // terminates rather than looping forever

        var nodes = await task;
        // Complete: both directions of the cycle are captured, and the reachable document is found too.
        Assert.Contains(nodes, n => n is { Type: "group", Id: "a", Relation: "member" });
        Assert.Contains(nodes, n => n is { Type: "group", Id: "b", Relation: "member" });
        Assert.Contains(nodes, n => n is { Type: "document", Id: "d1", Relation: "viewer" });
    }

    [Fact]
    public async Task LocalClosure_NoParents_ReturnsEmpty()
    {
        var (coverage, reader) = await Setup(NestedSchema);
        var nodes = await MembershipWalk.LocalClosure(
            reader, coverage, new MembershipWalk.SubjectKey("user", "nobody", CoreConstants.Ellipsis));
        Assert.Empty(nodes);
    }

    [Fact]
    public void ToCoveredCandidates_FiltersByTypeAndYieldRelation_SortedDistinct()
    {
        var nodes = new[]
        {
            new MembershipWalk.ResourceNode("document", "d2", "viewer"),
            new MembershipWalk.ResourceNode("document", "d1", "viewer"),
            new MembershipWalk.ResourceNode("document", "d1", "viewer"), // duplicate
            new MembershipWalk.ResourceNode("document", "d3", "editor"), // not a yield relation
            new MembershipWalk.ResourceNode("group", "g1", "member"),    // wrong type
        };
        var yields = ImmutableHashSet.Create("viewer");

        var result = MembershipWalk.ToCoveredCandidates(nodes, yields, "document", "user", "alice");

        Assert.Equal(new[] { "d1", "d2" }, result);
    }
}
