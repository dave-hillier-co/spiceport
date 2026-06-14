using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Datastore.Memory;
using Spiceport.Schema;

namespace Spiceport.Engine.Tests;

/// <summary>
/// Locks in the caveat-completeness guarantee that is the in-architecture equivalent of SpiceDB's
/// batched <c>ResultsSetting</c> / <c>REQUIRE_ALL_RESULTS</c> mechanism (issue #3, finding 5).
/// </summary>
/// <remarks>
/// SpiceDB dispatches a <em>batch</em> of resource ids with an "allow single result" optimization that
/// can short-circuit across resources; it forces <c>REQUIRE_ALL_RESULTS</c> whenever an incoming
/// relationship is caveated, so that every caveat is gathered into the final expression
/// (<c>internal/graph/check.go:482-512</c>). The port instead models one (resource, subject) per
/// dispatch and only ever short-circuits a union/arrow on a <em>definite, uncaveated</em> member —
/// never on a caveated one. Because <c>caveatExpr OR definitely-true</c> collapses to true, dropping a
/// caveat at that point cannot change a verdict, while every <em>undetermined</em> branch is
/// accumulated. These tests assert that property directly so a future "any-member" short-circuit
/// cannot silently regress it.
/// </remarks>
public class CaveatCompletenessTests
{
    private const string Schema = """
        caveat over_age(age int, min_age int) {
          age >= min_age
        }

        caveat ip_allowlist(user_ip ipaddress, cidr string) {
          user_ip.in_cidr(cidr)
        }

        definition user {}

        definition group {
          relation member: user with over_age
          relation guest: user with ip_allowlist
          permission access = member + guest
        }

        definition document {
          relation viewer: user with over_age
          relation ip_viewer: user with ip_allowlist
          relation editor: user
          relation team: group

          // Two caveated branches: a complete check must gather BOTH caveats.
          permission union_caveats = viewer + ip_viewer
          // A caveated branch unioned with a definite (uncaveated) branch: the definite member wins.
          permission caveat_or_definite = viewer + editor
          // Arrow into a nested union of two caveated branches.
          permission via_team = team->access
        }
        """;

    private static CheckEngine BuildEngine()
    {
        var compiled = SchemaCompiler.CompileSchema(Schema);
        return new CheckEngine(compiled.Namespaces, compiled.Caveats);
    }

    private static async Task<IDatastoreReader> Seed(params Relationship[] rels)
    {
        var store = new InMemoryDatastore();
        var rev = await store.ReadWriteTx(async tx =>
        {
            var updates = rels.Select(r => new RelationshipUpdate(r, UpdateOperation.Create)).ToList();
            await tx.WriteRelationships(updates);
        });
        return store.SnapshotReader(rev);
    }

    private static ObjectAndRelation Onr(string type, string id, string relation = CoreConstants.Ellipsis) =>
        new(type, id, relation);

    private static Relationship Plain(string resType, string resId, string resRel, ObjectAndRelation subject) =>
        Relationship.Create(new ObjectAndRelation(resType, resId, resRel), subject);

    private static Relationship Caveated(
        string resType, string resId, string resRel, ObjectAndRelation subject,
        string caveatName, IReadOnlyDictionary<string, object?> ctx) =>
        Relationship.Create(
            new ObjectAndRelation(resType, resId, resRel),
            subject,
            new ContextualizedCaveat(caveatName, ctx));

    [Fact]
    public async Task Union_OfTwoCaveatedBranches_GathersEveryCaveat()
    {
        // alice is a caveated viewer (over_age) AND a caveated ip_viewer (ip_allowlist). With no request
        // context both caveats are undetermined, so the union must be Caveated and surface BOTH missing
        // fields. A short-circuit that returned after the first caveated branch would drop "user_ip".
        var reader = await Seed(
            Caveated("document", "doc1", "viewer", Onr("user", "alice"), "over_age",
                new Dictionary<string, object?> { ["min_age"] = 18 }),
            Caveated("document", "doc1", "ip_viewer", Onr("user", "alice"), "ip_allowlist",
                new Dictionary<string, object?> { ["cidr"] = "10.0.0.0/8" }));
        var engine = BuildEngine();

        var result = await engine.Check(reader, "document", "doc1", "union_caveats", Onr("user", "alice"));

        Assert.Equal(Membership.Caveated, result.Verdict);
        Assert.Contains("age", result.MissingExprFields);
        Assert.Contains("user_ip", result.MissingExprFields);
    }

    [Fact]
    public async Task Union_OfTwoCaveatedBranches_OneSatisfied_CollapsesToMember()
    {
        // Same shape, but request context satisfies the over_age branch (age 21 >= 18). The union is then
        // definitely true regardless of the ip_allowlist branch, so the result is a plain Member: the
        // engine accumulated the caveat, then collapsed it with context.
        var reader = await Seed(
            Caveated("document", "doc1", "viewer", Onr("user", "alice"), "over_age",
                new Dictionary<string, object?> { ["min_age"] = 18 }),
            Caveated("document", "doc1", "ip_viewer", Onr("user", "alice"), "ip_allowlist",
                new Dictionary<string, object?> { ["cidr"] = "10.0.0.0/8" }));
        var engine = BuildEngine();

        var result = await engine.Check(
            reader, "document", "doc1", "union_caveats", Onr("user", "alice"),
            caveatContext: new Dictionary<string, object?> { ["age"] = 21 });

        Assert.Equal(Membership.Member, result.Verdict);
    }

    [Fact]
    public async Task Union_CaveatedBranchPlusDefiniteBranch_IsDefiniteMember()
    {
        // alice is a caveated viewer (over_age, no context) AND a plain editor. The editor branch is a
        // definite member, so the union short-circuits to Member. Dropping the viewer caveat here is sound
        // because `caveatExpr OR definitely-true == true`; the verdict is Member, not Caveated.
        var reader = await Seed(
            Caveated("document", "doc1", "viewer", Onr("user", "alice"), "over_age",
                new Dictionary<string, object?> { ["min_age"] = 18 }),
            Plain("document", "doc1", "editor", Onr("user", "alice")));
        var engine = BuildEngine();

        // No "age" supplied: were the viewer caveat NOT dropped, this would be Caveated.
        var result = await engine.Check(reader, "document", "doc1", "caveat_or_definite", Onr("user", "alice"));

        Assert.Equal(Membership.Member, result.Verdict);
        Assert.Empty(result.MissingExprFields);
    }

    [Fact]
    public async Task Arrow_IntoNestedUnion_GathersCaveatsFromEveryTarget()
    {
        // alice reaches `document:doc1#via_team` through `team->access`, where access = member + guest on
        // group:g1, and alice holds both a caveated member (over_age) and a caveated guest (ip_allowlist).
        // The arrow sub-dispatch must gather BOTH caveats: an "any member found" short-circuit across the
        // nested union would drop one.
        var reader = await Seed(
            Plain("document", "doc1", "team", Onr("group", "g1")),
            Caveated("group", "g1", "member", Onr("user", "alice"), "over_age",
                new Dictionary<string, object?> { ["min_age"] = 18 }),
            Caveated("group", "g1", "guest", Onr("user", "alice"), "ip_allowlist",
                new Dictionary<string, object?> { ["cidr"] = "10.0.0.0/8" }));
        var engine = BuildEngine();

        var result = await engine.Check(reader, "document", "doc1", "via_team", Onr("user", "alice"));

        Assert.Equal(Membership.Caveated, result.Verdict);
        Assert.Contains("age", result.MissingExprFields);
        Assert.Contains("user_ip", result.MissingExprFields);
    }
}
