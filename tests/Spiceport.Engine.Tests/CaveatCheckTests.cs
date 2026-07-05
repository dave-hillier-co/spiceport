using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Schema;

namespace Spiceport.Engine.Tests;

public class CaveatCheckTests
{
    private const string CaveatSchema = """
        caveat over_age(age int, min_age int) {
          age >= min_age
        }

        caveat ip_allowlist(user_ip ipaddress, cidr string) {
          user_ip.in_cidr(cidr)
        }

        definition user {}

        definition document {
          relation viewer: user with over_age
          relation ip_viewer: user with ip_allowlist
          permission view = viewer
          permission ip_view = ip_viewer
        }
        """;

    private const string ExpirationSchema = """
        use expiration

        definition user {}

        definition document {
          relation viewer: user with expiration
          permission view = viewer
        }
        """;

    private static CheckEngine BuildEngine(string schemaText)
    {
        var compiled = SchemaCompiler.CompileSchema(schemaText);
        return new CheckEngine(compiled.Namespaces, compiled.Caveats);
    }

    private static async Task<IDatastoreReader> Seed(params Relationship[] rels)
    {
        var store = new ReferenceDatastore();
        var rev = await store.ReadWriteTx(async tx =>
        {
            var updates = rels.Select(r => new RelationshipUpdate(r, UpdateOperation.Create)).ToList();
            await tx.WriteRelationships(updates);
        });
        return store.SnapshotReader(rev);
    }

    private static ObjectAndRelation Onr(string type, string id, string relation = CoreConstants.Ellipsis) =>
        new(type, id, relation);

    private static Relationship Caveated(
        string resType, string resId, string resRel, ObjectAndRelation subject,
        string caveatName, IReadOnlyDictionary<string, object?>? ctx = null,
        DateTimeOffset? expiration = null) =>
        Relationship.Create(
            new ObjectAndRelation(resType, resId, resRel),
            subject,
            new ContextualizedCaveat(caveatName, ctx),
            expiration);

    [Fact]
    public async Task Caveat_True_WithRequestContext_IsMember()
    {
        var reader = await Seed(
            Caveated("document", "doc1", "viewer", Onr("user", "alice"), "over_age",
                new Dictionary<string, object?> { ["min_age"] = 18 }));
        var engine = BuildEngine(CaveatSchema);

        var result = await engine.Check(
            reader, "document", "doc1", "view", Onr("user", "alice"),
            caveatContext: new Dictionary<string, object?> { ["age"] = 21 });

        Assert.Equal(Membership.Member, result.Verdict);
    }

    [Fact]
    public async Task Caveat_False_WithRequestContext_IsNotMember()
    {
        var reader = await Seed(
            Caveated("document", "doc1", "viewer", Onr("user", "alice"), "over_age",
                new Dictionary<string, object?> { ["min_age"] = 18 }));
        var engine = BuildEngine(CaveatSchema);

        var result = await engine.Check(
            reader, "document", "doc1", "view", Onr("user", "alice"),
            caveatContext: new Dictionary<string, object?> { ["age"] = 16 });

        Assert.Equal(Membership.NotMember, result.Verdict);
    }

    [Fact]
    public async Task Caveat_MissingContext_IsCaveated_WithMissingFields()
    {
        var reader = await Seed(
            Caveated("document", "doc1", "viewer", Onr("user", "alice"), "over_age",
                new Dictionary<string, object?> { ["min_age"] = 18 }));
        var engine = BuildEngine(CaveatSchema);

        // No "age" supplied at request time and none on the tuple.
        var result = await engine.Check(reader, "document", "doc1", "view", Onr("user", "alice"));

        Assert.Equal(Membership.Caveated, result.Verdict);
        Assert.Contains("age", result.MissingExprFields);
    }

    [Fact]
    public async Task Caveat_RelationshipContextOverridesRequestContext()
    {
        // SpiceDB precedence: context written on the relationship takes priority over context
        // supplied at check time. Relationship pins min_age=99; the request's min_age=18 is
        // ignored, so age=21 >= 99 is false -> not a member.
        var reader = await Seed(
            Caveated("document", "doc1", "viewer", Onr("user", "alice"), "over_age",
                new Dictionary<string, object?> { ["min_age"] = 99 }));
        var engine = BuildEngine(CaveatSchema);

        var result = await engine.Check(
            reader, "document", "doc1", "view", Onr("user", "alice"),
            caveatContext: new Dictionary<string, object?> { ["age"] = 21, ["min_age"] = 18 });

        Assert.Equal(Membership.NotMember, result.Verdict);
    }

    [Fact]
    public async Task Caveat_InCidr_InsideNetwork_IsMember()
    {
        var reader = await Seed(
            Caveated("document", "doc1", "ip_viewer", Onr("user", "alice"), "ip_allowlist",
                new Dictionary<string, object?> { ["cidr"] = "10.0.0.0/8" }));
        var engine = BuildEngine(CaveatSchema);

        var result = await engine.Check(
            reader, "document", "doc1", "ip_view", Onr("user", "alice"),
            caveatContext: new Dictionary<string, object?> { ["user_ip"] = "10.1.2.3" });

        Assert.Equal(Membership.Member, result.Verdict);
    }

    [Fact]
    public async Task Caveat_InCidr_OutsideNetwork_IsNotMember()
    {
        var reader = await Seed(
            Caveated("document", "doc1", "ip_viewer", Onr("user", "alice"), "ip_allowlist",
                new Dictionary<string, object?> { ["cidr"] = "10.0.0.0/8" }));
        var engine = BuildEngine(CaveatSchema);

        var result = await engine.Check(
            reader, "document", "doc1", "ip_view", Onr("user", "alice"),
            caveatContext: new Dictionary<string, object?> { ["user_ip"] = "192.168.1.1" });

        Assert.Equal(Membership.NotMember, result.Verdict);
    }

    // The MvccSnapshotReader pre-filters expiry against real UtcNow. To exercise the
    // engine's own pinned-clock filtering (rather than the datastore's), these tests use an expiry
    // far in the future so the datastore keeps the tuple, then pin the engine clock on either side.
    private static readonly DateTimeOffset FarFutureExpiry =
        new(2099, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task Expiration_NotYetExpired_PinnedClock_IsMember()
    {
        var reader = await Seed(
            Relationship.Create(
                new ObjectAndRelation("document", "doc1", "viewer"),
                Onr("user", "alice"),
                expiration: FarFutureExpiry));
        var engine = BuildEngine(ExpirationSchema);

        // now is before expiry -> relationship is live.
        var result = await engine.Check(
            reader, "document", "doc1", "view", Onr("user", "alice"),
            evaluationTime: new DateTimeOffset(2030, 6, 1, 0, 0, 0, TimeSpan.Zero));

        Assert.Equal(Membership.Member, result.Verdict);
    }

    [Fact]
    public async Task Expiration_Expired_PinnedClock_IsNotMember()
    {
        var reader = await Seed(
            Relationship.Create(
                new ObjectAndRelation("document", "doc1", "viewer"),
                Onr("user", "alice"),
                expiration: FarFutureExpiry));
        var engine = BuildEngine(ExpirationSchema);

        // now is after expiry -> relationship is filtered (absent) by the engine clock.
        var result = await engine.Check(
            reader, "document", "doc1", "view", Onr("user", "alice"),
            evaluationTime: new DateTimeOffset(2100, 6, 1, 0, 0, 0, TimeSpan.Zero));

        Assert.Equal(Membership.NotMember, result.Verdict);
    }
}
