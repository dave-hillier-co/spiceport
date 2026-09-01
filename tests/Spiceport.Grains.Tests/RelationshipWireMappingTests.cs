using Google.Protobuf.WellKnownTypes;
using Spiceport.Api;
using Spiceport.Grains.Abstractions;
using Xunit;
using V1 = Authzed.Api.V1;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Pins the wire mapping for a caveat with an empty name but a non-empty context (issue #42): the
/// name guard and the context guard must agree, so an empty name discards the whole caveat instead
/// of producing a <see cref="RelationshipWire"/> whose context belongs to no caveat. This must be
/// asserted at the mapper, not via a write/read round trip - WireConvert.ToRelationship discards
/// the orphan context downstream, so a round trip looks identical with or without the fix.
/// </summary>
public sealed class RelationshipWireMappingTests
{
    private static Struct OrphanContext()
    {
        var ctx = new Struct();
        ctx.Fields["orphan"] = Value.ForString("context");
        return ctx;
    }

    private static V1::Relationship V1Relationship(string caveatName) => new()
    {
        Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
        Relation = "viewer",
        Subject = new V1::SubjectReference
        {
            Object = new V1::ObjectReference { ObjectType = "user", ObjectId = "alice" },
        },
        OptionalCaveat = new V1::ContextualizedCaveat { CaveatName = caveatName, Context = OrphanContext() },
    };

    private static Spiceport.Protos.Relationship V0Relationship(string caveatName) => new()
    {
        Resource = new Spiceport.Protos.ObjectReference { ObjectType = "document", ObjectId = "readme" },
        ResourceRelation = "viewer",
        Subject = new Spiceport.Protos.SubjectReference
        {
            Object = new Spiceport.Protos.ObjectReference { ObjectType = "user", ObjectId = "alice" },
        },
        OptionalCaveat = new Spiceport.Protos.ContextualizedCaveat { CaveatName = caveatName, Context = OrphanContext() },
    };

    public static readonly TheoryData<string, Func<string, RelationshipWire>> Mappers = new()
    {
        { "AuthzedPermissionsV1Service", name => AuthzedPermissionsV1Service.ToWire(V1Relationship(name)) },
        { "BulkGrpcService", name => BulkGrpcService.ToWire(V0Relationship(name)) },
        { "PermissionsGrpcService", name => PermissionsGrpcService.ToWire(V0Relationship(name)) },
    };

    [Theory]
    [MemberData(nameof(Mappers))]
    public void Empty_caveat_name_discards_the_context_too(string mapper, Func<string, RelationshipWire> toWire)
    {
        _ = mapper;
        var wire = toWire("");

        Assert.Null(wire.CaveatName);
        Assert.Null(wire.CaveatContext);
    }

    [Theory]
    [MemberData(nameof(Mappers))]
    public void Named_caveat_keeps_name_and_context(string mapper, Func<string, RelationshipWire> toWire)
    {
        _ = mapper;
        var wire = toWire("only_on_tuesday");

        Assert.Equal("only_on_tuesday", wire.CaveatName);
        Assert.NotNull(wire.CaveatContext);
        Assert.True(wire.CaveatContext!.ContainsKey("orphan"));
    }
}
