using Grpc.Core;
using Spiceport.Api;
using V1 = Authzed.Api.V1;

namespace Spiceport.Grains.Tests;

public class SchemaFiltersTests
{
    private static RpcException Rejects(V1::ReflectionSchemaFilter filter) =>
        Assert.Throws<RpcException>(() => SchemaFilters.FromRequest([filter]));

    [Fact]
    public void Definition_and_caveat_filters_are_mutually_exclusive()
    {
        var ex = Rejects(new V1::ReflectionSchemaFilter
        {
            OptionalDefinitionNameFilter = "document",
            OptionalCaveatNameFilter = "only_on_tuesday",
        });

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal("cannot filter by both definition and caveat name", ex.Status.Detail);
    }

    [Fact]
    public void Relation_and_permission_filters_are_mutually_exclusive()
    {
        var ex = Rejects(new V1::ReflectionSchemaFilter
        {
            OptionalDefinitionNameFilter = "document",
            OptionalRelationNameFilter = "viewer",
            OptionalPermissionNameFilter = "view",
        });

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal("cannot filter by both relation and permission name", ex.Status.Detail);
    }

    [Theory]
    [InlineData("viewer", "")]
    [InlineData("", "view")]
    public void Relation_or_permission_filter_requires_a_definition_filter(string relation, string permission)
    {
        var ex = Rejects(new V1::ReflectionSchemaFilter
        {
            OptionalRelationNameFilter = relation,
            OptionalPermissionNameFilter = permission,
        });

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal("relation/permission filter requires a definition filter", ex.Status.Detail);
    }

    // SpiceDB has no dedicated caveat-vs-relation rule: a caveat+relation filter without a
    // definition gets the missing-definition error, same as here (issue #44).
    [Fact]
    public void Caveat_plus_relation_filter_reports_the_missing_definition()
    {
        var ex = Rejects(new V1::ReflectionSchemaFilter
        {
            OptionalCaveatNameFilter = "only_on_tuesday",
            OptionalRelationNameFilter = "viewer",
        });

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal("relation/permission filter requires a definition filter", ex.Status.Detail);
    }

    [Fact]
    public void Valid_filter_combinations_are_accepted()
    {
        var filters = SchemaFilters.FromRequest(
        [
            new V1::ReflectionSchemaFilter { OptionalDefinitionNameFilter = "document", OptionalRelationNameFilter = "viewer" },
            new V1::ReflectionSchemaFilter { OptionalDefinitionNameFilter = "document", OptionalPermissionNameFilter = "view" },
            new V1::ReflectionSchemaFilter { OptionalCaveatNameFilter = "only_on_tuesday" },
        ]);

        Assert.True(filters.MatchesDefinition("document"));
        Assert.True(filters.MatchesCaveat("only_on_tuesday"));
    }
}
