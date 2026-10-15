using Spiceport.Core;
using Spiceport.Engine;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Issue #53 through the path where it surfaced: an object id containing U+001F rides the exact visited
/// set from <see cref="OrleansDispatcher"/> into <c>CheckGrain</c> as a canonical <see cref="VisitKey"/>
/// string in the ambient RequestContext, and the grain must parse it back rather than fail the Check.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class VisitKeySeparatorMeshTests
{
    private const string Schema = """
        definition user {}

        definition group {
            relation member: user | group#member
        }
        """;

    [Fact]
    public async Task Check_on_an_id_containing_the_unit_separator_resolves_across_the_mesh()
    {
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(Schema, siloCount: 2);

        // group:a<US>b#member -> group:c#member -> user:x. The second hop is a grain call whose ambient
        // visited set carries the separator-bearing root key.
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(
        [
            new RelationshipUpdate(
                Relationship.Create(
                    new ObjectAndRelation("group", "a\u001Fb", "member"),
                    new ObjectAndRelation("group", "c", "member")),
                UpdateOperation.Create),
            new RelationshipUpdate(
                Relationship.Create(
                    new ObjectAndRelation("group", "c", "member"),
                    new ObjectAndRelation("user", "x", CoreConstants.Ellipsis)),
                UpdateOperation.Create),
        ]));

        var result = await cluster.Checker.Check(
            "group", "a\u001Fb", "member",
            new ObjectAndRelation("user", "x", CoreConstants.Ellipsis), null);

        Assert.Equal(Membership.Member, result.Verdict);
    }
}
