using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;

namespace Spiceport.Grains.Tests;

/// <summary>
/// The consequence of the structural <see cref="SchemaHash"/>, not just its value. A <c>WriteSchema</c>
/// token carries the structural hash, a check at that token pins it, and the per-silo
/// <see cref="SchemaResolver"/> serves whatever snapshot it already cached under that hash. A schema edit
/// the hash cannot see therefore keeps evaluating under the OLD compiled schema. A caveat-body edit is
/// exactly such an edit unless the hash covers the caveat's expression and parameter types.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class SchemaHashCaveatEditMeshTests
{
    private static string SchemaWithThreshold(int threshold) => $$"""
        caveat over(x int) {
          x > {{threshold}}
        }

        definition user {}

        definition document {
          relation viewer: user with over
          permission view = viewer
        }
        """;

    private static async Task<Membership> CheckView(MeshTestCluster cluster, string token)
    {
        var result = await cluster.Checker.Check(
            "document", "doc1", "view",
            new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis),
            caveatContext: new Dictionary<string, object?> { ["x"] = 50 },
            consistency: ConsistencyRequirement.AtExactSnapshot(new ZedToken(token)));
        return result.Verdict;
    }

    [Fact]
    public async Task Check_at_the_token_of_a_caveat_body_edit_evaluates_the_new_body()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(SchemaWithThreshold(1));
        await cluster.WriteSchema(SchemaWithThreshold(1));
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships([
            new RelationshipUpdate(
                Relationship.Create(
                    new ObjectAndRelation("document", "doc1", "viewer"),
                    new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis),
                    new ContextualizedCaveat("over", new Dictionary<string, object?>())),
                UpdateOperation.Create),
        ]));
        var before = await cluster.WriteSchema(SchemaWithThreshold(1));

        // Warm the resolver under the original caveat body (x > 1).
        Assert.Equal(Membership.Member, await CheckView(cluster, before.WrittenAtToken));

        var after = await cluster.WriteSchema(SchemaWithThreshold(100));

        Assert.Equal(Membership.NotMember, await CheckView(cluster, after.WrittenAtToken));
    }
}
