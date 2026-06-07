using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Replays the API's seeded document/viewer fixture THROUGH the grain mesh: with
/// <c>document:readme#viewer @ user:alice</c> seeded and the <c>view = viewer + editor</c> permission,
/// <c>user:alice</c> must resolve to a member (HasPermission) and an unseeded <c>user:bob</c> to a
/// non-member (NoPermission). This is the exact behavior the gRPC front door now serves via the root
/// dispatcher.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class SeededFixtureMeshTests
{
    private const string SchemaText = """
        definition user {}

        definition document {
            relation viewer: user
            relation editor: user
            permission view = viewer + editor
        }
        """;

    [Fact]
    public async Task DocumentViewer_Fixture_Through_Mesh_Yields_Alice_Member_Bob_NotMember()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(SchemaText);

        var rel = Relationship.Create(
            new ObjectAndRelation("document", "readme", "viewer"),
            new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis));
        await cluster.Datastore.ReadWriteTx(tx =>
            tx.WriteRelationships([new RelationshipUpdate(rel, UpdateOperation.Touch)]));

        var alice = await cluster.Checker.Check(
            "document", "readme", "view",
            new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis),
            caveatContext: null);

        var bob = await cluster.Checker.Check(
            "document", "readme", "view",
            new ObjectAndRelation("user", "bob", CoreConstants.Ellipsis),
            caveatContext: null);

        Assert.Equal(Membership.Member, alice.Verdict);
        Assert.Equal(Membership.NotMember, bob.Verdict);
    }
}
