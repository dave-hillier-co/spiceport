using Spiceport.Core;
using Spiceport.Datastore;

namespace Spiceport.Api;

/// <summary>
/// The embedded test schema and the relationships seeded at startup so a
/// <c>CheckPermission</c> call returns a real answer over the in-memory datastore.
/// </summary>
/// <remarks>
/// Deferred (later phases): loading schema/relationships from a conformance YAML or a
/// real persisted datastore; for this slice a single classic document/viewer fixture is enough.
/// </remarks>
public static class SeedData
{
    public const string SchemaText = """
        definition user {}

        definition document {
            relation viewer: user
            relation editor: user
            permission view = viewer + editor
        }
        """;

    public static async Task SeedAsync(IDatastore datastore)
    {
        ArgumentNullException.ThrowIfNull(datastore);

        await datastore.ReadWriteTx(async tx =>
        {
            var rel = Relationship.Create(
                new ObjectAndRelation("document", "readme", "viewer"),
                new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis));
            await tx.WriteRelationships([new RelationshipUpdate(rel, UpdateOperation.Touch)]);
        });
    }
}
