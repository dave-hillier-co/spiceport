using Spiceport.Core;
using Spiceport.Datastore;

namespace Spiceport.Api;

/// <summary>
/// The embedded test schema and the relationships seeded at startup so a
/// <c>CheckPermission</c> call returns a real answer over the in-memory datastore.
/// </summary>
/// <remarks>
/// Deferred (later phases): loading schema/relationships from a conformance YAML; for this slice a single
/// classic document/viewer fixture is enough. The datastore is now durable when Postgres storage is
/// configured, so seeding is idempotent: it only writes into an EMPTY store.
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

    /// <summary>
    /// Seeds the fixture relationship, but ONLY if the datastore is empty. With durable storage the grain
    /// state survives restarts, so re-seeding a populated store every boot would churn MVCC history (a
    /// Touch re-stamps the row at a new revision each time) without changing the live set. Returns true if
    /// the seed was written, false if an existing relationship was found and seeding was skipped.
    /// </summary>
    public static async Task<bool> SeedAsync(IDatastore datastore)
    {
        ArgumentNullException.ThrowIfNull(datastore);

        var head = await datastore.HeadRevision();
        var reader = datastore.SnapshotReader(head.Revision);
        await foreach (var _ in reader.QueryRelationships(new RelationshipsFilter()))
            return false; // already populated (e.g. resumed from durable storage) — leave it untouched

        await datastore.ReadWriteTx(async tx =>
        {
            var rel = Relationship.Create(
                new ObjectAndRelation("document", "readme", "viewer"),
                new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis));
            await tx.WriteRelationships([new RelationshipUpdate(rel, UpdateOperation.Touch)]);
        });
        return true;
    }
}
