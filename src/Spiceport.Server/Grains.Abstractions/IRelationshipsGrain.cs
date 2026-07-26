namespace Spiceport.Grains.Abstractions;

/// <summary>
/// The data-plane grain: schema reads/writes and relationship reads/writes/deletes. It is the write
/// side of the system (the check / reverse-ops grains are the read side).
/// </summary>
/// <remarks>
/// Every method here is a whole operation against the datastore (or the live schema provider) rather than
/// a per-key dispatch, so it is exposed on ONE grain keyed by the constant integer <see cref="Key"/> and
/// the implementation is <c>[StatelessWorker]</c> so the silo scales activations with load without
/// fragmenting any keyspace. Writes persist through the host-owned
/// <see cref="Spiceport.Datastore.IDatastore"/> and (for schema) swap the live schema snapshot; replies
/// carry an opaque revision token. The relationship READ ops (ReadRelationships, BulkExportRelationships)
/// and the reverse-ops reads (ExpandPermissionTree, LookupSubjects, LookupResources) run in-process via
/// <see cref="RelationshipReads"/> and <see cref="ReverseOps"/> respectively; this grain keeps the write
/// side and the on-demand counter/schema ops.
/// </remarks>
public interface IRelationshipsGrain : IGrainWithIntegerKey
{
    /// <summary>The fixed key every caller uses; the grain is a stateless worker so this is not an identity.</summary>
    public const long Key = 0;

    /// <summary>Compiles and installs a new schema, persisting it and swapping the live snapshot.</summary>
    Task<WriteSchemaReply> WriteSchema(WriteSchemaArgs args);

    /// <summary>Returns the current schema source text and a read-at token.</summary>
    Task<ReadSchemaReply> ReadSchema();

    /// <summary>Applies relationship mutations (create / touch / delete) in one transaction.</summary>
    Task<WriteRelationshipsReply> WriteRelationships(WriteRelationshipsArgs args);

    /// <summary>Deletes relationships matching the filter, optionally bounded by a limit.</summary>
    Task<DeleteRelationshipsReply> DeleteRelationships(DeleteRelationshipsArgs args);

    /// <summary>
    /// Loads an import's relationships with CREATE semantics in a single, all-or-nothing write
    /// transaction: a row that already exists, or repeats within the import, rejects the whole import
    /// (nothing applies) — real SpiceDB's ImportBulkRelationships behavior. The bulk-import gRPC
    /// services buffer the client stream and call this once — the grain stays request/response.
    /// </summary>
    Task<BulkImportRelationshipsReply> BulkImportRelationships(BulkImportRelationshipsArgs args);

    /// <summary>
    /// Registers an MVCC relationship counter under <c>args.Name</c> with the given filter. Throws
    /// <see cref="CounterOperationException"/> (<see cref="CounterErrorKind.AlreadyRegistered"/>) if a
    /// counter with that name is already live.
    /// </summary>
    Task<RegisterCounterReply> RegisterRelationshipCounter(RegisterCounterArgs args);

    /// <summary>
    /// Tombstones the live counter named <c>args.Name</c>. Throws <see cref="CounterOperationException"/>
    /// (<see cref="CounterErrorKind.NotRegistered"/>) if no such counter is live.
    /// </summary>
    Task<UnregisterCounterReply> UnregisterRelationshipCounter(UnregisterCounterArgs args);

    /// <summary>
    /// Computes, on demand, the count of relationships matching the registered counter's filter at a
    /// freshly resolved snapshot and returns the count plus a read-at token. Throws
    /// <see cref="CounterOperationException"/> (<see cref="CounterErrorKind.NotRegistered"/>) if no
    /// counter named <c>args.Name</c> is live.
    /// </summary>
    Task<CountRelationshipsReply> CountRelationships(CountRelationshipsArgs args);
}
