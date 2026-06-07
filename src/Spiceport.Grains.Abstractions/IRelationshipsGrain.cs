namespace Spiceport.Grains.Abstractions;

/// <summary>
/// The data-plane grain: schema reads/writes and relationship reads/writes/deletes. It is the write
/// side of the system (the check / reverse-ops grains are the read side).
/// </summary>
/// <remarks>
/// Like <see cref="IReverseOpsGrain"/>, every method here is a whole operation against the datastore
/// (or the live schema provider) rather than a per-key dispatch, so it is exposed on ONE grain keyed by
/// the constant integer <see cref="Key"/> and the implementation is <c>[StatelessWorker]</c> so the
/// silo scales activations with load without fragmenting any keyspace. Writes persist through the
/// host-owned <see cref="Spiceport.Datastore.IDatastore"/> and (for schema) swap the live schema
/// snapshot; replies carry an opaque revision token. Reads pin the optimized revision and page with an
/// opaque cursor, mirroring the reverse-ops convention.
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

    /// <summary>Reads relationships matching the filter, as a bounded page with an optional cursor.</summary>
    Task<ReadRelationshipsReply> ReadRelationships(ReadRelationshipsArgs args);

    /// <summary>
    /// Loads one batch of relationships as an efficient insert (create-or-overwrite, no per-row
    /// existence check) in a single, all-or-nothing write transaction. The bulk-import gRPC service
    /// consumes the client stream and calls this once per inbound batch — the grain stays
    /// request/response.
    /// </summary>
    Task<BulkImportRelationshipsReply> BulkImportRelationships(BulkImportRelationshipsArgs args);

    /// <summary>
    /// Returns one page of a bulk export over a single pinned snapshot. With no cursor the grain
    /// resolves and pins a revision from the request consistency and encodes it into the returned
    /// cursor; with a cursor it reads the exact revision the cursor encodes, so every page (and any
    /// resume) sees the same snapshot.
    /// </summary>
    Task<BulkExportRelationshipsReply> BulkExportRelationships(BulkExportRelationshipsArgs args);

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
