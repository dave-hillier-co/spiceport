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
}
