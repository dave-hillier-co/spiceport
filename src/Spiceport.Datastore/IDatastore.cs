using Spiceport.Core;

namespace Spiceport.Datastore;

/// <summary>A revision paired with the schema hash that was current at that revision.</summary>
/// <param name="Revision">The datastore revision.</param>
/// <param name="SchemaHash">The schema hash at this revision, or null if no schema has been written.</param>
public sealed record RevisionWithSchemaHash(IRevision Revision, string? SchemaHash = null);

/// <summary>Read-only accessor over a datastore snapshot at a fixed revision.</summary>
public interface IDatastoreReader
{
    /// <summary>Queries relationships from the resource side, matching the given filter.</summary>
    IAsyncEnumerable<Relationship> QueryRelationships(
        RelationshipsFilter filter,
        CancellationToken cancellationToken = default);

    /// <summary>Queries relationships from the subject side, matching the given subject filter.</summary>
    IAsyncEnumerable<Relationship> ReverseQueryRelationships(
        SubjectsFilter subjectsFilter,
        CancellationToken cancellationToken = default);

    /// <summary>Reads the unified stored schema bytes at this revision, or null if none has been written.</summary>
    Task<byte[]?> ReadStoredSchema(CancellationToken cancellationToken = default);

    /// <summary>True if this reader's snapshot is still valid (its revision has not been garbage collected).</summary>
    bool IsValid { get; }
}

/// <summary>A read-write transaction that will commit at a new revision.</summary>
public interface IReadWriteTransaction : IDatastoreReader
{
    /// <summary>The revision this transaction will commit as.</summary>
    IRevision NewRevision { get; }

    /// <summary>Applies relationship mutations (create / touch / delete).</summary>
    Task WriteRelationships(IReadOnlyList<RelationshipUpdate> mutations, CancellationToken cancellationToken = default);

    /// <summary>Deletes relationships matching the filter, optionally bounded by a limit.</summary>
    /// <returns>The number deleted and whether the limit was reached.</returns>
    Task<(ulong Count, bool ReachedLimit)> DeleteRelationships(
        RelationshipsFilter filter,
        ulong? limit = null,
        CancellationToken cancellationToken = default);

    /// <summary>Writes the unified stored schema bytes.</summary>
    Task WriteStoredSchema(byte[] schemaBytes, CancellationToken cancellationToken = default);

    /// <summary>Bulk loads relationships from a source. Returns the number loaded.</summary>
    Task<ulong> BulkLoad(IAsyncEnumerable<Relationship> relationships, CancellationToken cancellationToken = default);
}

/// <summary>An MVCC datastore: snapshot reads at any valid revision and serialized read-write transactions.</summary>
public interface IDatastore
{
    /// <summary>Returns a read-only snapshot reader at the given revision.</summary>
    /// <exception cref="RevisionNotFoundException">If the revision is not available.</exception>
    IDatastoreReader SnapshotReader(IRevision revision);

    /// <summary>Returns the current head revision (freshest committed state).</summary>
    Task<RevisionWithSchemaHash> HeadRevision(CancellationToken cancellationToken = default);

    /// <summary>Returns a revision suitable for caching, quantized to the configured interval.</summary>
    Task<RevisionWithSchemaHash> OptimizedRevision(CancellationToken cancellationToken = default);

    /// <summary>Runs a read-write transaction; commits if the function completes without throwing.</summary>
    /// <returns>The committed revision.</returns>
    Task<IRevision> ReadWriteTx(
        Func<IReadWriteTransaction, Task> transaction,
        CancellationToken cancellationToken = default);

    /// <summary>Returns true if the revision is still available.</summary>
    Task<bool> CheckRevision(IRevision revision, CancellationToken cancellationToken = default);

    /// <summary>Returns a stable unique id for this datastore instance.</summary>
    Task<string> GetUniqueId(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns a parser that decodes minted token revision strings back into <see cref="IRevision"/>
    /// for this datastore. The parser's <see cref="IRevisionParser.DatastoreUniqueId"/> matches
    /// <see cref="GetUniqueId"/>, so a token minted by this datastore decodes as <see cref="ZedTokenStatus.Valid"/>.
    /// </summary>
    Task<IRevisionParser> GetRevisionParser(CancellationToken cancellationToken = default);

    /// <summary>Releases resources held by the datastore.</summary>
    Task Close();
}
