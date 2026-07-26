using Orleans.Concurrency;

namespace Spiceport.Grains.Abstractions;

/// <summary>The kind of relationship mutation. Mirrors the core <c>UpdateOperation</c>.</summary>
public enum RelationshipUpdateOpWire
{
    /// <summary>Create or update (upsert).</summary>
    Touch = 0,

    /// <summary>Create; fails if it already exists.</summary>
    Create = 1,

    /// <summary>Delete.</summary>
    Delete = 2,
}

/// <summary>A relationship (tuple) on the wire: resource + subject ONR, optional caveat and expiration.</summary>
[GenerateSerializer]
public sealed record RelationshipWire(
    [property: Id(0)] string ResourceType,
    [property: Id(1)] string ResourceId,
    [property: Id(2)] string ResourceRelation,
    [property: Id(3)] string SubjectType,
    [property: Id(4)] string SubjectId,
    [property: Id(5)] string SubjectRelation,
    [property: Id(6)] string? CaveatName,
    [property: Id(7)] IReadOnlyDictionary<string, object?>? CaveatContext,
    [property: Id(8)] DateTimeOffset? Expiration);

/// <summary>
/// One item of a relationship stream (<see cref="RelationshipReads.ReadRelationships"/> /
/// <see cref="RelationshipReads.BulkExportRelationships"/>): a relationship plus the opaque resume cursor
/// positioned immediately after it. For ReadRelationships the cursor is the canonical tuple string
/// (resumption skips tuples at or before it) and <see cref="ReadAtToken"/> carries the per-message
/// ZedToken; for BulkExport the cursor pins the export revision plus the last tuple (so a reconnect reads
/// the exact same snapshot) and <see cref="ReadAtToken"/> is empty (that RPC carries no per-item token).
/// Runs entirely in-process — it crosses no grain boundary, so it is a plain record.
/// </summary>
/// <param name="Relationship">The relationship on the wire.</param>
/// <param name="ResumeCursor">The opaque resume cursor positioned immediately after this relationship.</param>
/// <param name="ReadAtToken">The read-at ZedToken for this item (ReadRelationships), or empty (BulkExport).</param>
public sealed record RelationshipStreamItem(
    RelationshipWire Relationship,
    string ResumeCursor,
    string ReadAtToken = "");

/// <summary>A single relationship mutation on the wire.</summary>
[GenerateSerializer]
public sealed record RelationshipUpdateWire(
    [property: Id(0)] RelationshipUpdateOpWire Operation,
    [property: Id(1)] RelationshipWire Relationship);

/// <summary>The operation a <see cref="PreconditionWire"/> asserts about its filter.</summary>
public enum PreconditionOpWire
{
    /// <summary>The filter must match at least one relationship.</summary>
    MustMatch = 0,

    /// <summary>The filter must not match any relationship.</summary>
    MustNotMatch = 1,
}

/// <summary>
/// A precondition checked atomically, inside the write transaction, against the same snapshot the writes
/// commit at. If it fails the whole write is rejected and nothing commits.
/// </summary>
[GenerateSerializer]
public sealed record PreconditionWire(
    [property: Id(0)] PreconditionOpWire Operation,
    [property: Id(1)] RelationshipsFilterWire Filter);

/// <summary>Arguments for <see cref="IRelationshipsGrain.WriteRelationships"/>.</summary>
[GenerateSerializer]
public sealed record WriteRelationshipsArgs(
    [property: Id(0)] IReadOnlyList<RelationshipUpdateWire> Updates,
    [property: Id(1)] IReadOnlyList<PreconditionWire>? Preconditions = null);

/// <summary>Reply for <see cref="IRelationshipsGrain.WriteRelationships"/>.</summary>
[GenerateSerializer]
public sealed record WriteRelationshipsReply(
    [property: Id(0)] string WrittenAtToken);

/// <summary>
/// The subset of the datastore relationships filter the data-plane API surfaces: resource-side
/// constraints plus a single subject selector. Null/empty fields place no constraint.
/// </summary>
[GenerateSerializer]
public sealed record RelationshipsFilterWire(
    [property: Id(0)] string? ResourceType,
    [property: Id(1)] string? ResourceIdPrefix,
    [property: Id(2)] IReadOnlyList<string>? ResourceIds,
    [property: Id(3)] string? ResourceRelation,
    [property: Id(4)] string? SubjectType,
    [property: Id(5)] IReadOnlyList<string>? SubjectIds,
    [property: Id(6)] string? SubjectRelation);

/// <summary>Arguments for <see cref="IRelationshipsGrain.DeleteRelationships"/>.</summary>
[GenerateSerializer]
public sealed record DeleteRelationshipsArgs(
    [property: Id(0)] RelationshipsFilterWire Filter,
    [property: Id(1)] ulong? OptionalLimit,
    [property: Id(2)] IReadOnlyList<PreconditionWire>? Preconditions = null);

/// <summary>Reply for <see cref="IRelationshipsGrain.DeleteRelationships"/>.</summary>
[GenerateSerializer]
public sealed record DeleteRelationshipsReply(
    [property: Id(0)] ulong DeletedCount,
    [property: Id(1)] bool ReachedLimit,
    [property: Id(2)] string DeletedAtToken);

/// <summary>Arguments for <see cref="RelationshipReads.ReadRelationships"/>. <c>Limit</c> is advisory.
/// Crosses no grain boundary — a plain record.</summary>
public sealed record ReadRelationshipsArgs(
    RelationshipsFilterWire Filter,
    int? Limit,
    string? Cursor,
    ConsistencyWire? Consistency = null);

/// <summary>Arguments for <see cref="IRelationshipsGrain.WriteSchema"/>.</summary>
[GenerateSerializer]
public sealed record WriteSchemaArgs(
    [property: Id(0)] string SchemaText);

/// <summary>Reply for <see cref="IRelationshipsGrain.WriteSchema"/>.</summary>
[GenerateSerializer]
public sealed record WriteSchemaReply(
    [property: Id(0)] string WrittenAtToken);

/// <summary>Reply for <see cref="IRelationshipsGrain.ReadSchema"/>.</summary>
[GenerateSerializer]
public sealed record ReadSchemaReply(
    [property: Id(0)] string SchemaText,
    [property: Id(1)] string ReadAtToken);

/// <summary>
/// Arguments for <see cref="IRelationshipsGrain.BulkImportRelationships"/>: an entire import's
/// relationships, loaded with CREATE semantics in a single write transaction — a row that already
/// exists (or repeats within the import) rejects the whole import and nothing applies, matching real
/// SpiceDB's ImportBulkRelationships.
/// </summary>
[GenerateSerializer]
public sealed record BulkImportRelationshipsArgs(
    [property: Id(0)] IReadOnlyList<RelationshipWire> Relationships);

/// <summary>Reply for <see cref="IRelationshipsGrain.BulkImportRelationships"/>: this batch's load.</summary>
[GenerateSerializer]
public sealed record BulkImportRelationshipsReply(
    [property: Id(0)] ulong NumLoaded,
    [property: Id(1)] string LoadedAtToken);

/// <summary>
/// Arguments for <see cref="RelationshipReads.BulkExportRelationships"/>: an export over a single pinned
/// snapshot. With no cursor the read resolves and pins a revision from <see cref="Consistency"/>; with a
/// cursor it reads the exact revision the cursor encodes (the consistency is then ignored).
/// <see cref="Limit"/> is advisory (the caller applies batching/limit by how it consumes the stream).
/// Crosses no grain boundary — a plain record.
/// </summary>
public sealed record BulkExportRelationshipsArgs(
    RelationshipsFilterWire Filter,
    int Limit,
    string? Cursor,
    ConsistencyWire? Consistency = null);

/// <summary>
/// Distinguishes the two on-demand counter failures so the gRPC front door can map them to the right
/// status code. The underlying datastore exceptions are not Orleans-serializable across the grain
/// boundary, so the grain rethrows a serializable <see cref="CounterOperationException"/> carrying this.
/// </summary>
public enum CounterErrorKind
{
    /// <summary>A counter with the same name is already registered.</summary>
    AlreadyRegistered = 0,

    /// <summary>No counter with the given name is registered.</summary>
    NotRegistered = 1,
}

/// <summary>
/// Serializable carrier for a counter failure raised inside the grain. Mirrors how schema-compile
/// failures are rethrown as a serializable type across the Orleans boundary.
/// </summary>
[GenerateSerializer]
public sealed class CounterOperationException : Exception
{
    public CounterOperationException(CounterErrorKind kind, string message) : base(message) => Kind = kind;

    [Id(0)]
    public CounterErrorKind Kind { get; }
}

/// <summary>Arguments for <see cref="IRelationshipsGrain.RegisterRelationshipCounter"/>.</summary>
[GenerateSerializer]
public sealed record RegisterCounterArgs(
    [property: Id(0)] string Name,
    [property: Id(1)] RelationshipsFilterWire Filter);

/// <summary>Reply for <see cref="IRelationshipsGrain.RegisterRelationshipCounter"/> (empty).</summary>
[GenerateSerializer]
public sealed record RegisterCounterReply;

/// <summary>Arguments for <see cref="IRelationshipsGrain.UnregisterRelationshipCounter"/>.</summary>
[GenerateSerializer]
public sealed record UnregisterCounterArgs(
    [property: Id(0)] string Name);

/// <summary>Reply for <see cref="IRelationshipsGrain.UnregisterRelationshipCounter"/> (empty).</summary>
[GenerateSerializer]
public sealed record UnregisterCounterReply;

/// <summary>Arguments for <see cref="IRelationshipsGrain.CountRelationships"/>.</summary>
[GenerateSerializer]
public sealed record CountRelationshipsArgs(
    [property: Id(0)] string Name);

/// <summary>Reply for <see cref="IRelationshipsGrain.CountRelationships"/>: the on-demand count + read-at token.</summary>
[GenerateSerializer]
public sealed record CountRelationshipsReply(
    [property: Id(0)] ulong Count,
    [property: Id(1)] string ReadAtToken);
