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

/// <summary>Arguments for <see cref="IRelationshipsGrain.ReadRelationships"/>.</summary>
[GenerateSerializer]
public sealed record ReadRelationshipsArgs(
    [property: Id(0)] RelationshipsFilterWire Filter,
    [property: Id(1)] int? Limit,
    [property: Id(2)] string? Cursor,
    [property: Id(3)] ConsistencyWire? Consistency = null);

/// <summary>Reply for <see cref="IRelationshipsGrain.ReadRelationships"/>.</summary>
[GenerateSerializer]
public sealed record ReadRelationshipsReply(
    [property: Id(0)] IReadOnlyList<RelationshipWire> Relationships,
    [property: Id(1)] string? Cursor,
    [property: Id(2)] string ReadAtToken);

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
