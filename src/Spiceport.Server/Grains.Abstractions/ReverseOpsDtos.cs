namespace Spiceport.Grains.Abstractions;

/// <summary>
/// The collapsed membership of a found subject / resource, mirroring the gRPC <c>Permissionship</c>:
/// either an unconditional member or a caveated member with the unresolved caveat parameter names.
/// </summary>
/// <remarks>
/// Non-members are never represented — they are simply not yielded. The reverse engine ops already
/// shear caveats against the request context, so the cross-grain reply carries the post-context
/// collapsed shape (a missing-fields list) rather than a verbatim caveat-expression tree.
/// </remarks>
[GenerateSerializer]
public sealed record Permissionship(
    [property: Id(0)] bool IsCaveated,
    [property: Id(1)] IReadOnlyList<string> MissingContextParams)
{
    /// <summary>An unconditional member.</summary>
    public static Permissionship Member { get; } = new(false, []);

    /// <summary>A caveated member with the given unresolved caveat parameter names.</summary>
    public static Permissionship Caveated(IReadOnlyList<string> missing) => new(true, missing);
}

// ---- ExpandPermissionTree ----

/// <summary>Whether expansion descends into non-terminal usersets. Mirrors the engine's <c>ExpandMode</c>.</summary>
public enum ExpandModeWire
{
    /// <summary>Expand one level only.</summary>
    Shallow = 0,

    /// <summary>Expand non-terminal usersets recursively.</summary>
    Recursive = 1,
}

/// <summary>The set operation a tree node combines its children with. Mirrors <c>SetOperationType</c>.</summary>
public enum SetOpWire
{
    /// <summary>Union (OR).</summary>
    Union = 0,

    /// <summary>Intersection (AND).</summary>
    Intersection = 1,

    /// <summary>Exclusion (base AND NOT excluded).</summary>
    Exclusion = 2,
}

/// <summary>Arguments for <see cref="IReverseOpsGrain.ExpandPermissionTree"/>.</summary>
/// <param name="ResourceType">The resource namespace.</param>
/// <param name="ResourceId">The resource object id.</param>
/// <param name="Permission">The relation or permission to expand.</param>
/// <param name="Mode">Shallow or recursive expansion.</param>
/// <param name="Consistency">The consistency requirement; null means minimize-latency (default).</param>
[GenerateSerializer]
public sealed record ExpandTreeArgs(
    [property: Id(0)] string ResourceType,
    [property: Id(1)] string ResourceId,
    [property: Id(2)] string Permission,
    [property: Id(3)] ExpandModeWire Mode,
    [property: Id(4)] ConsistencyWire? Consistency = null);

/// <summary>The reply from <see cref="IReverseOpsGrain.ExpandPermissionTree"/>: the whole tree root.</summary>
/// <param name="Root">The expanded tree root.</param>
/// <param name="ExpandedAtToken">The ZedToken for the revision actually evaluated.</param>
[GenerateSerializer]
public sealed record ExpandTreeReply(
    [property: Id(0)] ExpandTreeNodeWire Root,
    [property: Id(1)] string ExpandedAtToken = "");

/// <summary>
/// A serializable node of an expanded permission tree, structurally mirroring the engine's
/// <c>PermissionTreeNode</c>. Exactly one of <see cref="Subjects"/> (leaf) or <see cref="Children"/>
/// (set operation) is populated; <see cref="Operation"/> applies only to set-operation nodes.
/// </summary>
/// <param name="ExpandedType">Object type of the resource ONR this node expands.</param>
/// <param name="ExpandedId">Object id of the resource ONR this node expands.</param>
/// <param name="ExpandedRelation">Relation/permission of the resource ONR this node expands.</param>
/// <param name="CaveatMissingFields">Caveat params gating the whole node (non-empty = node is caveated).</param>
/// <param name="IsLeaf">True for a leaf (direct subjects); false for a set-operation node.</param>
/// <param name="Operation">The combining operation for a set-operation node.</param>
/// <param name="Subjects">The directly-written subjects, for a leaf node.</param>
/// <param name="Children">The child nodes, for a set-operation node.</param>
[GenerateSerializer]
public sealed record ExpandTreeNodeWire(
    [property: Id(0)] string ExpandedType,
    [property: Id(1)] string ExpandedId,
    [property: Id(2)] string ExpandedRelation,
    [property: Id(3)] IReadOnlyList<string> CaveatMissingFields,
    [property: Id(4)] bool IsLeaf,
    [property: Id(5)] SetOpWire Operation,
    [property: Id(6)] IReadOnlyList<ExpandSubjectWire> Subjects,
    [property: Id(7)] IReadOnlyList<ExpandTreeNodeWire> Children);

/// <summary>A directly-written subject within an expand leaf node.</summary>
/// <param name="SubjectType">The subject namespace.</param>
/// <param name="SubjectId">The subject object id ("*" for a wildcard).</param>
/// <param name="SubjectRelation">The subject relation (ellipsis for a terminal subject).</param>
/// <param name="IsWildcard">True when the subject is the public wildcard.</param>
/// <param name="CaveatMissingFields">Caveat params gating this subject (non-empty = subject is caveated).</param>
[GenerateSerializer]
public sealed record ExpandSubjectWire(
    [property: Id(0)] string SubjectType,
    [property: Id(1)] string SubjectId,
    [property: Id(2)] string SubjectRelation,
    [property: Id(3)] bool IsWildcard,
    [property: Id(4)] IReadOnlyList<string> CaveatMissingFields);

// ---- LookupSubjects ----

/// <summary>Arguments for <see cref="IReverseOpsGrain.LookupSubjects"/>.</summary>
/// <param name="ResourceType">The resource namespace.</param>
/// <param name="ResourceId">The resource object id.</param>
/// <param name="Permission">The relation or permission.</param>
/// <param name="SubjectType">The requested subject namespace.</param>
/// <param name="SubjectRelation">The requested subject relation (ellipsis for terminal subjects).</param>
/// <param name="Context">Optional request-time caveat context for collapsing caveated subjects.</param>
/// <param name="Limit">Soft page size; null or 0 for the engine default / unbounded in this slice.</param>
/// <param name="Cursor">Opaque continuation token from a prior page; null to start.</param>
/// <param name="Consistency">The consistency requirement; null means minimize-latency (default).</param>
[GenerateSerializer]
public sealed record LookupSubjectsArgs(
    [property: Id(0)] string ResourceType,
    [property: Id(1)] string ResourceId,
    [property: Id(2)] string Permission,
    [property: Id(3)] string SubjectType,
    [property: Id(4)] string SubjectRelation,
    [property: Id(5)] IReadOnlyDictionary<string, object?>? Context,
    [property: Id(6)] int? Limit,
    [property: Id(7)] string? Cursor,
    [property: Id(8)] ConsistencyWire? Consistency = null);

/// <summary>A bounded page of found subjects with an optional continuation cursor.</summary>
/// <param name="Subjects">The subjects found in this page.</param>
/// <param name="Cursor">A continuation token, or null/empty when the enumeration is exhausted.</param>
/// <param name="LookedUpAtToken">The ZedToken for the revision actually evaluated.</param>
[GenerateSerializer]
public sealed record LookupSubjectsReply(
    [property: Id(0)] IReadOnlyList<FoundSubjectWire> Subjects,
    [property: Id(1)] string? Cursor,
    [property: Id(2)] string LookedUpAtToken = "");

/// <summary>A subject found by a lookup, with its collapsed permissionship.</summary>
/// <param name="SubjectId">The subject object id ("*" for a wildcard).</param>
/// <param name="IsWildcard">True when the subject is the public wildcard.</param>
/// <param name="Permissionship">Member or caveated (with missing context params).</param>
[GenerateSerializer]
public sealed record FoundSubjectWire(
    [property: Id(0)] string SubjectId,
    [property: Id(1)] bool IsWildcard,
    [property: Id(2)] Permissionship Permissionship);

// ---- LookupResources ----

/// <summary>Arguments for <see cref="IReverseOpsGrain.LookupResources"/>.</summary>
/// <param name="ResourceType">The resource namespace to enumerate.</param>
/// <param name="Permission">The relation or permission.</param>
/// <param name="SubjectType">The subject namespace.</param>
/// <param name="SubjectId">The subject object id.</param>
/// <param name="SubjectRelation">The subject relation (ellipsis for terminal subjects).</param>
/// <param name="Context">Optional request-time caveat context.</param>
/// <param name="Limit">Soft page size; null or 0 for the engine default / unbounded in this slice.</param>
/// <param name="Cursor">Opaque continuation token from a prior page; null to start.</param>
/// <param name="Consistency">The consistency requirement; null means minimize-latency (default).</param>
[GenerateSerializer]
public sealed record LookupResourcesArgs(
    [property: Id(0)] string ResourceType,
    [property: Id(1)] string Permission,
    [property: Id(2)] string SubjectType,
    [property: Id(3)] string SubjectId,
    [property: Id(4)] string SubjectRelation,
    [property: Id(5)] IReadOnlyDictionary<string, object?>? Context,
    [property: Id(6)] int? Limit,
    [property: Id(7)] string? Cursor,
    [property: Id(8)] ConsistencyWire? Consistency = null);

/// <summary>A bounded page of found resources with an optional continuation cursor.</summary>
/// <param name="Resources">The resources found in this page.</param>
/// <param name="Cursor">A continuation token, or null/empty when the enumeration is exhausted.</param>
/// <param name="LookedUpAtToken">The ZedToken for the revision actually evaluated.</param>
[GenerateSerializer]
public sealed record LookupResourcesReply(
    [property: Id(0)] IReadOnlyList<FoundResourceWire> Resources,
    [property: Id(1)] string? Cursor,
    [property: Id(2)] string LookedUpAtToken = "");

/// <summary>A resource found by a lookup, with its collapsed permissionship.</summary>
/// <param name="ResourceId">The reachable resource object id.</param>
/// <param name="Permissionship">Member or caveated (with missing context params).</param>
/// <param name="AfterResultCursor">
/// The opaque resume cursor positioned immediately after this resource, so a client can resume the
/// stream right after it (mirrors v1 <c>after_result_cursor</c>). Null when no cursor is available.
/// </param>
[GenerateSerializer]
public sealed record FoundResourceWire(
    [property: Id(0)] string ResourceId,
    [property: Id(1)] Permissionship Permissionship,
    [property: Id(2)] string? AfterResultCursor = null);
