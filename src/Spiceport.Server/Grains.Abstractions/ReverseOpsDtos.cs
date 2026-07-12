namespace Spiceport.Grains.Abstractions;

/// <summary>
/// The collapsed membership of a found subject / resource, mirroring the gRPC <c>Permissionship</c>:
/// either an unconditional member or a caveated member with the unresolved caveat parameter names.
/// </summary>
/// <remarks>
/// Non-members are never represented — they are simply not yielded. The reverse engine ops already
/// shear caveats against the request context, so this shape carries the post-context collapsed shape
/// (a missing-fields list) rather than a verbatim caveat-expression tree. Runs entirely in-process
/// between <see cref="ReverseOps"/> and the gRPC front doors — it crosses no grain boundary, so it is a
/// plain record with no Orleans serialization attributes.
/// </remarks>
public sealed record Permissionship(
    bool IsCaveated,
    IReadOnlyList<string> MissingContextParams)
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

/// <summary>Arguments for <see cref="ReverseOps.ExpandPermissionTree"/>.</summary>
/// <param name="ResourceType">The resource namespace.</param>
/// <param name="ResourceId">The resource object id.</param>
/// <param name="Permission">The relation or permission to expand.</param>
/// <param name="Mode">Shallow or recursive expansion.</param>
/// <param name="Consistency">The consistency requirement; null means minimize-latency (default).</param>
public sealed record ExpandTreeArgs(
    string ResourceType,
    string ResourceId,
    string Permission,
    ExpandModeWire Mode,
    ConsistencyWire? Consistency = null);

/// <summary>The reply from <see cref="ReverseOps.ExpandPermissionTree"/>: the whole tree root.</summary>
/// <param name="Root">The expanded tree root.</param>
/// <param name="ExpandedAtToken">The ZedToken for the revision actually evaluated.</param>
public sealed record ExpandTreeReply(
    ExpandTreeNodeWire Root,
    string ExpandedAtToken = "");

/// <summary>
/// A node of an expanded permission tree, structurally mirroring the engine's <c>PermissionTreeNode</c>.
/// Exactly one of <see cref="Subjects"/> (leaf) or <see cref="Children"/> (set operation) is populated;
/// <see cref="Operation"/> applies only to set-operation nodes.
/// </summary>
/// <param name="ExpandedType">Object type of the resource ONR this node expands.</param>
/// <param name="ExpandedId">Object id of the resource ONR this node expands.</param>
/// <param name="ExpandedRelation">Relation/permission of the resource ONR this node expands.</param>
/// <param name="CaveatMissingFields">Caveat params gating the whole node (non-empty = node is caveated).</param>
/// <param name="IsLeaf">True for a leaf (direct subjects); false for a set-operation node.</param>
/// <param name="Operation">The combining operation for a set-operation node.</param>
/// <param name="Subjects">The directly-written subjects, for a leaf node.</param>
/// <param name="Children">The child nodes, for a set-operation node.</param>
public sealed record ExpandTreeNodeWire(
    string ExpandedType,
    string ExpandedId,
    string ExpandedRelation,
    IReadOnlyList<string> CaveatMissingFields,
    bool IsLeaf,
    SetOpWire Operation,
    IReadOnlyList<ExpandSubjectWire> Subjects,
    IReadOnlyList<ExpandTreeNodeWire> Children);

/// <summary>A directly-written subject within an expand leaf node.</summary>
/// <param name="SubjectType">The subject namespace.</param>
/// <param name="SubjectId">The subject object id ("*" for a wildcard).</param>
/// <param name="SubjectRelation">The subject relation (ellipsis for a terminal subject).</param>
/// <param name="IsWildcard">True when the subject is the public wildcard.</param>
/// <param name="CaveatMissingFields">Caveat params gating this subject (non-empty = subject is caveated).</param>
public sealed record ExpandSubjectWire(
    string SubjectType,
    string SubjectId,
    string SubjectRelation,
    bool IsWildcard,
    IReadOnlyList<string> CaveatMissingFields);

// ---- LookupSubjects ----

/// <summary>Arguments for <see cref="ReverseOps.StreamLookupSubjects"/>. <c>Limit</c> is advisory.</summary>
/// <param name="ResourceType">The resource namespace.</param>
/// <param name="ResourceId">The resource object id.</param>
/// <param name="Permission">The relation or permission.</param>
/// <param name="SubjectType">The requested subject namespace.</param>
/// <param name="SubjectRelation">The requested subject relation (ellipsis for terminal subjects).</param>
/// <param name="Context">Optional request-time caveat context for collapsing caveated subjects.</param>
/// <param name="Limit">Soft page size; null or 0 for the engine default / unbounded in this slice.</param>
/// <param name="Cursor">Opaque continuation token from a prior page; null to start.</param>
/// <param name="Consistency">The consistency requirement; null means minimize-latency (default).</param>
public sealed record LookupSubjectsArgs(
    string ResourceType,
    string ResourceId,
    string Permission,
    string SubjectType,
    string SubjectRelation,
    IReadOnlyDictionary<string, object?>? Context,
    int? Limit,
    string? Cursor,
    ConsistencyWire? Consistency = null);

/// <summary>A subject found by a lookup, with its collapsed permissionship.</summary>
/// <param name="SubjectId">The subject object id ("*" for a wildcard).</param>
/// <param name="IsWildcard">True when the subject is the public wildcard.</param>
/// <param name="Permissionship">Member or caveated (with missing context params).</param>
public sealed record FoundSubjectWire(
    string SubjectId,
    bool IsWildcard,
    Permissionship Permissionship);

/// <summary>
/// One item of the <see cref="ReverseOps.StreamLookupSubjects"/> stream: a found subject plus the opaque
/// resume cursor positioned immediately after it. The cursor lets a client-facing limited stream resume
/// with byte-identical token semantics.
/// </summary>
/// <param name="Subject">The found subject with its collapsed permissionship.</param>
/// <param name="ResumeCursor">The opaque resume cursor positioned immediately after this subject.</param>
/// <param name="LookedUpAtToken">The ZedToken for the revision actually evaluated (constant across the stream).</param>
public sealed record FoundSubjectStreamItem(
    FoundSubjectWire Subject,
    string ResumeCursor,
    string LookedUpAtToken = "");

// ---- LookupResources ----

/// <summary>Arguments for <see cref="ReverseOps.StreamLookupResources"/>.</summary>
/// <param name="ResourceType">The resource namespace to enumerate.</param>
/// <param name="Permission">The relation or permission.</param>
/// <param name="SubjectType">The subject namespace.</param>
/// <param name="SubjectId">The subject object id.</param>
/// <param name="SubjectRelation">The subject relation (ellipsis for terminal subjects).</param>
/// <param name="Context">Optional request-time caveat context.</param>
/// <param name="Limit">Soft page size; null or 0 for the engine default / unbounded in this slice.</param>
/// <param name="Cursor">Opaque continuation token from a prior page; null to start.</param>
/// <param name="Consistency">The consistency requirement; null means minimize-latency (default).</param>
public sealed record LookupResourcesArgs(
    string ResourceType,
    string Permission,
    string SubjectType,
    string SubjectId,
    string SubjectRelation,
    IReadOnlyDictionary<string, object?>? Context,
    int? Limit,
    string? Cursor,
    ConsistencyWire? Consistency = null);

/// <summary>A resource found by a lookup, with its collapsed permissionship.</summary>
/// <param name="ResourceId">The reachable resource object id.</param>
/// <param name="Permissionship">Member or caveated (with missing context params).</param>
/// <param name="AfterResultCursor">
/// The opaque resume cursor positioned immediately after this resource, so a client can resume the
/// stream right after it (mirrors v1 <c>after_result_cursor</c>). Null when no cursor is available.
/// </param>
/// <param name="LookedUpAtToken">The ZedToken for the revision actually evaluated (constant across the stream).</param>
/// <remarks>
/// This record doubles as its own stream item for <see cref="ReverseOps.StreamLookupResources"/>: it
/// already carries the per-item resume cursor, so no wrapper is needed.
/// </remarks>
public sealed record FoundResourceWire(
    string ResourceId,
    Permissionship Permissionship,
    string? AfterResultCursor = null,
    string LookedUpAtToken = "");
