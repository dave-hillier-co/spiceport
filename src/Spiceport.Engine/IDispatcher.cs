using System.Collections.Immutable;
using Spiceport.Core;

namespace Spiceport.Engine;

/// <summary>
/// A visited-set key identifying an in-flight (resource, subject) check, used by the cycle guard.
/// </summary>
/// <remarks>
/// Carried inside <see cref="ResolverMeta"/> rather than a closure so that a dispatch request is a
/// self-contained, serializable description of a sub-problem (a later phase may dispatch across
/// process boundaries).
/// </remarks>
/// <param name="ResourceType">The resource namespace.</param>
/// <param name="ResourceId">The resource object id.</param>
/// <param name="ResourceRelation">The resource relation/permission.</param>
/// <param name="SubjectType">The subject namespace.</param>
/// <param name="SubjectId">The subject object id.</param>
/// <param name="SubjectRelation">The subject relation (ellipsis for direct subjects).</param>
public readonly record struct VisitKey(
    string ResourceType, string ResourceId, string ResourceRelation,
    string SubjectType, string SubjectId, string SubjectRelation)
{
    /// <summary>Builds a key from a (resource, subject) ONR pair.</summary>
    public static VisitKey Of(ObjectAndRelation resource, ObjectAndRelation subject) =>
        new(resource.ObjectType, resource.ObjectId, resource.Relation,
            subject.ObjectType, subject.ObjectId, subject.Relation);
}

/// <summary>
/// The cross-cutting metadata threaded through every dispatched sub-problem: which revision to
/// evaluate against, how much recursion budget remains, and the cycle-guard visited set.
/// </summary>
/// <remarks>
/// Deliberately carries the revision <em>identity</em> (an <see cref="IRevision"/>), not an
/// <c>IDatastoreReader</c>, so the request is serializable: a dispatcher resolves a reader for the
/// revision itself. <see cref="Visited"/> is an immutable set so each sub-problem can extend it
/// without mutating its parent's.
/// </remarks>
/// <param name="Revision">The pinned revision identity to evaluate against.</param>
/// <param name="DepthRemaining">The remaining recursion depth budget.</param>
/// <param name="Visited">The set of (resource, subject) pairs currently in-flight on this path.</param>
public sealed record ResolverMeta(
    IRevision Revision,
    int DepthRemaining,
    ImmutableHashSet<VisitKey> Visited);

/// <summary>
/// A single sub-problem to evaluate: "is <paramref name="Subject"/> a member of
/// <paramref name="Resource"/>?", together with the cross-cutting <paramref name="Meta"/>.
/// </summary>
/// <param name="Resource">The resource ONR (object type, id and relation/permission).</param>
/// <param name="Subject">The subject ONR.</param>
/// <param name="Meta">The revision, depth budget and cycle-guard set for this sub-problem.</param>
public sealed record DispatchCheckRequest(
    ObjectAndRelation Resource,
    ObjectAndRelation Subject,
    ResolverMeta Meta);

/// <summary>
/// The result of dispatching one sub-problem: the engine's internal branch (tri-state membership
/// plus an optional gating caveat) augmented with a cycle-cut flag.
/// </summary>
/// <remarks>
/// <see cref="CycleCut"/> is true when the computation bottomed out on a visited-set cutoff anywhere
/// in its subtree. It is propagated upward and does not change the verdict; a later caching phase
/// uses it to avoid caching cycle-affected results.
/// </remarks>
/// <param name="Member">True if the subject is a (possibly caveated) member.</param>
/// <param name="Caveat">An optional caveat expression gating membership; null = unconditional.</param>
/// <param name="CycleCut">True if a visited-set cutoff was hit anywhere in this subtree.</param>
public readonly record struct DispatchCheckResult(bool Member, CaveatExpression? Caveat, bool CycleCut)
{
    /// <summary>A fully-determined member (no caveat, no cycle cut).</summary>
    public static readonly DispatchCheckResult DefiniteMember = new(true, null, false);

    /// <summary>Not a member (no cycle cut).</summary>
    public static readonly DispatchCheckResult None = new(false, null, false);

    /// <summary>Not a member, reached via a visited-set cutoff (cycle cut set).</summary>
    public static readonly DispatchCheckResult Cut = new(false, null, true);

    /// <summary>True if this is a member with no caveat (enables short-circuiting unions/arrows).</summary>
    public bool IsDetermined => Member && Caveat is null;
}

/// <summary>
/// The dispatch seam. Every recursive sub-problem in a check flows through
/// <see cref="DispatchCheck"/> rather than a direct self-call, so the work can be intercepted,
/// counted, cached or (later) relocated to another process.
/// </summary>
public interface IDispatcher
{
    /// <summary>Evaluates a single sub-problem and returns its branch result.</summary>
    /// <param name="request">The sub-problem (resource, subject, meta).</param>
    /// <param name="ct">A cancellation token.</param>
    Task<DispatchCheckResult> DispatchCheck(DispatchCheckRequest request, CancellationToken ct);
}
