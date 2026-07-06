using Spiceport.Core;

namespace Spiceport.Engine;

/// <summary>
/// A key identifying an in-flight (resource, subject) check. Recorded into the traversal bloom so the
/// dispatcher can detect a LIKELY loop and bypass a same-key grain re-entry; it does not gate verdicts.
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
/// evaluate against, how much recursion budget remains, and the traversal-bloom loop hint.
/// </summary>
/// <remarks>
/// Deliberately carries the revision <em>identity</em> (an <see cref="IRevision"/>), not an
/// <c>IDatastoreReader</c>, so the request is serializable: a dispatcher resolves a reader for the
/// revision itself. <see cref="Bloom"/> is an immutable (copy-on-add) bounded Bloom filter so each
/// sub-problem can extend the loop hint without mutating its parent's, at a bounded wire/CPU cost
/// (SpiceDB's traversal-bloom design). Termination rests SOLELY on <see cref="DepthRemaining"/>; the
/// bloom never affects a verdict. Its only job is the dispatcher's singleflight-style loop bypass: a
/// bloom hit forces a (correct) in-process step instead of re-entering a busy same-key grain.
/// </remarks>
/// <param name="Revision">The pinned revision identity to evaluate against.</param>
/// <param name="DepthRemaining">The remaining recursion depth budget (the sole termination guarantee).</param>
/// <param name="Bloom">The bounded traversal-bloom loop hint over (resource, subject) pairs on this path.</param>
/// <param name="SchemaHash">
/// The schema hash effective at <paramref name="Revision"/>, pinned ONCE at the check root (from the
/// resolved revision's authoritative <c>SchemaHashAt</c>) and carried unchanged to every child — schema
/// is a pure function of the pinned revision, exactly like the tuples read at it, so it must not drift
/// mid-tree. Null means no schema is persisted at the revision (the seed-only window): the dispatcher
/// and grain then fall back to the ambient current schema, which is the identical embedded seed on every
/// silo. When non-null it is authoritative and cluster-consistent (every silo folds the same log), so the
/// grain evaluates under exactly this schema rather than whatever its local <c>ISchemaProvider.Current</c>
/// happens to hold.
/// </param>
public sealed record ResolverMeta(
    IRevision Revision,
    int DepthRemaining,
    TraversalBloom Bloom,
    string? SchemaHash = null);

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
/// <see cref="CycleCut"/> is the "depth/loop-affected, non-cacheable" flag. It no longer signals a
/// visited-set cycle cut (correctness now rests solely on <see cref="ResolverMeta.DepthRemaining"/>);
/// it marks a subtree whose result may have been shaped by the depth budget or a loop bypass and so must
/// not be written to the shared branch cache. It is propagated upward and does not change the verdict; a
/// later caching phase uses it to avoid caching depth/loop-affected results.
/// <para>
/// <see cref="DepthRequired"/> is the recursion depth this result actually consumed below itself
/// (a leaf = 1, a combining node = max(children) + 1), mirroring SpiceDB's <c>ResponseMeta.DepthRequired</c>.
/// The caching layer gates a cache HIT on <c>DepthRemaining &gt;= DepthRequired</c> so a result computed
/// under a tighter budget — which may be truncated — is never reused under a larger budget.
/// </para>
/// </remarks>
/// <param name="Member">True if the subject is a (possibly caveated) member.</param>
/// <param name="Caveat">An optional caveat expression gating membership; null = unconditional.</param>
/// <param name="CycleCut">True if this subtree was depth- or loop-affected and must not be cached.</param>
/// <param name="DepthRequired">The recursion depth consumed below this node (leaf = 1).</param>
public readonly record struct DispatchCheckResult(
    bool Member, CaveatExpression? Caveat, bool CycleCut, int DepthRequired = 1)
{
    /// <summary>A fully-determined member (no caveat, not depth/loop-affected).</summary>
    public static readonly DispatchCheckResult DefiniteMember = new(true, null, false);

    /// <summary>Not a member (not depth/loop-affected).</summary>
    public static readonly DispatchCheckResult None = new(false, null, false);

    /// <summary>Not a member, marked depth/loop-affected (non-cacheable).</summary>
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
