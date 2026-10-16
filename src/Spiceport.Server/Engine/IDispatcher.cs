using System.Collections.Immutable;
using System.Globalization;
using System.Text;
using Spiceport.Core;

namespace Spiceport.Engine;

/// <summary>
/// A key identifying an in-flight (resource, subject) check. Recorded into the exact visited set so the
/// dispatcher can detect a genuine same-path revisit and bypass a same-key grain re-entry; it does not
/// gate verdicts.
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
    /// <summary>
    /// The separator between a part's length prefix and its characters (U+001F). It is not what makes
    /// the rendering injective -- nothing on the request path enforces the SpiceDB object-id grammar, so
    /// an id may contain it -- the length prefix is.
    /// </summary>
    private const char Separator = (char)0x1F;

    /// <summary>Builds a key from a (resource, subject) ONR pair.</summary>
    public static VisitKey Of(ObjectAndRelation resource, ObjectAndRelation subject) =>
        new(resource.ObjectType, resource.ObjectId, resource.Relation,
            subject.ObjectType, subject.ObjectId, subject.Relation);

    /// <summary>
    /// Renders this key as a single string, suitable for carrying across a wire boundary (e.g.
    /// <see cref="Grains.Abstractions.DispatchContext"/>'s ambient RequestContext). Each part is framed as
    /// <c>{length}U+001F{part}</c> (length in invariant decimal UTF-16 code units) and the frames are
    /// concatenated, so the rendering is injective for any six strings, whatever characters they contain.
    /// </summary>
    public string ToCanonicalString()
    {
        var builder = new StringBuilder();
        foreach (var part in (ReadOnlySpan<string>)[ResourceType, ResourceId, ResourceRelation, SubjectType, SubjectId, SubjectRelation])
            builder.Append(part.Length.ToString(CultureInfo.InvariantCulture)).Append(Separator).Append(part);
        return builder.ToString();
    }

    /// <summary>
    /// Parses a canonical string produced by <see cref="ToCanonicalString"/>. Throws
    /// <see cref="FormatException"/> on a malformed value (a bad, zero-padded or overrunning length
    /// prefix, a missing part, or trailing characters) rather than silently defaulting any field.
    /// </summary>
    public static VisitKey FromCanonicalString(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        var parts = new string[6];
        var at = 0;
        for (var i = 0; i < parts.Length; i++)
        {
            var mark = value.IndexOf(Separator, at);
            if (mark < 0)
                throw new FormatException(
                    $"Malformed VisitKey canonical string: expected 6 length-prefixed parts, got {i}.");
            var prefix = value.AsSpan(at, mark - at);
            // ToCanonicalString never emits a leading zero, so rejecting one keeps parsing the exact
            // inverse of rendering: each key has one canonical string and no other string parses to it.
            if ((prefix.Length > 1 && prefix[0] == '0')
                || !int.TryParse(prefix, NumberStyles.None, CultureInfo.InvariantCulture, out var length))
                throw new FormatException($"Malformed VisitKey canonical string: bad length prefix for part {i}.");
            var start = mark + 1;
            if (length > value.Length - start)
                throw new FormatException($"Malformed VisitKey canonical string: part {i} overruns the value.");
            parts[i] = value.Substring(start, length);
            at = start + length;
        }
        if (at != value.Length)
            throw new FormatException("Malformed VisitKey canonical string: trailing characters after 6 parts.");
        return new VisitKey(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]);
    }
}

/// <summary>
/// The cross-cutting metadata threaded through every dispatched sub-problem: which revision to
/// evaluate against, how much recursion budget remains, and the exact visited-set loop hint.
/// </summary>
/// <remarks>
/// Deliberately carries the revision <em>identity</em> (an <see cref="IRevision"/>), not an
/// <c>IDatastoreReader</c>, so the request is serializable: a dispatcher resolves a reader for the
/// revision itself. <see cref="Visited"/> is an immutable (copy-on-add) exact set of every
/// (resource, subject) pair visited on this path, bounded by the max recursion depth (at most 50
/// entries), so each sub-problem can extend it without mutating its parent's. Termination rests SOLELY
/// on <see cref="DepthRemaining"/>; the visited set never affects a verdict. Its only job is the
/// dispatcher's singleflight-style loop bypass: a hit means this sub-problem is genuinely already
/// in-flight on this path, and forces an in-process step instead of re-entering a busy same-key grain.
/// </remarks>
/// <param name="Revision">The pinned revision identity to evaluate against.</param>
/// <param name="DepthRemaining">The remaining recursion depth budget (the sole termination guarantee).</param>
/// <param name="Visited">The exact set of (resource, subject) pairs visited on this path (≤ max-depth entries).</param>
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
    ImmutableHashSet<VisitKey> Visited,
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
