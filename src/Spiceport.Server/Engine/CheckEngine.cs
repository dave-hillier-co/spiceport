using System.Collections.Immutable;
using Spiceport.Core;
using Spiceport.Datastore;

namespace Spiceport.Engine;

/// <summary>
/// Evaluates SpiceDB-style permission checks against a schema model and a datastore reader.
/// </summary>
/// <remarks>
/// Implements <c>Check(resource, relation, subject, revision)</c> as a recursive walk of the
/// schema's relation graph: direct tuples (including ellipsis subjects and <c>:*</c> public
/// wildcards), computed usersets, tuple-to-userset arrows, and union / intersection / exclusion
/// set operations. Subject-relation walking is handled by dispatching a sub-check on each
/// intermediate non-terminal subject. Recursion is bounded by a configurable depth limit and a
/// visited-set cycle guard.
/// <para>
/// Caveated tuples carry a CEL condition that is combined across union (OR), intersection (AND),
/// exclusion (base AND NOT excluded) and arrows (tupleset caveat AND computed result), then
/// collapsed to a final verdict using the supplied request context: <see cref="Membership.Member"/>
/// (no caveat or caveat true), <see cref="Membership.NotMember"/> (caveat false / no path), or
/// <see cref="Membership.Caveated"/> (undetermined, with missing fields).
/// </para>
/// <para>
/// Expiration: a relationship whose <see cref="Relationship.OptionalExpiration"/> is at or before
/// the evaluation "now" (from the supplied clock, defaulting to system UTC) is treated as absent.
/// </para>
/// </remarks>
public sealed class CheckEngine
{
    /// <summary>The default maximum recursion depth.</summary>
    public const int DefaultMaxDepth = 50;

    private readonly ImmutableDictionary<string, NamespaceDefinition> _namespaces;
    private readonly int _maxDepth;
    private readonly CaveatEvaluator _caveatEvaluator;

    /// <summary>
    /// Creates a check engine over the given schema definitions.
    /// </summary>
    /// <param name="namespaces">The compiled namespace definitions that make up the schema.</param>
    /// <param name="maxDepth">The maximum recursion depth before a check fails.</param>
    public CheckEngine(IEnumerable<NamespaceDefinition> namespaces, int maxDepth = DefaultMaxDepth)
        : this(namespaces, caveats: null, maxDepth)
    {
    }

    /// <summary>
    /// Creates a check engine over the given schema definitions and caveat definitions.
    /// </summary>
    /// <param name="namespaces">The compiled namespace definitions that make up the schema.</param>
    /// <param name="caveats">The compiled caveat definitions, or null if the schema has none.</param>
    /// <param name="maxDepth">The maximum recursion depth before a check fails.</param>
    public CheckEngine(
        IEnumerable<NamespaceDefinition> namespaces,
        IEnumerable<CaveatDefinition>? caveats,
        int maxDepth = DefaultMaxDepth)
    {
        ArgumentNullException.ThrowIfNull(namespaces);
        _namespaces = namespaces.ToImmutableDictionary(ns => ns.Name);
        _caveatEvaluator = new CaveatEvaluator(caveats ?? []);
        _maxDepth = maxDepth;
    }

    /// <summary>
    /// Checks whether <paramref name="subject"/> is a member of
    /// <paramref name="resourceType"/>:<paramref name="resourceId"/>#<paramref name="relation"/>
    /// as of the given <paramref name="reader"/>'s snapshot.
    /// </summary>
    /// <param name="reader">A datastore reader pinned to the revision to evaluate against.</param>
    /// <param name="resourceType">The resource namespace.</param>
    /// <param name="resourceId">The resource object id.</param>
    /// <param name="relation">The relation or permission to check.</param>
    /// <param name="subject">The subject ONR (may carry a subrelation; use ellipsis for direct subjects).</param>
    /// <param name="caveatContext">Optional request-time caveat context (overrides relationship context).</param>
    /// <param name="evaluationTime">
    /// Optional pinned evaluation "now" for expiration filtering; defaults to system UTC.
    /// </param>
    /// <param name="cancellationToken">A cancellation token.</param>
    public Task<CheckResult> Check(
        IDatastoreReader reader,
        string resourceType,
        string resourceId,
        string relation,
        ObjectAndRelation subject,
        IReadOnlyDictionary<string, object?>? caveatContext = null,
        DateTimeOffset? evaluationTime = null,
        IRevision? atRevision = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(subject);
        var resource = new ObjectAndRelation(resourceType, resourceId, relation);
        return Check(reader, resource, subject, caveatContext, evaluationTime, atRevision, cancellationToken);
    }

    /// <summary>
    /// Checks whether <paramref name="subject"/> is a member of the given <paramref name="resource"/> ONR.
    /// </summary>
    /// <param name="reader">A datastore reader pinned to the revision to evaluate against.</param>
    /// <param name="resource">The resource ONR (object type, id and relation/permission).</param>
    /// <param name="subject">The subject ONR.</param>
    /// <param name="caveatContext">Optional request-time caveat context (overrides relationship context).</param>
    /// <param name="evaluationTime">
    /// Optional pinned evaluation "now" for expiration filtering; defaults to system UTC.
    /// </param>
    /// <param name="cancellationToken">A cancellation token.</param>
    public async Task<CheckResult> Check(
        IDatastoreReader reader,
        ObjectAndRelation resource,
        ObjectAndRelation subject,
        IReadOnlyDictionary<string, object?>? caveatContext = null,
        DateTimeOffset? evaluationTime = null,
        IRevision? atRevision = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(resource);
        ArgumentNullException.ThrowIfNull(subject);

        var now = evaluationTime ?? SystemClock.Instance.UtcNow;
        var state = new CheckState();

        // In-process drive: build a LocalDispatcher that resolves any revision back to the single
        // reader we were handed, then dispatch the top-level sub-problem through it. The request
        // carries a placeholder revision identity (so it is serializable later) while the resolver
        // closes over the pinned reader.
        var local = new LocalDispatcher(
            _namespaces,
            _ => reader,
            now,
            state);

        // The local dispatcher is both the top-level entry point and its own onward seam: with the
        // caller-side branch cache retired (the CheckGrain activation memo is the one cache now, see
        // docs/future-work.md item 1.3), a bare in-process Check simply runs the recursive walk once.
        IDispatcher dispatcher = local;

        // Carry the caller-supplied real read revision when available (informational identity only —
        // `_readerFor` above already closes over the pinned reader); fall back to the in-process
        // placeholder identity when none is supplied.
        var (revision, mode) = atRevision is null
            ? ((IRevision)InProcessRevision.Instance, RevisionMode.Optimized)
            : (atRevision, RevisionMode.Exact);
        var meta = new ResolverMeta(revision, _maxDepth, TraversalBloom.ForDepth(_maxDepth), mode);
        var request = new DispatchCheckRequest(resource, subject, meta);
        var result = await dispatcher.DispatchCheck(request, cancellationToken).ConfigureAwait(false);

        // Collapse stays the per-request, post-dispatch step: evaluate the accumulated caveat with
        // the request-time context. (CycleCut is computed/propagated but does not affect the verdict.)
        return Collapse(result, caveatContext, state.DispatchCount);
    }

    /// <summary>
    /// Collapses a pre-context dispatch result into the public, possibly-caveated verdict by evaluating
    /// the accumulated caveat expression against the request-time context.
    /// </summary>
    /// <remarks>
    /// This is the per-request, post-dispatch step. It is the same step the in-process <see cref="Check"/>
    /// applies after dispatch, exposed so callers driving the dispatch seam directly (e.g. the Orleans
    /// root dispatcher behind the API) collapse a shared, cached pre-context branch with their own
    /// request context. <c>CycleCut</c> is ignored: it does not affect the verdict.
    /// </remarks>
    /// <param name="result">The pre-context dispatch branch (membership + caveat expression).</param>
    /// <param name="caveatContext">The request-time caveat context, or null.</param>
    /// <param name="dispatchCount">The dispatch count to report on the result.</param>
    public CheckResult Collapse(
        DispatchCheckResult result,
        IReadOnlyDictionary<string, object?>? caveatContext,
        int dispatchCount = 0)
    {
        if (!result.Member)
            return new CheckResult(Membership.NotMember, dispatchCount);

        if (result.Caveat is null)
            return new CheckResult(Membership.Member, dispatchCount);

        var evaluated = _caveatEvaluator.EvaluateExpression(result.Caveat, caveatContext);
        return evaluated.Outcome switch
        {
            CaveatOutcome.DefinitelyTrue => new CheckResult(Membership.Member, dispatchCount),
            CaveatOutcome.DefinitelyFalse => new CheckResult(Membership.NotMember, dispatchCount),
            _ => new CheckResult(Membership.Caveated, dispatchCount, evaluated.MissingFields),
        };
    }
}
