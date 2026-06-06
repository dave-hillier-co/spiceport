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
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(subject);
        var resource = new ObjectAndRelation(resourceType, resourceId, relation);
        return Check(reader, resource, subject, caveatContext, evaluationTime, cancellationToken);
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
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(resource);
        ArgumentNullException.ThrowIfNull(subject);

        var now = evaluationTime ?? SystemClock.Instance.UtcNow;
        var ctx = new Context(reader, now, cancellationToken);
        var branch = await CheckOnr(ctx, resource, subject, _maxDepth, []).ConfigureAwait(false);

        return Collapse(branch, caveatContext, ctx.DispatchCount);
    }

    /// <summary>Collapses an internal branch result into the public, possibly-caveated verdict.</summary>
    private CheckResult Collapse(
        Branch branch,
        IReadOnlyDictionary<string, object?>? caveatContext,
        int dispatchCount)
    {
        if (!branch.Member)
            return new CheckResult(Membership.NotMember, dispatchCount);

        if (branch.Caveat is null)
            return new CheckResult(Membership.Member, dispatchCount);

        var result = _caveatEvaluator.EvaluateExpression(branch.Caveat, caveatContext);
        return result.Outcome switch
        {
            CaveatOutcome.DefinitelyTrue => new CheckResult(Membership.Member, dispatchCount),
            CaveatOutcome.DefinitelyFalse => new CheckResult(Membership.NotMember, dispatchCount),
            _ => new CheckResult(Membership.Caveated, dispatchCount, result.MissingFields),
        };
    }

    /// <summary>Mutable per-check evaluation context.</summary>
    private sealed class Context(IDatastoreReader reader, DateTimeOffset now, CancellationToken cancellationToken)
    {
        public IDatastoreReader Reader { get; } = reader;
        public DateTimeOffset Now { get; } = now;
        public CancellationToken CancellationToken { get; } = cancellationToken;
        public int DispatchCount { get; set; }
    }

    /// <summary>A visited-set key identifying an in-flight (resource, subject) check.</summary>
    private readonly record struct VisitKey(
        string ResourceType, string ResourceId, string ResourceRelation,
        string SubjectType, string SubjectId, string SubjectRelation);

    private static VisitKey KeyOf(ObjectAndRelation resource, ObjectAndRelation subject) =>
        new(resource.ObjectType, resource.ObjectId, resource.Relation,
            subject.ObjectType, subject.ObjectId, subject.Relation);

    /// <summary>The core recursive entry point for a single (resource, subject) pair.</summary>
    private async Task<Branch> CheckOnr(
        Context ctx,
        ObjectAndRelation resource,
        ObjectAndRelation subject,
        int depth,
        ImmutableHashSet<VisitKey> visited)
    {
        ctx.CancellationToken.ThrowIfCancellationRequested();
        ctx.DispatchCount++;

        if (depth <= 0)
            return Branch.None;

        // Fast path: the resource ONR is literally the subject ONR.
        if (OnrEquals(resource, subject))
            return Branch.DefiniteMember;

        // Cycle guard.
        var key = KeyOf(resource, subject);
        if (visited.Contains(key))
            return Branch.None;
        visited = visited.Add(key);

        var relation = LookupRelation(resource.ObjectType, resource.Relation);
        if (relation is null)
            return Branch.None;

        // Permission: evaluate its rewrite. Base relation: match against written tuples.
        return relation.UsersetRewrite is { } rewrite
            ? await CheckRewrite(ctx, resource, subject, rewrite.Operation, depth, visited).ConfigureAwait(false)
            : await CheckDirect(ctx, resource, subject, depth, visited).ConfigureAwait(false);
    }

    /// <summary>Matches a base relation's directly-written tuples, walking non-terminal subjects.</summary>
    private async Task<Branch> CheckDirect(
        Context ctx,
        ObjectAndRelation resource,
        ObjectAndRelation subject,
        int depth,
        ImmutableHashSet<VisitKey> visited)
    {
        var filter = new RelationshipsFilter
        {
            OptionalResourceType = resource.ObjectType,
            OptionalResourceIds = [resource.ObjectId],
            OptionalResourceRelation = resource.Relation,
        };

        // Accumulated union over all matching tuples for this (resource, subject).
        var found = Branch.None;

        // Non-terminal intermediates (with the caveat on the tuple reaching them) to recurse into.
        List<(ObjectAndRelation Onr, CaveatExpression? Parent)>? intermediates = null;

        await foreach (var rel in ctx.Reader.QueryRelationships(filter, ctx.CancellationToken).ConfigureAwait(false))
        {
            if (IsExpired(ctx, rel))
                continue;

            var s = rel.Subject;
            var tupleCaveat = CaveatOf(rel);

            // Direct exact match (e.g. tuple subject is user:alice#... and so is the subject).
            if (OnrEquals(s, subject))
            {
                found = found.Or(Branch.CaveatedMember(tupleCaveat));
                if (found.IsDetermined)
                    return found;
                continue;
            }

            // Public wildcard tuple: e.g. user:* matches any subject of type "user".
            if (s.IsPublicWildcard && s.ObjectType == subject.ObjectType && s.Relation == subject.Relation)
            {
                found = found.Or(Branch.CaveatedMember(tupleCaveat));
                if (found.IsDetermined)
                    return found;
                continue;
            }

            // Non-terminal subject (a userset like group:eng#member): recurse into it.
            if (s.Relation != CoreConstants.Ellipsis && !s.IsPublicWildcard)
            {
                (intermediates ??= []).Add((s, tupleCaveat));
            }
        }

        if (intermediates is null)
            return found;

        foreach (var (intermediate, parent) in intermediates)
        {
            var sub = await CheckOnr(ctx, intermediate, subject, depth - 1, visited).ConfigureAwait(false);
            if (!sub.Member)
                continue;

            // The tuple's own caveat ANDs with the computed result of the userset.
            var combined = Branch.CaveatedMember(CaveatExpression.CombineAnd(parent, sub.Caveat));
            found = found.Or(combined);
            if (found.IsDetermined)
                return found;
        }

        return found;
    }

    /// <summary>Evaluates a set operation (union / intersection / exclusion).</summary>
    private async Task<Branch> CheckRewrite(
        Context ctx,
        ObjectAndRelation resource,
        ObjectAndRelation subject,
        SetOperation operation,
        int depth,
        ImmutableHashSet<VisitKey> visited)
    {
        switch (operation.Type)
        {
            case SetOperationType.Union:
            {
                var acc = Branch.None;
                foreach (var child in operation.Children)
                {
                    var b = await CheckChild(ctx, resource, subject, child, depth, visited).ConfigureAwait(false);
                    acc = acc.Or(b);
                    if (acc.IsDetermined)
                        return acc;
                }
                return acc;
            }

            case SetOperationType.Intersection:
            {
                if (operation.Children.Count == 0)
                    return Branch.None;
                var acc = Branch.DefiniteMember;
                foreach (var child in operation.Children)
                {
                    var b = await CheckChild(ctx, resource, subject, child, depth, visited).ConfigureAwait(false);
                    if (!b.Member)
                        return Branch.None; // short-circuit: empty intersection.
                    acc = acc.And(b);
                }
                return acc;
            }

            case SetOperationType.Exclusion:
            {
                if (operation.Children.Count == 0)
                    return Branch.None;

                // base AND NOT child1 AND NOT child2 ...
                var acc = await CheckChild(ctx, resource, subject, operation.Children[0], depth, visited).ConfigureAwait(false);
                if (!acc.Member)
                    return Branch.None;

                for (var i = 1; i < operation.Children.Count; i++)
                {
                    var excluded = await CheckChild(ctx, resource, subject, operation.Children[i], depth, visited).ConfigureAwait(false);
                    acc = acc.Subtract(excluded);
                    if (!acc.Member)
                        return Branch.None;
                }
                return acc;
            }

            default:
                return Branch.None;
        }
    }

    /// <summary>Evaluates a single set-operation operand.</summary>
    private async Task<Branch> CheckChild(
        Context ctx,
        ObjectAndRelation resource,
        ObjectAndRelation subject,
        SetOperationChild child,
        int depth,
        ImmutableHashSet<VisitKey> visited)
    {
        switch (child)
        {
            case SetOperationChild.This:
                // Legacy "_this": the directly-written tuples on this relation.
                return await CheckDirect(ctx, resource, subject, depth, visited).ConfigureAwait(false);

            case SetOperationChild.Nil:
                return Branch.None;

            case SetOperationChild.Self:
                return resource.ObjectType == subject.ObjectType
                       && resource.ObjectId == subject.ObjectId
                       && subject.Relation == CoreConstants.Ellipsis
                    ? Branch.DefiniteMember
                    : Branch.None;

            case SetOperationChild.ComputedUsersetChild(var cu):
                return await CheckComputedUserset(ctx, resource, subject, cu, depth, visited).ConfigureAwait(false);

            case SetOperationChild.TupleToUsersetChild(var ttu):
                return await CheckTupleToUserset(
                    ctx, resource, subject, ttu.TuplesetRelation, ttu.ComputedUserset,
                    TupleToUsersetFunction.Any, depth, visited).ConfigureAwait(false);

            case SetOperationChild.FunctionedTupleToUsersetChild(var fttu):
                return await CheckTupleToUserset(
                    ctx, resource, subject, fttu.TuplesetRelation, fttu.ComputedUserset,
                    fttu.Function, depth, visited).ConfigureAwait(false);

            case SetOperationChild.NestedRewrite(var nested):
                return await CheckRewrite(ctx, resource, subject, nested.Operation, depth, visited).ConfigureAwait(false);

            default:
                return Branch.None;
        }
    }

    /// <summary>
    /// Evaluates a computed userset: re-check the subject against a different relation on the
    /// resource itself.
    /// </summary>
    private async Task<Branch> CheckComputedUserset(
        Context ctx,
        ObjectAndRelation resource,
        ObjectAndRelation subject,
        ComputedUserset cu,
        int depth,
        ImmutableHashSet<VisitKey> visited)
    {
        var target = resource.WithRelation(cu.Relation);
        return await CheckOnr(ctx, target, subject, depth - 1, visited).ConfigureAwait(false);
    }

    /// <summary>
    /// Evaluates a tuple-to-userset arrow: walk the tupleset relation on the resource, then for each
    /// reached object compute the userset relation. The tupleset tuple's own caveat ANDs with the
    /// computed result. <c>Any</c> unions the per-target results; <c>All</c> intersects them.
    /// </summary>
    private async Task<Branch> CheckTupleToUserset(
        Context ctx,
        ObjectAndRelation resource,
        ObjectAndRelation subject,
        string tuplesetRelation,
        ComputedUserset computed,
        TupleToUsersetFunction function,
        int depth,
        ImmutableHashSet<VisitKey> visited)
    {
        var filter = new RelationshipsFilter
        {
            OptionalResourceType = resource.ObjectType,
            OptionalResourceIds = [resource.ObjectId],
            OptionalResourceRelation = tuplesetRelation,
        };

        // Each reached target, paired with the caveat on the tupleset tuple that reached it.
        List<(ObjectAndRelation Onr, CaveatExpression? Parent)>? targets = null;

        await foreach (var rel in ctx.Reader.QueryRelationships(filter, ctx.CancellationToken).ConfigureAwait(false))
        {
            if (IsExpired(ctx, rel))
                continue;

            var reached = rel.Subject;

            // Wildcards cannot have a relation computed on them; skip.
            if (reached.IsPublicWildcard)
                continue;

            var target = new ObjectAndRelation(reached.ObjectType, reached.ObjectId, computed.Relation);
            (targets ??= []).Add((target, CaveatOf(rel)));
        }

        if (targets is null)
        {
            // No intermediates reached via the tupleset relation: neither ALL nor ANY is a
            // member. SpiceDB's `.all()` is not vacuously true over an empty set — with no
            // relationships there is nothing to grant access, so the result is not-member.
            return Branch.None;
        }

        if (function == TupleToUsersetFunction.All)
        {
            var acc = Branch.DefiniteMember;
            foreach (var (target, parent) in targets)
            {
                var sub = await CheckOnr(ctx, target, subject, depth - 1, visited).ConfigureAwait(false);
                if (!sub.Member)
                    return Branch.None;
                acc = acc.And(Branch.CaveatedMember(CaveatExpression.CombineAnd(parent, sub.Caveat)));
            }
            return acc;
        }

        var any = Branch.None;
        foreach (var (target, parent) in targets)
        {
            var sub = await CheckOnr(ctx, target, subject, depth - 1, visited).ConfigureAwait(false);
            if (!sub.Member)
                continue;
            any = any.Or(Branch.CaveatedMember(CaveatExpression.CombineAnd(parent, sub.Caveat)));
            if (any.IsDetermined)
                return any;
        }
        return any;
    }

    private Relation? LookupRelation(string objectType, string relationName)
    {
        if (!_namespaces.TryGetValue(objectType, out var ns))
            return null;
        foreach (var r in ns.Relations)
        {
            if (r.Name == relationName)
                return r;
        }
        return null;
    }

    /// <summary>True if the relationship has expired as of the evaluation "now".</summary>
    private static bool IsExpired(Context ctx, Relationship rel) =>
        rel.OptionalExpiration is { } exp && exp <= ctx.Now;

    /// <summary>Wraps a tuple's optional caveat as a leaf expression, or null if uncaveated.</summary>
    private static CaveatExpression? CaveatOf(Relationship rel) =>
        rel.OptionalCaveat is { } c ? CaveatExpression.FromCaveat(c) : null;

    private static bool OnrEquals(ObjectAndRelation a, ObjectAndRelation b) =>
        a.ObjectType == b.ObjectType && a.ObjectId == b.ObjectId && a.Relation == b.Relation;
}
