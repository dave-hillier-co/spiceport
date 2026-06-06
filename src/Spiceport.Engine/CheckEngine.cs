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
/// visited-set cycle guard. Caveats are treated as opaque and are not evaluated (a matching
/// caveated tuple is currently treated as a member).
/// </remarks>
public sealed class CheckEngine
{
    /// <summary>The default maximum recursion depth.</summary>
    public const int DefaultMaxDepth = 50;

    private readonly ImmutableDictionary<string, NamespaceDefinition> _namespaces;
    private readonly int _maxDepth;

    /// <summary>
    /// Creates a check engine over the given schema definitions.
    /// </summary>
    /// <param name="namespaces">The compiled namespace definitions that make up the schema.</param>
    /// <param name="maxDepth">The maximum recursion depth before a check fails.</param>
    public CheckEngine(IEnumerable<NamespaceDefinition> namespaces, int maxDepth = DefaultMaxDepth)
    {
        ArgumentNullException.ThrowIfNull(namespaces);
        _namespaces = namespaces.ToImmutableDictionary(ns => ns.Name);
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
    /// <param name="cancellationToken">A cancellation token.</param>
    public Task<CheckResult> Check(
        IDatastoreReader reader,
        string resourceType,
        string resourceId,
        string relation,
        ObjectAndRelation subject,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(subject);
        var resource = new ObjectAndRelation(resourceType, resourceId, relation);
        return Check(reader, resource, subject, cancellationToken);
    }

    /// <summary>
    /// Checks whether <paramref name="subject"/> is a member of the given <paramref name="resource"/> ONR.
    /// </summary>
    public async Task<CheckResult> Check(
        IDatastoreReader reader,
        ObjectAndRelation resource,
        ObjectAndRelation subject,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(resource);
        ArgumentNullException.ThrowIfNull(subject);

        var ctx = new Context(reader, cancellationToken);
        var verdict = await CheckOnr(ctx, resource, subject, _maxDepth, []).ConfigureAwait(false);
        return new CheckResult(verdict, ctx.DispatchCount);
    }

    /// <summary>Mutable per-check evaluation context.</summary>
    private sealed class Context(IDatastoreReader reader, CancellationToken cancellationToken)
    {
        public IDatastoreReader Reader { get; } = reader;
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
    private async Task<Membership> CheckOnr(
        Context ctx,
        ObjectAndRelation resource,
        ObjectAndRelation subject,
        int depth,
        ImmutableHashSet<VisitKey> visited)
    {
        ctx.CancellationToken.ThrowIfCancellationRequested();
        ctx.DispatchCount++;

        if (depth <= 0)
            return Membership.NotMember;

        // Fast path: the resource ONR is literally the subject ONR.
        if (OnrEquals(resource, subject))
            return Membership.Member;

        // Cycle guard.
        var key = KeyOf(resource, subject);
        if (visited.Contains(key))
            return Membership.NotMember;
        visited = visited.Add(key);

        var relation = LookupRelation(resource.ObjectType, resource.Relation);
        if (relation is null)
            return Membership.NotMember;

        // Permission: evaluate its rewrite. Base relation: match against written tuples.
        return relation.UsersetRewrite is { } rewrite
            ? await CheckRewrite(ctx, resource, subject, rewrite.Operation, depth, visited).ConfigureAwait(false)
            : await CheckDirect(ctx, resource, subject, depth, visited).ConfigureAwait(false);
    }

    /// <summary>Matches a base relation's directly-written tuples, walking non-terminal subjects.</summary>
    private async Task<Membership> CheckDirect(
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

        // Collect non-terminal intermediates to recurse into after the direct scan.
        List<ObjectAndRelation>? intermediates = null;

        await foreach (var rel in ctx.Reader.QueryRelationships(filter, ctx.CancellationToken).ConfigureAwait(false))
        {
            var s = rel.Subject;

            // Direct exact match (e.g. subject is user:alice#... and tuple subject is user:alice#...).
            if (OnrEquals(s, subject))
                return Membership.Member;

            // Public wildcard tuple: e.g. user:* matches any subject of type "user".
            if (s.IsPublicWildcard && s.ObjectType == subject.ObjectType && s.Relation == subject.Relation)
                return Membership.Member;

            // Non-terminal subject (a userset like group:eng#member): recurse into it.
            if (s.Relation != CoreConstants.Ellipsis && !s.IsPublicWildcard)
            {
                (intermediates ??= []).Add(s);
            }
        }

        if (intermediates is null)
            return Membership.NotMember;

        foreach (var intermediate in intermediates)
        {
            var sub = await CheckOnr(ctx, intermediate, subject, depth - 1, visited).ConfigureAwait(false);
            if (sub == Membership.Member)
                return Membership.Member;
        }

        return Membership.NotMember;
    }

    /// <summary>Evaluates a set operation (union / intersection / exclusion).</summary>
    private async Task<Membership> CheckRewrite(
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
                foreach (var child in operation.Children)
                {
                    if (await CheckChild(ctx, resource, subject, child, depth, visited).ConfigureAwait(false) == Membership.Member)
                        return Membership.Member;
                }
                return Membership.NotMember;

            case SetOperationType.Intersection:
                foreach (var child in operation.Children)
                {
                    if (await CheckChild(ctx, resource, subject, child, depth, visited).ConfigureAwait(false) != Membership.Member)
                        return Membership.NotMember;
                }
                return Membership.Member;

            case SetOperationType.Exclusion:
                if (operation.Children.Count == 0)
                    return Membership.NotMember;

                // base AND NOT child1 AND NOT child2 ...
                var baseVerdict = await CheckChild(ctx, resource, subject, operation.Children[0], depth, visited).ConfigureAwait(false);
                if (baseVerdict != Membership.Member)
                    return Membership.NotMember;

                for (var i = 1; i < operation.Children.Count; i++)
                {
                    if (await CheckChild(ctx, resource, subject, operation.Children[i], depth, visited).ConfigureAwait(false) == Membership.Member)
                        return Membership.NotMember;
                }
                return Membership.Member;

            default:
                return Membership.NotMember;
        }
    }

    /// <summary>Evaluates a single set-operation operand.</summary>
    private async Task<Membership> CheckChild(
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
                return Membership.NotMember;

            case SetOperationChild.Self:
                return resource.ObjectType == subject.ObjectType
                       && resource.ObjectId == subject.ObjectId
                       && subject.Relation == CoreConstants.Ellipsis
                    ? Membership.Member
                    : Membership.NotMember;

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
                return Membership.NotMember;
        }
    }

    /// <summary>
    /// Evaluates a computed userset: re-check the subject against a different relation, either
    /// on the same resource (TupleObject) or — when reached via an arrow — on a traversed object.
    /// </summary>
    private async Task<Membership> CheckComputedUserset(
        Context ctx,
        ObjectAndRelation resource,
        ObjectAndRelation subject,
        ComputedUserset cu,
        int depth,
        ImmutableHashSet<VisitKey> visited)
    {
        // For a rewrite child, the computed userset is always on the resource itself.
        var target = resource.WithRelation(cu.Relation);
        return await CheckOnr(ctx, target, subject, depth - 1, visited).ConfigureAwait(false);
    }

    /// <summary>
    /// Evaluates a tuple-to-userset arrow: walk the tupleset relation on the resource, then for
    /// each reached object compute the userset relation. <c>Any</c> requires one match; <c>All</c>
    /// requires every reached object to match.
    /// </summary>
    private async Task<Membership> CheckTupleToUserset(
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

        // The targets reached via the tupleset; for each we compute `computed.Relation`.
        List<ObjectAndRelation>? targets = null;

        await foreach (var rel in ctx.Reader.QueryRelationships(filter, ctx.CancellationToken).ConfigureAwait(false))
        {
            var reached = rel.Subject;

            // Wildcards cannot have a relation computed on them; skip.
            if (reached.IsPublicWildcard)
                continue;

            var target = new ObjectAndRelation(reached.ObjectType, reached.ObjectId, computed.Relation);
            (targets ??= []).Add(target);
        }

        if (targets is null)
        {
            // No intermediates: ALL over an empty set is vacuously true; ANY is false.
            return function == TupleToUsersetFunction.All ? Membership.Member : Membership.NotMember;
        }

        if (function == TupleToUsersetFunction.All)
        {
            foreach (var target in targets)
            {
                if (await CheckOnr(ctx, target, subject, depth - 1, visited).ConfigureAwait(false) != Membership.Member)
                    return Membership.NotMember;
            }
            return Membership.Member;
        }

        foreach (var target in targets)
        {
            if (await CheckOnr(ctx, target, subject, depth - 1, visited).ConfigureAwait(false) == Membership.Member)
                return Membership.Member;
        }
        return Membership.NotMember;
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

    private static bool OnrEquals(ObjectAndRelation a, ObjectAndRelation b) =>
        a.ObjectType == b.ObjectType && a.ObjectId == b.ObjectId && a.Relation == b.Relation;
}
