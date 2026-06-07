using System.Collections.Immutable;
using Spiceport.Core;
using Spiceport.Datastore;

namespace Spiceport.Engine;

/// <summary>
/// An <see cref="IDispatcher"/> that performs one expansion step of the check graph in-process and
/// calls back through the (injected) <see cref="Dispatcher"/> for every further sub-problem.
/// </summary>
/// <remarks>
/// The local dispatcher and its "step" logic are the same object: <see cref="DispatchCheck"/> does
/// exactly one expansion (resolve the relation, match tuples, evaluate a rewrite) and routes any
/// recursion back out through <see cref="Dispatcher"/>. By default <see cref="Dispatcher"/> is
/// <c>this</c>, but a caller may set it to a decorator (e.g. a counting or caching wrapper) so that
/// every sub-problem flows through that decorator. Readers are resolved per request via the supplied
/// <see cref="_readerFor"/> resolver, keyed by the request's revision identity; for in-process use
/// this typically returns a reader pinned to the revision the public <c>Check</c> was given.
/// </remarks>
public sealed class LocalDispatcher : IDispatcher
{
    private readonly ImmutableDictionary<string, NamespaceDefinition> _namespaces;
    private readonly Func<IRevision, IDatastoreReader> _readerFor;
    private readonly DateTimeOffset _now;
    private readonly CheckState _state;

    /// <summary>
    /// Creates a local dispatcher over the given schema, reader resolver and evaluation clock.
    /// </summary>
    /// <param name="namespaces">The compiled namespace definitions keyed by name.</param>
    /// <param name="readerFor">Resolves a snapshot reader for a request's revision identity.</param>
    /// <param name="now">The pinned evaluation "now" used to filter expired relationships.</param>
    /// <param name="state">Shared per-check state (dispatch counter, cycle-cut accounting).</param>
    public LocalDispatcher(
        ImmutableDictionary<string, NamespaceDefinition> namespaces,
        Func<IRevision, IDatastoreReader> readerFor,
        DateTimeOffset now,
        CheckState state)
    {
        ArgumentNullException.ThrowIfNull(namespaces);
        ArgumentNullException.ThrowIfNull(readerFor);
        ArgumentNullException.ThrowIfNull(state);
        _namespaces = namespaces;
        _readerFor = readerFor;
        _now = now;
        _state = state;
        Dispatcher = this;
    }

    /// <summary>
    /// The dispatcher used for sub-problems. Defaults to <c>this</c>; set to a decorator to route
    /// every recursive sub-problem through it.
    /// </summary>
    public IDispatcher Dispatcher { get; set; }

    /// <inheritdoc/>
    public async Task<DispatchCheckResult> DispatchCheck(DispatchCheckRequest request, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(request);
        ct.ThrowIfCancellationRequested();
        _state.DispatchCount++;

        var resource = request.Resource;
        var subject = request.Subject;
        var meta = request.Meta;

        if (meta.DepthRemaining <= 0)
            return DispatchCheckResult.None;

        // Fast path: the resource ONR is literally the subject ONR.
        if (OnrEquals(resource, subject))
            return DispatchCheckResult.DefiniteMember;

        // Cycle guard: a repeated (resource, subject) on this path is a cut.
        var key = VisitKey.Of(resource, subject);
        if (meta.Visited.Contains(key))
            return DispatchCheckResult.Cut;
        meta = meta with { Visited = meta.Visited.Add(key) };

        var relation = LookupRelation(resource.ObjectType, resource.Relation);
        if (relation is null)
            return DispatchCheckResult.None;

        var reader = _readerFor(meta.Revision);

        return relation.UsersetRewrite is { } rewrite
            ? await CheckRewrite(reader, resource, subject, rewrite.Operation, meta, ct).ConfigureAwait(false)
            : await CheckDirect(reader, resource, subject, meta, ct).ConfigureAwait(false);
    }

    /// <summary>Dispatches a sub-problem at a decremented depth through the injected dispatcher.</summary>
    private Task<DispatchCheckResult> Sub(
        ObjectAndRelation resource, ObjectAndRelation subject, ResolverMeta meta, CancellationToken ct)
    {
        var subMeta = meta with { DepthRemaining = meta.DepthRemaining - 1 };
        return Dispatcher.DispatchCheck(new DispatchCheckRequest(resource, subject, subMeta), ct);
    }

    /// <summary>Matches a base relation's directly-written tuples, walking non-terminal subjects.</summary>
    private async Task<DispatchCheckResult> CheckDirect(
        IDatastoreReader reader,
        ObjectAndRelation resource,
        ObjectAndRelation subject,
        ResolverMeta meta,
        CancellationToken ct)
    {
        var filter = new RelationshipsFilter
        {
            OptionalResourceType = resource.ObjectType,
            OptionalResourceIds = [resource.ObjectId],
            OptionalResourceRelation = resource.Relation,
        };

        var found = DispatchCheckResult.None;
        List<(ObjectAndRelation Onr, CaveatExpression? Parent)>? intermediates = null;

        await foreach (var rel in reader.QueryRelationships(filter, ct).ConfigureAwait(false))
        {
            if (IsExpired(rel))
                continue;

            var s = rel.Subject;
            var tupleCaveat = CaveatOf(rel);

            if (OnrEquals(s, subject))
            {
                found = Or(found, CaveatedMember(tupleCaveat));
                if (found.IsDetermined)
                    return found;
                continue;
            }

            if (s.IsPublicWildcard && s.ObjectType == subject.ObjectType && s.Relation == subject.Relation)
            {
                found = Or(found, CaveatedMember(tupleCaveat));
                if (found.IsDetermined)
                    return found;
                continue;
            }

            if (s.Relation != CoreConstants.Ellipsis && !s.IsPublicWildcard)
            {
                (intermediates ??= []).Add((s, tupleCaveat));
            }
        }

        if (intermediates is null)
            return found;

        foreach (var (intermediate, parent) in intermediates)
        {
            var sub = await Sub(intermediate, subject, meta, ct).ConfigureAwait(false);
            found = found with { CycleCut = found.CycleCut || sub.CycleCut };
            if (!sub.Member)
                continue;

            var combined = CaveatedMember(CaveatExpression.CombineAnd(parent, sub.Caveat));
            found = Or(found, combined);
            if (found.Member && found.Caveat is null)
                return found;
        }

        return found;
    }

    /// <summary>Evaluates a set operation (union / intersection / exclusion).</summary>
    private async Task<DispatchCheckResult> CheckRewrite(
        IDatastoreReader reader,
        ObjectAndRelation resource,
        ObjectAndRelation subject,
        SetOperation operation,
        ResolverMeta meta,
        CancellationToken ct)
    {
        switch (operation.Type)
        {
            case SetOperationType.Union:
            {
                var acc = DispatchCheckResult.None;
                foreach (var child in operation.Children)
                {
                    var b = await CheckChild(reader, resource, subject, child, meta, ct).ConfigureAwait(false);
                    acc = acc with { CycleCut = acc.CycleCut || b.CycleCut };
                    acc = Or(acc, b);
                    if (acc.Member && acc.Caveat is null)
                        return acc;
                }
                return acc;
            }

            case SetOperationType.Intersection:
            {
                if (operation.Children.Count == 0)
                    return DispatchCheckResult.None;
                var acc = DispatchCheckResult.DefiniteMember;
                var cut = false;
                foreach (var child in operation.Children)
                {
                    var b = await CheckChild(reader, resource, subject, child, meta, ct).ConfigureAwait(false);
                    cut = cut || b.CycleCut;
                    if (!b.Member)
                        return DispatchCheckResult.None with { CycleCut = cut };
                    acc = And(acc, b);
                }
                return acc with { CycleCut = cut };
            }

            case SetOperationType.Exclusion:
            {
                if (operation.Children.Count == 0)
                    return DispatchCheckResult.None;

                var acc = await CheckChild(reader, resource, subject, operation.Children[0], meta, ct).ConfigureAwait(false);
                var cut = acc.CycleCut;
                if (!acc.Member)
                    return DispatchCheckResult.None with { CycleCut = cut };

                for (var i = 1; i < operation.Children.Count; i++)
                {
                    var excluded = await CheckChild(reader, resource, subject, operation.Children[i], meta, ct).ConfigureAwait(false);
                    cut = cut || excluded.CycleCut;
                    acc = Subtract(acc, excluded);
                    if (!acc.Member)
                        return DispatchCheckResult.None with { CycleCut = cut };
                }
                return acc with { CycleCut = cut };
            }

            default:
                return DispatchCheckResult.None;
        }
    }

    /// <summary>Evaluates a single set-operation operand.</summary>
    private async Task<DispatchCheckResult> CheckChild(
        IDatastoreReader reader,
        ObjectAndRelation resource,
        ObjectAndRelation subject,
        SetOperationChild child,
        ResolverMeta meta,
        CancellationToken ct)
    {
        switch (child)
        {
            case SetOperationChild.This:
                return await CheckDirect(reader, resource, subject, meta, ct).ConfigureAwait(false);

            case SetOperationChild.Nil:
                return DispatchCheckResult.None;

            case SetOperationChild.Self:
                return resource.ObjectType == subject.ObjectType
                       && resource.ObjectId == subject.ObjectId
                       && subject.Relation == CoreConstants.Ellipsis
                    ? DispatchCheckResult.DefiniteMember
                    : DispatchCheckResult.None;

            case SetOperationChild.ComputedUsersetChild(var cu):
                return await Sub(resource.WithRelation(cu.Relation), subject, meta, ct).ConfigureAwait(false);

            case SetOperationChild.TupleToUsersetChild(var ttu):
                return await CheckTupleToUserset(
                    reader, resource, subject, ttu.TuplesetRelation, ttu.ComputedUserset,
                    TupleToUsersetFunction.Any, meta, ct).ConfigureAwait(false);

            case SetOperationChild.FunctionedTupleToUsersetChild(var fttu):
                return await CheckTupleToUserset(
                    reader, resource, subject, fttu.TuplesetRelation, fttu.ComputedUserset,
                    fttu.Function, meta, ct).ConfigureAwait(false);

            case SetOperationChild.NestedRewrite(var nested):
                return await CheckRewrite(reader, resource, subject, nested.Operation, meta, ct).ConfigureAwait(false);

            default:
                return DispatchCheckResult.None;
        }
    }

    /// <summary>
    /// Evaluates a tuple-to-userset arrow: walk the tupleset relation on the resource, then for each
    /// reached object compute the userset relation, dispatching each as a sub-problem.
    /// </summary>
    private async Task<DispatchCheckResult> CheckTupleToUserset(
        IDatastoreReader reader,
        ObjectAndRelation resource,
        ObjectAndRelation subject,
        string tuplesetRelation,
        ComputedUserset computed,
        TupleToUsersetFunction function,
        ResolverMeta meta,
        CancellationToken ct)
    {
        var filter = new RelationshipsFilter
        {
            OptionalResourceType = resource.ObjectType,
            OptionalResourceIds = [resource.ObjectId],
            OptionalResourceRelation = tuplesetRelation,
        };

        List<(ObjectAndRelation Onr, CaveatExpression? Parent)>? targets = null;

        await foreach (var rel in reader.QueryRelationships(filter, ct).ConfigureAwait(false))
        {
            if (IsExpired(rel))
                continue;

            var reached = rel.Subject;
            if (reached.IsPublicWildcard)
                continue;

            var target = new ObjectAndRelation(reached.ObjectType, reached.ObjectId, computed.Relation);
            (targets ??= []).Add((target, CaveatOf(rel)));
        }

        if (targets is null)
            return DispatchCheckResult.None;

        if (function == TupleToUsersetFunction.All)
        {
            var acc = DispatchCheckResult.DefiniteMember;
            var cut = false;
            foreach (var (target, parent) in targets)
            {
                var sub = await Sub(target, subject, meta, ct).ConfigureAwait(false);
                cut = cut || sub.CycleCut;
                if (!sub.Member)
                    return DispatchCheckResult.None with { CycleCut = cut };
                acc = And(acc, CaveatedMember(CaveatExpression.CombineAnd(parent, sub.Caveat)));
            }
            return acc with { CycleCut = cut };
        }

        var any = DispatchCheckResult.None;
        foreach (var (target, parent) in targets)
        {
            var sub = await Sub(target, subject, meta, ct).ConfigureAwait(false);
            any = any with { CycleCut = any.CycleCut || sub.CycleCut };
            if (!sub.Member)
                continue;
            any = Or(any, CaveatedMember(CaveatExpression.CombineAnd(parent, sub.Caveat)));
            if (any.Member && any.Caveat is null)
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

    private bool IsExpired(Relationship rel) =>
        rel.OptionalExpiration is { } exp && exp <= _now;

    private static CaveatExpression? CaveatOf(Relationship rel) =>
        rel.OptionalCaveat is { } c ? CaveatExpression.FromCaveat(c) : null;

    private static bool OnrEquals(ObjectAndRelation a, ObjectAndRelation b) =>
        a.ObjectType == b.ObjectType && a.ObjectId == b.ObjectId && a.Relation == b.Relation;

    private static DispatchCheckResult CaveatedMember(CaveatExpression? caveat) => new(true, caveat, false);

    // --- Branch algebra over DispatchCheckResult (membership + caveat), preserving cycle-cut. ---

    private static DispatchCheckResult Or(DispatchCheckResult a, DispatchCheckResult b)
    {
        var cut = a.CycleCut || b.CycleCut;
        if (a.IsDetermined || b.IsDetermined)
            return new DispatchCheckResult(true, null, cut);
        if (!a.Member)
            return b with { CycleCut = cut };
        if (!b.Member)
            return a with { CycleCut = cut };
        return new DispatchCheckResult(true, CaveatExpression.CombineOr(a.Caveat, b.Caveat), cut);
    }

    private static DispatchCheckResult And(DispatchCheckResult a, DispatchCheckResult b)
    {
        var cut = a.CycleCut || b.CycleCut;
        if (!a.Member || !b.Member)
            return new DispatchCheckResult(false, null, cut);
        return new DispatchCheckResult(true, CaveatExpression.CombineAnd(a.Caveat, b.Caveat), cut);
    }

    private static DispatchCheckResult Subtract(DispatchCheckResult baseResult, DispatchCheckResult excluded)
    {
        var cut = baseResult.CycleCut || excluded.CycleCut;
        if (!baseResult.Member)
            return new DispatchCheckResult(false, null, cut);
        if (excluded.IsDetermined)
            return new DispatchCheckResult(false, null, cut);
        if (!excluded.Member)
            return baseResult with { CycleCut = cut };
        return new DispatchCheckResult(true, CaveatExpression.Subtract(baseResult.Caveat, excluded.Caveat!), cut);
    }
}
