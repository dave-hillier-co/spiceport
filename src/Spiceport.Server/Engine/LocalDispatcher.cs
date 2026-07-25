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
/// <para>
/// <b>Caveat-completeness invariant.</b> This dispatcher models one (resource, subject) per dispatch,
/// so it has no analogue of SpiceDB's batched <c>ResultsSetting</c>. SpiceDB batches many resource ids
/// with an "allow single result" short-circuit and must force <c>REQUIRE_ALL_RESULTS</c> when any
/// incoming relationship is caveated, so every caveat reaches the final expression
/// (<c>internal/graph/check.go:482-512</c>). The equivalent guarantee here rests on a single rule:
/// every union / arrow accumulation below short-circuits ONLY on a <em>definite, uncaveated</em> member
/// (<c>IsDetermined</c>, or <c>Member &amp;&amp; Caveat is null</c>) — never on a caveated branch. Since
/// <c>caveatExpr OR definitely-true</c> collapses to true, that drop cannot change a verdict, while
/// every undetermined branch is OR-accumulated and survives to <see cref="CheckEngine.Collapse"/>. Do
/// NOT add an "any member found" early return that fires on a caveated result: it would silently drop
/// caveats and is the exact regression covered by <c>CaveatCompletenessTests</c> (issue #3, finding 5).
/// </para>
/// </remarks>
public sealed class LocalDispatcher : IDispatcher
{
    private readonly ImmutableDictionary<string, NamespaceDefinition> _namespaces;
    private readonly Func<IRevision, IGraphReader> _readerFor;
    private readonly DateTimeOffset _now;

    /// <summary>
    /// Creates a local dispatcher over the given schema, reader resolver and evaluation clock.
    /// </summary>
    /// <param name="namespaces">The compiled namespace definitions keyed by name.</param>
    /// <param name="readerFor">Resolves a snapshot reader for a request's revision identity.</param>
    /// <param name="now">The pinned evaluation "now" used to filter expired relationships.</param>
    public LocalDispatcher(
        ImmutableDictionary<string, NamespaceDefinition> namespaces,
        Func<IRevision, IGraphReader> readerFor,
        DateTimeOffset now)
    {
        ArgumentNullException.ThrowIfNull(namespaces);
        ArgumentNullException.ThrowIfNull(readerFor);
        _namespaces = namespaces;
        _readerFor = readerFor;
        _now = now;
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

        var resource = request.Resource;
        var subject = request.Subject;
        var meta = request.Meta;

        // Depth exhaustion is the ONLY termination guarantee (SpiceDB's dispatch.CheckDepth). It raises
        // MaxDepthExceededError (gRPC FailedPrecondition) rather than returning a definitive non-member,
        // so a graph deeper than maxDepth — or a genuine cycle — fails the request instead of producing a
        // confident (and cacheable) false negative. A true cycle simply consumes depth here until it
        // throws, matching SpiceDB exactly: there is NO visited-set cut on the verdict path.
        if (meta.DepthRemaining <= 0)
            throw new MaxDepthExceededException();

        // Fast path: the resource ONR is literally the subject ONR.
        if (OnrEquals(resource, subject))
            return DispatchCheckResult.DefiniteMember;

        // Record this (resource, subject) into the exact visited set — RECORD ONLY, never Contains->Cut.
        // Correctness does not rest on the visited set; it is kept solely so the dispatcher
        // (OrleansDispatcher) can detect a genuine same-path revisit on the next hop and mark that hop's
        // result CycleCut so it is never memoized, mirroring SpiceDB's singleflight loop guard. The grain
        // call itself still happens normally either way (CheckGrain is reentrant) — a visited-set hit is
        // now exact, never a false positive, but it still only forces an uncached hop; it never changes a
        // verdict.
        var key = VisitKey.Of(resource, subject);
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
    private async Task<DispatchCheckResult> Sub(
        ObjectAndRelation resource, ObjectAndRelation subject, ResolverMeta meta, CancellationToken ct)
    {
        var subMeta = meta with { DepthRemaining = meta.DepthRemaining - 1 };
        var child = await Dispatcher.DispatchCheck(new DispatchCheckRequest(resource, subject, subMeta), ct)
            .ConfigureAwait(false);
        // This dispatch hop consumes one level of depth: the result this node returns requires one more
        // than whatever its child required. Mirrors SpiceDB's addCallToResponseMetadata (DepthRequired+1).
        return child with { DepthRequired = child.DepthRequired + 1 };
    }

    /// <summary>Matches a base relation's directly-written tuples, walking non-terminal subjects.</summary>
    private async Task<DispatchCheckResult> CheckDirect(
        IGraphReader reader,
        ObjectAndRelation resource,
        ObjectAndRelation subject,
        ResolverMeta meta,
        CancellationToken ct)
    {
        // Subject-filter pushdown (scalability-program 3.2). The selector union below is a SUPERSET of
        // everything the consumption loop below can use — that superset argument is the load-bearing
        // correctness claim, so keep it in sync with the loop:
        //   1. the EXACT subject (terminal match): type + id + the subject's relation
        //      (NonEllipsisRelation for a concrete relation; the deliberately-loose ellipsis branch of
        //      SubjectRelationFilter is fine because the loop's exact OnrEquals re-check stays);
        //   2. the type-scoped PUBLIC WILDCARD short-circuit: type + "*" with IncludeEllipsisRelation
        //      (SpiceDB wildcards cannot carry a subject relation);
        //   3. every NON-TERMINAL subject (OnlyNonEllipsisRelations, no type/id constraint): the userset
        //      references the loop re-dispatches into. Dropping these would break recursion — which is
        //      why a bare subject==S pushdown would be WRONG.
        // The loop consumes exactly those three row categories and nothing else; its post-filtering is
        // unchanged as belt-and-braces. RelationshipsFilter.Matches ANDs the selectors in identically on
        // the reference-model path and on the sharded path (where the shard also applies them
        // server-side), so engine-over-reference and mesh verdicts cannot diverge.
        var subjectRelationFilter = subject.Relation == CoreConstants.Ellipsis
            ? new SubjectRelationFilter(IncludeEllipsisRelation: true)
            : new SubjectRelationFilter(NonEllipsisRelation: subject.Relation);
        var filter = new RelationshipsFilter
        {
            OptionalResourceType = resource.ObjectType,
            OptionalResourceIds = [resource.ObjectId],
            OptionalResourceRelation = resource.Relation,
            OptionalSubjectsSelectors =
            [
                new SubjectsSelector(subject.ObjectType, [subject.ObjectId], subjectRelationFilter),
                new SubjectsSelector(
                    subject.ObjectType,
                    [CoreConstants.PublicWildcard],
                    new SubjectRelationFilter(IncludeEllipsisRelation: true)),
                new SubjectsSelector(RelationFilter: new SubjectRelationFilter(OnlyNonEllipsisRelations: true)),
            ],
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
            // Carry the cycle-cut AND the depth this child consumed up into the accumulator, even when the
            // child is a non-member: the depth we walked is what gates cache reuse, regardless of verdict.
            found = found with
            {
                CycleCut = found.CycleCut || sub.CycleCut,
                DepthRequired = Math.Max(found.DepthRequired, sub.DepthRequired),
            };
            if (!sub.Member)
                continue;

            var combined = CaveatedMember(CaveatExpression.CombineAnd(parent, sub.Caveat))
                with { DepthRequired = sub.DepthRequired };
            found = Or(found, combined);
            if (found.Member && found.Caveat is null)
                return found;
        }

        return found;
    }

    /// <summary>Evaluates a set operation (union / intersection / exclusion).</summary>
    private async Task<DispatchCheckResult> CheckRewrite(
        IGraphReader reader,
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
                    // Caveat-completeness: only a DEFINITE (uncaveated) member short-circuits the union; a
                    // caveated accumulator keeps gathering so every branch's caveat survives. See the
                    // class remarks (issue #3, finding 5) — do not relax this to `acc.Member`.
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
                var depth = 1;
                foreach (var child in operation.Children)
                {
                    var b = await CheckChild(reader, resource, subject, child, meta, ct).ConfigureAwait(false);
                    cut = cut || b.CycleCut;
                    depth = Math.Max(depth, b.DepthRequired);
                    if (!b.Member)
                        return DispatchCheckResult.None with { CycleCut = cut, DepthRequired = depth };
                    acc = And(acc, b);
                }
                return acc with { CycleCut = cut, DepthRequired = depth };
            }

            case SetOperationType.Exclusion:
            {
                if (operation.Children.Count == 0)
                    return DispatchCheckResult.None;

                var acc = await CheckChild(reader, resource, subject, operation.Children[0], meta, ct).ConfigureAwait(false);
                var cut = acc.CycleCut;
                var depth = acc.DepthRequired;
                if (!acc.Member)
                    return DispatchCheckResult.None with { CycleCut = cut, DepthRequired = depth };

                for (var i = 1; i < operation.Children.Count; i++)
                {
                    var excluded = await CheckChild(reader, resource, subject, operation.Children[i], meta, ct).ConfigureAwait(false);
                    cut = cut || excluded.CycleCut;
                    depth = Math.Max(depth, excluded.DepthRequired);
                    acc = Subtract(acc, excluded);
                    if (!acc.Member)
                        return DispatchCheckResult.None with { CycleCut = cut, DepthRequired = depth };
                }
                return acc with { CycleCut = cut, DepthRequired = depth };
            }

            default:
                return DispatchCheckResult.None;
        }
    }

    /// <summary>Evaluates a single set-operation operand.</summary>
    private async Task<DispatchCheckResult> CheckChild(
        IGraphReader reader,
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
        IGraphReader reader,
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
            var depth = 1;
            foreach (var (target, parent) in targets)
            {
                var sub = await Sub(target, subject, meta, ct).ConfigureAwait(false);
                cut = cut || sub.CycleCut;
                depth = Math.Max(depth, sub.DepthRequired);
                if (!sub.Member)
                    return DispatchCheckResult.None with { CycleCut = cut, DepthRequired = depth };
                acc = And(acc, CaveatedMember(CaveatExpression.CombineAnd(parent, sub.Caveat))
                    with { DepthRequired = sub.DepthRequired });
            }
            return acc with { CycleCut = cut, DepthRequired = depth };
        }

        var any = DispatchCheckResult.None;
        foreach (var (target, parent) in targets)
        {
            var sub = await Sub(target, subject, meta, ct).ConfigureAwait(false);
            any = any with
            {
                CycleCut = any.CycleCut || sub.CycleCut,
                DepthRequired = Math.Max(any.DepthRequired, sub.DepthRequired),
            };
            if (!sub.Member)
                continue;
            any = Or(any, CaveatedMember(CaveatExpression.CombineAnd(parent, sub.Caveat))
                with { DepthRequired = sub.DepthRequired });
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

    // DepthRequired propagates as the max consumed by any combined branch, so a result's required depth
    // reflects the deepest sub-problem it actually walked (mirrors SpiceDB's max(DepthRequired) folding).
    private static DispatchCheckResult Or(DispatchCheckResult a, DispatchCheckResult b)
    {
        var cut = a.CycleCut || b.CycleCut;
        var depth = Math.Max(a.DepthRequired, b.DepthRequired);
        if (a.IsDetermined || b.IsDetermined)
            return new DispatchCheckResult(true, null, cut, depth);
        if (!a.Member)
            return b with { CycleCut = cut, DepthRequired = depth };
        if (!b.Member)
            return a with { CycleCut = cut, DepthRequired = depth };
        return new DispatchCheckResult(true, CaveatExpression.CombineOr(a.Caveat, b.Caveat), cut, depth);
    }

    private static DispatchCheckResult And(DispatchCheckResult a, DispatchCheckResult b)
    {
        var cut = a.CycleCut || b.CycleCut;
        var depth = Math.Max(a.DepthRequired, b.DepthRequired);
        if (!a.Member || !b.Member)
            return new DispatchCheckResult(false, null, cut, depth);
        return new DispatchCheckResult(true, CaveatExpression.CombineAnd(a.Caveat, b.Caveat), cut, depth);
    }

    private static DispatchCheckResult Subtract(DispatchCheckResult baseResult, DispatchCheckResult excluded)
    {
        var cut = baseResult.CycleCut || excluded.CycleCut;
        var depth = Math.Max(baseResult.DepthRequired, excluded.DepthRequired);
        if (!baseResult.Member)
            return new DispatchCheckResult(false, null, cut, depth);
        if (excluded.IsDetermined)
            return new DispatchCheckResult(false, null, cut, depth);
        if (!excluded.Member)
            return baseResult with { CycleCut = cut, DepthRequired = depth };
        return new DispatchCheckResult(true, CaveatExpression.Subtract(baseResult.Caveat, excluded.Caveat!), cut, depth);
    }
}
