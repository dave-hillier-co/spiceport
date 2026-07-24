using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using Spiceport.Core;
using Spiceport.Datastore;

namespace Spiceport.Engine;

/// <summary>
/// Reverse-walks the userset-rewrite structure of a relation to enumerate the subjects (of a
/// requested type and optional subrelation) that hold it on a resource.
/// </summary>
/// <remarks>
/// Port of SpiceDB's <c>internal/graph/lookupsubjects.go</c>. Results stream as an
/// <see cref="IAsyncEnumerable{T}"/>. Like <see cref="ExpandEngine"/> this walks the rewrite
/// structurally and does not consult any reachability graph (matching SpiceDB, where
/// <c>lookupsubjects.go</c> never touches reachability). Caveats are carried verbatim and combined
/// across union (OR), intersection (AND), exclusion (base AND NOT excluded) and arrows; a non-null
/// caveat on a yielded <see cref="FoundSubject"/> is the "Caveated marker". Wildcards (<c>"*"</c>)
/// are carried verbatim as <see cref="FoundSubject.IsWildcard"/>. Recursion is bounded by a depth
/// limit and a visited-set cycle guard so cyclic schemas terminate.
/// </remarks>
public sealed class LookupSubjectsEngine
{
    /// <summary>The default maximum recursion depth.</summary>
    public const int DefaultMaxDepth = 50;

    private readonly ImmutableDictionary<string, NamespaceDefinition> _namespaces;
    private readonly int _maxDepth;

    /// <summary>Creates a lookup-subjects engine over the given schema definitions.</summary>
    /// <param name="namespaces">The compiled namespace definitions that make up the schema.</param>
    /// <param name="maxDepth">The maximum recursion depth before traversal stops.</param>
    public LookupSubjectsEngine(IEnumerable<NamespaceDefinition> namespaces, int maxDepth = DefaultMaxDepth)
    {
        ArgumentNullException.ThrowIfNull(namespaces);
        _namespaces = namespaces.ToImmutableDictionary(ns => ns.Name);
        _maxDepth = maxDepth;
    }

    /// <summary>
    /// Enumerates the subjects of <paramref name="subjectType"/> (with the given
    /// <paramref name="subjectRelation"/>) that hold <paramref name="resource"/>'s relation, as of the
    /// reader's snapshot.
    /// </summary>
    /// <param name="reader">A graph reader pinned to the revision to evaluate against.</param>
    /// <param name="resource">The resource ONR (object type, id and relation/permission).</param>
    /// <param name="subjectType">The requested subject namespace.</param>
    /// <param name="subjectRelation">The requested subject relation; ellipsis for terminal subjects.</param>
    /// <param name="evaluationTime">Optional pinned "now" for expiration filtering; defaults to system UTC.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    public IAsyncEnumerable<FoundSubject> LookupSubjects(
        IGraphReader reader,
        ObjectAndRelation resource,
        string subjectType,
        string subjectRelation = CoreConstants.Ellipsis,
        DateTimeOffset? evaluationTime = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(resource);
        ArgumentException.ThrowIfNullOrEmpty(subjectType);
        var now = evaluationTime ?? SystemClock.Instance.UtcNow;
        return LookupAsync(reader, resource, subjectType, subjectRelation, now, _maxDepth, ImmutableHashSet<string>.Empty, cancellationToken);
    }

    private async IAsyncEnumerable<FoundSubject> LookupAsync(
        IGraphReader reader,
        ObjectAndRelation resource,
        string subjectType,
        string subjectRelation,
        DateTimeOffset now,
        int depthRemaining,
        ImmutableHashSet<string> visited,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var collected = await Collect(reader, resource, subjectType, subjectRelation, now, depthRemaining, visited, ct).ConfigureAwait(false);
        foreach (var found in collected.ToFoundSubjects())
        {
            ct.ThrowIfCancellationRequested();
            yield return found;
        }
    }

    // Collects the full subject set for a sub-problem into a combinable map. Set operations need the
    // whole child set before combining (intersection/exclusion), so collection is materialized rather
    // than streamed internally; the public surface still streams the final result.
    private async Task<SubjectSet> Collect(
        IGraphReader reader,
        ObjectAndRelation resource,
        string subjectType,
        string subjectRelation,
        DateTimeOffset now,
        int depthRemaining,
        ImmutableHashSet<string> visited,
        CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        var result = new SubjectSet();

        // Self short-circuit: the resource itself is a subject of the requested (type, relation).
        if (subjectType == resource.ObjectType && subjectRelation == resource.Relation)
            result.Add(resource.ObjectId, caveat: null, isWildcard: resource.IsPublicWildcard);

        var key = $"{resource.ObjectType}:{resource.ObjectId}#{resource.Relation}";
        if (depthRemaining <= 0 || visited.Contains(key))
            return result;
        visited = visited.Add(key);

        var relation = LookupRelation(resource.ObjectType, resource.Relation);
        if (relation is null)
            return result;

        if (relation.UsersetRewrite is { } rewrite)
        {
            var rewritten = await CollectRewrite(reader, resource, rewrite.Operation, subjectType, subjectRelation, now, depthRemaining, visited, ct).ConfigureAwait(false);
            result.UnionWith(rewritten);
        }
        else
        {
            var direct = await CollectDirect(reader, resource, relation.Name, subjectType, subjectRelation, now, depthRemaining, visited, ct).ConfigureAwait(false);
            result.UnionWith(direct);
        }

        return result;
    }

    /// <summary>Reverse-walks a base relation's tuples (port of <c>lookupDirectSubjects</c>).</summary>
    private async Task<SubjectSet> CollectDirect(
        IGraphReader reader,
        ObjectAndRelation resource,
        string relation,
        string subjectType,
        string subjectRelation,
        DateTimeOffset now,
        int depthRemaining,
        ImmutableHashSet<string> visited,
        CancellationToken ct)
    {
        var filter = new RelationshipsFilter
        {
            OptionalResourceType = resource.ObjectType,
            OptionalResourceIds = [resource.ObjectId],
            OptionalResourceRelation = relation,
        };

        var result = new SubjectSet();
        await foreach (var rel in reader.QueryRelationships(filter, ct).ConfigureAwait(false))
        {
            if (IsExpired(rel, now))
                continue;

            var subject = rel.Subject;
            var tupleCaveat = CaveatOf(rel);

            // Direct match of (type, relation). This includes a :* wildcard matching (type, ellipsis):
            // "all subjects of this type" semantics, carried verbatim with IsWildcard.
            if (subject.ObjectType == subjectType && subject.Relation == subjectRelation)
            {
                result.Add(subject.ObjectId, tupleCaveat, subject.IsPublicWildcard);
                continue;
            }

            // Non-terminal subrelation (not ellipsis, not wildcard): recurse and AND-in this tuple's caveat.
            if (subject.Relation != CoreConstants.Ellipsis && !subject.IsPublicWildcard)
            {
                var nested = await Collect(reader, subject, subjectType, subjectRelation, now, depthRemaining - 1, visited, ct).ConfigureAwait(false);
                result.UnionWith(nested.WithAndedCaveat(tupleCaveat));
            }
        }

        return result;
    }

    /// <summary>Reverse-walks a rewrite set operation (port of <c>lookupViaRewrite</c>/<c>lookupSetOperation</c>).</summary>
    private async Task<SubjectSet> CollectRewrite(
        IGraphReader reader,
        ObjectAndRelation resource,
        SetOperation operation,
        string subjectType,
        string subjectRelation,
        DateTimeOffset now,
        int depthRemaining,
        ImmutableHashSet<string> visited,
        CancellationToken ct)
    {
        var childSets = new List<SubjectSet>(operation.Children.Count);
        foreach (var child in operation.Children)
            childSets.Add(await CollectChild(reader, resource, child, subjectType, subjectRelation, now, depthRemaining, visited, ct).ConfigureAwait(false));

        return operation.Type switch
        {
            SetOperationType.Union => SubjectSet.Union(childSets),
            SetOperationType.Intersection => SubjectSet.Intersect(childSets),
            SetOperationType.Exclusion => SubjectSet.Exclude(childSets),
            _ => new SubjectSet(),
        };
    }

    /// <summary>Reverse-walks a single set-operation operand.</summary>
    private async Task<SubjectSet> CollectChild(
        IGraphReader reader,
        ObjectAndRelation resource,
        SetOperationChild child,
        string subjectType,
        string subjectRelation,
        DateTimeOffset now,
        int depthRemaining,
        ImmutableHashSet<string> visited,
        CancellationToken ct)
    {
        switch (child)
        {
            case SetOperationChild.This:
                return await CollectDirect(reader, resource, resource.Relation, subjectType, subjectRelation, now, depthRemaining, visited, ct).ConfigureAwait(false);

            case SetOperationChild.Nil:
                return new SubjectSet();

            case SetOperationChild.Self:
            {
                // The resource itself, at ellipsis, is a subject if it matches the request.
                var s = new SubjectSet();
                if (subjectType == resource.ObjectType && subjectRelation == CoreConstants.Ellipsis)
                    s.Add(resource.ObjectId, caveat: null, isWildcard: resource.IsPublicWildcard);
                return s;
            }

            case SetOperationChild.ComputedUsersetChild(var cu):
                return await Collect(reader, resource.WithRelation(cu.Relation), subjectType, subjectRelation, now, depthRemaining - 1, visited, ct).ConfigureAwait(false);

            case SetOperationChild.TupleToUsersetChild(var ttu):
                return await CollectTupleToUserset(
                    reader, resource, ttu.TuplesetRelation, ttu.ComputedUserset,
                    TupleToUsersetFunction.Any, subjectType, subjectRelation, now, depthRemaining, visited, ct).ConfigureAwait(false);

            case SetOperationChild.FunctionedTupleToUsersetChild(var fttu):
                return await CollectTupleToUserset(
                    reader, resource, fttu.TuplesetRelation, fttu.ComputedUserset,
                    fttu.Function, subjectType, subjectRelation, now, depthRemaining, visited, ct).ConfigureAwait(false);

            case SetOperationChild.NestedRewrite(var nested):
                return await CollectRewrite(reader, resource, nested.Operation, subjectType, subjectRelation, now, depthRemaining, visited, ct).ConfigureAwait(false);

            default:
                return new SubjectSet();
        }
    }

    /// <summary>
    /// Reverse-walks a tuple-to-userset arrow (port of <c>lookupViaTupleToUserset</c> /
    /// <c>lookupViaIntersectionTupleToUserset</c>): traverse the tupleset, recurse on each reached
    /// object's computed relation, AND-in the tupleset tuple caveat, then union (<c>.any()</c>) or
    /// intersect (<c>.all()</c>) across reached objects. An empty <c>.all()</c> tupleset yields nothing.
    /// </summary>
    private async Task<SubjectSet> CollectTupleToUserset(
        IGraphReader reader,
        ObjectAndRelation resource,
        string tuplesetRelation,
        ComputedUserset computed,
        TupleToUsersetFunction function,
        string subjectType,
        string subjectRelation,
        DateTimeOffset now,
        int depthRemaining,
        ImmutableHashSet<string> visited,
        CancellationToken ct)
    {
        var filter = new RelationshipsFilter
        {
            OptionalResourceType = resource.ObjectType,
            OptionalResourceIds = [resource.ObjectId],
            OptionalResourceRelation = tuplesetRelation,
        };

        var perTarget = new List<SubjectSet>();
        await foreach (var rel in reader.QueryRelationships(filter, ct).ConfigureAwait(false))
        {
            if (IsExpired(rel, now))
                continue;

            var reached = rel.Subject;
            if (reached.IsPublicWildcard)
                continue;

            var target = computed.Object == ComputedUsersetObject.TupleObject
                ? new ObjectAndRelation(resource.ObjectType, resource.ObjectId, computed.Relation)
                : new ObjectAndRelation(reached.ObjectType, reached.ObjectId, computed.Relation);

            var nested = await Collect(reader, target, subjectType, subjectRelation, now, depthRemaining - 1, visited, ct).ConfigureAwait(false);
            perTarget.Add(nested.WithAndedCaveat(CaveatOf(rel)));
        }

        return function == TupleToUsersetFunction.All
            ? SubjectSet.Intersect(perTarget)
            : SubjectSet.Union(perTarget);
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

    private static bool IsExpired(Relationship rel, DateTimeOffset now) =>
        rel.OptionalExpiration is { } exp && exp <= now;

    private static CaveatExpression? CaveatOf(Relationship rel) =>
        rel.OptionalCaveat is { } c ? CaveatExpression.FromCaveat(c) : null;

    /// <summary>
    /// A combinable set of found subjects with first-class wildcard semantics. Port of SpiceDB's
    /// <c>internal/datasets/basesubjectset.go</c>: concrete subjects are tracked by id, and the
    /// public wildcard (<c>"*"</c>) is tracked separately as a single optional entry carrying its own
    /// caveat and a list of <em>excluded</em> concrete subjects.
    /// </summary>
    /// <remarks>
    /// Unlike a plain set, a union between a wildcard and a concrete keeps BOTH present. Intersecting a
    /// wildcard with a concrete yields the concrete (a concrete matches the wildcard, modulo
    /// exclusions and caveats); subtracting a wildcard removes all concretes modulo its exclusions.
    /// A concrete/exclusion whose entry has a null caveat is unconditional.
    /// </remarks>
    private sealed class SubjectSet
    {
        // Concrete subjects by id (never the wildcard id).
        private readonly Dictionary<string, Concrete> _concrete = new();
        // The single optional public wildcard with its exclusions, or null when absent.
        private Wildcard? _wildcard;

        private readonly record struct Concrete(string Id, CaveatExpression? Caveat);

        private sealed record Wildcard(CaveatExpression? Caveat, IReadOnlyList<Concrete> Excluded);

        /// <summary>Adds a subject (concrete or wildcard) via union semantics.</summary>
        public void Add(string id, CaveatExpression? caveat, bool isWildcard)
        {
            if (id == CoreConstants.PublicWildcard || isWildcard)
            {
                Add(new Wildcard(caveat, []));
                return;
            }
            Add(new Concrete(id, caveat));
        }

        private void Add(Concrete adding)
        {
            // Union the wildcard with this concrete (the wildcard's exclusion of this id, if any, is
            // weakened) and union the concrete with any existing same-id concrete.
            if (_concrete.TryGetValue(adding.Id, out var existing))
                _concrete[adding.Id] = new Concrete(adding.Id, CaveatExpression.OrKeepOther(existing.Caveat, adding.Caveat));
            else
                _concrete[adding.Id] = adding;

            if (_wildcard is { } w)
                _wildcard = UnionWildcardWithConcrete(w, adding);
        }

        private void Add(Wildcard adding)
        {
            _wildcard = UnionWildcardWithWildcard(_wildcard, adding);
            // Re-union the wildcard with every concrete so its exclusions stay consistent.
            if (_wildcard is { } w)
            {
                foreach (var c in _concrete.Values)
                    w = UnionWildcardWithConcrete(w, c);
                _wildcard = w;
            }
        }

        public void UnionWith(SubjectSet other)
        {
            foreach (var c in other._concrete.Values)
                Add(c);
            if (other._wildcard is { } w)
                Add(w);
        }

        /// <summary>Returns a copy of this set with <paramref name="caveat"/> AND-combined into every entry.</summary>
        public SubjectSet WithAndedCaveat(CaveatExpression? caveat)
        {
            if (caveat is null)
                return this;
            var copy = new SubjectSet();
            foreach (var (id, c) in _concrete)
                copy._concrete[id] = new Concrete(id, CaveatExpression.CombineAnd(caveat, c.Caveat));
            if (_wildcard is { } w)
            {
                // The caveat constrains whether the wildcard applies; exclusions are unchanged.
                copy._wildcard = new Wildcard(CaveatExpression.CombineAnd(caveat, w.Caveat), w.Excluded);
            }
            return copy;
        }

        public IEnumerable<FoundSubject> ToFoundSubjects()
        {
            foreach (var c in _concrete.Values)
                yield return new FoundSubject(c.Id, c.Caveat, IsWildcard: false);
            if (_wildcard is { } w)
            {
                var excluded = w.Excluded.Count == 0
                    ? null
                    : w.Excluded
                        .Select(e => new FoundSubject(e.Id, e.Caveat, IsWildcard: false))
                        .ToList();
                yield return new FoundSubject(
                    CoreConstants.PublicWildcard, w.Caveat, IsWildcard: true, ExcludedSubjects: excluded);
            }
        }

        public static SubjectSet Union(IReadOnlyList<SubjectSet> sets)
        {
            var result = new SubjectSet();
            foreach (var s in sets)
                result.UnionWith(s);
            return result;
        }

        public static SubjectSet Intersect(IReadOnlyList<SubjectSet> sets)
        {
            if (sets.Count == 0)
                return new SubjectSet();

            var result = sets[0].Clone();
            for (var i = 1; i < sets.Count; i++)
                result.IntersectWith(sets[i]);
            return result;
        }

        public static SubjectSet Exclude(IReadOnlyList<SubjectSet> sets)
        {
            if (sets.Count == 0)
                return new SubjectSet();

            var result = sets[0].Clone();
            for (var i = 1; i < sets.Count; i++)
                result.SubtractAll(sets[i]);
            return result;
        }

        private SubjectSet Clone()
        {
            var copy = new SubjectSet();
            foreach (var (id, c) in _concrete)
                copy._concrete[id] = c;
            copy._wildcard = _wildcard;
            return copy;
        }

        // ---- intersection (port of IntersectionDifference) ----

        private void IntersectWith(SubjectSet other)
        {
            var existingWildcard = _wildcard;
            var otherWildcard = other._wildcard;

            var newWildcard = IntersectWildcardWithWildcard(existingWildcard, otherWildcard);

            var updated = new Dictionary<string, Concrete>(_concrete.Count);
            foreach (var concrete in _concrete.Values)
            {
                Concrete? otherConcrete =
                    other._concrete.TryGetValue(concrete.Id, out var oc) ? oc : null;

                var concreteIntersected = IntersectConcreteWithConcrete(concrete, otherConcrete);
                var otherWildcardIntersected = IntersectConcreteWithWildcard(concrete, otherWildcard);

                var result = UnionConcreteWithConcrete(concreteIntersected, otherWildcardIntersected);
                if (result is { } r)
                    updated[concrete.Id] = r;
            }

            if (existingWildcard is not null)
            {
                foreach (var otherSubject in other._concrete.Values)
                {
                    var existingWildcardIntersect = IntersectConcreteWithWildcard(otherSubject, existingWildcard);
                    if (updated.TryGetValue(otherSubject.Id, out var existingUpdated))
                    {
                        var result = UnionConcreteWithConcrete(existingUpdated, existingWildcardIntersect);
                        if (result is { } r)
                            updated[otherSubject.Id] = r;
                    }
                    else if (existingWildcardIntersect is { } ewi)
                    {
                        updated[otherSubject.Id] = ewi;
                    }
                }
            }

            _concrete.Clear();
            foreach (var (id, c) in updated)
                _concrete[id] = c;
            _wildcard = newWildcard;
        }

        // ---- subtraction (port of SubtractAll / Subtract) ----

        private void SubtractAll(SubjectSet other)
        {
            foreach (var c in other._concrete.Values)
                Subtract(new Concrete(c.Id, c.Caveat), isWildcard: false, []);
            if (other._wildcard is { } w)
                Subtract(new Concrete(CoreConstants.PublicWildcard, w.Caveat), isWildcard: true, w.Excluded);
        }

        private void Subtract(Concrete toRemove, bool isWildcard, IReadOnlyList<Concrete> wildcardExclusions)
        {
            if (isWildcard)
            {
                var wildcardToRemove = new Wildcard(toRemove.Caveat, wildcardExclusions);
                foreach (var id in _concrete.Keys.ToList())
                {
                    var updated = SubtractWildcardFromConcrete(_concrete[id], wildcardToRemove);
                    if (updated is { } u)
                        _concrete[id] = u;
                    else
                        _concrete.Remove(id);
                }

                var (newWildcard, concretesToAdd) = SubtractWildcardFromWildcard(_wildcard, wildcardToRemove);
                _wildcard = newWildcard;
                foreach (var c in concretesToAdd)
                    _concrete[c.Id] = c;
                return;
            }

            if (_concrete.TryGetValue(toRemove.Id, out var existing))
            {
                var updated = SubtractConcreteFromConcrete(existing, toRemove);
                if (updated is { } u)
                    _concrete[toRemove.Id] = u;
                else
                    _concrete.Remove(toRemove.Id);
            }

            if (_wildcard is { } w)
                _wildcard = SubtractConcreteFromWildcard(w, toRemove);
        }

        // ---- combinators (ports of the basesubjectset.go helpers) ----

        private static Concrete? UnionConcreteWithConcrete(Concrete? existing, Concrete? adding)
        {
            if (existing is null)
                return adding;
            if (adding is null)
                return existing;
            // A null (unconditional) caveat on either side dominates the OR.
            return new Concrete(existing.Value.Id, CaveatExpression.CombineOr(existing.Value.Caveat, adding.Value.Caveat));
        }

        private static Wildcard UnionWildcardWithWildcard(Wildcard? existing, Wildcard adding)
        {
            if (existing is null)
                return adding;

            // The union wildcard applies when either applies; its exclusions are those excluded by
            // BOTH (intersection of exclusion sets), since an exclusion only survives if both exclude it.
            var caveat = CaveatExpression.OrKeepOther(existing.Caveat, adding.Caveat);
            var addingExclusions = adding.Excluded.ToDictionary(e => e.Id);
            var newExclusions = new List<Concrete>();
            foreach (var ex in existing.Excluded)
            {
                if (addingExclusions.TryGetValue(ex.Id, out var other))
                {
                    // Excluded by both: survives as an exclusion only when both exclusions hold (AND).
                    var c = CaveatExpression.CombineAnd(ex.Caveat, other.Caveat);
                    newExclusions.Add(new Concrete(ex.Id, c));
                }
            }
            return new Wildcard(caveat, newExclusions);
        }

        private static Wildcard UnionWildcardWithConcrete(Wildcard wildcard, Concrete adding)
        {
            // Adding a concrete to a wildcard weakens any matching exclusion: the concrete is now
            // present, so the exclusion only holds when the concrete's caveat is false AND the prior
            // exclusion held. A non-caveated concrete removes the exclusion outright.
            var newExclusions = new List<Concrete>();
            foreach (var ex in wildcard.Excluded)
            {
                if (ex.Id != adding.Id)
                {
                    newExclusions.Add(ex);
                    continue;
                }
                if (adding.Caveat is null)
                    continue; // unconditional concrete: exclusion removed.
                // exclusion survives as ex.Caveat AND NOT(adding.Caveat).
                var c = CaveatExpression.CombineAnd(ex.Caveat, CaveatExpression.Invert(adding.Caveat));
                if (c is not null)
                    newExclusions.Add(new Concrete(ex.Id, c));
            }
            return new Wildcard(wildcard.Caveat, newExclusions);
        }

        private static Concrete? IntersectConcreteWithConcrete(Concrete first, Concrete? second)
        {
            if (second is null)
                return null;
            return new Concrete(first.Id, CaveatExpression.CombineAnd(first.Caveat, second.Value.Caveat));
        }

        private static Wildcard? IntersectWildcardWithWildcard(Wildcard? first, Wildcard? second)
        {
            if (first is null || second is null)
                return null;
            // Intersection: AND of conditionals, UNION of exclusions.
            var exclusions = new Dictionary<string, Concrete>();
            foreach (var e in first.Excluded)
                exclusions[e.Id] = exclusions.TryGetValue(e.Id, out var x)
                    ? new Concrete(e.Id, CaveatExpression.OrKeepOther(x.Caveat, e.Caveat))
                    : e;
            foreach (var e in second.Excluded)
                exclusions[e.Id] = exclusions.TryGetValue(e.Id, out var x)
                    ? new Concrete(e.Id, CaveatExpression.OrKeepOther(x.Caveat, e.Caveat))
                    : e;
            return new Wildcard(CaveatExpression.CombineAnd(first.Caveat, second.Caveat), exclusions.Values.ToList());
        }

        private static Concrete? IntersectConcreteWithWildcard(Concrete concrete, Wildcard? wildcard)
        {
            if (wildcard is null)
                return null;

            var exclusion = wildcard.Excluded.FirstOrDefault(e => e.Id == concrete.Id);
            var isExcluded = wildcard.Excluded.Any(e => e.Id == concrete.Id);

            if (!isExcluded && wildcard.Caveat is null)
                return concrete; // {tom} & {*} => {tom}
            if (!isExcluded && wildcard.Caveat is not null)
                return new Concrete(concrete.Id, CaveatExpression.CombineAnd(concrete.Caveat, wildcard.Caveat));
            if (isExcluded && exclusion.Caveat is null)
                return null; // unconditionally excluded.
            // excluded conditionally: concrete AND wildcard AND NOT(exclusion).
            return new Concrete(concrete.Id,
                CaveatExpression.CombineAnd(concrete.Caveat,
                    CaveatExpression.CombineAnd(wildcard.Caveat, CaveatExpression.Invert(exclusion.Caveat))));
        }

        private static Concrete? SubtractConcreteFromConcrete(Concrete existing, Concrete toRemove)
        {
            if (toRemove.Caveat is null)
                return null;
            // existing AND NOT(toRemove).
            return new Concrete(existing.Id,
                CaveatExpression.CombineAnd(existing.Caveat, CaveatExpression.Invert(toRemove.Caveat)));
        }

        private static Wildcard SubtractConcreteFromWildcard(Wildcard wildcard, Concrete concreteToRemove)
        {
            // Subtracting a concrete adds it to the wildcard's exclusions.
            var newExclusions = new List<Concrete>();
            var found = false;
            foreach (var ex in wildcard.Excluded)
            {
                if (ex.Id == concreteToRemove.Id)
                {
                    // shortcircuited OR: either side non-caveated => exclusion non-caveated.
                    var c = CaveatExpression.CombineOr(ex.Caveat, concreteToRemove.Caveat);
                    newExclusions.Add(new Concrete(ex.Id, c));
                    found = true;
                }
                else
                {
                    newExclusions.Add(ex);
                }
            }
            if (!found)
                newExclusions.Add(concreteToRemove);
            return new Wildcard(wildcard.Caveat, newExclusions);
        }

        private static Concrete? SubtractWildcardFromConcrete(Concrete existingConcrete, Wildcard wildcardToRemove)
        {
            var exclusion = wildcardToRemove.Excluded.FirstOrDefault(e => e.Id == existingConcrete.Id);
            var isExcluded = wildcardToRemove.Excluded.Any(e => e.Id == existingConcrete.Id);

            if (!isExcluded)
            {
                // Not in the exclusions: removed when the wildcard applies. Unconditional wildcard
                // removes it outright; a caveated wildcard keeps it conditional on NOT(wildcard).
                if (wildcardToRemove.Caveat is null)
                    return null;
                return new Concrete(existingConcrete.Id,
                    CaveatExpression.CombineAnd(existingConcrete.Caveat, CaveatExpression.Invert(wildcardToRemove.Caveat)));
            }

            // Excluded from the wildcard: present unless the exclusion itself is conditional.
            if (exclusion.Caveat is null)
                return existingConcrete;

            // Present when the exclusion holds OR the wildcard does not apply.
            var exclusionConditional = CaveatExpression.OrKeepOther(CaveatExpression.Invert(wildcardToRemove.Caveat), exclusion.Caveat);
            return new Concrete(existingConcrete.Id,
                CaveatExpression.CombineAnd(existingConcrete.Caveat, exclusionConditional));
        }

        private static (Wildcard? Wildcard, IReadOnlyList<Concrete> ConcretesToAdd) SubtractWildcardFromWildcard(
            Wildcard? existing, Wildcard toRemove)
        {
            if (existing is null)
                return (null, []);

            // Unconditional removal with no exclusions: {*} - {*} => {}.
            if (toRemove.Caveat is null && toRemove.Excluded.Count == 0)
                return (null, []);

            var existingExclusions = existing.Excluded.ToDictionary(e => e.Id);
            var concretesToAdd = new List<Concrete>();
            foreach (var excludedSubject in toRemove.Excluded)
            {
                var hasExisting = existingExclusions.TryGetValue(excludedSubject.Id, out var existingExclusion);
                if (!hasExisting || existingExclusion.Caveat is not null)
                {
                    var expr = CaveatExpression.CombineAnd(
                        CaveatExpression.CombineAnd(existing.Caveat, toRemove.Caveat),
                        excludedSubject.Caveat);
                    if (hasExisting && existingExclusion.Caveat is not null)
                    {
                        expr = CaveatExpression.CombineAnd(
                            CaveatExpression.CombineAnd(
                                CaveatExpression.CombineAnd(existing.Caveat, toRemove.Caveat),
                                CaveatExpression.Invert(existingExclusion.Caveat)),
                            excludedSubject.Caveat);
                    }
                    concretesToAdd.Add(new Concrete(excludedSubject.Id, expr));
                }
            }

            var combined = CaveatExpression.CombineAnd(existing.Caveat, CaveatExpression.Invert(toRemove.Caveat));
            if (combined is not null)
                return (new Wildcard(combined, existing.Excluded), concretesToAdd);
            return (null, concretesToAdd);
        }
    }
}
