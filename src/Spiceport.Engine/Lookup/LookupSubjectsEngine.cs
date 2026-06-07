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
    /// <param name="reader">A datastore reader pinned to the revision to evaluate against.</param>
    /// <param name="resource">The resource ONR (object type, id and relation/permission).</param>
    /// <param name="subjectType">The requested subject namespace.</param>
    /// <param name="subjectRelation">The requested subject relation; ellipsis for terminal subjects.</param>
    /// <param name="evaluationTime">Optional pinned "now" for expiration filtering; defaults to system UTC.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    public IAsyncEnumerable<FoundSubject> LookupSubjects(
        IDatastoreReader reader,
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
        IDatastoreReader reader,
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
        IDatastoreReader reader,
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
        IDatastoreReader reader,
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
        IDatastoreReader reader,
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
        IDatastoreReader reader,
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
        IDatastoreReader reader,
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
    /// A combinable set of found subjects keyed by subject id, tracking each subject's accumulated
    /// caveat and wildcard flag. Models SpiceDB's per-child <c>(subjectId -&gt; caveat)</c> reducer.
    /// </summary>
    /// <remarks>
    /// A subject whose entry has a null caveat is unconditionally present (a caveat-free member
    /// dominates an OR). Wildcards are carried verbatim by id (<c>"*"</c>); full wildcard×concrete set
    /// algebra is deferred per the design — the cross-check oracle validates membership.
    /// </remarks>
    private sealed class SubjectSet
    {
        // id -> (caveat, isWildcard). A present key with a null caveat means unconditional.
        private readonly Dictionary<string, Entry> _entries = new();

        private readonly record struct Entry(CaveatExpression? Caveat, bool IsWildcard);

        public void Add(string id, CaveatExpression? caveat, bool isWildcard)
        {
            // Union semantics for repeated adds of the same id within a set: OR the caveats.
            if (_entries.TryGetValue(id, out var existing))
                _entries[id] = new Entry(CaveatExpression.CombineOr(existing.Caveat, caveat), existing.IsWildcard || isWildcard);
            else
                _entries[id] = new Entry(caveat, isWildcard);
        }

        public void UnionWith(SubjectSet other)
        {
            foreach (var (id, e) in other._entries)
                Add(id, e.Caveat, e.IsWildcard);
        }

        /// <summary>Returns a copy of this set with <paramref name="caveat"/> AND-combined into every entry.</summary>
        public SubjectSet WithAndedCaveat(CaveatExpression? caveat)
        {
            if (caveat is null)
                return this;
            var copy = new SubjectSet();
            foreach (var (id, e) in _entries)
                copy._entries[id] = new Entry(CaveatExpression.CombineAnd(caveat, e.Caveat), e.IsWildcard);
            return copy;
        }

        public IEnumerable<FoundSubject> ToFoundSubjects()
        {
            foreach (var (id, e) in _entries)
                yield return new FoundSubject(id, e.Caveat, e.IsWildcard || id == CoreConstants.PublicWildcard);
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

            var result = new SubjectSet();
            foreach (var (id, e) in sets[0]._entries)
            {
                var caveat = e.Caveat;
                var isWildcard = e.IsWildcard;
                var inAll = true;
                for (var i = 1; i < sets.Count; i++)
                {
                    if (!sets[i]._entries.TryGetValue(id, out var other))
                    {
                        inAll = false;
                        break;
                    }
                    caveat = CaveatExpression.CombineAnd(caveat, other.Caveat);
                    isWildcard = isWildcard || other.IsWildcard;
                }
                if (inAll)
                    result._entries[id] = new Entry(caveat, isWildcard);
            }
            return result;
        }

        public static SubjectSet Exclude(IReadOnlyList<SubjectSet> sets)
        {
            if (sets.Count == 0)
                return new SubjectSet();

            var result = new SubjectSet();
            // base minus the union of the rest.
            var excluded = Union(sets.Skip(1).ToList());
            foreach (var (id, baseEntry) in sets[0]._entries)
            {
                if (!excluded._entries.TryGetValue(id, out var exc))
                {
                    // Not excluded at all: survives with its base caveat.
                    result._entries[id] = baseEntry;
                    continue;
                }

                // Unconditionally excluded => removed.
                if (exc.Caveat is null)
                    continue;

                // Conditionally excluded => survives as base AND NOT excluded.
                var caveat = CaveatExpression.Subtract(baseEntry.Caveat, exc.Caveat);
                result._entries[id] = new Entry(caveat, baseEntry.IsWildcard);
            }
            return result;
        }
    }
}
