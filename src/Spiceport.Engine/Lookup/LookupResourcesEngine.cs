using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using Spiceport.Core;
using Spiceport.Datastore;

namespace Spiceport.Engine;

/// <summary>
/// Enumerates the resources of a given type/permission that a subject can reach, using the schema's
/// <see cref="ReachabilityGraph"/> to follow only productive edges (reverse traversal).
/// </summary>
/// <remarks>
/// Port of SpiceDB's <c>internal/graph/lookupresources3.go</c>. This is the only API that consults the
/// reachability graph and <see cref="IDatastoreReader.ReverseQueryRelationships"/>. Results stream as
/// an <see cref="IAsyncEnumerable{T}"/>; a caller applies a limit simply by stopping enumeration.
/// <para>
/// Correctness for intersection / exclusion is achieved the way SpiceDB does it: compute a candidate
/// set via reverse traversal, then confirm each candidate with the trusted <see cref="CheckEngine"/>
/// (no second exclusion implementation). Caveated candidate relationships are sheared early against
/// the request context; partial caveats survive and surface as <see cref="Membership.Caveated"/> with
/// <see cref="FoundResource.MissingContextParams"/>. Recursion is bounded by a depth limit and a
/// visited-set cycle guard.
/// </para>
/// </remarks>
public sealed class LookupResourcesEngine
{
    /// <summary>The default maximum recursion depth.</summary>
    public const int DefaultMaxDepth = 50;

    private readonly ImmutableDictionary<string, NamespaceDefinition> _namespaces;
    private readonly ReachabilityGraph _reachability;
    private readonly CheckEngine _check;
    private readonly CaveatEvaluator _caveats;
    private readonly int _maxDepth;

    /// <summary>Creates a lookup-resources engine over the given schema definitions.</summary>
    /// <param name="namespaces">The compiled namespace definitions that make up the schema.</param>
    /// <param name="maxDepth">The maximum recursion depth before traversal stops.</param>
    public LookupResourcesEngine(IEnumerable<NamespaceDefinition> namespaces, int maxDepth = DefaultMaxDepth)
        : this(namespaces, caveats: null, maxDepth)
    {
    }

    /// <summary>Creates a lookup-resources engine over the given schema and caveat definitions.</summary>
    /// <param name="namespaces">The compiled namespace definitions that make up the schema.</param>
    /// <param name="caveats">The compiled caveat definitions, or null if the schema has none.</param>
    /// <param name="maxDepth">The maximum recursion depth before traversal stops.</param>
    public LookupResourcesEngine(
        IEnumerable<NamespaceDefinition> namespaces,
        IEnumerable<CaveatDefinition>? caveats,
        int maxDepth = DefaultMaxDepth)
    {
        ArgumentNullException.ThrowIfNull(namespaces);
        _namespaces = namespaces.ToImmutableDictionary(ns => ns.Name);
        var caveatList = caveats?.ToList();
        _reachability = ReachabilityGraph.ForSchema(_namespaces);
        _check = new CheckEngine(_namespaces.Values, caveatList, maxDepth);
        _caveats = new CaveatEvaluator(caveatList ?? []);
        _maxDepth = maxDepth;
    }

    /// <summary>
    /// Enumerates the resources of <paramref name="resourceType"/> on which
    /// <paramref name="subjectType"/>:<paramref name="subjectId"/>(#<paramref name="subjectRelation"/>)
    /// holds <paramref name="permission"/>, as of the reader's snapshot.
    /// </summary>
    /// <param name="reader">A datastore reader pinned to the revision to evaluate against.</param>
    /// <param name="subjectType">The subject namespace.</param>
    /// <param name="subjectId">The subject object id.</param>
    /// <param name="subjectRelation">The subject relation; ellipsis for terminal subjects.</param>
    /// <param name="resourceType">The resource namespace to enumerate.</param>
    /// <param name="permission">The relation or permission to enumerate.</param>
    /// <param name="caveatContext">Optional request-time caveat context.</param>
    /// <param name="evaluationTime">Optional pinned "now" for expiration filtering; defaults to system UTC.</param>
    /// <param name="cursor">Optional resume token from a prior partial enumeration.</param>
    /// <param name="limit">Optional soft limit; the caller may also simply stop enumerating.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    public async IAsyncEnumerable<FoundResource> LookupResources(
        IDatastoreReader reader,
        string subjectType,
        string subjectId,
        string subjectRelation,
        string resourceType,
        string permission,
        IReadOnlyDictionary<string, object?>? caveatContext = null,
        DateTimeOffset? evaluationTime = null,
        LookupResourcesCursor? cursor = null,
        int? limit = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentException.ThrowIfNullOrEmpty(subjectType);
        ArgumentException.ThrowIfNullOrEmpty(subjectId);
        ArgumentException.ThrowIfNullOrEmpty(resourceType);
        ArgumentException.ThrowIfNullOrEmpty(permission);

        var now = evaluationTime ?? SystemClock.Instance.UtcNow;
        var target = new RelationReference(resourceType, permission);
        var subjectRel = new RelationReference(subjectType, subjectRelation);
        var terminalSubject = new ObjectAndRelation(subjectType, subjectId, subjectRelation);
        var sections = cursor?.Sections ?? [];

        // Working set keyed by resource id; the initial subject maps to itself.
        var initial = new Dictionary<string, ResourceState>
        {
            [subjectId] = new ResourceState([subjectId], Membership.Member, []),
        };

        var seen = new HashSet<string>();
        var emitted = 0;
        await foreach (var found in LookupRec(
            reader, subjectRel, initial, target, terminalSubject, caveatContext, now,
            _maxDepth, ImmutableHashSet<string>.Empty, sections, cancellationToken))
        {
            if (!seen.Add(found.ResourceId))
                continue;
            yield return found;
            emitted++;
            if (limit is { } l && emitted >= l)
                yield break;
        }
    }

    // Finds resources of `target` reachable from the given subjects (keyed by id with back-mapping).
    private async IAsyncEnumerable<FoundResource> LookupRec(
        IDatastoreReader reader,
        RelationReference subjectRel,
        IReadOnlyDictionary<string, ResourceState> subjects,
        RelationReference target,
        ObjectAndRelation terminalSubject,
        IReadOnlyDictionary<string, object?>? caveatContext,
        DateTimeOffset now,
        int depthRemaining,
        ImmutableHashSet<string> visited,
        ImmutableList<LookupResourcesCursorSection> cursorSections,
        [EnumeratorCancellation] CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        if (subjects.Count == 0)
            yield break;

        // Portion #1: the subjects already ARE resources of the target relation.
        // We still fall through to Portion #2 afterwards: when the target is a *recursive*
        // relation (one that admits a userset subject of its own (namespace, relation), e.g.
        // `member: user | group#member`), a matched userset is simultaneously a resource AND a
        // subject of further resources up the chain, so the reverse walk must continue. For the
        // common terminal case there is no such self-referential entrypoint, so Portion #2 yields
        // nothing and the behaviour is unchanged. Duplicate resource ids are deduped by the caller.
        if (subjectRel.Namespace == target.Namespace && subjectRel.Relation == target.Relation)
        {
            foreach (var id in subjects.Keys.OrderBy(x => x, StringComparer.Ordinal))
            {
                var state = subjects[id];
                yield return new FoundResource(id, state.ForSubjectIds, state.Membership, state.Missing);
            }
        }

        var subjectIds = subjects.Keys.OrderBy(x => x, StringComparer.Ordinal).ToList();

        // Cycle guard keyed by (relation, subject-id set): a same-relation chain with new ids may
        // continue, but a revisit of the same relation over the same ids (cyclic data) stops.
        var visitKey = $"{subjectRel.Namespace}#{subjectRel.Relation}|{string.Join(",", subjectIds)}";
        if (depthRemaining <= 0 || visited.Contains(visitKey))
            yield break;
        visited = visited.Add(visitKey);

        // Portion #2: entrypoint-pruned reverse traversal.
        var entrypoints = _reachability.EntrypointsForSubjectToResource(subjectRel, target, optimizedFirstOnly: false);
        if (entrypoints.Count == 0)
            yield break;

        // Cursor: the section (if any) for this nesting level positions the resumed entrypoint.
        var thisSection = cursorSections.Count > 0 ? cursorSections[0] : null;
        var deeperSections = cursorSections.Count > 0 ? cursorSections.RemoveAt(0) : [];

        for (var epIndex = 0; epIndex < entrypoints.Count; epIndex++)
        {
            ct.ThrowIfCancellationRequested();

            if (thisSection is { } sec && epIndex < sec.EntrypointIndex)
                continue;

            var skipUpToResourceId = thisSection is { } s && epIndex == s.EntrypointIndex
                ? s.LastResourceId
                : null;

            var entrypoint = entrypoints[epIndex];

            // Candidate (resource id -> back-mapped state) of the entrypoint's containing relation.
            var candidates = await CollectCandidates(
                reader, entrypoint, subjectRel, subjectIds, subjects, caveatContext, now, ct)
                .ConfigureAwait(false);
            if (candidates.Count == 0)
                continue;

            // Check-filter conditional entrypoints (intersection / exclusion / .all()): confirm the
            // candidate genuinely holds the containing relation for the terminal subject.
            if (!entrypoint.IsDirectResult)
                candidates = await FilterByCheck(
                    reader, entrypoint.ContainingRelation, candidates, terminalSubject, caveatContext, now, ct)
                    .ConfigureAwait(false);

            if (candidates.Count == 0)
                continue;

            var nestedSections = thisSection is { } s2 && epIndex == s2.EntrypointIndex ? deeperSections : [];
            await foreach (var found in LookupRec(
                reader, entrypoint.ContainingRelation, candidates, target, terminalSubject, caveatContext, now,
                depthRemaining - 1, visited, nestedSections, ct).ConfigureAwait(false))
            {
                if (skipUpToResourceId is { } skip && string.CompareOrdinal(found.ResourceId, skip) <= 0)
                    continue;

                var cursorOut = new LookupResourcesCursor(
                    ImmutableList.Create(new LookupResourcesCursorSection(epIndex, found.ResourceId)));
                yield return found with { AfterCursor = cursorOut };
            }
        }
    }

    /// <summary>
    /// Collects candidate resources of the entrypoint's containing relation reachable from the input
    /// subjects via this entrypoint, applying early caveat shearing. Port of <c>entrypointIter</c>.
    /// </summary>
    private async Task<Dictionary<string, ResourceState>> CollectCandidates(
        IDatastoreReader reader,
        ReachabilityEntrypoint entrypoint,
        RelationReference subjectRel,
        IReadOnlyList<string> subjectIds,
        IReadOnlyDictionary<string, ResourceState> subjects,
        IReadOnlyDictionary<string, object?>? caveatContext,
        DateTimeOffset now,
        CancellationToken ct)
    {
        var result = new Dictionary<string, ResourceState>();

        switch (entrypoint.Kind)
        {
            case ReachabilityEntrypointKind.Self:
            case ReachabilityEntrypointKind.ComputedUserset:
            {
                // Structural rewrite: re-key the input subject set as the containing relation. No query.
                foreach (var id in subjectIds)
                    Merge(result, id, subjects[id]);
                return result;
            }

            case ReachabilityEntrypointKind.Relation:
            {
                var filter = BuildSubjectsFilter(
                    subjectRel, subjectIds, entrypoint.TargetRelation,
                    includeWildcard: subjectRel.Relation == CoreConstants.Ellipsis);

                await foreach (var rel in reader.ReverseQueryRelationships(filter, ct).ConfigureAwait(false))
                {
                    if (IsExpired(rel, now))
                        continue;
                    if (rel.Resource.ObjectType != entrypoint.TargetRelation.Namespace ||
                        rel.Resource.Relation != entrypoint.TargetRelation.Relation)
                        continue;

                    AddCandidate(result, rel, subjects, caveatContext);
                }
                return result;
            }

            case ReachabilityEntrypointKind.TupleToUserset:
            {
                // Arrow: reverse-query the tupleset relation of the containing namespace. Arrows ignore
                // the subject's own relation, so match on namespace only.
                var tupleset = entrypoint.TuplesetRelation!;
                var containerNs = entrypoint.ContainingRelation.Namespace;

                var filter = new SubjectsFilter(
                    SubjectType: subjectRel.Namespace,
                    OptionalSubjectIds: subjectIds,
                    RelationFilter: SubjectRelationFilter.Any,
                    OptionalResourceType: containerNs,
                    OptionalResourceRelation: tupleset);

                await foreach (var rel in reader.ReverseQueryRelationships(filter, ct).ConfigureAwait(false))
                {
                    if (IsExpired(rel, now))
                        continue;
                    if (rel.Resource.ObjectType != containerNs || rel.Resource.Relation != tupleset)
                        continue;

                    AddCandidate(result, rel, subjects, caveatContext);
                }
                return result;
            }

            default:
                return result;
        }
    }

    /// <summary>Confirms candidates via Check against the terminal subject, keeping Member/Caveated.</summary>
    private async Task<Dictionary<string, ResourceState>> FilterByCheck(
        IDatastoreReader reader,
        RelationReference containingRelation,
        Dictionary<string, ResourceState> candidates,
        ObjectAndRelation terminalSubject,
        IReadOnlyDictionary<string, object?>? caveatContext,
        DateTimeOffset now,
        CancellationToken ct)
    {
        var filtered = new Dictionary<string, ResourceState>();
        foreach (var (resourceId, state) in candidates)
        {
            ct.ThrowIfCancellationRequested();
            var result = await _check.Check(
                reader,
                new ObjectAndRelation(containingRelation.Namespace, resourceId, containingRelation.Relation),
                terminalSubject, caveatContext, now, ct).ConfigureAwait(false);

            if (result.Verdict == Membership.Member)
            {
                filtered[resourceId] = state with { Membership = Membership.Member, Missing = [] };
            }
            else if (result.Verdict == Membership.Caveated)
            {
                var missing = new HashSet<string>(state.Missing);
                foreach (var f in result.MissingExprFields)
                    missing.Add(f);
                filtered[resourceId] = state with { Membership = Membership.Caveated, Missing = missing.ToList() };
            }
        }
        return filtered;
    }

    private static SubjectsFilter BuildSubjectsFilter(
        RelationReference subjectRel,
        IReadOnlyList<string> subjectIds,
        RelationReference targetRelation,
        bool includeWildcard)
    {
        var ids = includeWildcard
            ? subjectIds.Append(CoreConstants.PublicWildcard).ToList()
            : subjectIds;

        var relFilter = subjectRel.Relation == CoreConstants.Ellipsis
            ? new SubjectRelationFilter(IncludeEllipsisRelation: true)
            : new SubjectRelationFilter(NonEllipsisRelation: subjectRel.Relation);

        return new SubjectsFilter(
            SubjectType: subjectRel.Namespace,
            OptionalSubjectIds: ids,
            RelationFilter: relFilter,
            OptionalResourceType: targetRelation.Namespace,
            OptionalResourceRelation: targetRelation.Relation);
    }

    /// <summary>Adds a found relationship as a candidate resource, shearing its caveat early.</summary>
    private void AddCandidate(
        Dictionary<string, ResourceState> result,
        Relationship rel,
        IReadOnlyDictionary<string, ResourceState> subjects,
        IReadOnlyDictionary<string, object?>? caveatContext)
    {
        // Which input subject(s) does this relationship's subject map back to? A wildcard subject
        // matches every input subject of the type.
        var subjectId = rel.Subject.ObjectId;
        List<string> backIds;
        if (subjectId == CoreConstants.PublicWildcard)
            backIds = subjects.Keys.ToList();
        else if (subjects.ContainsKey(subjectId))
            backIds = [subjectId];
        else
            return;

        // Caveat shearing against the request context.
        var membership = Membership.Member;
        var localMissing = new List<string>();
        if (rel.OptionalCaveat is { } caveat)
        {
            var evaluated = _caveats.Evaluate(caveat.CaveatName, caveat.Context, caveatContext);
            switch (evaluated.Outcome)
            {
                case CaveatOutcome.DefinitelyFalse:
                    return; // sheared off.
                case CaveatOutcome.Caveated:
                    membership = Membership.Caveated;
                    localMissing.AddRange(evaluated.MissingFields);
                    break;
            }
        }

        // Back-map and union the originating subject states.
        var forSubjectIds = new HashSet<string>();
        var missing = new HashSet<string>(localMissing);
        foreach (var b in backIds)
        {
            var s = subjects[b];
            foreach (var fid in s.ForSubjectIds)
                forSubjectIds.Add(fid);
            foreach (var f in s.Missing)
                missing.Add(f);
            if (s.Membership == Membership.Caveated)
                membership = Membership.Caveated;
        }

        var finalMissing = membership == Membership.Caveated ? missing.ToList() : [];
        var state = new ResourceState(
            forSubjectIds.OrderBy(x => x, StringComparer.Ordinal).ToList(),
            membership,
            finalMissing);

        Merge(result, rel.Resource.ObjectId, state);
    }

    private static void Merge(Dictionary<string, ResourceState> into, string resourceId, ResourceState state)
    {
        if (!into.TryGetValue(resourceId, out var existing))
        {
            into[resourceId] = state;
            return;
        }

        var forSubjects = new HashSet<string>(existing.ForSubjectIds);
        foreach (var f in state.ForSubjectIds)
            forSubjects.Add(f);

        // Member dominates Caveated (a non-caveated path makes the resource an unconditional member).
        Membership membership;
        List<string> missing;
        if (existing.Membership == Membership.Member || state.Membership == Membership.Member)
        {
            membership = Membership.Member;
            missing = [];
        }
        else
        {
            membership = Membership.Caveated;
            var m = new HashSet<string>(existing.Missing);
            foreach (var f in state.Missing)
                m.Add(f);
            missing = m.ToList();
        }

        into[resourceId] = new ResourceState(
            forSubjects.OrderBy(x => x, StringComparer.Ordinal).ToList(), membership, missing);
    }

    private static bool IsExpired(Relationship rel, DateTimeOffset now) =>
        rel.OptionalExpiration is { } exp && exp <= now;

    /// <summary>
    /// A resource candidate's back-mapping: which input subject ids reached it, its membership so far,
    /// and any accumulated missing caveat params.
    /// </summary>
    private sealed record ResourceState(
        IReadOnlyList<string> ForSubjectIds,
        Membership Membership,
        IReadOnlyList<string> Missing);
}
