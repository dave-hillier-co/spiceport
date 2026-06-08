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
        _reachability = ReachabilityGraph.ForSchema(_namespaces, ReachabilityMode.First);
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

        // No global dedup: like SpiceDB, the traversal is a single deterministic stream and the cursor is a
        // position in it, so a resource reachable via several entrypoints is emitted once per entrypoint.
        // This is what makes a paged enumeration equal the unpaged one — a bounded cursor cannot carry a
        // cross-page "already seen" set. Within a chunk, duplicate (resource,subject) pairs are still merged.
        var emitted = 0;
        await foreach (var found in LookupRec(
            reader, subjectRel, initial, target, terminalSubject, caveatContext, now,
            _maxDepth, ImmutableHashSet<string>.Empty, sections, cancellationToken))
        {
            yield return found;
            emitted++;
            if (limit is { } l && emitted >= l)
                yield break;
        }
    }

    /// <summary>How many raw relationships a query entrypoint consumes per streamed chunk. The datastore
    /// keyset advances one chunk at a time, so chunk boundaries must be deterministic over the ordered
    /// stream; chunking by a fixed raw count satisfies that.</summary>
    private const int ChunkSize = 100;

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

        // The cursor section (if any) for this nesting level positions the resume; deeper sections
        // position the levels below. A sentinel entrypoint index of -1 marks a resume *within* Portion #1
        // (the self-match results); a non-negative index marks a resume in Portion #2, meaning Portion #1
        // was already fully drained on a prior page and must be skipped entirely here.
        var thisSection = cursorSections.Count > 0 ? cursorSections[0] : null;
        var deeperSections = cursorSections.Count > 0 ? cursorSections.RemoveAt(0) : [];

        const int Portion1Section = -1;
        var resumingPortion2 = thisSection is { EntrypointIndex: >= 0 };

        // Portion #1: the subjects already ARE resources of the target relation. We still fall through to
        // Portion #2 afterwards: when the target is a *recursive* relation (one admitting a userset subject
        // of its own (namespace, relation), e.g. `member: user | group#member`), a matched userset is
        // simultaneously a resource AND a subject of further resources up the chain, so the reverse walk
        // must continue. For the common terminal case Portion #2 yields nothing.
        if (subjectRel.Namespace == target.Namespace && subjectRel.Relation == target.Relation && !resumingPortion2)
        {
            var portion1SkipUpTo = thisSection is { EntrypointIndex: Portion1Section } p1
                ? p1.LastResourceId
                : null;

            foreach (var id in subjects.Keys.OrderBy(x => x, StringComparer.Ordinal))
            {
                if (portion1SkipUpTo is { } skip1 && string.CompareOrdinal(id, skip1) <= 0)
                    continue;

                var state = subjects[id];
                // Decorate every self-match with a single-section leaf cursor; parent levels prepend their
                // own sections as it bubbles up, building the full nesting stack.
                var cursorOut = new LookupResourcesCursor(
                    ImmutableList.Create(new LookupResourcesCursorSection(Portion1Section, LastResourceId: id)));
                yield return new FoundResource(id, state.ForSubjectIds, state.Membership, state.Missing)
                    with { AfterCursor = cursorOut };
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

        // Only honour the section's entrypoint index when it is a Portion #2 section (>= 0). A Portion #1
        // section (-1) means "resume mid self-match"; Portion #2 then starts from its first entrypoint.
        var p2Section = thisSection is { EntrypointIndex: >= 0 } ? thisSection : null;

        for (var epIndex = 0; epIndex < entrypoints.Count; epIndex++)
        {
            ct.ThrowIfCancellationRequested();

            // Entrypoints before the resumed one were fully drained on a prior page; skip them.
            if (p2Section is { } sec && epIndex < sec.EntrypointIndex)
                continue;

            var isResumeEp = p2Section is { } s && epIndex == s.EntrypointIndex;
            var entrypoint = entrypoints[epIndex];

            if (entrypoint.Kind is ReachabilityEntrypointKind.Self or ReachabilityEntrypointKind.ComputedUserset)
            {
                // Structural rewrite: re-key the (bounded) input subject set as the containing relation and
                // recurse once. No datastore scan, so the within-level position is carried entirely by the
                // deeper sections; the section emitted for this level is structural (null keyset).
                var candidates = new Dictionary<string, ResourceState>();
                foreach (var id in subjectIds)
                    Merge(candidates, id, subjects[id]);

                if (!entrypoint.IsDirectResult)
                    candidates = await FilterByCheck(
                        reader, entrypoint.ContainingRelation, candidates, terminalSubject, caveatContext, now, ct)
                        .ConfigureAwait(false);
                if (candidates.Count == 0)
                    continue;

                var innerSections = isResumeEp ? deeperSections : [];
                var section = new LookupResourcesCursorSection(epIndex);
                await foreach (var found in LookupRec(
                    reader, entrypoint.ContainingRelation, candidates, target, terminalSubject, caveatContext, now,
                    depthRemaining - 1, visited, innerSections, ct).ConfigureAwait(false))
                {
                    yield return Prepend(found, section);
                }
                continue;
            }

            // Query entrypoint (Relation / arrow): stream the reverse query in the deterministic BySubject
            // order, in fixed-size chunks, resuming after the keyset carried in the cursor. Each emitted
            // result's section keyset is the PRIOR chunk's final relationship, so resuming re-fetches the
            // in-progress chunk and re-positions within it via the deeper sections.
            var afterKeyset = isResumeEp ? p2Section!.AfterKeyset : null;
            var sectionKeyset = afterKeyset;
            var firstChunk = true;

            await foreach (var chunk in StreamCandidateChunks(
                reader, entrypoint, subjectRel, subjectIds, subjects, caveatContext, afterKeyset, ct)
                .ConfigureAwait(false))
            {
                var candidates = chunk.Candidates;
                if (candidates.Count > 0 && !entrypoint.IsDirectResult)
                    candidates = await FilterByCheck(
                        reader, entrypoint.ContainingRelation, candidates, terminalSubject, caveatContext, now, ct)
                        .ConfigureAwait(false);

                if (candidates.Count > 0)
                {
                    var innerSections = firstChunk && isResumeEp ? deeperSections : [];
                    var section = new LookupResourcesCursorSection(epIndex, AfterKeyset: sectionKeyset);
                    await foreach (var found in LookupRec(
                        reader, entrypoint.ContainingRelation, candidates, target, terminalSubject, caveatContext, now,
                        depthRemaining - 1, visited, innerSections, ct).ConfigureAwait(false))
                    {
                        yield return Prepend(found, section);
                    }
                }

                sectionKeyset = chunk.LastKeyset;
                firstChunk = false;
            }
        }
    }

    /// <summary>Prepends this nesting level's resume section onto a bubbled-up result's cursor stack.</summary>
    private static FoundResource Prepend(FoundResource found, LookupResourcesCursorSection section)
    {
        var inner = found.AfterCursor?.Sections ?? ImmutableList<LookupResourcesCursorSection>.Empty;
        return found with { AfterCursor = new LookupResourcesCursor(inner.Insert(0, section)) };
    }

    /// <summary>One streamed chunk of candidate resources plus the datastore keyset of its final relationship.</summary>
    private readonly record struct CandidateChunk(
        Dictionary<string, ResourceState> Candidates,
        RelationshipReference LastKeyset);

    /// <summary>
    /// Streams candidate resources of a query entrypoint's containing relation in the deterministic
    /// BySubject order, grouped into fixed-size chunks, applying early caveat shearing. The chunk's
    /// <see cref="CandidateChunk.LastKeyset"/> is the last raw relationship consumed, so resuming the scan
    /// after it continues exactly where the chunk ended. Port of SpiceDB's <c>relationshipsChunk</c> stream.
    /// </summary>
    private async IAsyncEnumerable<CandidateChunk> StreamCandidateChunks(
        IDatastoreReader reader,
        ReachabilityEntrypoint entrypoint,
        RelationReference subjectRel,
        IReadOnlyList<string> subjectIds,
        IReadOnlyDictionary<string, ResourceState> subjects,
        IReadOnlyDictionary<string, object?>? caveatContext,
        RelationshipReference? afterKeyset,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var filter = BuildEntrypointFilter(entrypoint, subjectRel, subjectIds);
        var options = new ReverseQueryOptions(ReverseQuerySort.BySubject, afterKeyset);

        // The datastore already applies expiration + the subject/resource filter and yields in BySubject
        // order, so each yielded relationship is a real, deterministic stream position. We chunk by raw
        // count so chunk boundaries reproduce exactly on resume.
        var current = new Dictionary<string, ResourceState>();
        RelationshipReference? last = null;
        var count = 0;

        await foreach (var rel in reader.ReverseQueryRelationships(filter, options, ct).ConfigureAwait(false))
        {
            AddCandidate(current, rel, subjects, caveatContext);
            last = rel.Reference;
            if (++count < ChunkSize)
                continue;

            yield return new CandidateChunk(current, last);
            current = new Dictionary<string, ResourceState>();
            count = 0;
        }

        if (count > 0)
            yield return new CandidateChunk(current, last!);
    }

    /// <summary>Builds the subject-side filter for a query entrypoint (Relation or arrow).</summary>
    private static SubjectsFilter BuildEntrypointFilter(
        ReachabilityEntrypoint entrypoint, RelationReference subjectRel, IReadOnlyList<string> subjectIds)
    {
        if (entrypoint.Kind == ReachabilityEntrypointKind.TupleToUserset)
        {
            // Arrow: reverse-query the tupleset relation of the containing namespace. Arrows ignore the
            // subject's own relation, so match on namespace only.
            return new SubjectsFilter(
                SubjectType: subjectRel.Namespace,
                OptionalSubjectIds: subjectIds,
                RelationFilter: SubjectRelationFilter.Any,
                OptionalResourceType: entrypoint.ContainingRelation.Namespace,
                OptionalResourceRelation: entrypoint.TuplesetRelation!);
        }

        return BuildSubjectsFilter(
            subjectRel, subjectIds, entrypoint.TargetRelation,
            includeWildcard: subjectRel.Relation == CoreConstants.Ellipsis);
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
                terminalSubject, caveatContext, now, cancellationToken: ct).ConfigureAwait(false);

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
