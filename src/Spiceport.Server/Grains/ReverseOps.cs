using System.Runtime.CompilerServices;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// The reverse engine ops (LookupSubjects, LookupResources, ExpandPermissionTree) served IN-PROCESS —
/// the same pattern <see cref="Spiceport.Api.AuthzedWatchV1Service"/> uses deliberately for Watch (see
/// its remarks). These ops are pure per-request walks with nothing to memoize, so an Orleans grain hop
/// would buy only compute placement, not a consistency or caching benefit — unlike
/// <see cref="CheckGrain"/>, <see cref="ISubjectFrontierGrain"/>, and <see cref="IMembershipWalkGrain"/>,
/// which memoize state across calls and so stay real grains, called from these in-process walks exactly
/// as they were called from the retired stream grains.
/// </summary>
/// <remarks>
/// The pinning / index-acquisition / caveat-collapse logic is the same as before the retired grains: see
/// <see cref="ReverseOpsSupport"/> for the shared PinRevision / AcquireCoveredCandidates / TryCollapse helpers.
/// The reader handed to each ENGINE comes from the <see cref="IGraphReaderSource"/> seam (the shard
/// mesh) at the pinned revision; schema resolution goes
/// through the <see cref="ISchemaSource"/> seam (a sequencer read once per hash per silo, cached by
/// <see cref="SchemaResolver"/>); <see cref="IDatastore"/> remains only for revision resolution and
/// token minting (PinRevision). Since this is no longer grain code, the streaming ops take the caller's
/// plain <see cref="CancellationToken"/> with no Orleans grain-method plumbing — simpler than the
/// retired grain's per-stream-activation scheme, with identical per-item cancellation behavior.
/// </remarks>
public sealed class ReverseOps(
    IDatastore datastore,
    ISchemaSource schemaSource,
    ISchemaProvider schemaProvider,
    SchemaResolver schemaResolver,
    IGrainFactory grainFactory,
    MembershipWalkOptions membershipWalkOptions,
    IGraphReaderSource readerSource,
    SubjectFrontierMemoOptions? frontierMemoOptions = null)
{
    private readonly SubjectFrontierMemoOptions _frontierMemoOptions = frontierMemoOptions ?? new SubjectFrontierMemoOptions();

    /// <summary>
    /// Resolves the compiled schema effective at the pinned revision (the same schema the confirming Check
    /// mesh evaluates under), rather than the possibly-stale ambient current schema on a non-writer silo.
    /// </summary>
    private Task<SchemaSnapshot> ResolveSchema(string? schemaHash, IRevision revision, CancellationToken ct) =>
        schemaResolver.ResolveAsync(schemaHash, schemaSource, revision, schemaProvider.Current, ct);

    /// <summary>Expands the resource's permission into a structural permission tree.</summary>
    public async Task<ExpandTreeReply> ExpandPermissionTree(ExpandTreeArgs args)
    {
        ArgumentNullException.ThrowIfNull(args);
        var (now, token, revision, schemaHash) = await ReverseOpsSupport
            .PinRevision(datastore, args.Consistency, CancellationToken.None);

        var schema = await ResolveSchema(schemaHash, revision, CancellationToken.None);
        var engine = new ExpandEngine(schema.Namespaces);
        var mode = args.Mode == ExpandModeWire.Recursive ? ExpandMode.Recursive : ExpandMode.Shallow;
        var resource = new ObjectAndRelation(args.ResourceType, args.ResourceId, args.Permission);

        // The engine walk reads through the IGraphReaderSource seam at the same pinned revision; token
        // minting stays on PinRevision and schema resolution on the ISchemaSource seam.
        var tree = await engine.ExpandPermissionTree(
            readerSource.GraphReaderAt(revision), resource, mode, now, CancellationToken.None);

        // Expand carries verbatim caveat expressions; with no request context we collapse each against
        // an empty context so caveated nodes/subjects surface their missing parameter names.
        var evaluator = new CaveatEvaluator(schema.Caveats);
        return new ExpandTreeReply(ToWire(tree, evaluator), token);
    }

    private static ExpandTreeNodeWire ToWire(PermissionTreeNode node, CaveatEvaluator evaluator)
    {
        var nodeMissing = MissingOf(node.Caveat, evaluator);
        return node switch
        {
            PermissionTreeNode.Leaf leaf => new ExpandTreeNodeWire(
                leaf.Expanded.ObjectType, leaf.Expanded.ObjectId, leaf.Expanded.Relation,
                nodeMissing, IsLeaf: true, SetOpWire.Union,
                Subjects: leaf.Subjects.Select(s => ToWire(s, evaluator)).ToList(),
                Children: []),

            PermissionTreeNode.SetOp op => new ExpandTreeNodeWire(
                op.Expanded.ObjectType, op.Expanded.ObjectId, op.Expanded.Relation,
                nodeMissing, IsLeaf: false, ToWire(op.Operation),
                Subjects: [],
                Children: op.Children.Select(c => ToWire(c, evaluator)).ToList()),

            _ => throw new NotSupportedException($"Unknown permission tree node: {node.GetType().Name}"),
        };
    }

    private static ExpandSubjectWire ToWire(DirectSubject subject, CaveatEvaluator evaluator) =>
        new(subject.Subject.ObjectType, subject.Subject.ObjectId, subject.Subject.Relation,
            subject.Subject.IsPublicWildcard, MissingOf(subject.Caveat, evaluator));

    // Expand has no request context, so a caveat collapses to its missing parameter names (or empty
    // when the caveat is statically determinable). A definitely-false caveat still surfaces no fields
    // here — the structural tree is not pruned by Expand (it carries the structure verbatim).
    private static IReadOnlyList<string> MissingOf(CaveatExpression? caveat, CaveatEvaluator evaluator) =>
        caveat is null ? [] : evaluator.EvaluateExpression(caveat, requestContext: null).MissingFields;

    private static SetOpWire ToWire(SetOperationType op) => op switch
    {
        SetOperationType.Union => SetOpWire.Union,
        SetOperationType.Intersection => SetOpWire.Intersection,
        SetOperationType.Exclusion => SetOpWire.Exclusion,
        _ => SetOpWire.Union,
    };

    /// <summary>
    /// Streams the subjects (of the requested type/subrelation) holding the resource's permission, one at a
    /// time, each with the opaque cursor positioned immediately after it. The caller applies any client
    /// limit by stopping enumeration; there is no page cap inside this walk.
    /// </summary>
    public async IAsyncEnumerable<FoundSubjectStreamItem> StreamLookupSubjects(
        LookupSubjectsArgs args,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        cancellationToken.ThrowIfCancellationRequested();
        var (now, token, revision, schemaHash) = await ReverseOpsSupport
            .PinRevision(datastore, args.Consistency, cancellationToken);

        var schema = await ResolveSchema(schemaHash, revision, cancellationToken);
        var evaluator = new CaveatEvaluator(schema.Caveats);
        var resource = new ObjectAndRelation(args.ResourceType, args.ResourceId, args.Permission);
        var after = ReverseOpsCursorCodec.DecodeSubjectId(args.Cursor);

        // BELOW the pin/schema resolution: the pre-context frontier is either served from the
        // SubjectFrontierGrain activation memo (an exact memo of this identical computation at a pinned
        // identity, same class as CheckGrain's memo) or walked live via the engine, unchanged. Either way
        // the result feeds the SAME cursor-skip / caveat-collapse loop below, so the two paths cannot drift.
        var walk = _frontierMemoOptions.Enabled
            ? await MemoizedFrontier(resource, args, revision, schemaHash, cancellationToken)
            : new LookupSubjectsEngine(schema.Namespaces)
                .LookupSubjects(
                    readerSource.GraphReaderAt(revision), resource, args.SubjectType, args.SubjectRelation,
                    now, cancellationToken);

        await foreach (var found in walk.WithCancellation(cancellationToken))
        {
            // Deterministic-by-id resume: skip ids at or before the cursor.
            if (after is { } a && string.CompareOrdinal(found.SubjectId, a) <= 0)
                continue;

            // Collapse the verbatim caveat against the request context.
            if (!ReverseOpsSupport.TryCollapse(found.Caveat, args.Context, evaluator, out var permissionship))
                continue; // sheared off entirely.

            // NOTE: FoundSubject.ExcludedSubjects (wildcard exclusions) are not yet carried over the wire —
            // FoundSubjectWire has no excluded-subjects field, so the client-facing shape drops them
            // (unchanged from before). The engine (and the memoized frontier, which mirrors it
            // byte-for-byte) preserves them internally; only this client-edge wire shape drops them.
            var subject = new FoundSubjectWire(found.SubjectId, found.IsWildcard, permissionship);
            yield return new FoundSubjectStreamItem(
                subject, ReverseOpsCursorCodec.EncodeSubjectId(found.SubjectId), token);
        }
    }

    /// <summary>
    /// Resolves the pre-context frontier via the <see cref="ISubjectFrontierGrain"/> activation memo and
    /// replays it as an <see cref="IAsyncEnumerable{T}"/> in the engine's own walk order (no sort/reorder),
    /// so it slots into the identical post-processing loop the live engine walk uses.
    /// </summary>
    private async Task<IAsyncEnumerable<FoundSubject>> MemoizedFrontier(
        ObjectAndRelation resource, LookupSubjectsArgs args, IRevision revision, string? schemaHash,
        CancellationToken cancellationToken)
    {
        var key = SubjectFrontierKey.Build(
            resource, args.SubjectType, args.SubjectRelation,
            revision.ToString(), schemaHash ?? schemaProvider.Current.SchemaHash);
        var frontierGrain = grainFactory.GetGrain<ISubjectFrontierGrain>(key);

        // The caller's own token drives the grain call directly: Orleans' native cancellation-token
        // support propagates it to the activation, so caller cancellation is never left unobserved.
        var reply = await frontierGrain.GetFrontier(cancellationToken);

        return ToAsyncEnumerable(reply.Subjects.Select(FrontierWire.FromWire).ToList());
    }

    private static async IAsyncEnumerable<FoundSubject> ToAsyncEnumerable(
        IReadOnlyList<FoundSubject> items,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var item in items)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return item;
        }
        await Task.CompletedTask;
    }

    /// <summary>
    /// Streams the resources (of the requested type) on which the subject holds the permission, one at a
    /// time, each with the opaque cursor positioned immediately after it — on every path, limited or not,
    /// as SpiceDB does. When <see cref="LookupResourcesArgs.Limit"/> or an engine cursor is set this runs
    /// the live traversal, whose cursors are engine positions; an unlimited, cursorless enumeration may
    /// take the Leopard fast path (a complete candidate set confirmed by Check, streamed in ordinal id
    /// order), whose cursors are candidate-resume cursors. The caller applies any client limit by stopping.
    /// </summary>
    public async IAsyncEnumerable<FoundResourceWire> StreamLookupResources(
        LookupResourcesArgs args,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        cancellationToken.ThrowIfCancellationRequested();
        var (now, token, revision, schemaHash) = await ReverseOpsSupport
            .PinRevision(datastore, args.Consistency, cancellationToken);

        // Resolve the schema effective at the pinned revision (the same schema the confirming Check mesh
        // evaluates under), so the candidate superset and the verdicts that confirm it are schema-consistent
        // and cluster-consistent — never the possibly-stale ambient current schema on a non-writer silo. The
        // snapshot is captured once so the engine's namespaces/caveats and its pre-built reachability graph
        // all come from the same schema, even if a concurrent write swaps the ambient current mid-call.
        var snapshot = await ResolveSchema(schemaHash, revision, cancellationToken);
        var engine = new LookupResourcesEngine(snapshot.Namespaces, snapshot.Caveats, snapshot.ReachabilityFirst);
        var reader = readerSource.GraphReaderAt(revision);
        var limit = args.Limit is { } l && l > 0 ? l : (int?)null;

        // A candidate-resume cursor continues a stream the Leopard accelerator produced: that stream is the
        // covered candidates in ordinal id order, so the resume re-derives the same ordered set and skips
        // ids at or before the last one emitted. Any limit is applied by the caller stopping.
        if (ReverseOpsCursorCodec.TryDecodeCandidateResume(args.Cursor, out var lastCandidate))
        {
            await foreach (var item in CandidateOrderedStream(
                    engine, reader, snapshot, args, revision, now, token, lastCandidate, cancellationToken)
                .WithCancellation(cancellationToken))
            {
                yield return item;
            }
            yield break;
        }

        var startCursor = ReverseOpsCursorCodec.DecodeResources(args.Cursor);

        // A client limit or an engine cursor runs the cursor-bearing live traversal (a paged walk must
        // resume where the engine left off). An unlimited, cursorless enumeration may take the Leopard
        // accelerator (null unless it decides this shape is covered): it dispatches the membership-walk
        // grain mesh and confirms every candidate with Check, so verdicts are unchanged from the live
        // traversal, and each item carries a candidate-resume cursor.
        var candidates = await ReverseOpsSupport.AcquireCoveredCandidates(
            grainFactory, membershipWalkOptions, snapshot,
            args.SubjectType, args.SubjectId, args.SubjectRelation, args.ResourceType, args.Permission,
            revision, hasCursorOrLimit: args.Cursor is not null || limit is not null, cancellationToken);

        if (candidates is not null)
        {
            await foreach (var item in ConfirmCandidates(
                    engine, reader, args, OrdinalAfter(candidates, after: null), now, token, cancellationToken)
                .WithCancellation(cancellationToken))
            {
                yield return item;
            }
            yield break;
        }

        // The engine traversal reads through the IGraphReaderSource seam at the same pinned revision;
        // token minting stays on PinRevision, schema resolution on the ISchemaSource seam, and cursor
        // handling on the codec above.
        await foreach (var found in engine.LookupResources(
                reader, args.SubjectType, args.SubjectId, args.SubjectRelation,
                args.ResourceType, args.Permission, args.Context, now, startCursor, limit, cancellationToken)
            .WithCancellation(cancellationToken))
        {
            yield return new FoundResourceWire(
                found.ResourceId, ToPermissionship(found), ReverseOpsCursorCodec.Encode(found.AfterCursor), token);
        }
    }

    /// <summary>
    /// Resumes a candidate-ordered stream after <paramref name="lastCandidate"/>. The accelerator is asked
    /// again as for a fresh enumeration; if it declines (disabled, uncovered, or an incomplete walk), the
    /// live traversal is drained instead, deduplicated by resource id, and put in the same ordinal order,
    /// so the resume is positioned identically either way; under a client limit the drain retains only the
    /// page it can return.
    /// </summary>
    private async IAsyncEnumerable<FoundResourceWire> CandidateOrderedStream(
        LookupResourcesEngine engine,
        IGraphReader reader,
        SchemaSnapshot snapshot,
        LookupResourcesArgs args,
        IRevision revision,
        DateTimeOffset now,
        string token,
        string lastCandidate,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var candidates = await ReverseOpsSupport.AcquireCoveredCandidates(
            grainFactory, membershipWalkOptions, snapshot,
            args.SubjectType, args.SubjectId, args.SubjectRelation, args.ResourceType, args.Permission,
            revision, hasCursorOrLimit: false, cancellationToken);

        if (candidates is not null)
        {
            await foreach (var item in ConfirmCandidates(
                    engine, reader, args, OrdinalAfter(candidates, lastCandidate), now, token, cancellationToken)
                .WithCancellation(cancellationToken))
            {
                yield return item;
            }
            yield break;
        }

        // Live fallback: one result per resource id. A resource reached through several entrypoints is
        // a member if any path makes it one; otherwise it keeps the first caveated result's missing params.
        // The traversal is unordered, so the whole result set is walked, but only ids after the cursor are
        // kept and, under a client limit, only the `limit` lowest of them: retained memory is bounded by
        // the page size. An id evicted for being above the retained maximum can never re-enter, because
        // the retained set only ever gets lower, so later results for it are discarded the same way.
        var cap = args.Limit is { } l && l > 0 ? l : (int?)null;
        var byId = new Dictionary<string, FoundResource>(StringComparer.Ordinal);
        var retained = new SortedSet<string>(StringComparer.Ordinal);
        await foreach (var found in engine.LookupResources(
                reader, args.SubjectType, args.SubjectId, args.SubjectRelation,
                args.ResourceType, args.Permission, args.Context, now, cursor: null, limit: null, cancellationToken)
            .WithCancellation(cancellationToken))
        {
            var id = found.ResourceId;
            if (string.CompareOrdinal(id, lastCandidate) <= 0)
                continue;

            if (byId.TryGetValue(id, out var existing))
            {
                if (existing.Membership != Membership.Member && found.Membership == Membership.Member)
                    byId[id] = found;
                continue;
            }

            if (cap is { } c && retained.Count >= c)
            {
                if (string.CompareOrdinal(id, retained.Max) > 0)
                    continue;
                byId.Remove(retained.Max!);
                retained.Remove(retained.Max!);
            }

            byId[id] = found;
            retained.Add(id);
        }

        foreach (var id in OrdinalAfter(byId.Keys, lastCandidate))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return new FoundResourceWire(
                id, ToPermissionship(byId[id]), ReverseOpsCursorCodec.EncodeCandidateResume(id), token);
        }
    }

    /// <summary>Confirms ordered covered candidates with Check, each item carrying a candidate-resume cursor.</summary>
    private static async IAsyncEnumerable<FoundResourceWire> ConfirmCandidates(
        LookupResourcesEngine engine,
        IGraphReader reader,
        LookupResourcesArgs args,
        IReadOnlyList<string> orderedCandidates,
        DateTimeOffset now,
        string token,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var found in engine.LookupResources(
                reader, args.SubjectType, args.SubjectId, args.SubjectRelation,
                args.ResourceType, args.Permission, orderedCandidates, args.Context, now,
                cursor: null, limit: null, cancellationToken)
            .WithCancellation(cancellationToken))
        {
            yield return new FoundResourceWire(
                found.ResourceId, ToPermissionship(found),
                ReverseOpsCursorCodec.EncodeCandidateResume(found.ResourceId), token);
        }
    }

    /// <summary>The distinct ids in ordinal order, keeping only those strictly after <paramref name="after"/>.</summary>
    private static List<string> OrdinalAfter(IEnumerable<string> ids, string? after) =>
        ids.Distinct(StringComparer.Ordinal)
            .Where(id => after is null || string.CompareOrdinal(id, after) > 0)
            .Order(StringComparer.Ordinal)
            .ToList();

    private static Permissionship ToPermissionship(FoundResource found) =>
        found.Membership == Membership.Caveated
            ? Permissionship.Caveated(found.MissingContextParams)
            : Permissionship.Member;
}
