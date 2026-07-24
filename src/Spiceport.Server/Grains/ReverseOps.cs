using System.Runtime.CompilerServices;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// The reverse engine ops (LookupSubjects, LookupResources, ExpandPermissionTree) served IN-PROCESS over
/// the local silo's <see cref="IDatastore"/> projection — the same pattern <see cref="Spiceport.Api.AuthzedWatchV1Service"/>
/// uses deliberately for Watch (see its remarks). Every read these ops perform is served from the per-silo
/// <c>SiloProjection</c> (local memory), so an Orleans grain hop bought only compute placement, not a
/// consistency or caching benefit — unlike <see cref="CheckGrain"/>, <see cref="ISubjectFrontierGrain"/>,
/// and <see cref="IMembershipWalkGrain"/>, which memoize state across calls and so stay real grains, called
/// from these in-process walks exactly as they were called from the retired stream grains.
/// </summary>
/// <remarks>
/// The pinning / index-acquisition / caveat-collapse logic is the same as before the retired grains: see
/// <see cref="ReverseOpsSupport"/> for the shared PinReader / AcquireCoveredCandidates / TryCollapse helpers.
/// The reader handed to each ENGINE comes from the <see cref="IGraphReaderSource"/> seam (projection or
/// shard mesh, per <see cref="GraphReaderOptions"/>) at the pinned revision; everything else about
/// PinReader — token mint, schema resolution, cursor handling — keeps the pinned <c>IDatastoreReader</c>.
/// Since this is no longer grain code, the streaming ops take the caller's plain <see cref="CancellationToken"/>
/// with no Orleans grain-method plumbing — simpler than the retired grain's per-stream-activation scheme,
/// with identical per-item cancellation behavior.
/// </remarks>
public sealed class ReverseOps(
    IDatastore datastore,
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
    private Task<SchemaSnapshot> ResolveSchema(string? schemaHash, IDatastoreReader reader, CancellationToken ct) =>
        schemaResolver.ResolveAsync(schemaHash, reader, schemaProvider.Current, ct);

    /// <summary>Expands the resource's permission into a structural permission tree.</summary>
    public async Task<ExpandTreeReply> ExpandPermissionTree(ExpandTreeArgs args)
    {
        ArgumentNullException.ThrowIfNull(args);
        var (reader, now, token, revision, schemaHash) = await ReverseOpsSupport
            .PinReader(datastore, args.Consistency, CancellationToken.None);

        var schema = await ResolveSchema(schemaHash, reader, CancellationToken.None);
        var engine = new ExpandEngine(schema.Namespaces);
        var mode = args.Mode == ExpandModeWire.Recursive ? ExpandMode.Recursive : ExpandMode.Shallow;
        var resource = new ObjectAndRelation(args.ResourceType, args.ResourceId, args.Permission);

        // The engine walk reads through the IGraphReaderSource seam at the same pinned revision; the
        // pinned IDatastoreReader above keeps token minting and schema resolution — see IGraphReaderSource.
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
        var (reader, now, token, revision, schemaHash) = await ReverseOpsSupport
            .PinReader(datastore, args.Consistency, cancellationToken);

        var schema = await ResolveSchema(schemaHash, reader, cancellationToken);
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
    /// time, each with the opaque cursor positioned immediately after it. When <see cref="LookupResourcesArgs.Limit"/>
    /// is set this runs the cursor-bearing live traversal (so every item carries a resume cursor); an
    /// unlimited, cursorless enumeration may take the Leopard fast path (a complete candidate set confirmed
    /// by Check). The caller applies any client limit by stopping.
    /// </summary>
    public async IAsyncEnumerable<FoundResourceWire> StreamLookupResources(
        LookupResourcesArgs args,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        cancellationToken.ThrowIfCancellationRequested();
        var (reader, now, token, revision, schemaHash) = await ReverseOpsSupport
            .PinReader(datastore, args.Consistency, cancellationToken);

        // Resolve the schema effective at the pinned revision (the same schema the confirming Check mesh
        // evaluates under), so the candidate superset and the verdicts that confirm it are schema-consistent
        // and cluster-consistent — never the possibly-stale ambient current schema on a non-writer silo. The
        // snapshot is captured once so the engine's namespaces/caveats and its pre-built reachability graph
        // all come from the same schema, even if a concurrent write swaps the ambient current mid-call.
        var snapshot = await ResolveSchema(schemaHash, reader, cancellationToken);
        var engine = new LookupResourcesEngine(snapshot.Namespaces, snapshot.Caveats, snapshot.ReachabilityFirst);
        var startCursor = ReverseOpsCursorCodec.DecodeResources(args.Cursor);

        // A supplied client limit means the caller needs per-item resume cursors: pass the limit into the
        // engine so it runs the cursor-bearing live traversal (the Leopard fast path yields no per-item
        // cursor and is reachable only for an unlimited, cursorless enumeration, where the caller wants no
        // cursors). This mirrors the retired grain's engine-invocation decision exactly.
        var limit = args.Limit is { } l && l > 0 ? l : (int?)null;

        // The Leopard accelerator (null unless it decides this request is a fresh, unpaged enumeration of
        // a covered shape). Dispatches the membership-walk grain mesh and confirms every candidate with
        // Check, so verdicts are unchanged from the live traversal.
        var candidates = await ReverseOpsSupport.AcquireCoveredCandidates(
            grainFactory, membershipWalkOptions, snapshot,
            args.SubjectType, args.SubjectId, args.SubjectRelation, args.ResourceType, args.Permission,
            revision, hasCursorOrLimit: args.Cursor is not null || limit is not null, cancellationToken);

        // The engine traversal reads through the IGraphReaderSource seam at the same pinned revision;
        // the pinned IDatastoreReader above keeps token minting, schema resolution and cursor handling.
        await foreach (var found in engine.LookupResources(
                readerSource.GraphReaderAt(revision), args.SubjectType, args.SubjectId, args.SubjectRelation,
                args.ResourceType, args.Permission, candidates, args.Context, now, startCursor, limit, cancellationToken)
            .WithCancellation(cancellationToken))
        {
            var permissionship = found.Membership == Membership.Caveated
                ? Permissionship.Caveated(found.MissingContextParams)
                : Permissionship.Member;
            yield return new FoundResourceWire(
                found.ResourceId, permissionship, ReverseOpsCursorCodec.Encode(found.AfterCursor), token);
        }
    }
}
