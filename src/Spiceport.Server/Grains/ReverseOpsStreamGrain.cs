using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// Default-placement, Guid-keyed implementation of <see cref="IReverseOpsStreamGrain"/>: streams the reverse
/// engine ops as native Orleans <see cref="IAsyncEnumerable{T}"/> iterators over a pinned datastore snapshot,
/// and also serves the unary whole-resource permission tree expansion. The pinning / index / collapse logic
/// is shared across all three ops via <see cref="ReverseOpsSupport"/>, so there is exactly one copy.
/// </summary>
/// <remarks>
/// Each streaming call holds one enumeration for the life of a single stream (the calling service mints a
/// fresh Guid per streaming RPC), so this grain is deliberately NOT a <c>[StatelessWorker]</c> — see the
/// interface remarks. The engine ops are themselves <see cref="IAsyncEnumerable{T}"/>, so a nested
/// <c>await foreach</c> + <c>yield</c> genuinely streams: when the caller stops enumerating (a client limit
/// reached), the upstream engine walk stops too. The cancellation token is threaded into the engine walk (the
/// prior unary methods ignored it).
/// <para>
/// <see cref="ExpandPermissionTree"/> touches no field outside its own method body/locals — it holds no
/// per-stream state — so it is safe for many callers to share the one well-known activation
/// (<see cref="IReverseOpsStreamGrain.ExpandKey"/>) that Expand calls target, even though that activation may
/// also be independently backing an unrelated Guid-keyed streaming enumeration under a different key.
/// </para>
/// </remarks>
public sealed class ReverseOpsStreamGrain(
    IDatastore datastore,
    ISchemaProvider schemaProvider,
    MembershipIndexCache membershipIndex) : Grain, IReverseOpsStreamGrain
{
    private ImmutableList<NamespaceDefinition> Namespaces => schemaProvider.Current.Namespaces;
    private ImmutableList<CaveatDefinition> Caveats => schemaProvider.Current.Caveats;

    /// <inheritdoc />
    public async Task<ExpandTreeReply> ExpandPermissionTree(ExpandTreeArgs args)
    {
        ArgumentNullException.ThrowIfNull(args);
        var (reader, now, token, _) = await ReverseOpsSupport
            .PinReader(datastore, args.Consistency, CancellationToken.None)
            .ConfigureAwait(ReverseOpsSupport.ContinueOnCapturedContext);

        var engine = new ExpandEngine(Namespaces);
        var mode = args.Mode == ExpandModeWire.Recursive ? ExpandMode.Recursive : ExpandMode.Shallow;
        var resource = new ObjectAndRelation(args.ResourceType, args.ResourceId, args.Permission);

        var tree = await engine
            .ExpandPermissionTree(reader, resource, mode, now, CancellationToken.None)
            .ConfigureAwait(ReverseOpsSupport.ContinueOnCapturedContext);

        // Expand carries verbatim caveat expressions; with no request context we collapse each against
        // an empty context so caveated nodes/subjects surface their missing parameter names.
        var evaluator = new CaveatEvaluator(Caveats);
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

    /// <inheritdoc />
    public async IAsyncEnumerable<FoundSubjectStreamItem> StreamLookupSubjects(
        LookupSubjectsArgs args,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        cancellationToken.ThrowIfCancellationRequested();
        var (reader, now, token, _) = await ReverseOpsSupport
            .PinReader(datastore, args.Consistency, cancellationToken)
            .ConfigureAwait(ReverseOpsSupport.ContinueOnCapturedContext);

        var engine = new LookupSubjectsEngine(Namespaces);
        var evaluator = new CaveatEvaluator(Caveats);
        var resource = new ObjectAndRelation(args.ResourceType, args.ResourceId, args.Permission);
        var after = ReverseOpsCursorCodec.DecodeSubjectId(args.Cursor);

        await foreach (var found in engine
            .LookupSubjects(reader, resource, args.SubjectType, args.SubjectRelation, now, cancellationToken)
            .WithCancellation(cancellationToken))
        {
            // Deterministic-by-id resume: skip ids at or before the cursor.
            if (after is { } a && string.CompareOrdinal(found.SubjectId, a) <= 0)
                continue;

            // Collapse the verbatim caveat against the request context.
            if (!ReverseOpsSupport.TryCollapse(found.Caveat, args.Context, evaluator, out var permissionship))
                continue; // sheared off entirely.

            // NOTE: FoundSubject.ExcludedSubjects (wildcard exclusions) are not yet carried over the wire —
            // FoundSubjectWire has no excluded-subjects field, so the cross-silo path drops them (unchanged
            // from the prior unary grain). The in-process engine preserves them.
            var subject = new FoundSubjectWire(found.SubjectId, found.IsWildcard, permissionship);
            yield return new FoundSubjectStreamItem(
                subject, ReverseOpsCursorCodec.EncodeSubjectId(found.SubjectId), token);
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<FoundResourceWire> StreamLookupResources(
        LookupResourcesArgs args,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        cancellationToken.ThrowIfCancellationRequested();
        var (reader, now, token, revision) = await ReverseOpsSupport
            .PinReader(datastore, args.Consistency, cancellationToken)
            .ConfigureAwait(ReverseOpsSupport.ContinueOnCapturedContext);

        // Read the snapshot once so the engine's namespaces/caveats and its pre-built reachability graph
        // are guaranteed to come from the same schema, even if a concurrent Update() swaps schemaProvider.Current
        // mid-call.
        var snapshot = schemaProvider.Current;
        var engine = new LookupResourcesEngine(snapshot.Namespaces, snapshot.Caveats, snapshot.ReachabilityFirst);
        // The Leopard accelerator (null unless enabled). The engine consults it only for a fresh, unpaged
        // enumeration of a covered shape and confirms every candidate with Check, so verdicts are unchanged.
        var index = await ReverseOpsSupport
            .AcquireIndex(membershipIndex, snapshot.Namespaces, snapshot.Caveats, reader, revision, cancellationToken)
            .ConfigureAwait(ReverseOpsSupport.ContinueOnCapturedContext);
        var startCursor = ReverseOpsCursorCodec.DecodeResources(args.Cursor);

        // A supplied client limit means the caller needs per-item resume cursors: pass the limit into the
        // engine so it runs the cursor-bearing live traversal (the Leopard fast path yields no per-item
        // cursor and is reachable only for an unlimited, cursorless enumeration, where the caller wants no
        // cursors). This mirrors the prior unary grain's engine-invocation decision exactly.
        var limit = args.Limit is { } l && l > 0 ? l : (int?)null;

        await foreach (var found in engine.LookupResources(
                reader, args.SubjectType, args.SubjectId, args.SubjectRelation,
                args.ResourceType, args.Permission, index, args.Context, now, startCursor, limit, cancellationToken)
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
