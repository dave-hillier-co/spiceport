using System.Collections.Immutable;
using Orleans.Concurrency;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// Stateless-worker implementation of <see cref="IReverseOpsGrain"/>: runs the whole-resource /
/// whole-subject tree and reverse engine ops against a pinned datastore snapshot and maps their
/// results onto the serializable reply DTOs.
/// </summary>
/// <remarks>
/// Each call resolves the datastore's optimized (quantized) revision and a snapshot reader at it, builds
/// the relevant engine over the process-wide compiled schema, and runs the op. ExpandPermissionTree and
/// LookupSubjects walk the rewrite structurally; LookupResources uses the reachability graph and confirms
/// intersection / exclusion candidates with the engine's own Check (kept in-process for this slice — the
/// distributed Check mesh is exercised through <see cref="ICheckGrain"/>). The grain is
/// <c>[StatelessWorker]</c> so the silo scales activations with load; it carries no per-key identity, so
/// callers always use <see cref="IReverseOpsGrain.Key"/>.
/// </remarks>
[StatelessWorker]
public sealed class ReverseOpsGrain(
    IDatastore datastore,
    ISchemaProvider schemaProvider) : Grain, IReverseOpsGrain
{
    private ImmutableList<NamespaceDefinition> Namespaces => schemaProvider.Schema.Namespaces;
    private ImmutableList<CaveatDefinition> Caveats => schemaProvider.Schema.Caveats;

    /// <inheritdoc />
    public async Task<ExpandTreeReply> ExpandPermissionTree(ExpandTreeArgs args)
    {
        ArgumentNullException.ThrowIfNull(args);
        var (reader, now) = await PinReader().ConfigureAwait(ContinueOnCapturedContext);

        var engine = new ExpandEngine(Namespaces);
        var mode = args.Mode == ExpandModeWire.Recursive ? ExpandMode.Recursive : ExpandMode.Shallow;
        var resource = new ObjectAndRelation(args.ResourceType, args.ResourceId, args.Permission);

        var tree = await engine
            .ExpandPermissionTree(reader, resource, mode, now, CancellationToken.None)
            .ConfigureAwait(ContinueOnCapturedContext);

        // Expand carries verbatim caveat expressions; with no request context we collapse each against
        // an empty context so caveated nodes/subjects surface their missing parameter names.
        var evaluator = new CaveatEvaluator(Caveats);
        return new ExpandTreeReply(ToWire(tree, evaluator));
    }

    /// <inheritdoc />
    public async Task<LookupSubjectsReply> LookupSubjects(LookupSubjectsArgs args)
    {
        ArgumentNullException.ThrowIfNull(args);
        var (reader, now) = await PinReader().ConfigureAwait(ContinueOnCapturedContext);

        var engine = new LookupSubjectsEngine(Namespaces);
        var evaluator = new CaveatEvaluator(Caveats);
        var resource = new ObjectAndRelation(args.ResourceType, args.ResourceId, args.Permission);
        var after = ReverseOpsCursorCodec.DecodeSubjectId(args.Cursor);
        var limit = args.Limit is { } l && l > 0 ? l : (int?)null;

        var page = new List<FoundSubjectWire>();
        var lastId = (string?)null;
        var moreRemain = false;

        await foreach (var found in engine
            .LookupSubjects(reader, resource, args.SubjectType, args.SubjectRelation, now, CancellationToken.None))
        {
            // Deterministic-by-id resume: skip ids at or before the cursor.
            if (after is { } a && string.CompareOrdinal(found.SubjectId, a) <= 0)
                continue;

            // Collapse the verbatim caveat against the request context.
            if (!TryCollapse(found.Caveat, args.Context, evaluator, out var permissionship))
                continue; // sheared off entirely.

            if (limit is { } cap && page.Count >= cap)
            {
                moreRemain = true;
                break;
            }

            page.Add(new FoundSubjectWire(found.SubjectId, found.IsWildcard, permissionship));
            lastId = found.SubjectId;
        }

        var cursor = moreRemain && lastId is not null
            ? ReverseOpsCursorCodec.EncodeSubjectId(lastId)
            : null;
        return new LookupSubjectsReply(page, cursor);
    }

    /// <inheritdoc />
    public async Task<LookupResourcesReply> LookupResources(LookupResourcesArgs args)
    {
        ArgumentNullException.ThrowIfNull(args);
        var (reader, now) = await PinReader().ConfigureAwait(ContinueOnCapturedContext);

        var engine = new LookupResourcesEngine(Namespaces, Caveats);
        var startCursor = ReverseOpsCursorCodec.DecodeResources(args.Cursor);
        var limit = args.Limit is { } l && l > 0 ? l : (int?)null;

        var page = new List<FoundResourceWire>();
        LookupResourcesCursor? afterCursor = null;
        var produced = 0;
        var moreRemain = false;

        // Request one more than the page size so we can tell whether further results exist.
        var fetch = limit is { } cap ? cap + 1 : (int?)null;

        await foreach (var found in engine.LookupResources(
            reader, args.SubjectType, args.SubjectId, args.SubjectRelation,
            args.ResourceType, args.Permission, args.Context, now, startCursor, fetch, CancellationToken.None))
        {
            if (limit is { } cap2 && produced >= cap2)
            {
                moreRemain = true;
                break;
            }

            var permissionship = found.Membership == Membership.Caveated
                ? Permissionship.Caveated(found.MissingContextParams)
                : Permissionship.Member;
            page.Add(new FoundResourceWire(found.ResourceId, permissionship));
            afterCursor = found.AfterCursor;
            produced++;
        }

        var cursor = moreRemain ? ReverseOpsCursorCodec.Encode(afterCursor) : null;
        return new LookupResourcesReply(page, cursor);
    }

    // Orleans grain code must not ConfigureAwait(false); keep the captured context.
    private const ConfigureAwaitOptions ContinueOnCapturedContext = ConfigureAwaitOptions.ContinueOnCapturedContext;

    private async Task<(IDatastoreReader Reader, DateTimeOffset Now)> PinReader()
    {
        var optimized = await datastore.OptimizedRevision(CancellationToken.None)
            .ConfigureAwait(ContinueOnCapturedContext);
        return (datastore.SnapshotReader(optimized.Revision), DateTimeOffset.UtcNow);
    }

    /// <summary>
    /// Collapses a verbatim caveat against the request context. Returns false when the subject is
    /// definitely excluded; otherwise sets the collapsed permissionship (member or caveated).
    /// </summary>
    private static bool TryCollapse(
        CaveatExpression? caveat,
        IReadOnlyDictionary<string, object?>? context,
        CaveatEvaluator evaluator,
        out Permissionship permissionship)
    {
        if (caveat is null)
        {
            permissionship = Permissionship.Member;
            return true;
        }

        var result = evaluator.EvaluateExpression(caveat, context);
        switch (result.Outcome)
        {
            case CaveatOutcome.DefinitelyTrue:
                permissionship = Permissionship.Member;
                return true;
            case CaveatOutcome.Caveated:
                permissionship = Permissionship.Caveated(result.MissingFields);
                return true;
            default:
                permissionship = Permissionship.Member;
                return false;
        }
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
}
