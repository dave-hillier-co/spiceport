using System.Collections.Immutable;
using Orleans.Concurrency;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// Stateless-worker implementation of <see cref="IReverseOpsGrain"/>: runs the whole-resource permission
/// tree expansion (ExpandPermissionTree) against a pinned datastore snapshot and maps the result onto the
/// serializable reply DTO.
/// </summary>
/// <remarks>
/// Expand resolves the datastore's optimized (quantized) revision and a snapshot reader at it, builds the
/// ExpandEngine over the process-wide compiled schema, and walks the rewrite structurally. The grain is
/// <c>[StatelessWorker]</c> so the silo scales activations with load; it carries no per-key identity, so
/// callers always use <see cref="IReverseOpsGrain.Key"/>. The reverse LOOKUP ops (LookupSubjects,
/// LookupResources) moved to the Guid-keyed <see cref="ReverseOpsStreamGrain"/>, which streams them natively;
/// the shared pinning / index / collapse logic lives in <see cref="ReverseOpsSupport"/>. Expand stays here,
/// unary — it returns a whole tree with no cursor.
/// </remarks>
[StatelessWorker]
public sealed class ReverseOpsGrain(
    IDatastore datastore,
    ISchemaProvider schemaProvider) : Grain, IReverseOpsGrain
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
}
