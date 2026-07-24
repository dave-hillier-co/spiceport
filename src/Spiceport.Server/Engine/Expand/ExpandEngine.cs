using System.Collections.Immutable;
using Spiceport.Core;
using Spiceport.Datastore;

namespace Spiceport.Engine;

/// <summary>
/// Expands a resource ONR into a <see cref="PermissionTreeNode"/> tree that mirrors the
/// userset-rewrite structure of the expanded relation.
/// </summary>
/// <remarks>
/// Port of SpiceDB's <c>internal/graph/expand.go</c>. Unlike LookupResources this walks the rewrite
/// structurally and does not consult any reachability graph (matching SpiceDB, where
/// <c>expand.go</c> never touches reachability). Caveats are carried verbatim; the tree is the
/// structural expansion and is not collapsed against a request context. Recursion is bounded by a
/// depth limit and a visited-set cycle guard so cyclic schemas terminate.
/// </remarks>
public sealed class ExpandEngine
{
    /// <summary>The default maximum recursion depth.</summary>
    public const int DefaultMaxDepth = 50;

    private readonly ImmutableDictionary<string, NamespaceDefinition> _namespaces;
    private readonly int _maxDepth;

    /// <summary>Creates an expand engine over the given schema definitions.</summary>
    /// <param name="namespaces">The compiled namespace definitions that make up the schema.</param>
    /// <param name="maxDepth">The maximum recursion depth before expansion stops.</param>
    public ExpandEngine(IEnumerable<NamespaceDefinition> namespaces, int maxDepth = DefaultMaxDepth)
    {
        ArgumentNullException.ThrowIfNull(namespaces);
        _namespaces = namespaces.ToImmutableDictionary(ns => ns.Name);
        _maxDepth = maxDepth;
    }

    /// <summary>
    /// Expands <paramref name="resource"/> into a permission tree as of the given reader's snapshot.
    /// </summary>
    /// <param name="reader">A graph reader pinned to the revision to evaluate against.</param>
    /// <param name="resource">The resource ONR (object type, id and relation/permission) to expand.</param>
    /// <param name="mode">Shallow (one level) or Recursive (expand non-terminal usersets).</param>
    /// <param name="evaluationTime">Optional pinned "now" for expiration filtering; defaults to system UTC.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    public Task<PermissionTreeNode> ExpandPermissionTree(
        IGraphReader reader,
        ObjectAndRelation resource,
        ExpandMode mode = ExpandMode.Shallow,
        DateTimeOffset? evaluationTime = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(resource);
        var now = evaluationTime ?? SystemClock.Instance.UtcNow;
        return Expand(reader, resource, mode, now, _maxDepth, ImmutableHashSet<string>.Empty, cancellationToken);
    }

    private async Task<PermissionTreeNode> Expand(
        IGraphReader reader,
        ObjectAndRelation resource,
        ExpandMode mode,
        DateTimeOffset now,
        int depthRemaining,
        ImmutableHashSet<string> visited,
        CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        var key = $"{resource.ObjectType}:{resource.ObjectId}#{resource.Relation}";
        if (depthRemaining <= 0 || visited.Contains(key))
            return new PermissionTreeNode.Leaf(resource, []);
        visited = visited.Add(key);

        var relation = LookupRelation(resource.ObjectType, resource.Relation);
        if (relation is null)
            return new PermissionTreeNode.Leaf(resource, []);

        return relation.UsersetRewrite is { } rewrite
            ? await ExpandRewrite(reader, resource, rewrite.Operation, mode, now, depthRemaining, visited, ct).ConfigureAwait(false)
            : await ExpandDirect(reader, resource, mode, now, depthRemaining, visited, ct).ConfigureAwait(false);
    }

    /// <summary>Expands a base relation's directly-written tuples (port of <c>expandDirect</c>).</summary>
    private async Task<PermissionTreeNode> ExpandDirect(
        IGraphReader reader,
        ObjectAndRelation resource,
        ExpandMode mode,
        DateTimeOffset now,
        int depthRemaining,
        ImmutableHashSet<string> visited,
        CancellationToken ct)
    {
        var filter = new RelationshipsFilter
        {
            OptionalResourceType = resource.ObjectType,
            OptionalResourceIds = [resource.ObjectId],
            OptionalResourceRelation = resource.Relation,
        };

        var allSubjects = new List<DirectSubject>();
        var nonTerminal = new List<DirectSubject>();

        await foreach (var rel in reader.QueryRelationships(filter, ct).ConfigureAwait(false))
        {
            if (IsExpired(rel, now))
                continue;

            var ds = new DirectSubject(rel.Subject, CaveatOf(rel));
            allSubjects.Add(ds);

            // Terminal = ellipsis or wildcard; non-terminal = a subrelation (userset) to recurse into.
            if (rel.Subject.Relation != CoreConstants.Ellipsis && !rel.Subject.IsPublicWildcard)
                nonTerminal.Add(ds);
        }

        if (mode == ExpandMode.Shallow || nonTerminal.Count == 0)
            return new PermissionTreeNode.Leaf(resource, allSubjects);

        // Recursive: expand each non-terminal userset and union with the verbatim leaf, attaching
        // each child's tuple caveat (port of decorateWithCaveatIfNecessary).
        var children = new List<PermissionTreeNode>();
        foreach (var ds in nonTerminal)
        {
            var child = await Expand(reader, ds.Subject, mode, now, depthRemaining - 1, visited, ct).ConfigureAwait(false);
            children.Add(DecorateWithCaveat(child, ds.Caveat));
        }
        children.Add(new PermissionTreeNode.Leaf(resource, allSubjects));

        return new PermissionTreeNode.SetOp(resource, SetOperationType.Union, children);
    }

    /// <summary>Expands a rewrite set operation (port of <c>expandUsersetRewrite</c>/<c>expandSetOperation</c>).</summary>
    private async Task<PermissionTreeNode> ExpandRewrite(
        IGraphReader reader,
        ObjectAndRelation resource,
        SetOperation operation,
        ExpandMode mode,
        DateTimeOffset now,
        int depthRemaining,
        ImmutableHashSet<string> visited,
        CancellationToken ct)
    {
        var children = new List<PermissionTreeNode>();
        foreach (var child in operation.Children)
            children.Add(await ExpandChild(reader, resource, child, mode, now, depthRemaining, visited, ct).ConfigureAwait(false));

        return new PermissionTreeNode.SetOp(resource, operation.Type, children);
    }

    /// <summary>Expands a single set-operation operand.</summary>
    private async Task<PermissionTreeNode> ExpandChild(
        IGraphReader reader,
        ObjectAndRelation resource,
        SetOperationChild child,
        ExpandMode mode,
        DateTimeOffset now,
        int depthRemaining,
        ImmutableHashSet<string> visited,
        CancellationToken ct)
    {
        switch (child)
        {
            case SetOperationChild.This:
                return await ExpandDirect(reader, resource, mode, now, depthRemaining, visited, ct).ConfigureAwait(false);

            case SetOperationChild.Nil:
                return new PermissionTreeNode.Leaf(resource, []);

            case SetOperationChild.Self:
                // The resource itself treated as a subject at ellipsis (port of selfExpansion).
                return new PermissionTreeNode.Leaf(
                    resource,
                    [new DirectSubject(new ObjectAndRelation(resource.ObjectType, resource.ObjectId, CoreConstants.Ellipsis))]);

            case SetOperationChild.ComputedUsersetChild(var cu):
                return await Expand(reader, resource.WithRelation(cu.Relation), mode, now, depthRemaining - 1, visited, ct).ConfigureAwait(false);

            case SetOperationChild.TupleToUsersetChild(var ttu):
                return await ExpandTupleToUserset(
                    reader, resource, ttu.TuplesetRelation, ttu.ComputedUserset,
                    TupleToUsersetFunction.Any, mode, now, depthRemaining, visited, ct).ConfigureAwait(false);

            case SetOperationChild.FunctionedTupleToUsersetChild(var fttu):
                return await ExpandTupleToUserset(
                    reader, resource, fttu.TuplesetRelation, fttu.ComputedUserset,
                    fttu.Function, mode, now, depthRemaining, visited, ct).ConfigureAwait(false);

            case SetOperationChild.NestedRewrite(var nested):
                return await ExpandRewrite(reader, resource, nested.Operation, mode, now, depthRemaining, visited, ct).ConfigureAwait(false);

            default:
                return new PermissionTreeNode.Leaf(resource, []);
        }
    }

    /// <summary>
    /// Expands a tuple-to-userset arrow (port of <c>expandTupleToUserset</c>): walk the tupleset
    /// relation, and for each reached object build a child by computing the userset relation on it,
    /// decorating with the tupleset tuple's caveat. <c>.all()</c> produces an intersection over the
    /// per-target children; <c>.any()</c> a union.
    /// </summary>
    private async Task<PermissionTreeNode> ExpandTupleToUserset(
        IGraphReader reader,
        ObjectAndRelation resource,
        string tuplesetRelation,
        ComputedUserset computed,
        TupleToUsersetFunction function,
        ExpandMode mode,
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

        var children = new List<PermissionTreeNode>();
        await foreach (var rel in reader.QueryRelationships(filter, ct).ConfigureAwait(false))
        {
            if (IsExpired(rel, now))
                continue;

            var reached = rel.Subject;
            if (reached.IsPublicWildcard)
                continue;

            // ComputedUsersetObject.TupleObject => compute on the resource; TupleUsersetObject =>
            // compute on the traversed subject. Arrows traverse, so the common case is on the subject.
            var target = computed.Object == ComputedUsersetObject.TupleObject
                ? new ObjectAndRelation(resource.ObjectType, resource.ObjectId, computed.Relation)
                : new ObjectAndRelation(reached.ObjectType, reached.ObjectId, computed.Relation);

            var child = await Expand(reader, target, mode, now, depthRemaining - 1, visited, ct).ConfigureAwait(false);
            children.Add(DecorateWithCaveat(child, CaveatOf(rel)));
        }

        var op = function == TupleToUsersetFunction.All ? SetOperationType.Intersection : SetOperationType.Union;
        return new PermissionTreeNode.SetOp(resource, op, children);
    }

    private static PermissionTreeNode DecorateWithCaveat(PermissionTreeNode node, CaveatExpression? caveat)
    {
        if (caveat is null)
            return node;
        return node with { Caveat = CaveatExpression.CombineAnd(caveat, node.Caveat) };
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
}
