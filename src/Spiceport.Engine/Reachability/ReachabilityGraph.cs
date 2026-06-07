using System.Collections.Concurrent;
using System.Collections.Immutable;
using Spiceport.Core;

namespace Spiceport.Engine;

/// <summary>
/// The structural precompute that lets <c>LookupResources</c> follow only productive edges from a
/// subject type to a resource relation.
/// </summary>
/// <remarks>
/// Port of SpiceDB's <c>pkg/schema/reachabilitygraphbuilder.go</c> + <c>reachabilitygraph.go</c>.
/// Unlike SpiceDB (which builds per-target graphs lazily) this is built eagerly and immutably once
/// per compiled schema, because our schemas are small and finite. It is a pure function of the
/// namespace definitions (no datastore, no revision), so instances are memoized process-wide keyed by
/// <see cref="SchemaHash"/>.
/// <para>
/// For each <c>(namespace, relation)</c> the graph holds two indices over the entrypoints that reach
/// that relation: one keyed by subject <em>type</em> (for wildcard / public links) and one keyed by
/// subject <em>(type, relation)</em> (for concrete links and subject-relation chains), exactly as
/// SpiceDB's <c>EntrypointsBySubjectType</c> / <c>EntrypointsBySubjectRelation</c>.
/// </para>
/// </remarks>
public sealed class ReachabilityGraph
{
    private static readonly ConcurrentDictionary<string, ReachabilityGraph> Cache = new();

    private readonly ImmutableDictionary<string, NamespaceDefinition> _namespaces;

    // (resourceNs, resourceRel) -> per-target reachability indices.
    private readonly Dictionary<RelationReference, TargetGraph> _byTarget;

    private ReachabilityGraph(ImmutableDictionary<string, NamespaceDefinition> namespaces)
    {
        _namespaces = namespaces;
        _byTarget = new Dictionary<RelationReference, TargetGraph>();
        Build();
    }

    /// <summary>
    /// Returns the reachability graph for the given schema, building it once and reusing it for any
    /// schema with the same <see cref="SchemaHash"/>.
    /// </summary>
    public static ReachabilityGraph ForSchema(ImmutableDictionary<string, NamespaceDefinition> namespaces)
    {
        ArgumentNullException.ThrowIfNull(namespaces);
        var key = SchemaHash.Compute(namespaces);
        return Cache.GetOrAdd(key, _ => new ReachabilityGraph(namespaces));
    }

    /// <summary>
    /// Returns the productive entrypoints by which a subject of <paramref name="subject"/> can reach
    /// <paramref name="resource"/>. Port of <c>AllEntrypointsForSubjectToResource</c> /
    /// <c>collectEntrypoints</c>: collects direct entrypoints for the subject ref and recurses through
    /// any non-ellipsis subject-relation chains (e.g. <c>group#member</c> reached via <c>team#member</c>).
    /// </summary>
    /// <param name="subject">The subject (type, relation) holding the permission.</param>
    /// <param name="resource">The resource (type, relation/permission) being reached.</param>
    /// <param name="optimizedFirstOnly">
    /// When true, stop after the first matching entrypoint (port of <c>FirstEntrypointsForSubjectToResource</c>).
    /// LookupResources uses false (full set) for correctness, filtering via Check.
    /// </param>
    public IReadOnlyList<ReachabilityEntrypoint> EntrypointsForSubjectToResource(
        RelationReference subject,
        RelationReference resource,
        bool optimizedFirstOnly = false)
    {
        ArgumentNullException.ThrowIfNull(subject);
        ArgumentNullException.ThrowIfNull(resource);

        var collected = new List<ReachabilityEntrypoint>();
        var seenKeys = new HashSet<string>();
        var visited = new HashSet<RelationReference>();
        Collect(resource, subject, collected, seenKeys, visited, optimizedFirstOnly);
        return collected;
    }

    private void Collect(
        RelationReference resource,
        RelationReference subject,
        List<ReachabilityEntrypoint> collected,
        HashSet<string> seenKeys,
        HashSet<RelationReference> visited,
        bool firstOnly)
    {
        if (!visited.Add(resource))
            return;

        if (!_byTarget.TryGetValue(resource, out var target))
            return;

        // Direct entrypoints for the subject type (wildcards) and (type, relation) (concrete).
        if (target.BySubjectType.TryGetValue(subject.Namespace, out var byType))
            AddAll(byType, collected, seenKeys);

        if (firstOnly && collected.Count > 0)
            return;

        if (target.BySubjectRelation.TryGetValue(subject, out var byRel))
            AddAll(byRel, collected, seenKeys);

        if (firstOnly && collected.Count > 0)
            return;

        // Recurse through non-ellipsis subject-relation chains, in a stable order.
        foreach (var subjectRel in target.BySubjectRelation.Keys
                     .Where(k => k.Relation != CoreConstants.Ellipsis)
                     .OrderBy(k => k.Namespace, StringComparer.Ordinal)
                     .ThenBy(k => k.Relation, StringComparer.Ordinal))
        {
            Collect(subjectRel, subject, collected, seenKeys, visited, firstOnly);
            if (firstOnly && collected.Count > 0)
                return;
        }
    }

    private static void AddAll(
        IReadOnlyList<ReachabilityEntrypoint> entrypoints,
        List<ReachabilityEntrypoint> collected,
        HashSet<string> seenKeys)
    {
        foreach (var ep in entrypoints)
        {
            var key = $"{(int)ep.Kind}|{ep.TargetRelation.Namespace}#{ep.TargetRelation.Relation}|{ep.ComputedUsersetRelation}|{ep.TuplesetRelation}|{(int)ep.ResultStatus}";
            if (seenKeys.Add(key))
                collected.Add(ep);
        }
    }

    // ---- Build ----------------------------------------------------------

    private void Build()
    {
        foreach (var ns in _namespaces.Values)
        {
            foreach (var relation in ns.Relations)
            {
                var target = new RelationReference(ns.Name, relation.Name);
                var graph = GetOrCreate(target);

                if (relation.UsersetRewrite is { } rewrite)
                {
                    WalkRewrite(graph, ns.Name, target, rewrite.Operation, EntrypointResultStatus.DirectResult);
                }
                else if (relation.TypeInformation is { } typeInfo)
                {
                    AddSubjectLinks(graph, target, typeInfo);
                }
            }
        }
    }

    /// <summary>Port of <c>addSubjectLinks</c>: each allowed subject becomes a Relation entrypoint.</summary>
    private void AddSubjectLinks(TargetGraph graph, RelationReference target, TypeInformation typeInfo)
    {
        foreach (var allowed in typeInfo.AllowedDirectRelations)
        {
            var ep = new ReachabilityEntrypoint(
                ReachabilityEntrypointKind.Relation,
                target,
                target,
                ResultStatus: EntrypointResultStatus.DirectResult);

            if (allowed.IsPublicWildcard)
                graph.AddBySubjectType(allowed.ObjectType, ep);
            else
                graph.AddBySubjectRelation(
                    new RelationReference(allowed.ObjectType, allowed.RelationName ?? CoreConstants.Ellipsis), ep);
        }
    }

    /// <summary>Port of <c>computeRewriteReachability</c>/<c>computeRewriteOpReachability</c>.</summary>
    private void WalkRewrite(
        TargetGraph graph,
        string thisNs,
        RelationReference target,
        SetOperation operation,
        EntrypointResultStatus status)
    {
        // Intersection/exclusion children are reachable only conditionally.
        var childStatus = operation.Type == SetOperationType.Union
            ? status
            : EntrypointResultStatus.ConditionalResult;

        foreach (var child in operation.Children)
            WalkChild(graph, thisNs, target, child, childStatus);
    }

    private void WalkChild(
        TargetGraph graph,
        string thisNs,
        RelationReference target,
        SetOperationChild child,
        EntrypointResultStatus status)
    {
        switch (child)
        {
            case SetOperationChild.ComputedUsersetChild(var cu):
            {
                var ep = new ReachabilityEntrypoint(
                    ReachabilityEntrypointKind.ComputedUserset,
                    target, target,
                    ComputedUsersetRelation: cu.Relation,
                    ResultStatus: status);
                graph.AddBySubjectRelation(new RelationReference(thisNs, cu.Relation), ep);
                break;
            }

            case SetOperationChild.TupleToUsersetChild(var ttu):
                AddTtu(graph, thisNs, target, ttu.TuplesetRelation, ttu.ComputedUserset.Relation, status);
                break;

            case SetOperationChild.FunctionedTupleToUsersetChild(var fttu):
            {
                var ttuStatus = fttu.Function == TupleToUsersetFunction.All
                    ? EntrypointResultStatus.ConditionalResult
                    : status;
                AddTtu(graph, thisNs, target, fttu.TuplesetRelation, fttu.ComputedUserset.Relation, ttuStatus);
                break;
            }

            case SetOperationChild.NestedRewrite(var nested):
                WalkRewrite(graph, thisNs, target, nested.Operation, status);
                break;

            case SetOperationChild.Self:
            {
                var ep = new ReachabilityEntrypoint(
                    ReachabilityEntrypointKind.Self, target, target, ResultStatus: status);
                graph.AddBySubjectRelation(new RelationReference(thisNs, CoreConstants.Ellipsis), ep);
                break;
            }

            // Nil / This produce no entrypoints (This is not valid in a compiled rewrite).
            default:
                break;
        }
    }

    /// <summary>
    /// Port of <c>computeTTUReachability</c>: for each allowed type on the tupleset relation that
    /// actually has the computed relation, emit a TupleToUserset entrypoint keyed by
    /// <c>(allowedType, computedRelation)</c>.
    /// </summary>
    private void AddTtu(
        TargetGraph graph,
        string thisNs,
        RelationReference target,
        string tuplesetRelation,
        string computedRelation,
        EntrypointResultStatus status)
    {
        var tuplesetRel = LookupRelation(thisNs, tuplesetRelation);
        if (tuplesetRel?.TypeInformation is not { } typeInfo)
            return;

        foreach (var allowed in typeInfo.AllowedDirectRelations)
        {
            if (allowed.IsPublicWildcard)
                continue; // arrows over a wildcard tupleset are not productive here.

            // Guard: the allowed type must actually have the computed relation (HasRelation).
            if (!HasRelation(allowed.ObjectType, computedRelation))
                continue;

            var ep = new ReachabilityEntrypoint(
                ReachabilityEntrypointKind.TupleToUserset,
                target, target,
                ComputedUsersetRelation: computedRelation,
                TuplesetRelation: tuplesetRelation,
                ResultStatus: status);

            graph.AddBySubjectRelation(
                new RelationReference(allowed.ObjectType, computedRelation), ep);
        }
    }

    private TargetGraph GetOrCreate(RelationReference target)
    {
        if (!_byTarget.TryGetValue(target, out var g))
        {
            g = new TargetGraph();
            _byTarget[target] = g;
        }
        return g;
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

    private bool HasRelation(string objectType, string relationName) =>
        LookupRelation(objectType, relationName) is not null;

    /// <summary>Per-target entrypoint indices. Port of <c>core.ReachabilityGraph</c>.</summary>
    private sealed class TargetGraph
    {
        public Dictionary<string, List<ReachabilityEntrypoint>> BySubjectType { get; } = new();
        public Dictionary<RelationReference, List<ReachabilityEntrypoint>> BySubjectRelation { get; } = new();

        public void AddBySubjectType(string subjectType, ReachabilityEntrypoint ep)
        {
            if (!BySubjectType.TryGetValue(subjectType, out var list))
            {
                list = new List<ReachabilityEntrypoint>();
                BySubjectType[subjectType] = list;
            }
            list.Add(ep);
        }

        public void AddBySubjectRelation(RelationReference subject, ReachabilityEntrypoint ep)
        {
            if (!BySubjectRelation.TryGetValue(subject, out var list))
            {
                list = new List<ReachabilityEntrypoint>();
                BySubjectRelation[subject] = list;
            }
            list.Add(ep);
        }
    }
}
