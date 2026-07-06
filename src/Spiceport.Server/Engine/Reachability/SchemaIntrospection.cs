using System.Collections.Immutable;
using Spiceport.Core;

namespace Spiceport.Engine;

/// <summary>
/// Schema-introspection walks for the <c>ComputablePermissions</c> and <c>DependentRelations</c> RPCs.
/// Pure functions of the compiled namespace definitions (plus the memoized
/// <see cref="ReachabilityGraph"/>); no datastore, no revision.
/// </summary>
/// <remarks>
/// Port of SpiceDB's <c>pkg/schema</c> reflection helpers
/// (<c>ComputablePermissions</c>/<c>RelationsEncounteredForSubject</c> and
/// <c>DependentRelations</c>/<c>RelationsEncounteredForResource</c>).
/// </remarks>
public static class SchemaIntrospection
{
    /// <summary>
    /// Forward closure: the relations/permissions reachable <em>from</em> the given relation when it acts
    /// as a subject. Port of <c>ComputablePermissions</c> / <c>RelationsEncounteredForSubject</c>.
    /// </summary>
    /// <param name="namespaces">The compiled namespaces, keyed by name.</param>
    /// <param name="reachability">The pre-built (Full-mode) reachability graph for this schema.</param>
    /// <param name="definitionName">The starting definition.</param>
    /// <param name="relationName">The starting relation (empty defaults to the ellipsis subject).</param>
    /// <param name="optionalDefinitionNameFilter">Optional prefix filter on result definition names.</param>
    /// <returns>The reachable targets, deduped and ordinally sorted, excluding the input itself.</returns>
    /// <exception cref="SchemaIntrospectionException">
    /// If the definition is unknown, or a non-empty relation is unknown.
    /// </exception>
    public static IReadOnlyList<RelationReference> ComputablePermissions(
        ImmutableDictionary<string, NamespaceDefinition> namespaces,
        ReachabilityGraph reachability,
        string definitionName,
        string relationName,
        string? optionalDefinitionNameFilter = null)
    {
        ArgumentNullException.ThrowIfNull(namespaces);
        ArgumentNullException.ThrowIfNull(reachability);

        if (!namespaces.TryGetValue(definitionName, out var def))
            throw new SchemaIntrospectionException(
                SchemaIntrospectionErrorKind.DefinitionNotFound,
                $"object definition `{definitionName}` not found");

        var relation = string.IsNullOrEmpty(relationName) ? CoreConstants.Ellipsis : relationName;
        if (relation != CoreConstants.Ellipsis && !def.Relations.Any(r => r.Name == relation))
            throw new SchemaIntrospectionException(
                SchemaIntrospectionErrorKind.RelationNotFound,
                $"relation/permission `{relation}` not found under definition `{definitionName}`");

        var graph = reachability;
        var input = new RelationReference(definitionName, relation);

        var results = new HashSet<RelationReference>();
        var worklist = new Queue<RelationReference>();
        var seenSubjects = new HashSet<RelationReference> { input };
        worklist.Enqueue(input);

        while (worklist.Count > 0)
        {
            var subject = worklist.Dequeue();
            foreach (var target in graph.Targets)
            {
                var entrypoints = graph.EntrypointsForSubjectToResource(subject, target, optimizedFirstOnly: false);
                if (entrypoints.Count == 0)
                    continue;

                // `target` is reachable from `subject`; add it and continue the closure outward.
                if (results.Add(target) && seenSubjects.Add(target))
                    worklist.Enqueue(target);
                else
                    seenSubjects.Add(target);
            }
        }

        results.Remove(input);

        return Project(results, namespaces, optionalDefinitionNameFilter);
    }

    /// <summary>
    /// Inverse closure: every relation/arrow a permission transitively depends on. Port of
    /// <c>DependentRelations</c> / <c>RelationsEncounteredForResource</c>.
    /// </summary>
    /// <param name="namespaces">The compiled namespaces, keyed by name.</param>
    /// <param name="definitionName">The definition owning the permission.</param>
    /// <param name="permissionName">The permission to walk.</param>
    /// <returns>The referenced relations/permissions, deduped and ordinally sorted, excluding the input.</returns>
    /// <exception cref="SchemaIntrospectionException">
    /// If the definition or permission is unknown, or the target is a base relation rather than a permission.
    /// </exception>
    public static IReadOnlyList<RelationReference> DependentRelations(
        ImmutableDictionary<string, NamespaceDefinition> namespaces,
        string definitionName,
        string permissionName)
    {
        ArgumentNullException.ThrowIfNull(namespaces);

        if (!namespaces.TryGetValue(definitionName, out var def))
            throw new SchemaIntrospectionException(
                SchemaIntrospectionErrorKind.DefinitionNotFound,
                $"object definition `{definitionName}` not found");

        var permission = def.Relations.FirstOrDefault(r => r.Name == permissionName);
        if (permission is null)
            throw new SchemaIntrospectionException(
                SchemaIntrospectionErrorKind.RelationNotFound,
                $"permission `{permissionName}` not found under definition `{definitionName}`");

        if (!permission.IsPermission)
            throw new SchemaIntrospectionException(
                SchemaIntrospectionErrorKind.NotAPermission,
                $"`{permissionName}` is a relation, not a permission, under definition `{definitionName}`");

        var input = new RelationReference(definitionName, permissionName);
        var results = new HashSet<RelationReference>();
        var visitedPermissions = new HashSet<RelationReference>();

        WalkPermission(namespaces, definitionName, permission, results, visitedPermissions);

        results.Remove(input);

        return Project(results, namespaces, optionalDefinitionNameFilter: null);
    }

    private static void WalkPermission(
        ImmutableDictionary<string, NamespaceDefinition> namespaces,
        string defName,
        Relation permission,
        HashSet<RelationReference> results,
        HashSet<RelationReference> visitedPermissions)
    {
        if (!visitedPermissions.Add(new RelationReference(defName, permission.Name)))
            return;
        if (permission.UsersetRewrite is not { } rewrite)
            return;

        WalkOperation(namespaces, defName, rewrite.Operation, results, visitedPermissions);
    }

    private static void WalkOperation(
        ImmutableDictionary<string, NamespaceDefinition> namespaces,
        string defName,
        SetOperation operation,
        HashSet<RelationReference> results,
        HashSet<RelationReference> visitedPermissions)
    {
        foreach (var child in operation.Children)
            WalkChild(namespaces, defName, child, results, visitedPermissions);
    }

    private static void WalkChild(
        ImmutableDictionary<string, NamespaceDefinition> namespaces,
        string defName,
        SetOperationChild child,
        HashSet<RelationReference> results,
        HashSet<RelationReference> visitedPermissions)
    {
        switch (child)
        {
            case SetOperationChild.ComputedUsersetChild(var cu):
                Reference(namespaces, defName, cu.Relation, results, visitedPermissions);
                break;

            case SetOperationChild.TupleToUsersetChild(var ttu):
                WalkArrow(namespaces, defName, ttu.TuplesetRelation, ttu.ComputedUserset.Relation,
                    results, visitedPermissions);
                break;

            case SetOperationChild.FunctionedTupleToUsersetChild(var fttu):
                WalkArrow(namespaces, defName, fttu.TuplesetRelation, fttu.ComputedUserset.Relation,
                    results, visitedPermissions);
                break;

            case SetOperationChild.NestedRewrite(var nested):
                WalkOperation(namespaces, defName, nested.Operation, results, visitedPermissions);
                break;

            case SetOperationChild.Self:
                results.Add(new RelationReference(defName, CoreConstants.Ellipsis));
                break;

            // This / Nil reference nothing.
            default:
                break;
        }
    }

    private static void WalkArrow(
        ImmutableDictionary<string, NamespaceDefinition> namespaces,
        string defName,
        string tuplesetRelation,
        string computedRelation,
        HashSet<RelationReference> results,
        HashSet<RelationReference> visitedPermissions)
    {
        // The tupleset relation on this definition is itself a dependency.
        Reference(namespaces, defName, tuplesetRelation, results, visitedPermissions);

        // Then, for every allowed subject type of the tupleset relation that actually has the computed
        // relation, the (allowedType, computedRelation) is a dependency (mirrors ReachabilityGraph.AddTtu).
        var tuplesetRel = LookupRelation(namespaces, defName, tuplesetRelation);
        if (tuplesetRel?.TypeInformation is not { } typeInfo)
            return;

        foreach (var allowed in typeInfo.AllowedDirectRelations)
        {
            if (allowed.IsPublicWildcard)
                continue;
            Reference(namespaces, allowed.ObjectType, computedRelation, results, visitedPermissions);
        }
    }

    private static void Reference(
        ImmutableDictionary<string, NamespaceDefinition> namespaces,
        string defName,
        string relationName,
        HashSet<RelationReference> results,
        HashSet<RelationReference> visitedPermissions)
    {
        var target = LookupRelation(namespaces, defName, relationName);
        if (target is null)
            return;

        results.Add(new RelationReference(defName, relationName));

        // If the referenced relation is itself a permission, expand it transitively.
        if (target.IsPermission)
            WalkPermission(namespaces, defName, target, results, visitedPermissions);
    }

    private static Relation? LookupRelation(
        ImmutableDictionary<string, NamespaceDefinition> namespaces, string objectType, string relationName)
    {
        if (!namespaces.TryGetValue(objectType, out var ns))
            return null;
        foreach (var r in ns.Relations)
        {
            if (r.Name == relationName)
                return r;
        }
        return null;
    }

    private static IReadOnlyList<RelationReference> Project(
        HashSet<RelationReference> results,
        ImmutableDictionary<string, NamespaceDefinition> namespaces,
        string? optionalDefinitionNameFilter)
    {
        IEnumerable<RelationReference> filtered = results;
        if (!string.IsNullOrEmpty(optionalDefinitionNameFilter))
            filtered = filtered.Where(r => r.Namespace.StartsWith(optionalDefinitionNameFilter, StringComparison.Ordinal));

        return filtered
            .OrderBy(r => r.Namespace, StringComparer.Ordinal)
            .ThenBy(r => r.Relation, StringComparer.Ordinal)
            .ToList();
    }
}

/// <summary>The category of a <see cref="SchemaIntrospectionException"/>, for mapping to gRPC status.</summary>
public enum SchemaIntrospectionErrorKind
{
    /// <summary>The requested object definition does not exist.</summary>
    DefinitionNotFound,

    /// <summary>The requested relation/permission does not exist under the definition.</summary>
    RelationNotFound,

    /// <summary>The requested target is a base relation where a permission was required.</summary>
    NotAPermission,
}

/// <summary>Raised by schema-introspection walks for unknown or mis-typed inputs.</summary>
public sealed class SchemaIntrospectionException(SchemaIntrospectionErrorKind kind, string message)
    : Exception(message)
{
    /// <summary>The error category, used to map to the appropriate gRPC status code.</summary>
    public SchemaIntrospectionErrorKind Kind { get; } = kind;
}
