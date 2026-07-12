using System.Collections.Immutable;
using Spiceport.Core;

namespace Spiceport.Engine;

/// <summary>
/// Pure schema analysis for the Leopard membership accelerator: which <c>(resourceType, nameOrPermission)</c>
/// targets flatten, through union / intersection / exclusion of computed usersets over STORED base
/// relations, to a set of "yield" base relations whose edges directly make a resource a candidate — and the
/// full "scan set" of base relations that must be walked to find those edges (yields plus their traversal
/// closure). This is the byte-identical coverage analysis the retired <c>MembershipIndex.Build</c> used to
/// perform inline; it carries no datastore reader and no revision — a pure function of the compiled schema —
/// so it is built once per <see cref="Spiceport.Grains.SchemaSnapshot"/> (see
/// <see cref="Spiceport.Grains.SchemaSnapshot.MembershipCoverage"/>) rather than once per request.
/// </summary>
/// <remarks>
/// SAFETY MODEL — unchanged from the retired index: coverage is a CANDIDATE-set predicate — candidates, never verdicts.
/// A covered target's yield relations describe a COMPLETE superset of the resources a subject can reach; the
/// caller (<see cref="MembershipWalk"/> plus the confirming <see cref="CheckEngine"/>) is responsible for
/// exactness. For intersection/exclusion only the first (positive) operand seeds candidates (the rest can
/// only remove members, which Check re-applies). Any tuple-to-userset ARROW, a <c>Self</c>/<c>This</c>
/// legacy operand, or a computed userset on a subject the walk has already traversed reaches resources this
/// stored-edge flatten cannot enumerate — those abort coverage and the caller falls back to the live
/// traversal.
/// </remarks>
public sealed class MembershipCoverage
{
    private readonly Dictionary<(string Type, string Name), ImmutableHashSet<string>> _coveredTargets;
    private readonly ImmutableHashSet<(string Type, string Relation)> _scanSet;

    private MembershipCoverage(
        Dictionary<(string, string), ImmutableHashSet<string>> coveredTargets,
        ImmutableHashSet<(string Type, string Relation)> scanSet)
    {
        _coveredTargets = coveredTargets;
        _scanSet = scanSet;
    }

    /// <summary>True if no target in this schema is coverable (the accelerator is a no-op for it).</summary>
    public bool IsEmpty => _coveredTargets.Count == 0;

    /// <summary>
    /// The base relations (<c>(type, relation)</c>) a walk must consult — the union of every covered
    /// target's yield relations and their userset traversal closure. A <see cref="MembershipWalk"/> hop
    /// discards any reverse-query row whose (resource type, resource relation) is outside this set, because
    /// it cannot contribute to any covered target.
    /// </summary>
    public ImmutableHashSet<(string Type, string Relation)> ScanSet => _scanSet;

    /// <summary>
    /// If <paramref name="resourceType"/> / <paramref name="nameOrPermission"/> is a covered shape, sets
    /// <paramref name="yieldRelations"/> to the base relations on that resource type whose stored edges
    /// directly make a resource a candidate and returns true. Returns false for any shape this flatten does
    /// not cover (the caller must fall back to the live traversal).
    /// </summary>
    public bool TryGetYields(string resourceType, string nameOrPermission, out ImmutableHashSet<string> yieldRelations) =>
        _coveredTargets.TryGetValue((resourceType, nameOrPermission), out yieldRelations!);

    /// <summary>
    /// Builds the coverage analysis for every relation of every namespace in the schema. One pass over the
    /// compiled model; no datastore access.
    /// </summary>
    public static MembershipCoverage Build(IEnumerable<NamespaceDefinition> namespaces)
    {
        ArgumentNullException.ThrowIfNull(namespaces);
        var byType = namespaces.ToImmutableDictionary(ns => ns.Name);

        var covered = new Dictionary<(string, string), ImmutableHashSet<string>>();
        var scanSet = new HashSet<(string Type, string Relation)>();

        foreach (var ns in byType.Values)
        {
            foreach (var relation in ns.Relations)
            {
                var yields = new HashSet<string>();
                var closure = new HashSet<(string, string)>();
                // A target is coverable only when it flattens to stored base-relation edges (no arrows / self).
                if (!TryResolveYields(byType, ns.Name, relation.Name, yields, closure, new HashSet<(string, string)>()))
                    continue;
                if (yields.Count == 0)
                    continue;
                covered[(ns.Name, relation.Name)] = yields.ToImmutableHashSet();
                foreach (var c in closure)
                    scanSet.Add(c);
            }
        }

        return new MembershipCoverage(covered, scanSet.ToImmutableHashSet());
    }

    // Resolves the "yield" base relations on `type` that a relation/permission contributes, accumulating the
    // full set of base relations to scan (yields plus their userset traversal closure). Returns false (aborts
    // coverage) for any arrow / self / legacy operand or a missing relation. `visiting` guards permission cycles.
    private static bool TryResolveYields(
        ImmutableDictionary<string, NamespaceDefinition> byType,
        string type,
        string name,
        HashSet<string> yields,
        HashSet<(string, string)> closure,
        HashSet<(string, string)> visiting)
    {
        if (!byType.TryGetValue(type, out var ns))
            return false;
        var relation = ns.Relations.FirstOrDefault(r => r.Name == name);
        if (relation is null)
            return false;

        if (!relation.IsPermission)
        {
            // A stored base relation: its own edges yield resources of `type`, and its userset closure must be
            // all-base so the ancestor walk over those edges is complete.
            if (relation.TypeInformation is null)
                return false;
            if (!TryAddClosure(byType, type, name, closure))
                return false;
            yields.Add(name);
            return true;
        }

        if (!visiting.Add((type, name)))
            return true; // already being resolved on this path — a permission cycle contributes nothing new.
        try
        {
            return TryResolveOperation(byType, type, relation.UsersetRewrite!.Operation, yields, closure, visiting);
        }
        finally
        {
            visiting.Remove((type, name));
        }
    }

    private static bool TryResolveOperation(
        ImmutableDictionary<string, NamespaceDefinition> byType,
        string type,
        SetOperation operation,
        HashSet<string> yields,
        HashSet<(string, string)> closure,
        HashSet<(string, string)> visiting)
    {
        switch (operation.Type)
        {
            case SetOperationType.Union:
                // Members are the union of the children: every child must be coverable, all contribute candidates.
                foreach (var child in operation.Children)
                    if (!TryResolveChild(byType, type, child, yields, closure, visiting))
                        return false;
                return true;

            case SetOperationType.Intersection:
            case SetOperationType.Exclusion:
                // Members are a subset of the FIRST (positive) operand; it alone seeds a complete candidate
                // superset, and Check re-applies the intersection/exclusion. Later operands are ignored.
                return TryResolveChild(byType, type, operation.Children[0], yields, closure, visiting);

            default:
                return false;
        }
    }

    private static bool TryResolveChild(
        ImmutableDictionary<string, NamespaceDefinition> byType,
        string type,
        SetOperationChild child,
        HashSet<string> yields,
        HashSet<(string, string)> closure,
        HashSet<(string, string)> visiting) =>
        child switch
        {
            SetOperationChild.Nil => true, // contributes no members
            SetOperationChild.ComputedUsersetChild { Value: { Object: ComputedUsersetObject.TupleObject } cu } =>
                TryResolveYields(byType, type, cu.Relation, yields, closure, visiting),
            SetOperationChild.NestedRewrite { Value: { Operation: var op } } =>
                TryResolveOperation(byType, type, op, yields, closure, visiting),
            // Self, This, tuple-to-userset arrows, and computed usersets on a traversed subject reach resources
            // this stored-edge flatten cannot enumerate — abort coverage so the caller runs the live traversal.
            _ => false,
        };

    // Adds `(type, relation)` and every base relation reachable through its non-ellipsis userset subject types,
    // transitively, to `closure`. Returns false if the closure references a missing relation or a permission
    // (a rewrite cannot be flattened from stored edges).
    private static bool TryAddClosure(
        ImmutableDictionary<string, NamespaceDefinition> byType,
        string type,
        string relation,
        HashSet<(string, string)> closure)
    {
        var queue = new Queue<(string Type, string Relation)>();
        queue.Enqueue((type, relation));

        while (queue.Count > 0)
        {
            var current = queue.Dequeue();
            if (!closure.Add(current))
                continue;

            if (!byType.TryGetValue(current.Type, out var ns))
                return false;
            var rel = ns.Relations.FirstOrDefault(r => r.Name == current.Relation);
            if (rel is null || rel.IsPermission || rel.TypeInformation is null)
                return false;

            foreach (var allowed in rel.TypeInformation.AllowedDirectRelations)
            {
                if (allowed.Kind != AllowedRelationKind.Relation)
                    continue;
                var sub = allowed.RelationName ?? CoreConstants.Ellipsis;
                if (sub == CoreConstants.Ellipsis)
                    continue; // a terminal (leaf) subject type — no further userset edge.
                queue.Enqueue((allowed.ObjectType, sub));
            }
        }

        return true;
    }
}
