using System.Collections.Immutable;
using Spiceport.Core;
using Spiceport.Datastore;

namespace Spiceport.Engine;

/// <summary>
/// A flattened nested-group / userset membership index (a "Leopard"-style accelerator). For schema relations
/// and permissions that resolve purely to stored userset edges (the canonical nested-group pattern, e.g.
/// <c>relation member: user | group#member</c> and <c>relation viewer: user | group#member</c>), it
/// precomputes the reverse adjacency of those edges so the transitive set of resources a subject can reach is
/// walked in-memory instead of by repeated reverse datastore queries.
/// </summary>
/// <remarks>
/// SAFETY MODEL — the index is a CANDIDATE accelerator, never an oracle. <see cref="TryCoveredResources"/>
/// returns a COMPLETE candidate set (a superset of the true answer) for the shapes it covers; the caller
/// confirms every candidate with the trusted <see cref="CheckEngine"/>, so caveats, exclusions and
/// intersections are resolved exactly and an over-broad candidate can only cost an extra Check, never a wrong
/// verdict. Completeness is the index's only obligation, so candidate generation deliberately IGNORES caveats
/// and expirations (those only ever REMOVE members, which Check re-applies) and walks wildcard userset edges.
/// <para>
/// COVERAGE is conservative. A target <c>(type, nameOrPermission)</c> is covered when it resolves — through
/// union / intersection / exclusion of computed usersets over stored base relations — to a set of
/// "yield relations" on the resource type whose userset closure contains only stored base relations. For
/// intersection/exclusion only the first (positive) operand seeds candidates (the rest can only remove members,
/// which Check re-applies). Any tuple-to-userset ARROW (<c>parent-&gt;view</c>) or a self / legacy operand
/// aborts coverage — those reach resources this edge-flatten does not model — and the caller falls back to the
/// full live traversal. For a covered target, every path the live engine takes from the subject up to a
/// resource runs through stored edges captured here, so the reverse walk is complete.
/// </para>
/// The index is an immutable snapshot built at one revision for one schema hash; it is rebuilt (not mutated)
/// when either advances, and a caller must only use it when its <see cref="SchemaHash"/> matches the resolved
/// request hash and it was built at a revision at least as fresh as the request.
/// </remarks>
public sealed class MembershipIndex
{
    private readonly string _schemaHash;

    /// <summary>
    /// Covered targets: <c>(resourceType, nameOrPermission)</c> -> the "yield" base relations on that resource
    /// type whose stored edges directly make a resource a candidate. Other relations in the scan set are
    /// traversal-only (walked to find ancestor groups, but their resources are not candidates of this target).
    /// </summary>
    private readonly Dictionary<(string Type, string Name), ImmutableHashSet<string>> _coveredTargets;

    /// <summary>
    /// Reverse adjacency over the stored userset edges: a subject key (<c>type:id#relation</c>) maps to the
    /// resource nodes that name it as a subject. Walking it transitively yields every containing group/resource.
    /// </summary>
    private readonly Dictionary<string, List<ResourceNode>> _parents;

    private MembershipIndex(
        string schemaHash,
        Dictionary<(string, string), ImmutableHashSet<string>> coveredTargets,
        Dictionary<string, List<ResourceNode>> parents)
    {
        _schemaHash = schemaHash;
        _coveredTargets = coveredTargets;
        _parents = parents;
    }

    private readonly record struct ResourceNode(string Type, string Id, string Relation);

    /// <summary>The schema hash this index was built for (the keyspace it is valid in).</summary>
    public string SchemaHash => _schemaHash;

    /// <summary>True if the index covers no shape (it can then be skipped entirely).</summary>
    public bool IsEmpty => _coveredTargets.Count == 0;

    private static string Key(string type, string id, string relation) => $"{type}:{id}#{relation}";

    /// <summary>
    /// Builds the index by scanning, at the reader's revision, the stored relationships of every base relation
    /// that participates in a coverable target's flatten. One full scan per such relation; the result is an
    /// immutable snapshot.
    /// </summary>
    public static async Task<MembershipIndex> Build(
        IEnumerable<NamespaceDefinition> namespaces,
        IDatastoreReader reader,
        string schemaHash,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(namespaces);
        ArgumentNullException.ThrowIfNull(reader);

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

        var parents = new Dictionary<string, List<ResourceNode>>();
        foreach (var (type, rel) in scanSet)
        {
            var filter = new RelationshipsFilter { OptionalResourceType = type, OptionalResourceRelation = rel };
            await foreach (var r in reader.QueryRelationships(filter, cancellationToken).ConfigureAwait(false))
            {
                var subjectKey = Key(r.Subject.ObjectType, r.Subject.ObjectId, r.Subject.Relation);
                if (!parents.TryGetValue(subjectKey, out var list))
                    parents[subjectKey] = list = [];
                list.Add(new ResourceNode(r.Resource.ObjectType, r.Resource.ObjectId, r.Resource.Relation));
            }
        }

        return new MembershipIndex(schemaHash, covered, parents);
    }

    /// <summary>
    /// If <paramref name="resourceType"/> / <paramref name="permission"/> is a covered shape, sets
    /// <paramref name="resourceIds"/> to the COMPLETE candidate set of resource ids the subject may reach
    /// (sorted, distinct) and returns true; the caller must confirm each with Check. Returns false (and an
    /// empty list) for any shape the index does not cover, signalling the caller to run the live traversal.
    /// </summary>
    public bool TryCoveredResources(
        string subjectType,
        string subjectId,
        string subjectRelation,
        string resourceType,
        string permission,
        out IReadOnlyList<string> resourceIds)
    {
        resourceIds = [];

        // A wildcard subject is not a concrete membership query; leave it to the live engine.
        if (subjectId == CoreConstants.PublicWildcard)
            return false;
        if (!_coveredTargets.TryGetValue((resourceType, permission), out var yieldRelations))
            return false;

        var found = new SortedSet<string>(StringComparer.Ordinal);
        var visited = new HashSet<string>();
        var queue = new Queue<string>();

        // Seed from the concrete subject AND its same-type/relation wildcard, so a `type:*#rel` userset edge
        // (which makes every such subject a member) is followed too.
        Enqueue(Key(subjectType, subjectId, subjectRelation));
        Enqueue(Key(subjectType, CoreConstants.PublicWildcard, subjectRelation));

        while (queue.Count > 0)
        {
            var key = queue.Dequeue();
            if (!_parents.TryGetValue(key, out var parentNodes))
                continue;
            foreach (var node in parentNodes)
            {
                if (node.Type == resourceType && yieldRelations.Contains(node.Relation))
                    found.Add(node.Id);
                // The parent node, acting as a subject, may itself be contained by further resources — walk on.
                Enqueue(Key(node.Type, node.Id, node.Relation));
            }
        }

        // Reflexive userset self-membership: a subject `T:id#rel` can satisfy a permission on `T:id` through
        // the permission graph (e.g. a subject `document:d#viewer` reflexively holds `view = viewer` on
        // document:d, and a userset is a member of itself). Add the subject's own id as a candidate whenever it
        // shares the resource type; Check resolves whether it actually holds. Over-inclusion is safe.
        if (subjectType == resourceType)
            found.Add(subjectId);

        resourceIds = found.Count == 0 ? [] : found.ToList();
        return true;

        void Enqueue(string k)
        {
            if (visited.Add(k))
                queue.Enqueue(k);
        }
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
