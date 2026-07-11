using System.Collections.Immutable;
using Spiceport.Core;
using Spiceport.Datastore;

namespace Spiceport.Engine;

/// <summary>
/// The Leopard membership-walk primitive: one reverse-adjacency hop over a pinned MVCC snapshot, plus an
/// in-process BFS driver over it. This is the shared "walk definition" that both
/// <see cref="Spiceport.Grains.MembershipWalkGrain"/> (the addressable, sibling-recursing, cross-grain
/// driver) and <see cref="LocalClosure"/> (the in-process driver used by engine-level tests and the
/// equivalence gate) consume — the same one-walk-definition/two-drivers shape as
/// <c>LocalDispatcher</c>/<c>OrleansDispatcher</c> for Check.
/// </summary>
/// <remarks>
/// Because a walk runs over a reader pinned to one MVCC revision, it is revision-exact by construction —
/// unlike the retired <see cref="MembershipIndex"/>, there is no fold/catch-up machinery here: a walk at
/// revision R only ever sees rows live at R.
/// <para>
/// CANDIDATE SEMANTICS mirror the retired index's <c>Build</c> scan exactly: <see cref="DirectParents"/>
/// ignores caveats entirely (a candidate is confirmed by <c>CheckEngine</c>, which resolves the caveat), and
/// expired rows are excluded automatically because <see cref="IDatastoreReader.ReverseQueryRelationships"/>
/// (like <c>QueryRelationships</c>) already filters expiration at read time — there is nothing extra to do
/// here for either.
/// </para>
/// </remarks>
public static class MembershipWalk
{
    /// <summary>A subject/resource identity as walked: <c>type:id#relation</c>.</summary>
    public readonly record struct SubjectKey(string Type, string Id, string Relation)
    {
        /// <summary>The canonical <c>type:id#relation</c> string form, used as the visited-set key.</summary>
        public override string ToString() => $"{Type}:{Id}#{Relation}";
    }

    /// <summary>A resource node reached by a hop: the containing (type, id, relation) a subject was found on.</summary>
    public readonly record struct ResourceNode(string Type, string Id, string Relation);

    /// <summary>
    /// One reverse-adjacency hop: every resource node that directly names <paramref name="subject"/> as a
    /// subject, restricted to the base relations <paramref name="coverage"/>'s scan set actually tracks (a
    /// row on a relation outside the scan set cannot contribute to any covered target, so it is discarded).
    /// </summary>
    public static async Task<IReadOnlyList<ResourceNode>> DirectParents(
        IDatastoreReader reader,
        MembershipCoverage coverage,
        SubjectKey subject,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(coverage);

        var relationFilter = subject.Relation == CoreConstants.Ellipsis
            ? new SubjectRelationFilter(IncludeEllipsisRelation: true)
            : new SubjectRelationFilter(NonEllipsisRelation: subject.Relation);

        var filter = new SubjectsFilter(
            SubjectType: subject.Type,
            OptionalSubjectIds: [subject.Id],
            RelationFilter: relationFilter);

        var results = new List<ResourceNode>();
        await foreach (var rel in reader.ReverseQueryRelationships(filter, options: null, cancellationToken)
            .ConfigureAwait(false))
        {
            // Belt-and-braces exact match on the subject relation: SubjectRelationFilter's ellipsis branch
            // (see its remarks) does not itself exclude non-ellipsis rows, so re-check exactly here rather
            // than lean on the datastore filter alone.
            if (rel.Subject.Relation != subject.Relation)
                continue;
            if (!coverage.ScanSet.Contains((rel.Resource.ObjectType, rel.Resource.Relation)))
                continue;
            results.Add(new ResourceNode(rel.Resource.ObjectType, rel.Resource.ObjectId, rel.Resource.Relation));
        }

        return results;
    }

    /// <summary>
    /// In-process BFS over <see cref="DirectParents"/> from <paramref name="subject"/>, with a visited-set
    /// cycle guard, seeding a second walk from the subject's wildcard identity
    /// (<c>type:*#relation</c>) so a <c>type:*#rel</c> userset edge (which makes every such subject a member)
    /// is followed too — mirroring the retired index's wildcard-seed rule. Terminates on a data cycle (e.g.
    /// group A containing group B containing group A) because the visited set is keyed by the canonical
    /// subject-key string.
    /// </summary>
    public static async Task<IReadOnlyList<ResourceNode>> LocalClosure(
        IDatastoreReader reader,
        MembershipCoverage coverage,
        SubjectKey subject,
        CancellationToken cancellationToken = default)
    {
        var visited = new HashSet<string>();
        var queue = new Queue<SubjectKey>();
        var found = new List<ResourceNode>();

        Enqueue(subject);
        Enqueue(subject with { Id = CoreConstants.PublicWildcard });

        while (queue.Count > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var current = queue.Dequeue();
            var parents = await DirectParents(reader, coverage, current, cancellationToken).ConfigureAwait(false);
            foreach (var node in parents)
            {
                found.Add(node);
                Enqueue(new SubjectKey(node.Type, node.Id, node.Relation));
            }
        }

        return found;

        void Enqueue(SubjectKey key)
        {
            if (visited.Add(key.ToString()))
                queue.Enqueue(key);
        }
    }

    /// <summary>
    /// Filters a walked node set down to the COMPLETE candidate resource-id set for one covered
    /// <c>(resourceType, permission)</c> target: nodes whose (type, relation) is one of
    /// <paramref name="yieldRelations"/>, plus the reflexive self-membership candidate when the subject and
    /// resource types match — the identical rules the retired <c>MembershipIndex.TryCoveredResources</c>
    /// applied, extracted here so the grain-mesh caller
    /// (<c>Spiceport.Grains.ReverseOpsSupport.AcquireCoveredCandidates</c>) and the in-process
    /// <see cref="LocalClosure"/> driver used by tests cannot drift on this rule. Returns sorted, distinct ids.
    /// </summary>
    public static IReadOnlyList<string> ToCoveredCandidates(
        IEnumerable<ResourceNode> nodes,
        ImmutableHashSet<string> yieldRelations,
        string resourceType,
        string subjectType,
        string subjectId)
    {
        ArgumentNullException.ThrowIfNull(nodes);
        ArgumentNullException.ThrowIfNull(yieldRelations);

        var found = new SortedSet<string>(StringComparer.Ordinal);
        foreach (var node in nodes)
        {
            if (node.Type == resourceType && yieldRelations.Contains(node.Relation))
                found.Add(node.Id);
        }

        // Reflexive userset self-membership (see MembershipCoverage's remarks): a subject `T:id#rel` can
        // reflexively hold a permission on `T:id`. Over-inclusion is safe — Check resolves it.
        if (subjectType == resourceType)
            found.Add(subjectId);

        return found.Count == 0 ? [] : found.ToList();
    }
}
