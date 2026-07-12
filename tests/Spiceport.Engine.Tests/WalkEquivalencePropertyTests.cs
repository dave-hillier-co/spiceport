using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Schema;

namespace Spiceport.Engine.Tests;

/// <summary>
/// Gate 1 -- completeness of the Leopard membership-walk accelerator on random graphs (see
/// <see cref="RandomAuthzWorlds"/> for the generator). For every seeded world, every covered
/// (resourceType, permission/relation) target, and every subject in the alphabet: the accelerator's
/// candidate set (<see cref="MembershipWalk.LocalClosure"/> + <see cref="MembershipWalk.ToCoveredCandidates"/>,
/// the exact production acquisition rules <c>Spiceport.Grains.ReverseOpsSupport.AcquireCoveredCandidates</c>
/// uses -- including the wildcard-root seed and the reflexive self-membership rule) fed into
/// <see cref="LookupResourcesEngine"/> MUST produce the IDENTICAL result set as the live traversal
/// (<c>coveredCandidateIds: null</c>). A walk that silently drops a candidate is a false negative that
/// Check confirmation alone cannot catch -- since <see cref="LookupResourcesEngine"/> only ever confirms
/// candidates the walk handed it, a missing candidate never reaches Check at all. This gate is what
/// catches that failure class. A separate, dedicated cyclic-graph test below also asserts the walk
/// terminates within a bounded time on a genuine group-membership cycle -- see its remarks for why that
/// is hand-built rather than drawn from <see cref="RandomAuthzWorlds"/> (whose group/folder nesting is
/// acyclic by construction, precisely so the other gates have a well-defined Check verdict to compare
/// against).
/// </summary>
/// <remarks>
/// STATED LIMITS (see <see cref="RandomAuthzWorlds"/>): no caveats/expiration in the generated worlds;
/// small fixed alphabet (5 users / 4 groups / 3 folders / 6 documents) so the query universe below is
/// exhaustive, not sampled.
/// </remarks>
public class WalkEquivalencePropertyTests
{
    public static IEnumerable<object[]> Seeds => RandomAuthzWorlds.Seeds;

    private static readonly (string ResourceType, string Permission)[] SubjectTargets =
    [
        ("document", "view"),
        ("document", "view_mono"),
        ("document", "viewer"),
        ("folder", "view"),
        ("folder", "viewer"),
        ("group", "member"),
    ];

    private static readonly (string ResourceType, string Permission)[] GroupSubjectTargets =
    [
        ("group", "member"),
        ("document", "view"),
        ("folder", "view"),
    ];

    [Theory]
    [MemberData(nameof(Seeds))]
    public async Task WalkCandidates_EqualLiveTraversal_AcrossWorld(int seed)
    {
        var world = RandomAuthzWorlds.Build(seed);
        var compiled = SchemaCompiler.CompileSchema(world.SchemaText);
        var engine = new LookupResourcesEngine(compiled.Namespaces, compiled.Caveats);
        var coverage = MembershipCoverage.Build(compiled.Namespaces);

        var store = new ReferenceDatastore();
        var rev = await store.ReadWriteTx(tx => tx.WriteRelationships(
            world.Relationships.Select(r => new RelationshipUpdate(r, UpdateOperation.Touch)).ToList()));
        var reader = store.SnapshotReader(rev);

        foreach (var (resourceType, permission) in SubjectTargets)
        {
            foreach (var userId in world.Users)
            {
                await AssertWalkEqualsLive(
                    engine, coverage, reader, "user", userId, CoreConstants.Ellipsis, resourceType, permission, seed);
            }
        }

        // A group can itself be walked as a subject (it may itself hold group#member on an ancestor
        // group), exercising the nested-group closure directly rather than only through a leaf user.
        foreach (var groupId in world.Groups)
        {
            foreach (var (resourceType, permission) in GroupSubjectTargets)
            {
                await AssertWalkEqualsLive(
                    engine, coverage, reader, "group", groupId, "member", resourceType, permission, seed);
            }
        }
    }

    private static async Task AssertWalkEqualsLive(
        LookupResourcesEngine engine, MembershipCoverage coverage, IDatastoreReader reader,
        string subjectType, string subjectId, string subjectRelation, string resourceType, string permission,
        int seed)
    {
        var live = await Collect(engine.LookupResources(
            reader, subjectType, subjectId, subjectRelation, resourceType, permission, coveredCandidateIds: null));

        IReadOnlyList<string>? candidates = null;
        if (coverage.TryGetYields(resourceType, permission, out var yields))
        {
            var walkTask = MembershipWalk.LocalClosure(
                reader, coverage, new MembershipWalk.SubjectKey(subjectType, subjectId, subjectRelation));
            var completed = await Task.WhenAny(walkTask, Task.Delay(TimeSpan.FromSeconds(5)));
            Assert.True(completed == walkTask,
                $"seed={seed}: membership walk did not terminate within 5s for subject " +
                $"{subjectType}:{subjectId}#{subjectRelation} -> {resourceType}/{permission}");
            var nodes = await walkTask;
            candidates = MembershipWalk.ToCoveredCandidates(nodes, yields, resourceType, subjectType, subjectId);
        }

        var walked = await Collect(engine.LookupResources(
            reader, subjectType, subjectId, subjectRelation, resourceType, permission, candidates));

        // Compare as a SET, not a list: the live (uncandidated) traversal deliberately emits one result
        // per REACHABILITY ENTRYPOINT with no global dedup (see LookupResourcesEngine's remarks on why --
        // it is what makes a paged enumeration equal the unpaged one), so a resource reachable via several
        // group memberships can legitimately appear more than once on the live side, while the
        // candidate-driven path confirms each unique candidate id exactly once. Neither multiplicity is
        // wrong; the completeness property this gate checks is over the (resourceId, membership) SET,
        // matching the gate's contract ("identical (resourceId, membership-collapsed) result set").
        var liveSet = live.ToHashSet();
        var walkedSet = walked.ToHashSet();
        Assert.True(liveSet.SetEquals(walkedSet),
            $"seed={seed}: walk-fed and live LookupResources diverged for subject " +
            $"{subjectType}:{subjectId}#{subjectRelation} -> {resourceType}/{permission}. " +
            $"live=[{string.Join(",", liveSet.Select(x => x.Id + ":" + x.M))}] " +
            $"walked=[{string.Join(",", walkedSet.Select(x => x.Id + ":" + x.M))}] " +
            $"candidates=[{(candidates is null ? "n/a" : string.Join(",", candidates))}]");
    }

    /// <summary>
    /// Dedicated cyclic-graph termination check: <see cref="RandomAuthzWorlds"/> keeps group/folder
    /// nesting acyclic by construction (see its remarks) so <see cref="CheckEngine"/> -- which has no
    /// cycle-cut on the verdict path -- never throws on the worlds the other gates exercise. That leaves
    /// the walk's OWN cycle handling untested by this file's main gate, so this test builds a minimal,
    /// genuine 2-group membership cycle directly (two of the seed's own group ids, so it still varies per
    /// seed) and walks INTO it from one of the cyclic nodes. It queries the base `member` relation only
    /// (no intersection/exclusion/arrow), so no Check confirmation is involved anywhere in this test --
    /// only <see cref="MembershipWalk.LocalClosure"/>'s own visited-set cycle-cut is exercised.
    /// </summary>
    [Theory]
    [MemberData(nameof(Seeds))]
    public async Task Walk_TerminatesOnGenuineGroupMembershipCycle(int seed)
    {
        var world = RandomAuthzWorlds.Build(seed);
        var compiled = SchemaCompiler.CompileSchema(world.SchemaText);
        var coverage = MembershipCoverage.Build(compiled.Namespaces);

        var g0 = world.Groups[0];
        var g1 = world.Groups[1];
        var cycle = new[]
        {
            Relationship.Create(new ObjectAndRelation("group", g0, "member"), new ObjectAndRelation("group", g1, "member")),
            Relationship.Create(new ObjectAndRelation("group", g1, "member"), new ObjectAndRelation("group", g0, "member")),
        };

        var store = new ReferenceDatastore();
        var rev = await store.ReadWriteTx(tx => tx.WriteRelationships(
            cycle.Select(r => new RelationshipUpdate(r, UpdateOperation.Touch)).ToList()));
        var reader = store.SnapshotReader(rev);

        var walkTask = MembershipWalk.LocalClosure(
            reader, coverage, new MembershipWalk.SubjectKey("group", g0, "member"));
        var completed = await Task.WhenAny(walkTask, Task.Delay(TimeSpan.FromSeconds(5)));
        Assert.True(completed == walkTask,
            $"seed={seed}: membership walk did not terminate within 5s on a genuine cycle between " +
            $"group:{g0}#member and group:{g1}#member");
        await walkTask; // Propagate any exception -- that would itself be a gate failure.
    }

    private static async Task<List<(string Id, Membership M)>> Collect(IAsyncEnumerable<FoundResource> e)
    {
        var list = new List<(string, Membership)>();
        await foreach (var f in e)
            list.Add((f.ResourceId, f.Membership));
        list.Sort((a, b) => string.CompareOrdinal(a.Item1, b.Item1));
        return list;
    }
}
