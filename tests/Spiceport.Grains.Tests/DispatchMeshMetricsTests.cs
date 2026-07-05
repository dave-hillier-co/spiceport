using Microsoft.Extensions.DependencyInjection;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Anti-hollow proof that the mesh is REAL now that there is no custom placement director, no
/// local-recurse shortcut, and no caller-side branch cache: a deep chain check must fan out into many
/// distinct <see cref="CheckGrain"/> activations, and on a multi-silo cluster those activations must
/// genuinely spread across more than one silo's process (Orleans' default placement + grain directory,
/// not a hand-rolled consistent-hash ring, is doing the routing). Ported from the deleted
/// <c>HybridDispatchMeshTests</c>/<c>HybridDispatchBenchmark</c> — this keeps the "recursion really
/// crosses silo boundaries" claim under test without the retired hybrid ON/OFF matrix.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class DispatchMeshMetricsTests
{
    /// <summary>
    /// A nested-group chain: <c>group.member = direct_member + parent-&gt;member</c>, so checking a deep
    /// group recurses through every ancestor group as a DISTINCT sub-problem key (a distinct grain per
    /// hop), spreading activations across the mesh.
    /// </summary>
    private const string ChainSchema = """
        definition user {}

        definition group {
            relation direct_member: user
            relation parent: group
            permission member = direct_member + parent->member
        }
        """;

    /// <summary>Seeds a chain g0 -> g1 -> ... -> g(n-1), with user u a direct member of g(n-1).</summary>
    private static async Task SeedChain(IDatastore datastore, int depth)
    {
        var updates = new List<RelationshipUpdate>();
        for (var i = 0; i < depth - 1; i++)
        {
            updates.Add(new RelationshipUpdate(
                Relationship.Create(
                    new ObjectAndRelation("group", $"g{i}", "parent"),
                    new ObjectAndRelation("group", $"g{i + 1}", CoreConstants.Ellipsis)),
                UpdateOperation.Create));
        }
        updates.Add(new RelationshipUpdate(
            Relationship.Create(
                new ObjectAndRelation("group", $"g{depth - 1}", "direct_member"),
                new ObjectAndRelation("user", "u", CoreConstants.Ellipsis)),
            UpdateOperation.Create));
        await datastore.ReadWriteTx(tx => tx.WriteRelationships(updates));
    }

    [Fact]
    public async Task Deep_chain_check_fans_out_into_many_distinct_grain_activations()
    {
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(ChainSchema, siloCount: 3);
        await SeedChain(cluster.Datastore, depth: 20);

        cluster.ResetMetrics();
        var result = await cluster.Checker.Check(
            "group", "g0", "member", new ObjectAndRelation("user", "u", CoreConstants.Ellipsis), null);
        Assert.Equal(Membership.Member, result.Verdict);

        var m = cluster.MetricsSnapshot();

        // Every sub-problem is a real grain call (no in-process local-recurse shortcut left), so a
        // 20-deep chain must cross the CheckDispatchIncomingCallFilter's boundary at least that many
        // times — one real dispatch hop per distinct CheckGrain key, counted at the filter itself rather
        // than inferred from the grain's own (memo-dependent) hit/miss bookkeeping.
        Assert.True(m.Dispatch >= 20,
            $"Expected the chain to fan into at least 20 real dispatch hops; saw {m.Dispatch}.");
    }

    [Fact]
    public async Task Multi_silo_cluster_genuinely_spreads_activations_across_more_than_one_silo()
    {
        // Random placement (an explicit opt-in for THIS assertion): Orleans 10's default placement is
        // ResourceOptimizedPlacement, a load-statistics heuristic whose local-silo preference margin may
        // legitimately keep a whole chain on the calling silo when silo load scores are close (observed
        // deterministically after heavy in-process cluster churn) — it makes no spread guarantee, so a
        // spread ASSERTION under it would be asserting behavior the runtime does not promise. Random
        // placement does guarantee spread (statistically overwhelming for 30 keys over 3 silos), keeping
        // this test's real claim — recursion crosses genuine process boundaries with no custom router —
        // sound and deterministic.
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(
            ChainSchema, siloCount: 3, useRandomPlacement: true);
        await SeedChain(cluster.Datastore, depth: 30);

        cluster.ResetMetrics();
        var result = await cluster.Checker.Check(
            "group", "g0", "member", new ObjectAndRelation("user", "u", CoreConstants.Ellipsis), null);
        Assert.Equal(Membership.Member, result.Verdict);

        // With no custom placement director, Orleans' own placement + the grain directory decide where
        // each of the chain's many distinct grain keys activates. Anti-hollow: on a real 3-silo cluster
        // that routing must genuinely land activations on more than one silo's own process, not collapse
        // the whole chain onto a single silo.
        var perSiloDispatches = cluster.AllSiloServices
            .Select(sp => sp.GetRequiredService<IDispatchMetrics>().Snapshot().Dispatch)
            .ToArray();

        Assert.True(perSiloDispatches.Count(count => count > 0) > 1,
            $"Expected activations to spread across more than one silo; per-silo dispatch hops were " +
            $"[{string.Join(", ", perSiloDispatches)}].");
    }
}
