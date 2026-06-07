using Microsoft.Extensions.DependencyInjection;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Anti-hollow proof that the local-recurse-vs-grain-hop hybrid is REAL and the toggle works: on a
/// 3-silo cluster a single check fans out across many distinct sub-problem keys; with the hybrid ON the
/// locally-owned sub-problems are served in-process (counted as <c>LocalRecurse</c>, contributing ZERO
/// extra cross-silo grain hops), while remotely-owned ones make a real cross-silo grain call
/// (<c>RemoteGrainHop</c>). With the hybrid OFF, EVERY sub-problem is a grain call.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class HybridDispatchMeshTests
{
    /// <summary>
    /// A nested-group chain: <c>group.member = direct_member + member-from parent</c>, so checking a deep
    /// group recurses through every ancestor group as a DISTINCT sub-problem key — spreading sub-problems
    /// across the hash ring and guaranteeing some land local and some remote.
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
            updates.Add(new RelationshipUpdate(Relationship.Create(new ObjectAndRelation("group", $"g{i}", "parent"), new ObjectAndRelation("group", $"g{i + 1}", CoreConstants.Ellipsis)), UpdateOperation.Create));
        }
        updates.Add(new RelationshipUpdate(Relationship.Create(new ObjectAndRelation("group", $"g{depth - 1}", "direct_member"), new ObjectAndRelation("user", "u", CoreConstants.Ellipsis)), UpdateOperation.Create));
        await datastore.ReadWriteTx(tx => tx.WriteRelationships(updates));
    }

    [Fact]
    public async Task Hybrid_ON_serves_locally_owned_subproblems_in_process_and_remote_ones_hop()
    {
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(
            ChainSchema, siloCount: 3, localRecurseEnabled: true);
        await SeedChain(cluster.Datastore, depth: 20);

        cluster.ResetMetrics();
        var result = await cluster.Checker.Check(
            "group", "g0", "member", new ObjectAndRelation("user", "u", CoreConstants.Ellipsis), null);
        Assert.Equal(Membership.Member, result.Verdict);

        var m = cluster.MetricsSnapshot();

        // Real recursion happened across many distinct sub-problems.
        Assert.True(m.LocalRecurse + m.RemoteGrainHop >= 10,
            $"Expected the chain to fan into many sub-problems; saw {m.LocalRecurse} local + {m.RemoteGrainHop} remote.");

        // The hybrid is real: with a 3-silo ring, SOME sub-problems are locally owned and served in
        // process (no cross-silo message). This is the headline claim — a non-zero local fraction.
        Assert.True(m.LocalRecurse > 0,
            $"Hybrid ON must serve locally-owned sub-problems in-process; LocalRecurse was {m.LocalRecurse}.");
    }

    [Fact]
    public async Task Hybrid_OFF_makes_every_subproblem_a_grain_hop_and_none_local()
    {
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(
            ChainSchema, siloCount: 3, localRecurseEnabled: false);
        await SeedChain(cluster.Datastore, depth: 20);

        cluster.ResetMetrics();
        var result = await cluster.Checker.Check(
            "group", "g0", "member", new ObjectAndRelation("user", "u", CoreConstants.Ellipsis), null);
        Assert.Equal(Membership.Member, result.Verdict);

        var m = cluster.MetricsSnapshot();

        // OFF = pre-hybrid behaviour: every sub-problem is a grain call, none served by local-recurse.
        Assert.Equal(0, m.LocalRecurse);
        Assert.True(m.RemoteGrainHop > 0,
            $"Hybrid OFF must dispatch every sub-problem as a grain call; RemoteGrainHop was {m.RemoteGrainHop}.");
    }

    [Fact]
    public async Task Hybrid_reduces_cross_silo_grain_hops_versus_off()
    {
        // ON: a known fraction of sub-problems served in-process. OFF: all are grain calls. The ON run's
        // cross-silo grain hops must be strictly fewer than OFF's for the SAME workload — the hybrid win.
        await using var on = await MeshTestCluster.CreateMultiSiloAsync(
            ChainSchema, siloCount: 3, localRecurseEnabled: true);
        await SeedChain(on.Datastore, depth: 30);
        on.ResetMetrics();
        await on.Checker.Check("group", "g0", "member",
            new ObjectAndRelation("user", "u", CoreConstants.Ellipsis), null);
        var onSnap = on.MetricsSnapshot();

        await using var off = await MeshTestCluster.CreateMultiSiloAsync(
            ChainSchema, siloCount: 3, localRecurseEnabled: false);
        await SeedChain(off.Datastore, depth: 30);
        off.ResetMetrics();
        await off.Checker.Check("group", "g0", "member",
            new ObjectAndRelation("user", "u", CoreConstants.Ellipsis), null);
        var offSnap = off.MetricsSnapshot();

        Assert.True(onSnap.RemoteGrainHop < offSnap.RemoteGrainHop,
            $"Hybrid ON should make fewer cross-silo grain hops than OFF; ON={onSnap.RemoteGrainHop}, OFF={offSnap.RemoteGrainHop}.");
    }

    [Fact]
    public async Task Single_silo_hybrid_ON_makes_zero_cross_silo_hops_everything_local()
    {
        // With one silo, the ring resolves every key to that silo, so the hybrid serves EVERYTHING in
        // process: zero cross-silo grain hops.
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(
            ChainSchema, siloCount: 1, localRecurseEnabled: true);
        await SeedChain(cluster.Datastore, depth: 20);

        cluster.ResetMetrics();
        var result = await cluster.Checker.Check(
            "group", "g0", "member", new ObjectAndRelation("user", "u", CoreConstants.Ellipsis), null);
        Assert.Equal(Membership.Member, result.Verdict);

        var m = cluster.MetricsSnapshot();
        Assert.Equal(0, m.RemoteGrainHop);
        Assert.True(m.LocalRecurse > 0, "Single-silo hybrid must serve sub-problems locally.");
    }

    [Fact]
    public async Task A_cross_silo_subproblem_activates_on_its_owner_silo()
    {
        // Force a specific sub-problem key, find its hash owner, and (entering from a different silo)
        // assert the grain actually activates on the owner — proving cross-silo placement is real.
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(ChainSchema, siloCount: 3);

        var anyOwnership = cluster.Services.GetRequiredService<ISiloOwnership>();
        var localAddr = anyOwnership.LocalSilo.ToParsableString();

        // Probe keys until we find one owned by a REMOTE silo (relative to the primary's container).
        string? remoteKey = null;
        for (var i = 0; i < 200 && remoteKey is null; i++)
        {
            var key = $"group/g{i}/member/user/u/.../rev1/h/0";
            if (anyOwnership.OwnerOf(key).ToParsableString() != localAddr)
                remoteKey = key;
        }
        Assert.NotNull(remoteKey);

        var predictedOwner = anyOwnership.OwnerOf(remoteKey!).ToParsableString();
        var actualOwner = await cluster.GrainFactory.GetGrain<ICheckGrain>(remoteKey!).GetHostSilo();

        Assert.NotEqual(localAddr, actualOwner);
        Assert.Equal(predictedOwner, actualOwner);
    }
}
