using System.Diagnostics;
using Spiceport.Core;
using Spiceport.Datastore;
using Xunit.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// In-process (TestCluster) benchmark proving the hybrid is real: it runs the same deep-recursion
/// workload across the 4-cell matrix {single-silo, 3-silo} × {hybrid OFF, hybrid ON} and reports, per
/// cell, the cluster-wide RemoteGrainHop / LocalRecurse / cache hit-miss counts plus p50/p99 latency and
/// throughput. The headline assertions: 3-silo ON makes strictly fewer cross-silo grain hops than 3-silo
/// OFF, and single-silo ON makes zero cross-silo hops. Runs entirely in-process — no host boot.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class HybridDispatchBenchmark(ITestOutputHelper output)
{
    private const string ChainSchema = """
        definition user {}

        definition group {
            relation direct_member: user
            relation parent: group
            permission member = direct_member + parent->member
        }
        """;

    private const int ChainDepth = 30;

    // Each root check targets a DISTINCT group family ("f{n}_g0"), so every check is a fresh dispatch
    // (distinct sub-problem keys) that actually crosses grain boundaries — the cache cannot serve it from
    // a prior check. This is what makes the hop counters meaningful, and spreads sub-problems across the
    // whole ring. (A repeated identical root would be a single cache hit and measure nothing about hops.)
    //
    // N is configurable so the SAME benchmark serves two purposes:
    //   • Default small N (20) keeps the run fast enough to live in the normal suite gate.
    //   • Set the BENCH_FAMILIES env var (e.g. BENCH_FAMILIES=2000) to drive a larger workload for a
    //     headline measurement, without touching code or booting a host (still in-process TestCluster).
    // The qualitative anti-hollow assertions hold at any N >= 1.
    private const int DefaultFamilies = 20;

    private static int Families =>
        int.TryParse(Environment.GetEnvironmentVariable("BENCH_FAMILIES"), out var n) && n > 0
            ? n
            : DefaultFamilies;

    private static async Task SeedFamilies(IDatastore datastore, int families, int depth)
    {
        var updates = new List<RelationshipUpdate>();
        for (var f = 0; f < families; f++)
        {
            var p = $"f{f}_";
            for (var i = 0; i < depth - 1; i++)
                updates.Add(new RelationshipUpdate(Relationship.Create(
                    new ObjectAndRelation("group", $"{p}g{i}", "parent"),
                    new ObjectAndRelation("group", $"{p}g{i + 1}", CoreConstants.Ellipsis)),
                    UpdateOperation.Create));
            updates.Add(new RelationshipUpdate(Relationship.Create(
                new ObjectAndRelation("group", $"{p}g{depth - 1}", "direct_member"),
                new ObjectAndRelation("user", "u", CoreConstants.Ellipsis)),
                UpdateOperation.Create));
        }
        await datastore.ReadWriteTx(tx => tx.WriteRelationships(updates));
    }

    private sealed record Cell(
        string Name, DispatchMetricsSnapshot Metrics, double P50Ms, double P99Ms, double Throughput);

    private static async Task<Cell> RunCell(string name, int siloCount, bool localRecurse)
    {
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(
            ChainSchema, siloCount: siloCount, localRecurseEnabled: localRecurse);
        await SeedFamilies(cluster.Datastore, Families, ChainDepth);

        var subject = new ObjectAndRelation("user", "u", CoreConstants.Ellipsis);

        cluster.ResetMetrics();
        var latencies = new double[Families];
        var swAll = Stopwatch.StartNew();
        for (var f = 0; f < Families; f++)
        {
            var sw = Stopwatch.StartNew();
            await cluster.Checker.Check("group", $"f{f}_g0", "member", subject, null);
            sw.Stop();
            latencies[f] = sw.Elapsed.TotalMilliseconds;
        }
        swAll.Stop();

        Array.Sort(latencies);
        var p50 = latencies[(int)(Families * 0.50)];
        var p99 = latencies[(int)(Families * 0.99)];
        var throughput = Families / swAll.Elapsed.TotalSeconds;

        return new Cell(name, cluster.MetricsSnapshot(), p50, p99, throughput);
    }

    [Fact]
    [Trait("Category", "Benchmark")]
    public async Task Benchmark_matrix_single_vs_multi_silo_hybrid_off_vs_on()
    {
        var single_off = await RunCell("single-silo OFF", siloCount: 1, localRecurse: false);
        var single_on = await RunCell("single-silo ON ", siloCount: 1, localRecurse: true);
        var multi_off = await RunCell("3-silo     OFF", siloCount: 3, localRecurse: false);
        var multi_on = await RunCell("3-silo     ON ", siloCount: 3, localRecurse: true);

        output.WriteLine($"Workload: chain depth {ChainDepth}, {Families} distinct root checks/cell " +
            $"(set BENCH_FAMILIES to scale N).");
        output.WriteLine("cell           | remoteHop | localRec | cacheHit | cacheMiss | p50 ms | p99 ms | checks/s");
        foreach (var c in new[] { single_off, single_on, multi_off, multi_on })
        {
            output.WriteLine(
                $"{c.Name} | {c.Metrics.RemoteGrainHop,9} | {c.Metrics.LocalRecurse,8} | " +
                $"{c.Metrics.CacheHit,8} | {c.Metrics.CacheMiss,9} | {c.P50Ms,6:F3} | {c.P99Ms,6:F3} | {c.Throughput,8:F0}");
        }

        // Headline claims, asserted (not just printed):
        // 1. single-silo ON makes ZERO cross-silo grain hops (everything local).
        Assert.Equal(0, single_on.Metrics.RemoteGrainHop);
        // 2. 3-silo ON makes strictly fewer cross-silo grain hops than 3-silo OFF (the hybrid win).
        Assert.True(multi_on.Metrics.RemoteGrainHop < multi_off.Metrics.RemoteGrainHop,
            $"3-silo ON remoteHop ({multi_on.Metrics.RemoteGrainHop}) must be < OFF ({multi_off.Metrics.RemoteGrainHop}).");
        // 3. The hybrid genuinely served work locally on the multi-silo cluster.
        Assert.True(multi_on.Metrics.LocalRecurse > 0);
    }
}
