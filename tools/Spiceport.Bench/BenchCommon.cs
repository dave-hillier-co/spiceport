using Microsoft.Extensions.DependencyInjection;
using Spiceport.Core;
using Spiceport.Grains;
using Spiceport.Grains.Abstractions;
using Spiceport.Grains.Tests;

namespace Spiceport.Bench;

/// <summary>
/// Shared scenario plumbing: cluster boot (single vs multi silo through the SAME MeshTestCluster
/// configurators the test suites use — the production silo wiring), cluster-wide metrics
/// reset/snapshot, and the standard check/write op factories the mixed-load scenarios share.
/// </summary>
public static class BenchCommon
{
    /// <summary>The shared flags every scenario accepts.</summary>
    public static readonly string[] SharedFlagNames = ["seed", "silos", "duration", "warmup", "json"];

    /// <summary>
    /// Boots a cluster: <see cref="MeshTestCluster.CreateAsync"/> for one silo, otherwise
    /// <see cref="MeshTestCluster.CreateMultiSiloAsync"/>. Cluster boots are strictly sequential
    /// (the harness is single-threaded across boots — the schema handoff statics require it).
    /// </summary>
    public static Task<MeshTestCluster> CreateClusterAsync(
        int silos, bool coLocateWithShards = false, TimeSpan? quantization = null)
    {
        return silos <= 1
            ? MeshTestCluster.CreateAsync(
                BenchWorld.SchemaText, coLocateWithShards: coLocateWithShards, quantization: quantization)
            : MeshTestCluster.CreateMultiSiloAsync(
                BenchWorld.SchemaText, siloCount: silos,
                coLocateWithShards: coLocateWithShards, quantization: quantization);
    }

    /// <summary>Resets BOTH metric seams (dispatch and sequencer) on every silo, bracketing one cell.</summary>
    public static void ResetAllMetrics(MeshTestCluster cluster)
    {
        cluster.ResetMetrics();
        foreach (var sp in cluster.AllSiloServices)
            sp.GetRequiredService<ISequencerMetrics>().Reset();
    }

    /// <summary>
    /// Arms a ONE-SHOT reset of both metric seams at the warmup-to-measured boundary: start the
    /// returned task immediately before launching the drivers (their stopwatches start at call time,
    /// so this delay tracks their warmup windows to within scheduling noise), await it after the
    /// drivers finish. This keeps the metrics snapshots covering (approximately) exactly the measured
    /// window — matching the latency recorders, which already exclude warmup-started ops — instead of
    /// silently folding warmup traffic (JIT-cold hops, cold-memo misses, warmup commits) into the
    /// reported counters. Ops in flight ACROSS the boundary bleed a partial op either way; that is
    /// inherent to counter snapshotting and negligible at harness scales.
    /// </summary>
    public static async Task ArmWarmupBoundaryResetAsync(MeshTestCluster cluster, TimeSpan warmup)
    {
        if (warmup > TimeSpan.Zero)
            await Task.Delay(warmup).ConfigureAwait(false);
        ResetAllMetrics(cluster);
    }

    /// <summary>
    /// One small UNTIMED pass on a throwaway single-silo cluster whose results are discarded: pays
    /// tiered-JIT promotion, type initialization, and serializer warm-once costs BEFORE the first
    /// real arm of an A/B scenario runs, so arm A does not eat the process-global cold start that
    /// arm B then benefits from (the classic A/B order effect). Runs both op classes (checks and
    /// writes) so both code paths are touched.
    /// </summary>
    public static async Task GlobalWarmPassAsync(int seed)
    {
        Console.Error.WriteLine("global warm pass (untimed, results discarded)...");
        var world = BenchWorld.Generate(seed, new BenchWorldOptions(
            Users: 20, Groups: 5, GroupMeanSize: 3, BigUsersetSizes: [16],
            Folders: 6, FolderDepth: 2, Documents: 30, NestingDepth: 1));
        await using var cluster = await CreateClusterAsync(silos: 1);
        var loadToken = await world.LoadAsync(cluster);

        var token = new LastWriteToken(loadToken);
        var docSampler = new ZipfSampler(world.Documents.Count);
        var writeOp = MakeWriteOp(cluster, world, docSampler, new Random(seed), token);
        var checkOp = MakeCheckOp(cluster, world, docSampler, ConsistencyMix.Parse("70/20/10"), token);
        var rngs = WorkerRngs(seed, 2);

        var discard = new LatencyRecorder(capacity: 1024);
        var writes = BenchDriver.RunOpenLoop(
            ratePerSecond: 10, warmup: TimeSpan.Zero, duration: TimeSpan.FromSeconds(1), writeOp, discard);
        await BenchDriver.RunClosedLoop(
            workers: 2, warmup: TimeSpan.Zero, duration: TimeSpan.FromSeconds(1),
            (w, _) => checkOp(rngs[w]), discard);
        await writes;
    }

    /// <summary>
    /// Mutable shared holder for the newest write token any writer observed — the freshness floor
    /// at-least-as-fresh checks chain to. Seed it with the world-load token before traffic starts.
    /// </summary>
    public sealed class LastWriteToken(string initial)
    {
        private string _token = initial;

        public string Get() => Volatile.Read(ref _token);

        public void Set(string token) => Volatile.Write(ref _token, token);
    }

    /// <summary>
    /// The standard steady-state write op: TOUCH one <c>document#viewer@user</c> row with the
    /// document drawn Zipf-skewed and the user uniform, both from the world's fixed alphabets —
    /// so the key space stays bounded no matter how long the run (touches converge, rows never
    /// grow without bound). Each completion publishes its commit token to <paramref name="token"/>.
    /// </summary>
    public static Func<Task> MakeWriteOp(
        MeshTestCluster cluster, BenchWorld world, ZipfSampler docSampler, Random rng, LastWriteToken token)
    {
        return async () =>
        {
            string doc, user;
            lock (rng)
            {
                doc = world.Documents[docSampler.Next(rng)];
                user = world.Users[rng.Next(world.Users.Count)];
            }
            var update = new RelationshipUpdateWire(
                RelationshipUpdateOpWire.Touch,
                new RelationshipWire("document", doc, "viewer", "user", user, CoreConstants.Ellipsis, null, null, null));
            // A plain array, not a collection expression: single-element collection expressions
            // lower to a compiler-synthesized list type Orleans has no copier for.
            var reply = await cluster.Relationships.WriteRelationships(
                new WriteRelationshipsArgs(new[] { update }));
            token.Set(reply.WrittenAtToken);
        };
    }

    /// <summary>
    /// The standard steady-state check op for closed-loop workers: <c>document.view</c> for a
    /// Zipf-skewed document and a uniform user, at a consistency rolled from <paramref name="mix"/>
    /// per op. Each worker passes its own <see cref="Random"/> so sampling is lock-free.
    /// </summary>
    public static Func<Random, Task> MakeCheckOp(
        MeshTestCluster cluster, BenchWorld world, ZipfSampler docSampler, ConsistencyMix mix, LastWriteToken token)
    {
        return async rng =>
        {
            var doc = world.Documents[docSampler.Next(rng)];
            var user = world.Users[rng.Next(world.Users.Count)];
            var consistency = mix.Pick(rng, token.Get);
            await cluster.Checker.Check(
                "document", doc, "view",
                new ObjectAndRelation("user", user, CoreConstants.Ellipsis),
                caveatContext: null, consistency: consistency);
        };
    }

    /// <summary>
    /// Per-worker deterministic RNGs so closed-loop sampling needs no locks. Seeds are derived by a
    /// splitmix64 step over the packed (seed, worker) pair — NOT <c>HashCode.Combine</c>, whose hash
    /// seed is randomized per process, which would silently break the harness's reproducibility
    /// contract (identical <c>--seed</c> must reproduce identical workload streams across invocations).
    /// </summary>
    public static Random[] WorkerRngs(int seed, int workers) =>
        Enumerable.Range(0, workers).Select(w => new Random(DeriveWorkerSeed(seed, w))).ToArray();

    /// <summary>
    /// Derivation: pack <paramref name="seed"/> (high 32 bits) with <paramref name="worker"/> (low 32
    /// bits), add splitmix64's golden-gamma increment, then apply its 64-to-64 finalizer mix; fold the
    /// two halves into the 32-bit <see cref="Random"/> seed. Fully deterministic, well-distributed
    /// across adjacent worker indices, and stable across processes and runtimes.
    /// </summary>
    private static int DeriveWorkerSeed(int seed, int worker)
    {
        var z = (((ulong)(uint)seed) << 32) | (uint)worker;
        z += 0x9E3779B97F4A7C15ul;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ul;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBul;
        z ^= z >> 31;
        return (int)(z ^ (z >> 32));
    }
}
