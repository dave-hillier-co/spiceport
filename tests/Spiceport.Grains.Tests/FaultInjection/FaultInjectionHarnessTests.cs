namespace Spiceport.Grains.Tests.FaultInjection;

/// <summary>
/// The deterministic fault-injection harness (docs/transaction-system.md §2 step 3, stage 2): seeded
/// workloads against the real sequencer and shard mesh, with storage failures and deactivations
/// injected at seeded points, every commit outcome and every read checked against
/// <see cref="Spiceport.Datastore.ReferenceDatastore"/>.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public sealed class FaultInjectionHarnessTests
{
    public static TheoryData<int> Seeds => new() { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };

    /// <summary>
    /// The harness's own gate: storage that acknowledges shard-projection writes but drops them is a
    /// lost-write bug the oracle must report, naming the seed so the run can be replayed.
    /// </summary>
    [Fact]
    public async Task A_lost_shard_write_is_reported_as_a_divergence()
    {
        var ex = await Assert.ThrowsAsync<FaultHarnessDivergenceException>(() =>
            FaultHarness.RunAsync(new FaultHarnessOptions(Seed: 1, Steps: 30) { LoseShardWrites = true }));

        Assert.Contains("seed 1", ex.Message);
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public async Task Seeded_runs_agree_with_the_reference_under_injected_faults(int seed)
    {
        var run = await FaultHarness.RunAsync(new FaultHarnessOptions(seed, Steps: 80));

        // Every run must actually inject faults and recover from them, or it proves nothing.
        Assert.True(run.SequencerFaultsFired > 0, "no sequencer write failure fired");
        Assert.True(run.ShardFaultsFired > 0, "no shard-projection write failure fired");
        Assert.Contains(run.Trace, line => line.StartsWith("deactivate", StringComparison.Ordinal));
    }

    public static TheoryData<int> ClusterSeeds => new() { 1, 2, 3, 4 };

    /// <summary>
    /// Four silos over storage that outlives any one of them: a seeded schedule also hard-kills
    /// secondary silos (whichever activations they held, the sequencer's included) and runs GC rounds
    /// under a short retention window, so recovery runs from storage alone and reads compare only at
    /// revisions the floor still covers.
    /// </summary>
    [Theory]
    [MemberData(nameof(ClusterSeeds))]
    public async Task Seeded_runs_agree_with_the_reference_across_silo_kills_and_gc_rounds(int seed)
    {
        var run = await FaultHarness.RunAsync(new FaultHarnessOptions(seed, Steps: 60)
        {
            Silos = 4,
            KillSilos = true,
            GcWindow = TimeSpan.FromMilliseconds(200),
        });

        Assert.True(run.SilosKilled > 0, "no silo was killed");
        Assert.True(run.SequencerKills > 0, "no kill took the sequencer's activation down");
        Assert.True(run.GcRounds > 0, "no GC round advanced the floor");
    }

    [Fact]
    public async Task A_seed_reproduces_its_run()
    {
        var first = await FaultHarness.RunAsync(new FaultHarnessOptions(Seed: 3, Steps: 40));
        var second = await FaultHarness.RunAsync(new FaultHarnessOptions(Seed: 3, Steps: 40));

        Assert.Equal(first.Trace, second.Trace);
    }
}
