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

    [Fact]
    public async Task A_seed_reproduces_its_run()
    {
        var first = await FaultHarness.RunAsync(new FaultHarnessOptions(Seed: 3, Steps: 40));
        var second = await FaultHarness.RunAsync(new FaultHarnessOptions(Seed: 3, Steps: 40));

        Assert.Equal(first.Trace, second.Trace);
    }
}
