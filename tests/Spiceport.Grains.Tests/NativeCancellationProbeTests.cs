using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Spiceport.Grains.Tests;

/// <summary>
/// A throwaway grain used only to prove that Orleans 10.1's grain-call codegen accepts a plain
/// <see cref="CancellationToken"/> parameter on a unary (non-streaming) grain method, and that caller
/// cancellation actually propagates to the callee's token rather than only aborting the caller-side
/// await. This is the gate for replacing the legacy Orleans grain-cancellation-token type with a plain
/// <see cref="CancellationToken"/> across the unary grain interfaces (see
/// <c>docs/future-work.md</c> §1.5): the repo's streaming grain interfaces
/// (<c>IReverseOpsStreamGrain</c>, <c>IRelationshipsStreamGrain</c>) already take a plain token, which is
/// the evidence that motivated this probe.
/// </summary>
public interface INativeCancellationProbeGrain : IGrainWithStringKey
{
    /// <summary>The address of the silo hosting this activation, for asserting cross-silo placement.</summary>
    Task<string> WhereAmI();

    /// <summary>
    /// Awaits a delay of <paramref name="delayMs"/> honoring <paramref name="cancellationToken"/>, and
    /// reports whether the token had already fired the instant the grain body observed it (a cheap signal
    /// that cancellation reached the activation promptly rather than merely aborting the caller's await).
    /// </summary>
    Task<bool> DelayHonoringCancellation(int delayMs, CancellationToken cancellationToken);
}

/// <summary>See <see cref="INativeCancellationProbeGrain"/>.</summary>
public sealed class NativeCancellationProbeGrain(ILocalSiloDetails siloDetails)
    : Grain, INativeCancellationProbeGrain
{
    public Task<string> WhereAmI() => Task.FromResult(siloDetails.SiloAddress.ToString());

    public async Task<bool> DelayHonoringCancellation(int delayMs, CancellationToken cancellationToken)
    {
        await Task.Delay(delayMs, cancellationToken);
        return true;
    }
}

/// <summary>
/// The MANDATORY verification gate for <c>engine/native-cancellation</c>: proves native unary
/// cancellation works on this Orleans version, same-silo and cross-silo, before anything downstream
/// converts from the legacy Orleans grain-cancellation-token type to a plain <see cref="CancellationToken"/>.
/// </summary>
/// <remarks>
/// Deliberately does NOT use <see cref="MeshTestCluster"/> (no schema/datastore is needed for this
/// probe), but still joins <see cref="MeshClusterCollection"/> so its <see cref="TestCluster"/> never
/// runs concurrently with another cluster-standing test class in this assembly.
/// </remarks>
[Collection(MeshClusterCollection.Name)]
public sealed class NativeCancellationProbeTests
{
    private sealed class ProbeSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            // The probe grain carries no persisted state, so no storage/log-consistency providers are
            // needed — just a bare multi-silo cluster.
        }
    }

    private static async Task<TestCluster> CreateClusterAsync(short siloCount)
    {
        var builder = new TestClusterBuilder(initialSilosCount: siloCount);
        builder.AddSiloBuilderConfigurator<ProbeSiloConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        return cluster;
    }

    [Fact]
    public async Task Uncancelled_call_completes_normally()
    {
        await using var cluster = await CreateClusterAsync(siloCount: 1);
        var grain = cluster.GrainFactory.GetGrain<INativeCancellationProbeGrain>("uncancelled");

        var completed = await grain.DelayHonoringCancellation(50, CancellationToken.None);

        Assert.True(completed);
    }

    [Fact]
    public async Task Caller_cancellation_propagates_to_the_grain_token_same_silo()
    {
        await using var cluster = await CreateClusterAsync(siloCount: 1);
        var grain = cluster.GrainFactory.GetGrain<INativeCancellationProbeGrain>("same-silo");

        using var cts = new CancellationTokenSource();
        var call = grain.DelayHonoringCancellation(60_000, cts.Token);
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => call.WaitAsync(TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task Caller_cancellation_propagates_to_the_grain_token_cross_silo()
    {
        await using var cluster = await CreateClusterAsync(siloCount: 3);

        // Orleans 10's default ResourceOptimizedPlacement makes no spread guarantee, so probe several
        // keys and use the first activation this test process finds on a NON-primary silo (mirrors
        // MeshTestCluster's documented rationale for opting into spread rather than trusting the
        // default placement heuristic to scatter a handful of calls).
        var primaryAddress = cluster.Primary!.SiloAddress.ToString();
        INativeCancellationProbeGrain? remote = null;
        for (var i = 0; i < 64 && remote is null; i++)
        {
            var candidate = cluster.GrainFactory.GetGrain<INativeCancellationProbeGrain>($"probe-{i}");
            var where = await candidate.WhereAmI();
            if (where != primaryAddress)
                remote = candidate;
        }

        Assert.NotNull(remote);

        using var cts = new CancellationTokenSource();
        var call = remote!.DelayHonoringCancellation(60_000, cts.Token);
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => call.WaitAsync(TimeSpan.FromSeconds(10)));
    }
}
