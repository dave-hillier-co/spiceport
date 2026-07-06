using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Test-only <see cref="IDatastoreProjectionHost"/>: builds its OWN private <see cref="SiloProjection"/> +
/// <see cref="LogWatchHub"/> pair rather than sharing the per-silo production ones. Production code always
/// goes through the silo-lifecycle-managed <see cref="DatastoreProjectionHost"/> (registered by
/// <see cref="SiloBuilderExtensions.AddDatastoreProjectionService"/>); this seam exists for tests that need a
/// GENUINELY ISOLATED hub — e.g. proving PUSH-driven Watch is real (grain-observer-driven), not a shared
/// in-process shortcut, by committing through one <see cref="GrainBackedDatastore"/>'s hub while asserting on
/// another's (see <c>Stage3WatchPushMeshTests</c>) — or a custom heartbeat cadence via
/// <paramref name="heartbeatInterval"/> passed to the constructor. This type owns the hub it creates, so
/// <see cref="DisposeAsync"/> tears it down; callers that construct one explicitly must dispose it (an
/// <c>await using</c> local, or letting a DI container that registered it as a singleton dispose it on
/// teardown).
/// </summary>
public sealed class PrivateProjectionHost : IDatastoreProjectionHost, IAsyncDisposable
{
    public SiloProjection Projection { get; }
    public LogWatchHub Hub { get; }

    public PrivateProjectionHost(IGrainFactory grainFactory, TimeSpan? heartbeatInterval = null)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        var grain = grainFactory.GetGrain<IDatastoreGrain>(IDatastoreGrain.Key);
        Projection = new SiloProjection(grain);
        Hub = new LogWatchHub(grain, grainFactory, heartbeatInterval);
    }

    public Task EnsureBootstrappedAsync(CancellationToken cancellationToken = default) =>
        Projection.WarmUpAsync(cancellationToken);

    public ValueTask DisposeAsync() => Hub.DisposeAsync();
}
