using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Test-only factory for a PRIVATE <see cref="LogWatchHub"/>, distinct from the DI-singleton hub the
/// production wiring shares per silo. This seam exists for tests that need a GENUINELY ISOLATED hub —
/// e.g. proving PUSH-driven Watch is real (grain-observer-driven), not a shared in-process shortcut, by
/// committing through one <see cref="GrainBackedDatastore"/>'s hub while asserting on another's (see
/// <c>Stage3WatchPushMeshTests</c>) — or a custom heartbeat cadence via
/// <paramref name="heartbeatInterval"/>. The hub returned is <see cref="IAsyncDisposable"/> and the
/// caller owns it (an <c>await using</c> local).
/// </summary>
internal static class IsolatedWatchHub
{
    public static LogWatchHub Create(IGrainFactory grainFactory, TimeSpan? heartbeatInterval = null)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        return new LogWatchHub(
            grainFactory.GetGrain<IDatastoreGrain>(IDatastoreGrain.Key), grainFactory, heartbeatInterval);
    }
}
