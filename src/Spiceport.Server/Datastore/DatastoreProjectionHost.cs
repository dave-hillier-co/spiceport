using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// Holds the CANONICAL per-silo <see cref="SiloProjection"/> + <see cref="LogWatchHub"/> pair — the one
/// shared identity that both the silo-lifecycle-managed <see cref="DatastoreProjectionService"/> and every
/// production <see cref="GrainBackedDatastore"/> instance on this silo reference.
/// </summary>
/// <remarks>
/// Reaching a LIVE Orleans <see cref="Orleans.Runtime.GrainService"/> instance from plain DI is not the
/// supported path: the only DI-reachable client is <see cref="Orleans.Runtime.Services.GrainServiceClient{T}"/>,
/// a message-passing RPC channel over a grain-service interface — routing every per-Check read through that
/// would put a hop back on the hot path this projection exists to avoid (see
/// <see cref="GrainBackedDatastore"/>'s remarks: reads must serve from local in-process state with no grain
/// hop per Check). So identity lives here, in a plain DI singleton, registered once per silo (see
/// <see cref="SiloBuilderExtensions.AddDatastoreProjectionService"/>). <see cref="DatastoreProjectionService"/>
/// owns LIFECYCLE ONLY over this SAME instance: it calls <see cref="EnsureBootstrappedAsync"/> and starts the
/// hub before the silo accepts traffic, and disposes the hub on silo shutdown.
/// </remarks>
public interface IDatastoreProjectionHost
{
    /// <summary>The per-silo materialized read projection (see <see cref="SiloProjection"/>).</summary>
    SiloProjection Projection { get; }

    /// <summary>The per-silo Watch notifier (see <see cref="LogWatchHub"/>).</summary>
    LogWatchHub Hub { get; }

    /// <summary>Triggers the projection's first bootstrap fetch (delegates to <see cref="SiloProjection.WarmUpAsync"/>).</summary>
    Task EnsureBootstrappedAsync(CancellationToken cancellationToken = default);
}

/// <summary>Default production <see cref="IDatastoreProjectionHost"/>: one per silo, one per DI container.</summary>
internal sealed class DatastoreProjectionHost : IDatastoreProjectionHost
{
    public SiloProjection Projection { get; }
    public LogWatchHub Hub { get; }

    public DatastoreProjectionHost(IGrainFactory grainFactory)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        var grain = grainFactory.GetGrain<IDatastoreGrain>(IDatastoreGrain.Key);
        Projection = new SiloProjection(grain);
        Hub = new LogWatchHub(grain, grainFactory);
    }

    public Task EnsureBootstrappedAsync(CancellationToken cancellationToken = default) =>
        Projection.WarmUpAsync(cancellationToken);
}
