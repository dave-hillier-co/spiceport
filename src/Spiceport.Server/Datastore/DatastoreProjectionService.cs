using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using Orleans.Services;

namespace Spiceport.Grains;

/// <summary>
/// Marker grain-service interface Orleans' <c>AddGrainService&lt;T&gt;</c> registration requires on the
/// concrete <see cref="GrainService"/> subclass (distinct from the base <see cref="IGrainService"/> every
/// <see cref="GrainService"/> already implements) to identify it as its own registrable grain-service type.
/// </summary>
public interface IDatastoreProjectionGrainService : IGrainService;

/// <summary>
/// Silo-lifecycle-managed owner of the per-silo <see cref="SiloProjection"/>/<see cref="LogWatchHub"/>
/// warm-up and teardown. The "one per silo" shape these two components have is exactly what an Orleans
/// <see cref="GrainService"/> is for (see <c>docs/future-work.md</c> §1.8): silo-lifecycle-managed and
/// running well before the silo accepts traffic.
/// </summary>
/// <remarks>
/// The actual <see cref="SiloProjection"/>/<see cref="LogWatchHub"/> instances live in the shared
/// <see cref="IDatastoreProjectionHost"/> DI singleton (see its doc comment for why identity lives there
/// rather than here). This <see cref="GrainService"/> owns LIFECYCLE ONLY over that same instance:
/// <list type="bullet">
/// <item><description>
/// <see cref="Start"/> runs at the silo's <c>RuntimeGrainServices</c> lifecycle stage (Orleans'
/// <c>ServiceLifecycleStage.RuntimeGrainServices</c> = 8000), strictly before <c>BecomeActive</c>/<c>Active</c>
/// (19999/20000) — i.e. strictly before the silo accepts client traffic. Blocking here on the projection's
/// first <see cref="Abstractions.IDatastoreGrain.ReadState"/> fetch means the first real request never pays
/// the bootstrap latency. Verified empirically against the Orleans TestingHost (see
/// <c>GrainServiceLifecycleTests</c>) that a grain call from within a GrainService's own <see cref="Start"/>
/// does not deadlock or race the target grain's activation — grain-call dispatch is independent of
/// GrainService lifecycle-stage progression, so no fire-and-forget fallback was needed.
/// </description></item>
/// <item><description>
/// <see cref="Stop"/> disposes the hub via <see cref="LogWatchHub.DisposeAsync"/> — a bounded/timeout-guarded
/// unsubscribe from the DatastoreGrain's observer set — so silo shutdown never hangs on a slow or failed
/// unsubscribe call. <see cref="GrainBackedDatastore"/> itself owns no lifetime of its own to dispose; the
/// hub's lifecycle belongs entirely to this <see cref="IDatastoreProjectionHost"/>-owning service.
/// </description></item>
/// </list>
/// </remarks>
public sealed class DatastoreProjectionService : GrainService, IDatastoreProjectionGrainService
{
    private readonly IDatastoreProjectionHost _host;

    public DatastoreProjectionService(
        GrainId grainId, Silo silo, ILoggerFactory loggerFactory, IDatastoreProjectionHost host)
        : base(grainId, silo, loggerFactory)
    {
        ArgumentNullException.ThrowIfNull(host);
        _host = host;
    }

    public override async Task Start()
    {
        // Bootstrap-before-traffic: block on the projection's first ReadState fetch and start the hub's
        // observer subscription + heartbeat here, before the silo declares itself active.
        await _host.EnsureBootstrappedAsync().ConfigureAwait(false);
        _host.Hub.EnsureStarted();
        await base.Start().ConfigureAwait(false);
    }

    public override async Task Stop()
    {
        await _host.Hub.DisposeAsync().ConfigureAwait(false);
        await base.Stop().ConfigureAwait(false);
    }
}
