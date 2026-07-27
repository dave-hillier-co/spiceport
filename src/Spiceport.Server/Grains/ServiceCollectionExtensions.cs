using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Hosting;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;
using Spiceport.Schema;

namespace Spiceport.Grains;

/// <summary>
/// DI registration for the check grain's supporting services and the silo-wide dispatch mesh.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the dynamic schema provider and the silo-wide dispatch mesh (the Orleans dispatcher)
    /// that the check grain and the API entry point depend on.
    /// </summary>
    /// <remarks>
    /// The <see cref="IDatastore"/> singleton is owned by the host (it must persist writes), so it is
    /// intentionally NOT registered here — it is only consumed. <see cref="IGrainFactory"/> is provided
    /// by Orleans. The schema is held by a <see cref="MutableSchemaProvider"/> seeded from
    /// <paramref name="schemaText"/>; the dispatch mesh reads the provider's CURRENT schema hash per
    /// request (via <see cref="ISchemaHashSource"/>), so a runtime schema swap is reflected in every new
    /// grain key and no pre-change <see cref="CheckGrain"/> activation memo is ever reused.
    /// </remarks>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="schemaText">The schema DSL text to seed the provider with at startup.</param>
    /// <param name="maxDepth">The check engine's maximum recursion depth.</param>
    /// <param name="batchConcurrency">
    /// The bounded fan-out width for <see cref="IPermissionChecker.BatchCheck"/>, mirroring SpiceDB's
    /// bulk-check <c>maxConcurrency</c>.
    /// </param>
    public static IServiceCollection AddSpiceportGrainServices(
        this IServiceCollection services,
        string schemaText,
        int maxDepth = CheckEngine.DefaultMaxDepth,
        int batchConcurrency = PermissionChecker.DefaultBatchConcurrency)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(schemaText);

        // The mutable, versioned, thread-safe schema provider. It is the single source of truth for
        // evaluation and the live schema hash; constructing it validates the seed schema (compile).
        var provider = new MutableSchemaProvider(schemaText);
        services.AddSingleton<ISchemaProvider>(provider);
        services.AddSingleton<ISchemaHashSource>(provider);

        // Per-silo compile-and-cache of the schema NAMED by each dispatch (its hash), resolved from the
        // schema bytes folded into the log on every silo. This is what lets a CheckGrain evaluate under the
        // schema its key pins — a pure function of the pinned revision — instead of the silo-local Current,
        // which only reflects a WriteSchema that landed on this silo. Seeded with the embedded startup
        // schema so the seed window (no WriteSchema persisted yet) resolves from the cache instead of
        // paying a per-dispatch sequencer ReadSchemaAt hop (see SchemaResolver.Seed).
        services.AddSingleton<SchemaResolver>(_ =>
        {
            var resolver = new SchemaResolver();
            resolver.Seed(provider.Current);
            return resolver;
        });

        // The silo-wide loop-bypass / activation-memo / dispatch counters.
        services.AddSingleton<IDispatchMetrics, DispatchMetrics>();

        // The sequencer-side inbound-call counters (docs/scalability-program.md Phase 0). Registered on
        // every silo like IDispatchMetrics; only the silo hosting the single DatastoreGrain activation
        // ever records, so summing every silo's snapshot yields cluster-wide totals.
        services.AddSingleton<ISequencerMetrics, SequencerMetrics>();

        // The per-silo sequencer write admission gate (issue #36): bounds this silo's in-flight commits
        // to the singleton sequencer so overload sheds (RESOURCE_EXHAUSTED, retryable) instead of
        // ramping into Orleans response timeouts. Default ON; retune or disable via an options override
        // (the last-registration-wins pattern the other toggles use).
        services.AddSingleton<SequencerAdmissionOptions>();
        services.AddSingleton<SequencerAdmission>(sp => new SequencerAdmission(
            sp.GetRequiredService<SequencerAdmissionOptions>(),
            sp.GetRequiredService<ISequencerMetrics>()));

        // The check-dispatch cross-cutting grain-call filters, registered as plain DI singletons: Orleans
        // resolves every IIncomingGrainCallFilter/IOutgoingGrainCallFilter from the SAME container the
        // silo runtime uses, so this one registration covers both co-hosted hosts (Api, Silo) and any
        // TestingHost cluster built by calling this same extension — there is nothing host-specific to
        // wire up separately. Both filters match ONLY ICheckGrain.DispatchCheck (see CheckDispatchFilter);
        // every other grain call in the mesh passes through untouched.
        services.AddSingleton<IOutgoingGrainCallFilter, CheckDispatchOutgoingCallFilter>();
        services.AddSingleton<IIncomingGrainCallFilter, CheckDispatchIncomingCallFilter>();

        // The engines' graph-read seam: always the IGraphShardGrain mesh (the retired per-silo
        // whole-graph projection alternative — and the flag that switched to it — is gone).
        services.AddSingleton<IGraphReaderSource>(sp =>
            new ShardedGraphReaderSource(sp.GetRequiredService<IGrainFactory>()));

        // The per-silo Watch notifier. One hub per silo container: GrainBackedDatastore pulses it on
        // local commits and parks Watch streams on it; it lazily starts its observer subscription +
        // heartbeat on first use (LogWatchHub.EnsureStarted) and the CONTAINER owns its lifetime — it is
        // an IAsyncDisposable singleton created by this factory, so silo teardown disposes it (a bounded,
        // timeout-guarded unsubscribe from the datastore grain's observer set). Also wired here as the
        // cross-silo SCHEMA propagation channel: applySchema swaps this silo's ISchemaProvider whenever
        // the hub receives a pushed or heartbeat-repaired schema change (see LogWatchHub's class doc) — a
        // WriteSchema on any silo would otherwise leave every OTHER silo's live schema stale forever.
        services.AddSingleton<LogWatchHub>(sp =>
        {
            var grainFactory = sp.GetRequiredService<IGrainFactory>();
            var provider = sp.GetRequiredService<ISchemaProvider>();
            var logger = sp.GetService<ILogger<LogWatchHub>>();
            return new LogWatchHub(
                grainFactory.GetGrain<IDatastoreGrain>(IDatastoreGrain.Key),
                grainFactory,
                applySchema: text => provider.Update(text),
                logger: logger);
        });

        // Historically LogWatchHub started lazily on this silo's first Watch call (EnsureStarted, called
        // from GrainBackedDatastore.Watch) — fine when the hub only mattered to Watch streams, since a
        // silo nobody is watching has no consumer to wake. Now that the SAME hub is also the cross-silo
        // SCHEMA propagation channel, that laziness is wrong: a silo that never opens a Watch stream must
        // still receive schema pushes/heartbeat repairs so its ISchemaProvider does not diverge forever.
        // A tiny hosted service starts the hub unconditionally at silo boot instead.
        services.AddHostedService<LogWatchHubStarter>();

        // The two storage-direct seams of the graph-sharded design (docs/graph-sharded-datastore.md
        // section 3), both resolved against the cluster-singleton sequencer grain via IGrainFactory:
        // schema-at-revision (consulted only on a SchemaResolver hash miss — once per hash per silo) and
        // the broad/admin scan seam (one snapshot fetch per scan; deliberately OFF the per-Check hot
        // path). Neither needs the per-silo projection.
        services.AddSingleton<ISchemaSource>(sp => new GrainSchemaSource(sp.GetRequiredService<IGrainFactory>()));
        services.AddSingleton<ISnapshotScanner>(sp => new GrainSnapshotScanner(sp.GetRequiredService<IGrainFactory>()));

        // The graph co-placement director (docs/graph-sharded-datastore.md section 5). The strategy
        // attribute is UNCONDITIONAL on the four graph grain classes (Orleans placement attributes attach
        // per class and cannot be conditional), so the director must be registered wherever those grains
        // can activate — i.e. wherever this mesh is wired — and the on/off decision lives in
        // GraphPlacementOptions (default ON — the enablement was gated on measurement and the
        // real-network rig's A/B decided it, docs/scalability-program.md section 3.5; when opted out
        // the director mirrors random placement, a pure inert pass-through). Opting out is a
        // deployment override via SiloBuilderExtensions.AddGraphLocalityPlacement (or an options
        // override).
        services.AddPlacementDirector<GraphLocalityPlacement, GraphLocalityPlacementDirector>();
        // TryAdd, not Add: a deployment override (SiloBuilderExtensions.AddGraphLocalityPlacement) may
        // legitimately run BEFORE this method (host builders configure the silo before the service
        // mesh), and an unconditional Add here would supersede that override under DI last-wins,
        // silently reverting co-location to the default with no error.
        services.TryAddSingleton<GraphPlacementOptions>();

        // Leopard membership-walk accelerator toggle (default ON; opt out via a registered options override).
        // The accelerator itself has no per-silo singleton to register: it is the addressable
        // IMembershipWalkGrain mesh (see MembershipWalkGrain), resolved on demand via IGrainFactory exactly
        // like every other grain, and its idle-collection age is wired by AddActivationMemoCollectionAge.
        services.AddSingleton<MembershipWalkOptions>();

        // CheckGrain's per-activation reply memo (default ON; opt out via a registered options override).
        // Also drives the grain's idle-collection age — see SiloBuilderExtensions.AddActivationMemoCollectionAge.
        services.AddSingleton<ActivationMemoOptions>();

        // SubjectFrontierGrain's per-activation LookupSubjects frontier memo (default ON; opt out via a
        // registered options override). Also drives the grain's idle-collection age — see
        // SiloBuilderExtensions.AddActivationMemoCollectionAge.
        services.AddSingleton<SubjectFrontierMemoOptions>();

        // The Orleans dispatcher turns each sub-problem into a grain call. This single instance is the
        // silo-wide root: the API enters through it AND each grain routes its child sub-problems back
        // through it (so ALL recursion crosses grain boundaries — there is no in-process local-recurse
        // shortcut; Orleans' own grain directory is the only router). It reads the live schema hash per
        // request through the provider's ISchemaHashSource. The one cache in the mesh is each CheckGrain
        // activation's own reply memo (see CheckGrain remarks) — there is no caller-side branch cache.
        // Both CheckGrain (a child's onward dispatcher) and PermissionChecker (the API entry) depend on
        // this SAME IDispatcher singleton directly — there is no DI cycle to break (that only existed
        // when the dispatcher's own local-recurse path needed to route back through itself) and so no
        // holder/late-bind indirection is needed.
        services.AddSingleton<IDispatcher>(sp => new OrleansDispatcher(
            sp.GetRequiredService<IGrainFactory>(),
            sp.GetRequiredService<ISchemaHashSource>(),
            sp.GetRequiredService<IDispatchMetrics>()));

        // Top-level entry used by the API: pins the optimized revision, dispatches through the root,
        // collapses with request context against the CURRENT schema's caveats (read per call).
        services.AddSingleton<IPermissionChecker>(sp => new PermissionChecker(
            sp.GetRequiredService<IDatastore>(),
            sp.GetRequiredService<ISchemaSource>(),
            sp.GetRequiredService<IDispatcher>(),
            sp.GetRequiredService<ISchemaProvider>(),
            sp.GetRequiredService<SchemaResolver>(),
            maxDepth,
            batchConcurrency));

        // The reverse-ops (LookupSubjects/LookupResources/ExpandPermissionTree) and relationship-read
        // (ReadRelationships/BulkExportRelationships) in-process helpers (the same pattern
        // AuthzedWatchV1Service uses for Watch): engine reads flow through IGraphReaderSource, broad scans
        // through ISnapshotScanner and schema resolution through ISchemaSource, dispatching onward to
        // SubjectFrontierGrain/MembershipWalkGrain/the check mesh exactly as the retired stream grains
        // did; IDatastore remains only for revision resolution and token minting. It is host-owned and not
        // registered here (see the remarks above), but that is fine — these are lazy factory registrations
        // resolved after the host completes its own DI setup.
        services.AddSingleton<ReverseOps>(sp => new ReverseOps(
            sp.GetRequiredService<IDatastore>(),
            sp.GetRequiredService<ISchemaSource>(),
            sp.GetRequiredService<ISchemaProvider>(),
            sp.GetRequiredService<SchemaResolver>(),
            sp.GetRequiredService<IGrainFactory>(),
            sp.GetRequiredService<MembershipWalkOptions>(),
            sp.GetRequiredService<IGraphReaderSource>(),
            sp.GetRequiredService<SubjectFrontierMemoOptions>()));

        services.AddSingleton<RelationshipReads>(sp => new RelationshipReads(
            sp.GetRequiredService<IDatastore>(),
            sp.GetRequiredService<ISchemaProvider>(),
            sp.GetRequiredService<ISnapshotScanner>()));

        return services;
    }

    /// <summary>
    /// Starts this silo's <see cref="LogWatchHub"/> at boot (see the registration comment above): resolves
    /// the DI singleton and calls <see cref="LogWatchHub.EnsureStarted"/>, which is idempotent, so this
    /// races harmlessly with any later lazy <c>GrainBackedDatastore.Watch</c> call that also starts it.
    /// </summary>
    private sealed class LogWatchHubStarter(LogWatchHub hub) : IHostedService
    {
        public Task StartAsync(CancellationToken cancellationToken)
        {
            hub.EnsureStarted();
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
