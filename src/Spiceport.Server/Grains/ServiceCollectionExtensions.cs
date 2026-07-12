using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Spiceport.Datastore;
using Spiceport.Engine;
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
        // which only reflects a WriteSchema that landed on this silo.
        services.AddSingleton<SchemaResolver>();

        // The silo-wide loop-bypass / activation-memo / dispatch counters.
        services.AddSingleton<IDispatchMetrics, DispatchMetrics>();

        // The check-dispatch cross-cutting grain-call filters, registered as plain DI singletons: Orleans
        // resolves every IIncomingGrainCallFilter/IOutgoingGrainCallFilter from the SAME container the
        // silo runtime uses, so this one registration covers both co-hosted hosts (Api, Silo) and any
        // TestingHost cluster built by calling this same extension — there is nothing host-specific to
        // wire up separately. Both filters match ONLY ICheckGrain.DispatchCheck (see CheckDispatchFilter);
        // every other grain call in the mesh passes through untouched.
        services.AddSingleton<IOutgoingGrainCallFilter, CheckDispatchOutgoingCallFilter>();
        services.AddSingleton<IIncomingGrainCallFilter, CheckDispatchIncomingCallFilter>();

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
            sp.GetRequiredService<IDispatcher>(),
            sp.GetRequiredService<ISchemaProvider>(),
            sp.GetRequiredService<SchemaResolver>(),
            maxDepth,
            batchConcurrency));

        // The reverse-ops (LookupSubjects/LookupResources/ExpandPermissionTree) and relationship-read
        // (ReadRelationships/BulkExportRelationships) in-process helpers: they read straight off the local
        // silo's IDatastore projection (the same pattern AuthzedWatchV1Service uses for Watch), dispatching
        // onward to SubjectFrontierGrain/MembershipWalkGrain/the check mesh exactly as the retired stream
        // grains did. IDatastore is host-owned and not registered here (see the remarks above), but that is
        // fine — these are lazy factory registrations resolved after the host completes its own DI setup.
        services.AddSingleton<ReverseOps>(sp => new ReverseOps(
            sp.GetRequiredService<IDatastore>(),
            sp.GetRequiredService<ISchemaProvider>(),
            sp.GetRequiredService<SchemaResolver>(),
            sp.GetRequiredService<IGrainFactory>(),
            sp.GetRequiredService<MembershipWalkOptions>(),
            sp.GetRequiredService<SubjectFrontierMemoOptions>()));

        services.AddSingleton<RelationshipReads>(sp => new RelationshipReads(
            sp.GetRequiredService<IDatastore>(),
            sp.GetRequiredService<ISchemaProvider>()));

        return services;
    }
}
