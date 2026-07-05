using Microsoft.Extensions.DependencyInjection;
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

        // The silo-wide loop-bypass / activation-memo counters.
        services.AddSingleton<IDispatchMetrics, DispatchMetrics>();

        // Per-silo Leopard membership-index accelerator (default ON; opt out via a registered options override).
        services.AddSingleton<MembershipIndexOptions>();
        services.AddSingleton<MembershipIndexCache>();

        // CheckGrain's per-activation reply memo (default ON; opt out via a registered options override).
        // Also drives the grain's idle-collection age — see SiloBuilderExtensions.AddActivationMemoCollectionAge.
        services.AddSingleton<ActivationMemoOptions>();

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
            maxDepth,
            batchConcurrency));

        return services;
    }
}
