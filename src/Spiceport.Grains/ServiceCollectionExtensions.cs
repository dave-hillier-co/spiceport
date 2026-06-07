using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;
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
    /// Registers the dynamic schema provider, revision quantizer, dispatch cache and the silo-wide
    /// dispatch mesh (Caching over Orleans) that the check grain and the API entry point depend on.
    /// </summary>
    /// <remarks>
    /// The <see cref="IDatastore"/> singleton is owned by the host (it must persist writes), so it is
    /// intentionally NOT registered here — it is only consumed. <see cref="IGrainFactory"/> is provided
    /// by Orleans. The schema is held by a <see cref="MutableSchemaProvider"/> seeded from
    /// <paramref name="schemaText"/>; the dispatch mesh reads the provider's CURRENT schema hash per
    /// request (via <see cref="ISchemaHashSource"/>), so a runtime schema swap is reflected in every new
    /// cache and grain key and pre-change cache entries are never reused.
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

        // Caching key inputs shared across the mesh.
        services.AddSingleton<IRevisionQuantizer>(_ => new TimestampRevisionQuantizer());
        services.AddSingleton<IDispatchCache>(_ => new InMemoryDispatchCache());

        // Consistent-hash ownership oracle: lets the dispatcher predict, from the same membership view
        // and hash ring as the placement director, which silo a sub-problem's grain would activate on.
        services.AddSingleton<ISiloOwnership>(sp => new SiloOwnership(
            sp.GetRequiredService<ILocalSiloDetails>(),
            sp.GetRequiredService<ISiloStatusOracle>()));

        // Hybrid toggle (local-recurse vs always-grain-hop) and the silo-wide hop counters.
        services.AddSingleton<OrleansDispatcherOptions>();
        services.AddSingleton<IDispatchMetrics, DispatchMetrics>();

        // The Orleans dispatcher turns each sub-problem into a grain call; the caching dispatcher
        // wraps it so the pre-context branch cache is shared across the whole mesh. This single
        // Caching(Orleans) instance is the silo-wide root: the API enters through it AND each grain
        // routes its child sub-problems back through it (so recursion crosses grain boundaries). Both
        // read the live schema hash per request through the provider's ISchemaHashSource.
        //
        // The HYBRID local-recurse path inside the OrleansDispatcher must route its in-process children
        // back through this SAME root (so the cache is shared and non-local children still hop). That is
        // a cycle (root → orleans → root), resolved by building an unbound SiloDispatcher holder first,
        // handing it to the dispatcher as its onward path, then binding the fully-built root into it.
        services.AddSingleton<ISiloDispatcher>(sp =>
        {
            var grains = sp.GetRequiredService<IGrainFactory>();
            var cache = sp.GetRequiredService<IDispatchCache>();
            var quantizer = sp.GetRequiredService<IRevisionQuantizer>();
            var hashSource = sp.GetRequiredService<ISchemaHashSource>();
            var ownership = sp.GetRequiredService<ISiloOwnership>();
            var options = sp.GetRequiredService<OrleansDispatcherOptions>();
            var metrics = sp.GetRequiredService<IDispatchMetrics>();
            var schemaProvider = sp.GetRequiredService<ISchemaProvider>();
            var datastore = sp.GetRequiredService<IDatastore>();

            var holder = new SiloDispatcher();
            var localRecurse = new LocalRecurseContext(schemaProvider, datastore, holder);
            var orleans = new OrleansDispatcher(grains, hashSource, ownership, options, metrics, localRecurse);
            var root = new CachingDispatcher(orleans, cache, quantizer, hashSource, metrics);
            holder.Bind(root);
            return holder;
        });

        // Top-level entry used by the API: pins the optimized revision, dispatches through the root,
        // collapses with request context against the CURRENT schema's caveats (read per call).
        services.AddSingleton<IPermissionChecker>(sp => new PermissionChecker(
            sp.GetRequiredService<IDatastore>(),
            sp.GetRequiredService<ISiloDispatcher>(),
            sp.GetRequiredService<ISchemaProvider>(),
            maxDepth,
            batchConcurrency));

        return services;
    }
}
