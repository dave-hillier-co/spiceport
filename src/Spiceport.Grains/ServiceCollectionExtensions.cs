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
    /// Registers the schema, schema hash, revision quantizer, check engine and the silo-wide dispatch
    /// mesh (Caching over Orleans) that the check grain and the API entry point depend on.
    /// </summary>
    /// <remarks>
    /// The <see cref="IDatastore"/> singleton is owned by the host (it must persist writes), so it is
    /// intentionally NOT registered here — it is only consumed. <see cref="IGrainFactory"/> is provided
    /// by Orleans. Everything else needed to compute a sub-problem (schema, hash, quantizer, engine,
    /// dispatchers) is registered here.
    /// </remarks>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="schemaText">The schema DSL text to compile once at startup.</param>
    /// <param name="maxDepth">The check engine's maximum recursion depth.</param>
    public static IServiceCollection AddSpiceportGrainServices(
        this IServiceCollection services,
        string schemaText,
        int maxDepth = CheckEngine.DefaultMaxDepth)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(schemaText);

        var compiled = SchemaCompiler.CompileSchema(schemaText);
        return services.AddSpiceportGrainServices(compiled, maxDepth);
    }

    /// <summary>
    /// Registers the schema, dispatch mesh and supporting services from an already-compiled schema.
    /// </summary>
    /// <param name="services">The service collection to add to.</param>
    /// <param name="schema">The compiled schema.</param>
    /// <param name="maxDepth">The check engine's maximum recursion depth.</param>
    public static IServiceCollection AddSpiceportGrainServices(
        this IServiceCollection services,
        CompiledSchema schema,
        int maxDepth = CheckEngine.DefaultMaxDepth)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(schema);

        var schemaHash = SchemaHash.Compute(schema.Namespaces, schema.Caveats);

        services.AddSingleton(schema);
        services.AddSingleton<ISchemaProvider>(new SchemaProvider(schema));

        // The engine is used only for its caveat Collapse step at the top of a check (its recursion
        // is replaced by the grain mesh); it stays the canonical in-process computation elsewhere.
        services.AddSingleton(_ => new CheckEngine(schema.Namespaces, schema.Caveats, maxDepth));

        // Caching key inputs shared across the mesh.
        services.AddSingleton<IRevisionQuantizer>(_ => new TimestampRevisionQuantizer());
        services.AddSingleton<IDispatchCache>(_ => new InMemoryDispatchCache());

        // The Orleans dispatcher turns each sub-problem into a grain call; the caching dispatcher
        // wraps it so the pre-context branch cache is shared across the whole mesh. This single
        // Caching(Orleans) instance is the silo-wide root: the API enters through it AND each grain
        // routes its child sub-problems back through it (so recursion crosses grain boundaries).
        services.AddSingleton<ISiloDispatcher>(sp =>
        {
            var grains = sp.GetRequiredService<IGrainFactory>();
            var cache = sp.GetRequiredService<IDispatchCache>();
            var quantizer = sp.GetRequiredService<IRevisionQuantizer>();
            var orleans = new OrleansDispatcher(grains, schemaHash);
            var root = new CachingDispatcher(orleans, cache, quantizer, schemaHash);
            return new SiloDispatcher(root);
        });

        // Top-level entry used by the API: pins the optimized revision, dispatches through the root,
        // collapses with request context.
        services.AddSingleton<IPermissionChecker>(sp => new PermissionChecker(
            sp.GetRequiredService<IDatastore>(),
            sp.GetRequiredService<ISiloDispatcher>(),
            sp.GetRequiredService<CheckEngine>(),
            maxDepth));

        return services;
    }
}
