using Microsoft.Extensions.DependencyInjection;
using Spiceport.Engine;
using Spiceport.Schema;

namespace Spiceport.Grains;

/// <summary>
/// DI registration for the check grain's supporting services.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the process-wide schema and check-engine singletons that
    /// <see cref="CheckGrain"/> depends on.
    /// </summary>
    /// <remarks>
    /// Registered as singletons because the schema is fixed for this slice and
    /// <see cref="CheckEngine"/> is immutable and thread-safe (its only mutable state is a
    /// per-<c>Check</c> context). The <see cref="Spiceport.Datastore.IDatastore"/> singleton is
    /// owned by the host (it must persist writes), so it is intentionally not registered here.
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
    /// Registers the schema and check-engine singletons from an already-compiled schema.
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

        services.AddSingleton(schema);
        services.AddSingleton<ISchemaProvider>(new SchemaProvider(schema));
        services.AddSingleton(_ => new CheckEngine(schema.Namespaces, schema.Caveats, maxDepth));

        return services;
    }
}
