using Spiceport.Schema;

namespace Spiceport.Grains;

/// <summary>
/// Supplies the process-wide compiled schema that the check engine evaluates against.
/// </summary>
/// <remarks>
/// For this slice the schema is fixed at startup, so a singleton holding a single
/// <see cref="CompiledSchema"/> is sufficient. When the schema becomes dynamic /
/// versioned, this becomes the place to resolve a schema (and its derived
/// <see cref="Spiceport.Engine.CheckEngine"/>) per revision.
/// </remarks>
public interface ISchemaProvider
{
    /// <summary>The compiled schema (namespace + caveat definitions).</summary>
    CompiledSchema Schema { get; }
}

/// <summary>
/// Default <see cref="ISchemaProvider"/> backed by a single, fixed compiled schema.
/// </summary>
public sealed class SchemaProvider(CompiledSchema schema) : ISchemaProvider
{
    /// <inheritdoc />
    public CompiledSchema Schema { get; } = schema;
}
