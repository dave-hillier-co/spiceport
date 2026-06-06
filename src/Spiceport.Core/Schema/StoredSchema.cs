using System.Collections.Immutable;

namespace Spiceport.Core;

/// <summary>
/// A complete compiled schema snapshot.
/// </summary>
/// <param name="Version">Schema version number.</param>
/// <param name="SchemaText">The original schema DSL text.</param>
/// <param name="SchemaHash">A stable hash of the schema, used for consistency checks.</param>
/// <param name="Namespaces">All object-type definitions, keyed by name.</param>
/// <param name="Caveats">All caveat definitions, keyed by name.</param>
public sealed record StoredSchema(
    uint Version,
    string SchemaText,
    string SchemaHash,
    ImmutableDictionary<string, NamespaceDefinition> Namespaces,
    ImmutableDictionary<string, CaveatDefinition> Caveats);
