using System.Collections.Immutable;

namespace Spiceport.Core;

/// <summary>
/// A type reference for a caveat parameter, supporting nested generic types (e.g. list&lt;int&gt;, map&lt;string, T&gt;).
/// </summary>
/// <param name="TypeName">The base type name (e.g. "string", "int", "list", "map").</param>
/// <param name="ChildTypes">Type arguments for generic types, or null for scalars.</param>
public sealed record CaveatTypeReference(
    string TypeName,
    ImmutableList<CaveatTypeReference>? ChildTypes = null);

/// <summary>
/// A schema definition for a caveat.
/// </summary>
/// <param name="Name">The caveat name (may contain "/" path segments).</param>
/// <param name="SerializedExpression">
/// The opaque compiled expression (e.g. serialized CEL). Stored as bytes; not interpreted by Core.
/// </param>
/// <param name="ParameterTypes">The caveat parameters keyed by name.</param>
public sealed record CaveatDefinition(
    string Name,
    byte[] SerializedExpression,
    IReadOnlyDictionary<string, CaveatTypeReference> ParameterTypes);
