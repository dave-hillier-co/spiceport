using System.Collections.Immutable;

namespace Spiceport.Core;

/// <summary>
/// A type reference for a caveat parameter, supporting nested generic types (e.g. list&lt;int&gt;, map&lt;string, T&gt;).
/// </summary>
/// <param name="TypeName">The base type name (e.g. "string", "int", "list", "map").</param>
/// <param name="ChildTypes">Type arguments for generic types, or null for scalars.</param>
public sealed record CaveatTypeReference(
    string TypeName,
    ImmutableList<CaveatTypeReference>? ChildTypes = null)
{
    /// <summary>Compares <see cref="ChildTypes"/> element-wise rather than by list reference.</summary>
    public bool Equals(CaveatTypeReference? other) =>
        other is not null
        && TypeName == other.TypeName
        && StructuralEquality.SequenceEqual(ChildTypes, other.ChildTypes);

    public override int GetHashCode() =>
        HashCode.Combine(TypeName, StructuralEquality.SequenceHash(ChildTypes));
}

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
    IReadOnlyDictionary<string, CaveatTypeReference> ParameterTypes)
{
    /// <summary>
    /// Compares <see cref="SerializedExpression"/> by bytes and <see cref="ParameterTypes"/> as an
    /// unordered map, rather than both by reference.
    /// </summary>
    public bool Equals(CaveatDefinition? other) =>
        other is not null
        && Name == other.Name
        && StructuralEquality.BytesEqual(SerializedExpression, other.SerializedExpression)
        && StructuralEquality.DictionaryEqual(ParameterTypes, other.ParameterTypes);

    public override int GetHashCode() =>
        HashCode.Combine(
            Name,
            StructuralEquality.BytesHash(SerializedExpression),
            StructuralEquality.DictionaryHash(ParameterTypes));
}
