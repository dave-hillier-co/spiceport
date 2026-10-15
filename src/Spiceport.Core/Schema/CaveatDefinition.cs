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
    /// <summary>
    /// Value equality, comparing <see cref="ChildTypes"/> element-wise rather than by list reference. A
    /// null list (a scalar) and an empty list stay distinct.
    /// </summary>
    public bool Equals(CaveatTypeReference? other) =>
        other is not null
        && string.Equals(TypeName, other.TypeName, StringComparison.Ordinal)
        && (ChildTypes is null || other.ChildTypes is null
            ? ChildTypes is null && other.ChildTypes is null
            : ChildTypes.SequenceEqual(other.ChildTypes));

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(TypeName, StringComparer.Ordinal);
        hash.Add(ChildTypes?.Count ?? -1);
        if (ChildTypes is not null)
        {
            foreach (var child in ChildTypes)
                hash.Add(child);
        }
        return hash.ToHashCode();
    }
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
    /// Value equality, comparing <see cref="SerializedExpression"/> and <see cref="ParameterTypes"/> by
    /// content rather than by array and dictionary reference.
    /// </summary>
    public bool Equals(CaveatDefinition? other)
    {
        if (other is null)
            return false;
        if (ReferenceEquals(this, other))
            return true;
        if (!string.Equals(Name, other.Name, StringComparison.Ordinal)
            || !StructuralEquality.BytesEqual(SerializedExpression, other.SerializedExpression)
            || ParameterTypes.Count != other.ParameterTypes.Count)
            return false;
        foreach (var (name, type) in ParameterTypes)
        {
            if (!other.ParameterTypes.TryGetValue(name, out var otherType) || !Equals(type, otherType))
                return false;
        }
        return true;
    }

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        var parameters = 0;
        foreach (var (name, type) in ParameterTypes)
            parameters += HashCode.Combine(StringComparer.Ordinal.GetHashCode(name), type);
        return HashCode.Combine(
            StringComparer.Ordinal.GetHashCode(Name),
            StructuralEquality.BytesHash(SerializedExpression),
            ParameterTypes.Count,
            parameters);
    }
}
