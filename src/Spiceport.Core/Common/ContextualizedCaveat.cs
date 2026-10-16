namespace Spiceport.Core;

/// <summary>
/// A reference to a caveat together with the runtime context values supplied on a relationship.
/// </summary>
/// <param name="CaveatName">The name of the caveat definition.</param>
/// <param name="Context">
/// Optional JSON-serializable context map. Values are scalars (string, number, bool),
/// lists, or nested maps. Null or empty means no context.
/// </param>
public sealed record ContextualizedCaveat(string CaveatName, IReadOnlyDictionary<string, object?>? Context = null)
{
    /// <summary>
    /// Value equality, comparing <see cref="Context"/> by content (values deeply; null and empty alike
    /// mean no context) rather than by dictionary reference.
    /// </summary>
    public bool Equals(ContextualizedCaveat? other) =>
        other is not null
        && string.Equals(CaveatName, other.CaveatName, StringComparison.Ordinal)
        && StructuralEquality.ContextEqual(Context, other.Context);

    /// <inheritdoc/>
    public override int GetHashCode() =>
        HashCode.Combine(StringComparer.Ordinal.GetHashCode(CaveatName), StructuralEquality.ContextHash(Context));
}
