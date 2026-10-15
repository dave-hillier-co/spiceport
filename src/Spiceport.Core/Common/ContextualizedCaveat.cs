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
    /// Compares <see cref="Context"/> deeply (nested maps unordered, lists in order, scalars by
    /// value) rather than by dictionary reference. A null context and an empty one are equal, since
    /// both mean no context.
    /// </summary>
    public bool Equals(ContextualizedCaveat? other) =>
        other is not null
        && CaveatName == other.CaveatName
        && StructuralEquality.DictionaryEqual(NullIfEmpty(Context), NullIfEmpty(other.Context));

    public override int GetHashCode() =>
        HashCode.Combine(CaveatName, StructuralEquality.DictionaryHash(NullIfEmpty(Context)));

    private static IReadOnlyDictionary<string, object?>? NullIfEmpty(IReadOnlyDictionary<string, object?>? context) =>
        context is { Count: 0 } ? null : context;
}
