namespace Spiceport.Core;

/// <summary>
/// A reference to a caveat together with the runtime context values supplied on a relationship.
/// </summary>
/// <param name="CaveatName">The name of the caveat definition.</param>
/// <param name="Context">
/// Optional JSON-serializable context map. Values are scalars (string, number, bool),
/// lists, or nested maps. Null or empty means no context.
/// </param>
public sealed record ContextualizedCaveat(string CaveatName, IReadOnlyDictionary<string, object?>? Context = null);
