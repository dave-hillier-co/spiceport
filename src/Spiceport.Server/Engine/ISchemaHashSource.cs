namespace Spiceport.Engine;

/// <summary>
/// Supplies the schema hash that is current at dispatch time, so the dispatch mesh's cache and grain
/// keys are scoped to the live schema rather than a value frozen at DI time.
/// </summary>
/// <remarks>
/// This abstraction lives in <c>Spiceport.Engine</c> (which the caching dispatcher is part of) so the
/// engine can read the live hash without taking a dependency on <c>Spiceport.Grains</c>. The mutable
/// schema provider in the grains layer implements it. Reading the hash per request is what makes the
/// cache correct across a schema swap: every new key carries the new hash, so pre-change entries are
/// never matched again (they age out), and no stale-schema result is ever reused.
/// </remarks>
public interface ISchemaHashSource
{
    /// <summary>The schema hash that is current right now.</summary>
    string CurrentSchemaHash { get; }
}

/// <summary>
/// A trivial <see cref="ISchemaHashSource"/> over a fixed hash, for the in-process check engine which
/// is constructed against a single, immutable schema (its caching cache key never needs to change).
/// </summary>
/// <param name="CurrentSchemaHash">The fixed schema hash.</param>
public sealed record FixedSchemaHashSource(string CurrentSchemaHash) : ISchemaHashSource;
