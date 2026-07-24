namespace Spiceport.Grains;

/// <summary>
/// Toggle for the graph-sharded read path (<c>docs/graph-sharded-datastore.md</c>, migration step 3).
/// Default OFF (opt-in).
/// </summary>
/// <remarks>
/// This is the step-3 SHADOW flag: the equivalence gates run the conformance corpus and mesh suites
/// both ways, and the default stays projection-backed (<see cref="UseShardedReader"/> false) until the
/// fold-equivalence gate — every read answered by both projection and shards must agree exactly — has
/// soaked. Flipping the default is migration step 5, not this flag's job.
/// </remarks>
public sealed record GraphReaderOptions
{
    /// <summary>
    /// When true, engine reads resolve through the <c>IGraphShardGrain</c> mesh
    /// (<see cref="ShardedGraphReader"/>); when false (the default), through the per-silo projection's
    /// snapshot reader.
    /// </summary>
    public bool UseShardedReader { get; init; }
}
