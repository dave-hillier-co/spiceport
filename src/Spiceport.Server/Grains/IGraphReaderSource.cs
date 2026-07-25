using Spiceport.Core;
using Spiceport.Datastore;

namespace Spiceport.Grains;

/// <summary>
/// The seam that hands the evaluation engines their revision-pinned <see cref="IGraphReader"/> — always
/// the <c>IGraphShardGrain</c> mesh (<see cref="ShardedGraphReader"/>); the retired per-silo whole-graph
/// projection alternative is gone. Schema resolution, token minting and every other snapshot-wide read
/// deliberately stay OFF this seam: only the graph-shaped reads the engines perform go to the shard mesh
/// (<c>docs/graph-sharded-datastore.md</c>). The fold-equivalence gates compare the shard mesh against a
/// sequencer-snapshot reader (<see cref="GrainBackedDatastore.SnapshotReader"/>, one full-state fetch per
/// reader) — see <c>ShardedReaderEquivalenceTests</c>.
/// </summary>
public interface IGraphReaderSource
{
    /// <summary>Returns a graph reader pinned at the given revision.</summary>
    IGraphReader GraphReaderAt(IRevision revision);
}

/// <summary>
/// The shard-mesh source: a <see cref="ShardedGraphReader"/> resolving each pinned read to the matching
/// <c>IGraphShardGrain</c>.
/// </summary>
internal sealed class ShardedGraphReaderSource : IGraphReaderSource
{
    private readonly IGrainFactory _grainFactory;

    public ShardedGraphReaderSource(IGrainFactory grainFactory)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        _grainFactory = grainFactory;
    }

    /// <inheritdoc />
    public IGraphReader GraphReaderAt(IRevision revision)
    {
        // Mirrors ReferenceDatastore.ToNanos: the timestamp form is the only revision identity the
        // datastore mints, so anything else is a caller bug, not a fallback case.
        var nanos = revision switch
        {
            TimestampRevision t => t.TimestampNanosSinceEpoch,
            _ => throw new InvalidRevisionException($"unsupported revision type: {revision.GetType().Name}"),
        };
        return new ShardedGraphReader(_grainFactory, nanos);
    }
}
