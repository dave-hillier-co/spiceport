using Spiceport.Core;
using Spiceport.Datastore;

namespace Spiceport.Grains;

/// <summary>
/// The seam that hands the evaluation engines their revision-pinned <see cref="IGraphReader"/> — the
/// ONLY reader choice the <see cref="GraphReaderOptions.UseShardedReader"/> flag switches. Schema
/// resolution, token minting and every other snapshot-wide read deliberately stay on the
/// <c>IDatastoreReader</c>/projection path regardless of the flag: only the graph-shaped reads the
/// engines perform move to the shard mesh (<c>docs/graph-sharded-datastore.md</c>, migration step 3).
/// </summary>
public interface IGraphReaderSource
{
    /// <summary>Returns a graph reader pinned at the given revision.</summary>
    IGraphReader GraphReaderAt(IRevision revision);
}

/// <summary>
/// The projection-backed source (the default): the pinned snapshot reader over the per-silo
/// projection, exactly the reader the engines consumed before the seam existed — flag OFF is
/// behavior-identical by construction.
/// </summary>
internal sealed class ProjectionGraphReaderSource : IGraphReaderSource
{
    private readonly IDatastore _datastore;

    public ProjectionGraphReaderSource(IDatastore datastore)
    {
        ArgumentNullException.ThrowIfNull(datastore);
        _datastore = datastore;
    }

    /// <inheritdoc />
    public IGraphReader GraphReaderAt(IRevision revision) => _datastore.SnapshotReader(revision);
}

/// <summary>
/// The shard-mesh source (the step-3 shadow path): a <see cref="ShardedGraphReader"/> resolving each
/// pinned read to the matching <c>IGraphShardGrain</c>.
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
