using System.Collections.Immutable;
using Orleans.Concurrency;

namespace Spiceport.Grains.Abstractions;

/// <summary>
/// The relationship rows of one graph shard visible at a requested revision. The reply is
/// <see cref="ImmutableAttribute"/> so a same-silo call hands out the snapshot by reference without
/// copying — the property the graph-sharded design relies on for hot shards
/// (<c>docs/graph-sharded-datastore.md</c> §4).
/// </summary>
[GenerateSerializer, Immutable]
public sealed record GraphShardRowsReply(
    [property: Id(0)] ImmutableList<RelationshipWire> Rows);

/// <summary>
/// A grain holding one adjacency slice of the graph — the versioned MVCC rows of a single
/// <see cref="GraphShardKeyWire"/>, folded from the datastore's event log restricted to that key. It is
/// keyed by the shard key's string form (see <c>GraphShardGrainKey</c>) and serves point-in-time reads
/// at any revision its GC window still covers.
/// </summary>
public interface IGraphShardGrain : IGrainWithStringKey
{
    /// <summary>
    /// Returns the shard's rows VISIBLE at <paramref name="revision"/> (half-open MVCC window
    /// <c>[created, deleted)</c>), catching the shard up on demand until its watermark covers the
    /// revision — the closed-timestamp gate, enforced per shard key.
    /// Visibility is filtered shard-side; EXPIRATION deliberately is NOT — it is a query-time concern of
    /// the caller (the evaluation "now", a caller-clock concern), mirroring <c>MvccSnapshotReader</c>, so
    /// the same shard reply serves callers with different clocks. Throws
    /// <c>RevisionNotFoundException</c> when the revision has fallen below the shard's GC floor. Marked
    /// <see cref="AlwaysInterleaveAttribute"/>: a pure read over the activation's folded state, so
    /// concurrent readers of a hot shard interleave instead of queueing (the
    /// <c>[ReadOnly]</c>-does-not-interleave lesson on <c>IDatastoreLog.ReadFrom</c> applies here too).
    /// <para>
    /// <paramref name="filter"/> is the subject-filter pushdown (scalability-program 3.2): null returns
    /// EVERY visible row; non-null returns only the visible rows matching the filter, applied
    /// server-side over the in-memory rows, so a point-membership read against a large userset returns
    /// O(matches) rows over the wire instead of O(userset). Filtering is a strict restriction of the
    /// same visible row set — callers keeping their own client-side <c>Matches</c> re-check see
    /// identical results with or without it.
    /// </para>
    /// </summary>
    [AlwaysInterleave]
    Task<GraphShardRowsReply> RowsAt(long revision, FullRelationshipsFilterWire? filter, CancellationToken cancellationToken);
}
