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

    /// <summary>
    /// The sequencer's projection write: folds <paramref name="events"/> (each touching this shard's
    /// key, ascending by revision) over the shard's PERSISTED state, applies <paramref name="gcFloor"/>,
    /// labels the result <paramref name="through"/>, and persists it before returning. Idempotent: events
    /// at or below the persisted label were already projected and are skipped, so the sequencer may
    /// replay a projection it is unsure completed. Returns whether the key holds any rows afterwards.
    /// </summary>
    Task<bool> Project(ImmutableList<LogEvent> events, long through, long gcFloor);

    /// <summary>
    /// The shard's PERSISTED projected state: its rows as of its label, which is complete for every
    /// event at or below the label and missing only the sequencer's pending events for this key.
    /// Marked <see cref="AlwaysInterleaveAttribute"/>: a pure read of state loaded at activation.
    /// </summary>
    [AlwaysInterleave]
    Task<GraphShardState> ReadProjected();
}

/// <summary>
/// One bucket of the shard index: the object ids of one direction and type, hashed into a fixed number
/// of buckets, whose shard grains have ever been projected. Enumerating the bucket grains of a type
/// lists every key of that type that may hold rows (a superset: a key whose rows were all deleted
/// stays listed). It replaces enumerating a key index held by the sequencer, so no single grain's
/// state grows with the graph.
/// </summary>
public interface IShardIndexGrain : IGrainWithStringKey
{
    /// <summary>Adds <paramref name="ids"/> to the bucket and persists it before returning. Idempotent.</summary>
    Task Add(ImmutableList<string> ids);

    /// <summary>The ids in the bucket. A pure read of state loaded at activation.</summary>
    [AlwaysInterleave]
    Task<ImmutableList<string>> ReadIds();
}
