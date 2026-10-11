using Orleans.Concurrency;

namespace Spiceport.Grains.Abstractions;

/// <summary>
/// The cluster-singleton datastore grain: the single source of truth for all relationship/schema/counter
/// state. It is an EVENT-SOURCED grain — the append-only log of <see cref="LogEvent"/>s is the source of
/// truth, and the materialized <see cref="DatastoreGrainState"/> is the fold over that log. It is keyed by
/// the constant integer <see cref="Key"/> so every silo routes to the ONE activation (single-activation
/// virtual actor), which makes multi-silo reads correct with zero replica lag, and makes the revision the
/// grain mints the cluster-wide serialization point. Writes are cheap appends.
/// </summary>
public interface IDatastoreGrain : IGrainWithIntegerKey, IDatastoreLog
{
    /// <summary>The fixed key of the single datastore activation.</summary>
    public const long Key = 0;

    /// <summary>
    /// Returns the full committed state, ASSEMBLED on demand for the admin plane (the scan seam, the
    /// compatibility ReadWriteTx write base, the equivalence gates): the sequencer holds no relationship
    /// rows, so this call enumerates every forward key through the shard index (plus the pending tail's
    /// keys) and folds each key's pending events over its shard grain's projected state. The cost is
    /// O(graph) grain calls and transfer PER CALL — never use it on the per-Check hot path (that reads
    /// through the shard mesh). Marked <see cref="AlwaysInterleaveAttribute"/> — see the remark on
    /// <see cref="Commit"/> — never <c>[ReadOnly]</c>, which would not interleave against the
    /// non-read-only <see cref="Commit"/>.
    /// </summary>
    [AlwaysInterleave]
    Task<DatastoreGrainState> ReadState();

    /// <summary>
    /// Returns the head revision and schema hash without shipping the whole state blob. Marked
    /// <see cref="AlwaysInterleaveAttribute"/> — see the remark on <see cref="Commit"/>.
    /// </summary>
    [AlwaysInterleave]
    Task<DatastoreHeadWire> GetHead();

    /// <summary>
    /// Returns the schema bytes effective at the given revision — the last persisted version with
    /// <c>Revision &lt;= revision</c> (exactly the in-memory <c>DatastoreState.SchemaAt</c> fold), or
    /// null when no schema was persisted at or before it (the seed-only window). This is the
    /// schema-at-revision read of the graph-sharded design's <c>ISchemaSource</c> seam: resolvers hit it
    /// only on a per-silo schema-hash cache miss, so the hop is once per hash per silo. Marked
    /// <see cref="AlwaysInterleaveAttribute"/> — a pure read over the confirmed fold, interleaving
    /// exactly like <see cref="ReadState"/>; never <c>[ReadOnly]</c>, which would not interleave past
    /// the non-read-only <see cref="Commit"/>.
    /// </summary>
    [AlwaysInterleave]
    Task<byte[]?> ReadSchemaAt(long revision);

    /// <summary>
    /// The current per-key slice for <paramref name="key"/> at the current head: the key's shard grain's
    /// projected state with the pending events that touch it folded over, relabeled to the head
    /// (sound because any event touching the key is either projected or pending), with the current GC
    /// floor applied. An admin-plane and test read; a shard grain hydrating itself uses
    /// <see cref="ReadShardTail"/> instead, so it never waits on a call back into itself.
    /// </summary>
    [AlwaysInterleave]
    Task<GraphShardState> ReadShard(GraphShardKeyWire key);

    /// <summary>
    /// The pending events touching <paramref name="key"/>, with the head and GC floor: what a shard
    /// grain folds over its own projected state to hydrate. Read the tail BEFORE re-reading the
    /// projected state it is folded over: a projection that lands in between is then already in the
    /// projected state and skipped by its label, while one that has not landed is still in the tail.
    /// Marked <see cref="AlwaysInterleaveAttribute"/> — a pure read of the journaled state.
    /// </summary>
    [AlwaysInterleave]
    Task<ShardTailWire> ReadShardTail(GraphShardKeyWire key);

    /// <summary>
    /// Projects every pending event into the shard grains it touches and the shard index, then journals
    /// that the projection has caught up. The sequencer does this itself every flush interval; calling
    /// it directly is a test and operations seam. A write: it queues behind the writes before it and
    /// never interleaves another, so the call itself is <see cref="AlwaysInterleaveAttribute"/> and
    /// waits in the sequencer's write queue rather than the activation's.
    /// </summary>
    [AlwaysInterleave]
    Task FlushProjection();

    /// <summary>
    /// Executes a declarative <see cref="CommitRequest"/> INSIDE the sequencer (docs/graph-sharded-datastore.md
    /// section 3): the single-writer activation evaluates preconditions, applies the
    /// mutations through the MVCC transaction over its own fold at head, mints the authoritative
    /// (timestamp) revision and appends the resulting canonical <see cref="LogEvent"/> (an append to
    /// durable grain storage, confirmed before the reply) — so a declarative commit (null
    /// <see cref="CommitRequest.ExpectedHead"/>) needs no caller retry loop. Commits are GROUP-COMMITTED:
    /// the call is <see cref="AlwaysInterleaveAttribute"/> so a commit arriving while a batch is being
    /// written can queue for the next one, and each batch is evaluated in arrival order and appended in
    /// one write. Batches, GC rounds and projection flushes are work items in one write queue, drained
    /// in arrival order by a single writer, so writes never interleave EACH OTHER: each request is evaluated against the head left by every write before it,
    /// and its append lands before any later write evaluates, which is both why the head cannot move
    /// under a declarative commit and why the caller-evaluated CAS of the lambda compatibility path (the
    /// optional <see cref="CommitRequest.ExpectedHead"/> compare) is exact. The reply is sent only once
    /// the batch is durable. Rejections are returned as STRUCTURED REPLY DATA
    /// (<see cref="CommitReply.Failure"/>), never as serialized exceptions, so the client rethrows its
    /// existing typed exceptions unchanged. The pure reads (this interface's
    /// <see cref="ReadState"/>/<see cref="GetHead"/>/<see cref="ReadSchemaAt"/>/<see cref="ReadShard"/>, and
    /// <see cref="IDatastoreLog.ReadFrom"/>) read the confirmed state, so they never observe a batch
    /// before it is durable.
    /// </summary>
    [AlwaysInterleave]
    Task<CommitReply> Commit(CommitRequest request);

    /// <summary>
    /// Registers (or refreshes) a head-advance observer and returns the current head, so one call serves as
    /// the subscription heartbeat AND the fallback head read: a subscriber that missed a push still observes
    /// the head it missed, and a subscriber dropped by grain reactivation is re-registered. Registration
    /// expires if not refreshed (observers are best-effort, non-durable client references).
    /// </summary>
    Task<DatastoreHeadWire> SubscribeWatch(IDatastoreWatcher watcher);

    /// <summary>Removes a head-advance observer (best-effort; expiry would remove it anyway).</summary>
    Task UnsubscribeWatch(IDatastoreWatcher watcher);

    /// <summary>
    /// Runs one round of MVCC garbage collection: computes a floor (bounded by the configured GC window,
    /// never above the current head), and — if it advances the floor already recorded — appends a GC
    /// <see cref="LogEvent"/> that collects history below it. This is both the reminder's periodic body
    /// and a directly callable test seam. Returns the new floor, or null if no collection was needed
    /// (the computed floor did not advance the current one). Like <see cref="FlushProjection"/>, it
    /// queues as one write and is <see cref="AlwaysInterleaveAttribute"/>.
    /// </summary>
    [AlwaysInterleave]
    Task<long?> RunGc();
}
