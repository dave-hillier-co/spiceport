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
    /// compatibility ReadWriteTx write base, the equivalence gates): under the thin-sequencer layout the
    /// grain no longer materializes the whole fold, so this call rebuilds it from the small state plus
    /// every indexed forward key's current shard state (dirty overlay winning per key). Semantics are
    /// identical to the retired materialized fold; the cost is O(graph) storage reads and transfer PER
    /// CALL — never use it on the per-Check hot path (that reads through <see cref="ReadShard"/> / the
    /// shard mesh). Marked <see cref="AlwaysInterleaveAttribute"/> — see the remark on
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
    /// The once-per-shard hydration read: the current per-key slice for <paramref name="key"/>, served
    /// WITHOUT any whole-state scan — the sequencer's dirty-buffer entry when the key was touched since
    /// its last flush, otherwise the key's durable shard row relabeled to the current head (sound because
    /// any touching event would have dirtied the key — the sharding fact), otherwise empty-at-head. The
    /// reply's <see cref="GraphShardState.AppliedRevision"/> is the CURRENT head and
    /// <see cref="GraphShardState.GcFloor"/> the current floor (applied to the rows) — a shard hydrated
    /// from this snapshot is exactly the per-key fold at that head (the sharding lemma). Marked
    /// <see cref="AlwaysInterleaveAttribute"/> — a pure read, interleaving exactly like
    /// <see cref="ReadState"/>; never <c>[ReadOnly]</c>, which would not interleave past the
    /// non-read-only <see cref="Commit"/> (the lesson documented on <see cref="IDatastoreLog.ReadFrom"/>).
    /// </summary>
    [AlwaysInterleave]
    Task<GraphShardState> ReadShard(GraphShardKeyWire key);

    /// <summary>
    /// Executes a declarative <see cref="CommitRequest"/> INSIDE the sequencer (docs/graph-sharded-datastore.md
    /// section 3): the single-threaded, non-reentrant activation evaluates preconditions, applies the
    /// mutations through the MVCC transaction over its own fold at head, mints the authoritative
    /// (timestamp) revision and appends the resulting canonical <see cref="LogEvent"/> (an append to
    /// durable grain storage, confirmed before the reply) — so a declarative commit (null
    /// <see cref="CommitRequest.ExpectedHead"/>) needs no caller retry loop. WRITES carry no interleave
    /// attribute, so they never interleave EACH OTHER: the head read at the top of the call and the
    /// append at the bottom stay atomic with respect to all other writes, which is both why the head
    /// cannot move under a declarative commit and why the caller-evaluated CAS of the lambda
    /// compatibility path (the optional <see cref="CommitRequest.ExpectedHead"/> compare) is exact.
    /// Rejections are returned as STRUCTURED REPLY DATA (<see cref="CommitReply.Failure"/>), never as
    /// serialized exceptions, so the client rethrows its existing typed exceptions unchanged. Only the
    /// explicitly <see cref="AlwaysInterleaveAttribute"/>-marked pure reads (this interface's
    /// <see cref="ReadState"/>/<see cref="GetHead"/>/<see cref="ReadSchemaAt"/>/<see cref="ReadShard"/>, and
    /// <see cref="IDatastoreLog.ReadFrom"/>) may run DURING an await inside this call — the activation's
    /// scheduler is still single-threaded, so an interleaved read only ever runs while this call itself
    /// is parked at an await, never concurrently with its own execution.
    /// </summary>
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
    /// (the computed floor did not advance the current one).
    /// </summary>
    Task<long?> RunGc();
}
