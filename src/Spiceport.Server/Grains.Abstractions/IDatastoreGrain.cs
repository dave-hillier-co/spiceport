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
    /// Returns the full materialized committed state (the write base / snapshot source). Marked
    /// <see cref="AlwaysInterleaveAttribute"/> — see the remark on <see cref="AppendCommit"/> — never
    /// <c>[ReadOnly]</c>, which would not interleave against the non-read-only <see cref="AppendCommit"/>.
    /// </summary>
    [AlwaysInterleave]
    Task<DatastoreGrainState> ReadState();

    /// <summary>
    /// Returns the head revision and schema hash without shipping the whole state blob. Marked
    /// <see cref="AlwaysInterleaveAttribute"/> — see the remark on <see cref="AppendCommit"/>.
    /// </summary>
    [AlwaysInterleave]
    Task<DatastoreHeadWire> GetHead();

    /// <summary>
    /// The once-per-shard hydration read: the per-key restriction of <see cref="ReadState"/>. Returns
    /// only the relationship rows whose forward/reverse slice matches <paramref name="key"/>
    /// (<see cref="GraphShardKeyWire.Matches"/>), with <see cref="GraphShardState.AppliedRevision"/> set
    /// to the state's head and <see cref="GraphShardState.GcFloor"/> carried over — a shard hydrated from
    /// this snapshot is exactly the per-key fold at that head (the sharding lemma). Marked
    /// <see cref="AlwaysInterleaveAttribute"/> — a pure read, interleaving exactly like
    /// <see cref="ReadState"/>; never <c>[ReadOnly]</c>, which would not interleave past the
    /// non-read-only <see cref="AppendCommit"/> (the lesson documented on <see cref="IDatastoreLog.ReadFrom"/>).
    /// </summary>
    [AlwaysInterleave]
    Task<GraphShardState> ReadShard(GraphShardKeyWire key);

    /// <summary>
    /// Appends a commit to the event log. The grain is the single serialization point: if its current head
    /// still equals <paramref name="expectedHead"/> it mints the new (timestamp) revision, builds the
    /// canonical <see cref="LogEvent"/> stamping that revision, raises + confirms it (an append to durable
    /// grain storage) and returns the new revision; if the head moved it returns null and the caller must
    /// reload and re-run its precondition-bearing lambda. WRITES carry no interleave attribute, so they
    /// never interleave EACH OTHER — the head-compare and the append stay atomic with respect to all other
    /// writes. Only the explicitly <see cref="AlwaysInterleaveAttribute"/>-marked pure reads (this
    /// interface's <see cref="ReadState"/>/<see cref="GetHead"/>, and <see cref="IDatastoreLog.ReadFrom"/>)
    /// may run DURING an await inside this call — the activation's scheduler is still single-threaded, so
    /// an interleaved read only ever runs while this call itself is parked at an await, never concurrently
    /// with its own execution.
    /// </summary>
    Task<long?> AppendCommit(long expectedHead, ProposedWrite write);

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
