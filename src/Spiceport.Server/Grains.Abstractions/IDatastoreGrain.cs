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

    /// <summary>Returns the full materialized committed state (the write base / snapshot source).</summary>
    Task<DatastoreGrainState> ReadState();

    /// <summary>Returns the head revision and schema hash without shipping the whole state blob.</summary>
    Task<DatastoreHeadWire> GetHead();

    /// <summary>
    /// Appends a commit to the event log. The grain is the single serialization point: if its current head
    /// still equals <paramref name="expectedHead"/> it mints the new (timestamp) revision, builds the
    /// canonical <see cref="LogEvent"/> stamping that revision, raises + confirms it (an append to durable
    /// grain storage) and returns the new revision; if the head moved it returns null and the caller must
    /// reload and re-run its precondition-bearing lambda. The non-reentrant single-activation turn makes the
    /// head-compare and the append atomic with respect to all other writes.
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
}
