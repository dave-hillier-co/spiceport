using Orleans.Concurrency;

namespace Spiceport.Grains.Abstractions;

/// <summary>
/// A grain observer notified when the datastore head advances (a commit appended to the log). This is the
/// PUSH side of the Watch feed: each silo's <c>LogWatchHub</c> registers one observer so cross-silo commits
/// wake parked Watch streams without polling. Delivery is best-effort (observers are non-durable client
/// references and the set empties when the grain reactivates), so the hub keeps a slow heartbeat that
/// resubscribes and pulls the head — a missed push costs at most one heartbeat of latency, never a lost event
/// (streams always read their own diffs from the log).
/// </summary>
public interface IDatastoreWatcher : IGrainObserver
{
    /// <summary>The head advanced to <paramref name="head"/>. One-way: the notify never blocks the commit.</summary>
    [OneWay]
    Task HeadAdvanced(long head);

    /// <summary>
    /// A schema change committed: <paramref name="schemaBytes"/> is the persisted UTF-8 schema DSL and
    /// <paramref name="storedHash"/> is its stored-bytes hash (<c>StoredSchemaHash.Compute</c> — the same
    /// hash <c>DatastoreHeadWire.SchemaHash</c> carries). Pushed alongside (not instead of) the matching
    /// <see cref="HeadAdvanced"/> notify, so a watcher that only cares about the head is unaffected.
    /// Schema changes are rare, so pushing the full payload (rather than just the hash, forcing a fetch
    /// hop on every recipient) is cheap; one-way and best-effort like <see cref="HeadAdvanced"/> — a
    /// missed push is repaired by the heartbeat backstop (<c>LogWatchHub</c>), which diffs
    /// <see cref="DatastoreHeadWire.SchemaHash"/> against the last hash it applied and fetches on a
    /// mismatch.
    /// </summary>
    [OneWay]
    Task SchemaAdvanced(byte[] schemaBytes, string storedHash);
}
