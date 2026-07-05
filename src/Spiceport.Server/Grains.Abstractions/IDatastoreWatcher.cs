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
}
