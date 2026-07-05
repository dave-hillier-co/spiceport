using System.Threading;
using Spiceport.Engine;

namespace Spiceport.Grains;

/// <summary>
/// Silo-singleton dispatch counters, aggregated across every grain/dispatch on a silo. Lets a
/// benchmark observe the real hop behaviour of the hybrid: how many sub-problems became cross-silo
/// grain calls (<see cref="RemoteGrainHop"/>) vs were served in-process on their owner silo
/// (<see cref="LocalRecurse"/>), plus branch-cache hit/miss. Summing the snapshot from every silo's
/// container yields cluster-wide totals.
/// </summary>
public interface IDispatchMetrics : ICacheMetrics
{
    /// <summary>A sub-problem whose owner was the local silo, served in-process (no cross-silo message).</summary>
    void RecordLocalRecurse();

    /// <summary>A sub-problem whose owner was a remote silo, sent as a (cross-silo) grain call.</summary>
    void RecordRemoteGrainHop();

    /// <summary>
    /// A <see cref="CheckGrain"/> per-activation reply memo hit (stage (a) of "Activation-as-cache"):
    /// the sub-problem was served from a warm activation's memoized pre-context reply without
    /// re-expanding the relation graph.
    /// </summary>
    void RecordMemoHit();

    /// <summary>
    /// A <see cref="CheckGrain"/> per-activation reply memo miss: the activation had no usable memo
    /// (cold, or an insufficient depth budget) and recomputed the sub-problem.
    /// </summary>
    void RecordMemoMiss();

    /// <summary>An immutable point-in-time snapshot of the counters.</summary>
    DispatchMetricsSnapshot Snapshot();

    /// <summary>Resets all counters to zero (to bracket one benchmark workload).</summary>
    void Reset();
}

/// <summary>An immutable snapshot of <see cref="IDispatchMetrics"/> counters.</summary>
/// <param name="LocalRecurse">In-process locally-owned dispatches.</param>
/// <param name="RemoteGrainHop">Cross-silo grain-call dispatches.</param>
/// <param name="CacheHit">Branch-cache hits.</param>
/// <param name="CacheMiss">Branch-cache misses.</param>
/// <param name="MemoHit">CheckGrain per-activation reply-memo hits.</param>
/// <param name="MemoMiss">CheckGrain per-activation reply-memo misses.</param>
public readonly record struct DispatchMetricsSnapshot(
    long LocalRecurse,
    long RemoteGrainHop,
    long CacheHit,
    long CacheMiss,
    long MemoHit = 0,
    long MemoMiss = 0)
{
    /// <summary>Component-wise sum, for aggregating snapshots across silos.</summary>
    public static DispatchMetricsSnapshot operator +(DispatchMetricsSnapshot a, DispatchMetricsSnapshot b) =>
        new(a.LocalRecurse + b.LocalRecurse, a.RemoteGrainHop + b.RemoteGrainHop,
            a.CacheHit + b.CacheHit, a.CacheMiss + b.CacheMiss,
            a.MemoHit + b.MemoHit, a.MemoMiss + b.MemoMiss);
}

/// <summary>Thread-safe atomic-counter <see cref="IDispatchMetrics"/>.</summary>
public sealed class DispatchMetrics : IDispatchMetrics
{
    private long _localRecurse;
    private long _remoteGrainHop;
    private long _cacheHit;
    private long _cacheMiss;
    private long _memoHit;
    private long _memoMiss;

    /// <inheritdoc />
    public void RecordLocalRecurse() => Interlocked.Increment(ref _localRecurse);

    /// <inheritdoc />
    public void RecordRemoteGrainHop() => Interlocked.Increment(ref _remoteGrainHop);

    /// <inheritdoc />
    public void RecordCacheHit() => Interlocked.Increment(ref _cacheHit);

    /// <inheritdoc />
    public void RecordCacheMiss() => Interlocked.Increment(ref _cacheMiss);

    /// <inheritdoc />
    public void RecordMemoHit() => Interlocked.Increment(ref _memoHit);

    /// <inheritdoc />
    public void RecordMemoMiss() => Interlocked.Increment(ref _memoMiss);

    /// <inheritdoc />
    public DispatchMetricsSnapshot Snapshot() => new(
        Interlocked.Read(ref _localRecurse),
        Interlocked.Read(ref _remoteGrainHop),
        Interlocked.Read(ref _cacheHit),
        Interlocked.Read(ref _cacheMiss),
        Interlocked.Read(ref _memoHit),
        Interlocked.Read(ref _memoMiss));

    /// <inheritdoc />
    public void Reset()
    {
        Interlocked.Exchange(ref _localRecurse, 0);
        Interlocked.Exchange(ref _remoteGrainHop, 0);
        Interlocked.Exchange(ref _cacheHit, 0);
        Interlocked.Exchange(ref _cacheMiss, 0);
        Interlocked.Exchange(ref _memoHit, 0);
        Interlocked.Exchange(ref _memoMiss, 0);
    }
}
