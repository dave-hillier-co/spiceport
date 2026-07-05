using System.Threading;
using Spiceport.Engine;

namespace Spiceport.Grains;

/// <summary>
/// Silo-singleton dispatch counters, aggregated across every grain/dispatch on a silo: how often the
/// traversal-bloom loop bypass fired (<see cref="RecordLoopBypass"/>), plus the <see cref="CheckGrain"/>
/// activation memo hit/miss. Summing the snapshot from every silo's container yields cluster-wide totals.
/// </summary>
public interface IDispatchMetrics
{
    /// <summary>
    /// A sub-problem whose (resource, subject) key was already on the traversal bloom (a likely same-key
    /// loop): the grain call still happens as normal (the grain is reentrant), but the caller forces
    /// <see cref="Engine.DispatchCheckResult.CycleCut"/> on the returned result so it is never memoized.
    /// </summary>
    void RecordLoopBypass();

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
/// <param name="LoopBypass">Traversal-bloom loop-bypass hits (grain call still made, result force-cut).</param>
/// <param name="MemoHit">CheckGrain per-activation reply-memo hits.</param>
/// <param name="MemoMiss">CheckGrain per-activation reply-memo misses.</param>
public readonly record struct DispatchMetricsSnapshot(
    long LoopBypass,
    long MemoHit = 0,
    long MemoMiss = 0)
{
    /// <summary>Component-wise sum, for aggregating snapshots across silos.</summary>
    public static DispatchMetricsSnapshot operator +(DispatchMetricsSnapshot a, DispatchMetricsSnapshot b) =>
        new(a.LoopBypass + b.LoopBypass, a.MemoHit + b.MemoHit, a.MemoMiss + b.MemoMiss);
}

/// <summary>Thread-safe atomic-counter <see cref="IDispatchMetrics"/>.</summary>
public sealed class DispatchMetrics : IDispatchMetrics
{
    private long _loopBypass;
    private long _memoHit;
    private long _memoMiss;

    /// <inheritdoc />
    public void RecordLoopBypass() => Interlocked.Increment(ref _loopBypass);

    /// <inheritdoc />
    public void RecordMemoHit() => Interlocked.Increment(ref _memoHit);

    /// <inheritdoc />
    public void RecordMemoMiss() => Interlocked.Increment(ref _memoMiss);

    /// <inheritdoc />
    public DispatchMetricsSnapshot Snapshot() => new(
        Interlocked.Read(ref _loopBypass),
        Interlocked.Read(ref _memoHit),
        Interlocked.Read(ref _memoMiss));

    /// <inheritdoc />
    public void Reset()
    {
        Interlocked.Exchange(ref _loopBypass, 0);
        Interlocked.Exchange(ref _memoHit, 0);
        Interlocked.Exchange(ref _memoMiss, 0);
    }
}
