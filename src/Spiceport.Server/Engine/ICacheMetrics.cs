namespace Spiceport.Engine;

/// <summary>
/// The branch-cache hit/miss counters the <see cref="CachingDispatcher"/> reports to. Defined in the
/// engine so the caching dispatcher (which lives here, with no dependency on the Orleans layer) can be
/// instrumented; the Orleans-layer silo metrics implement this plus the hop counters.
/// </summary>
public interface ICacheMetrics
{
    /// <summary>A branch-cache hit (the sub-problem's pre-context branch was served from cache).</summary>
    void RecordCacheHit();

    /// <summary>A branch-cache miss (the sub-problem had to be expanded).</summary>
    void RecordCacheMiss();
}
