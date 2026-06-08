using System.Text;
using Spiceport.Core;

namespace Spiceport.Engine;

/// <summary>
/// An <see cref="IDispatcher"/> decorator that caches the pre-context branch of each sub-problem so
/// that repeated identical sub-problems are served without re-expanding the relation graph.
/// </summary>
/// <remarks>
/// <para>
/// What is cached: the pre-context <see cref="DispatchCheckResult"/> (membership + caveat expression).
/// The final collapsed verdict is NOT cached — caveat context is applied per-request outside the
/// dispatcher (in <c>CheckEngine.Collapse</c>), so two requests with the same relationships but
/// different contexts correctly share a cached branch yet still produce different verdicts.
/// </para>
/// <para>
/// Cache key: (resourceType, resourceId, relation, subjectType, subjectId, subjectRelation,
/// quantizedRevision, schemaHash). It deliberately EXCLUDES the visited-set, the remaining depth
/// budget and any caveat context, so structurally-identical sub-problems collide regardless of where
/// they appear in the recursion.
/// </para>
/// <para>
/// Cycle correctness: a result with <see cref="DispatchCheckResult.CycleCut"/> set is returned but
/// never stored, because a cycle-cut result depends on the in-flight visited-set (which is excluded
/// from the key) and would be unsound to reuse on a different path.
/// </para>
/// </remarks>
public sealed class CachingDispatcher : IDispatcher
{
    private readonly IDispatcher _inner;
    private readonly IDispatchCache _cache;
    private readonly IRevisionQuantizer _quantizer;
    private readonly ISchemaHashSource _schemaHash;
    private readonly ICacheMetrics? _metrics;

    /// <summary>Creates a caching dispatcher over a delegate dispatcher.</summary>
    /// <param name="inner">The dispatcher that performs the actual expansion on a cache miss.</param>
    /// <param name="cache">The branch cache.</param>
    /// <param name="quantizer">Maps a request revision to a stable cache bucket.</param>
    /// <param name="schemaHash">Supplies the live schema hash, scoping cache entries to the current schema so a schema swap is never reused.</param>
    /// <param name="metrics">Optional branch-cache hit/miss counters (for benchmarking).</param>
    public CachingDispatcher(
        IDispatcher inner,
        IDispatchCache cache,
        IRevisionQuantizer quantizer,
        ISchemaHashSource schemaHash,
        ICacheMetrics? metrics = null)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentNullException.ThrowIfNull(quantizer);
        ArgumentNullException.ThrowIfNull(schemaHash);
        _inner = inner;
        _cache = cache;
        _quantizer = quantizer;
        _schemaHash = schemaHash;
        _metrics = metrics;
    }

    /// <inheritdoc/>
    public async Task<DispatchCheckResult> DispatchCheck(DispatchCheckRequest request, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(request);

        var key = BuildKey(request);
        if (_cache.TryGet(key, out var cached))
        {
            // SpiceDB's caching guard (internal/dispatch/caching/caching.go): only serve a cached result
            // when the current request has at least as much depth budget as the cached result CONSUMED
            // (DepthRemaining >= DepthRequired). The same structural sub-problem can be entered at
            // different remaining depths on different parent paths; a result computed under a tighter
            // budget might have bottomed out on depth, so reusing it under a larger budget could serve a
            // wrong verdict. On an insufficient-depth miss, fall through and recompute under this budget.
            if (request.Meta.DepthRemaining >= cached.DepthRequired)
            {
                _metrics?.RecordCacheHit();
                return cached;
            }
        }

        _metrics?.RecordCacheMiss();
        var result = await _inner.DispatchCheck(request, ct).ConfigureAwait(false);

        // Never cache cycle-affected results: they depend on the in-flight visited-set, which is not
        // part of the key, so reusing them on another path would be unsound.
        if (!result.CycleCut)
            _cache.Set(key, result);

        return result;
    }

    private string BuildKey(DispatchCheckRequest request)
    {
        var r = request.Resource;
        var s = request.Subject;

        // CORRECTNESS SEAM (Zanzibar consistency): only the OPTIMIZED (head-pinned) revision may be
        // folded into the quantized 5s bucket. An EXACT revision (FullyConsistent, AtExactSnapshot, or
        // AtLeastAsFresh when the supplied token is fresher than the optimized bucket) must be keyed by
        // its exact revision string, so it can NEVER read an entry that was quantized down to an older
        // bucket boundary — which would serve stale data. Keying exactly still lets identical exact-
        // revision checks share work, at a lower (intended) hit rate.
        var rev = request.Meta.Mode == RevisionMode.Optimized
            ? _quantizer.Quantize(request.Meta.Revision)
            : "x:" + request.Meta.Revision.ToString();

        // The key excludes visited-set, depth budget and caveat context by construction.
        return new StringBuilder(128)
            .Append(_schemaHash.CurrentSchemaHash).Append('|')
            .Append(rev).Append('|')
            .Append(r.ObjectType).Append(':').Append(r.ObjectId).Append('#').Append(r.Relation)
            .Append('@')
            .Append(s.ObjectType).Append(':').Append(s.ObjectId).Append('#').Append(s.Relation)
            .ToString();
    }
}
