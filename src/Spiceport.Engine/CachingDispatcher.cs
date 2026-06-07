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

    /// <summary>Creates a caching dispatcher over a delegate dispatcher.</summary>
    /// <param name="inner">The dispatcher that performs the actual expansion on a cache miss.</param>
    /// <param name="cache">The branch cache.</param>
    /// <param name="quantizer">Maps a request revision to a stable cache bucket.</param>
    /// <param name="schemaHash">Supplies the live schema hash, scoping cache entries to the current schema so a schema swap is never reused.</param>
    public CachingDispatcher(
        IDispatcher inner,
        IDispatchCache cache,
        IRevisionQuantizer quantizer,
        ISchemaHashSource schemaHash)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentNullException.ThrowIfNull(quantizer);
        ArgumentNullException.ThrowIfNull(schemaHash);
        _inner = inner;
        _cache = cache;
        _quantizer = quantizer;
        _schemaHash = schemaHash;
    }

    /// <inheritdoc/>
    public async Task<DispatchCheckResult> DispatchCheck(DispatchCheckRequest request, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(request);

        var key = BuildKey(request);
        if (_cache.TryGet(key, out var cached))
            return cached;

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
        var rev = _quantizer.Quantize(request.Meta.Revision);

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
