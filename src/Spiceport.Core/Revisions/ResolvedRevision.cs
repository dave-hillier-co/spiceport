namespace Spiceport.Core;

/// <summary>
/// Why <see cref="ResolvedRevision.Revision"/> was chosen: whether it came from the optimized
/// (quantized, cacheable) bucket or had to be pinned to an exact caller-supplied snapshot. This is
/// resolution-time provenance only — once a revision string is chosen, evaluation downstream (the
/// grain key, the snapshot read) depends solely on that string, not on why it was picked; see the
/// remarks on <c>Spiceport.Grains.GrainKey</c> for why no mode segment travels past resolution.
/// </summary>
public enum RevisionMode
{
    /// <summary>Sampled from the optimized (quantized) bucket for best latency/cache-hit rate.</summary>
    Optimized = 0,

    /// <summary>Pinned to an exact revision string derived from a caller-supplied token or head.</summary>
    Exact = 1,
}

/// <summary>The outcome of resolving a <see cref="ConsistencyRequirement"/> against a datastore.</summary>
/// <param name="Revision">The revision that will actually be evaluated.</param>
/// <param name="SchemaHash">The schema hash at that revision, if any.</param>
/// <param name="Mode">Why <paramref name="Revision"/> was chosen (see <see cref="RevisionMode"/>).</param>
public sealed record ResolvedRevision(IRevision Revision, string? SchemaHash, RevisionMode Mode);
