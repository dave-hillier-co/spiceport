namespace Spiceport.Core;

/// <summary>
/// How the resolved revision relates to the optimized cache bucket.
/// Determines the cache-key seam: <see cref="Optimized"/> revisions fold into the quantized
/// bucket; <see cref="Exact"/> revisions must be keyed by their exact string to avoid stale reads.
/// </summary>
public enum RevisionMode
{
    /// <summary>Quantizable into the optimized 5s bucket (high cache hit rate).</summary>
    Optimized = 0,

    /// <summary>Must be keyed by exact revision string; never folded into the optimized bucket.</summary>
    Exact = 1,
}

/// <summary>The outcome of resolving a <see cref="ConsistencyRequirement"/> against a datastore.</summary>
/// <param name="Revision">The revision that will actually be evaluated.</param>
/// <param name="SchemaHash">The schema hash at that revision, if any.</param>
/// <param name="Mode">Whether the revision is cacheable in the optimized bucket or must be keyed exactly.</param>
public sealed record ResolvedRevision(IRevision Revision, string? SchemaHash, RevisionMode Mode);
