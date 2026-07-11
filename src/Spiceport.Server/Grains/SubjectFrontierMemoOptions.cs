namespace Spiceport.Grains;

/// <summary>
/// Toggle and idle-collection tuning for <see cref="SubjectFrontierGrain"/>'s per-activation frontier
/// memo (the LookupSubjects analogue of stage (a) of "Activation-as-cache" — see
/// <c>docs/future-work.md</c> item 1.3). Default ON.
/// </summary>
public sealed record SubjectFrontierMemoOptions
{
    /// <summary>
    /// When false, <see cref="SubjectFrontierGrain"/> never consults or populates its memo and
    /// <see cref="ReverseOpsStreamGrain.StreamLookupSubjects"/> falls back to its direct engine walk.
    /// </summary>
    public bool Enabled { get; init; } = true;

    /// <summary>
    /// The <see cref="SubjectFrontierGrain"/> activation's idle-collection age. As with
    /// <see cref="ActivationMemoOptions.CollectionAge"/>, the grain key already embeds the quantized
    /// revision and schema hash, so idle activation collection at this age IS the memo's eviction
    /// policy. Default 2 minutes.
    /// </summary>
    public TimeSpan CollectionAge { get; init; } = TimeSpan.FromMinutes(2);

    /// <summary>
    /// The largest frontier size this activation will retain in its memo. A freshly computed frontier
    /// larger than this is still returned to the caller unconditionally — only the retention is capped,
    /// so an oversized frontier is served but not cached, bounding per-activation memory.
    /// </summary>
    public int MaxMemoSubjects { get; init; } = 4096;
}
