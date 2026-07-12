namespace Spiceport.Grains;

/// <summary>
/// Toggle and idle-collection tuning for <see cref="SubjectFrontierGrain"/>'s per-activation frontier
/// memo (the LookupSubjects analogue of stage (a) of "Activation-as-cache" — see
/// <c>docs/future-work.md</c> item 1.3). Default ON.
/// </summary>
/// <remarks>
/// When <see cref="MemoGrainOptions.Enabled"/> is false, <see cref="SubjectFrontierGrain"/> never
/// consults or populates its memo and <see cref="ReverseOps.StreamLookupSubjects"/> falls
/// back to its direct engine walk.
/// </remarks>
public sealed class SubjectFrontierMemoOptions : MemoGrainOptions
{
    /// <summary>
    /// The largest frontier size this activation will retain in its memo. A freshly computed frontier
    /// larger than this is still returned to the caller unconditionally — only the retention is capped,
    /// so an oversized frontier is served but not cached, bounding per-activation memory.
    /// </summary>
    public int MaxMemoSubjects { get; init; } = 4096;
}
