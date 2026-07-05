namespace Spiceport.Grains;

/// <summary>
/// Toggle and idle-collection tuning for <see cref="CheckGrain"/>'s per-activation reply memo (stage
/// (a) of "Activation-as-cache" — see <c>docs/future-work.md</c> item 1.3). Default ON.
/// </summary>
public sealed class ActivationMemoOptions
{
    /// <summary>
    /// When false, <see cref="CheckGrain"/> never consults or populates its memo and behaves exactly
    /// as it did before this feature existed.
    /// </summary>
    public bool Enabled { get; init; } = true;

    /// <summary>
    /// The <see cref="CheckGrain"/> activation's idle-collection age. The grain key already embeds the
    /// quantized revision and schema hash, so the keyspace rotates on its own every quantization window;
    /// idle activation collection at this age IS the memo's eviction policy — no separate cache/TTL
    /// bookkeeping is needed. Default 2 minutes.
    /// </summary>
    /// <remarks>
    /// Orleans' <c>GrainCollectionOptions.ClassSpecificCollectionAge</c> rejects an entry that does not
    /// exceed the silo's <c>CollectionQuantum</c> (default 1 minute); the wiring in
    /// <see cref="SiloBuilderExtensions.AddActivationMemoCollectionAge"/> clamps a smaller configured
    /// value up rather than failing configuration validation.
    /// </remarks>
    public TimeSpan CollectionAge { get; init; } = TimeSpan.FromMinutes(2);
}
