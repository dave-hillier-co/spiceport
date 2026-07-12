namespace Spiceport.Grains;

/// <summary>
/// Common toggle + idle-collection-age shape shared by every per-activation grain memo
/// (<see cref="ActivationMemoOptions"/>, <see cref="SubjectFrontierMemoOptions"/>,
/// <see cref="MembershipWalkOptions"/>). The owning grain's key already embeds the
/// revision/quantization and schema hash, so the keyspace rotates on its own as revisions/schema
/// advance; idle activation collection at <see cref="CollectionAge"/> IS the memo's eviction policy —
/// no separate cache/TTL bookkeeping is needed. Wired via
/// <see cref="SiloBuilderExtensions.AddActivationMemoCollectionAge"/>.
/// </summary>
public abstract class MemoGrainOptions
{
    /// <summary>When false, the owning grain never consults or populates its memo.</summary>
    public bool Enabled { get; init; } = true;

    /// <summary>The owning grain activation's idle-collection age. Default 2 minutes.</summary>
    /// <remarks>
    /// Orleans' <c>GrainCollectionOptions.ClassSpecificCollectionAge</c> rejects an entry that does not
    /// exceed the silo's <c>CollectionQuantum</c> (default 1 minute); the wiring in
    /// <see cref="SiloBuilderExtensions.AddActivationMemoCollectionAge"/> clamps a smaller configured
    /// value up rather than failing configuration validation.
    /// </remarks>
    public TimeSpan CollectionAge { get; init; } = TimeSpan.FromMinutes(2);
}
