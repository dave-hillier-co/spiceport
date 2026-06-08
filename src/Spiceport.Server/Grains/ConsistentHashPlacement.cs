using Orleans.Placement;
using Orleans.Runtime;
using Orleans.Runtime.Placement;

namespace Spiceport.Grains;

/// <summary>
/// A marker <see cref="PlacementStrategy"/> that routes a grain to the silo chosen by the deterministic
/// consistent-hash <see cref="HashRing"/> over the current membership view. Carries no state — the
/// matching <see cref="ConsistentHashPlacementDirector"/> holds the placement logic.
/// </summary>
[Serializable]
[GenerateSerializer]
[Immutable]
[SuppressReferenceTracking]
public sealed class ConsistentHashPlacementStrategy : PlacementStrategy
{
    /// <summary>The shared singleton instance, mirroring how Orleans' built-in strategies are exposed.</summary>
    internal static readonly ConsistentHashPlacementStrategy Singleton = new();
}

/// <summary>
/// Applies <see cref="ConsistentHashPlacementStrategy"/> to a grain class so its activations are placed
/// by consistent hash of the grain key (the canonical sub-problem identity).
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
public sealed class ConsistentHashPlacementAttribute : PlacementAttribute
{
    /// <summary>Creates the attribute, binding the grain to the consistent-hash strategy.</summary>
    public ConsistentHashPlacementAttribute()
        : base(ConsistentHashPlacementStrategy.Singleton)
    {
    }
}

/// <summary>
/// The <see cref="IPlacementDirector"/> for <see cref="ConsistentHashPlacementStrategy"/>: it places a
/// grain on the silo that <see cref="HashRing.Owner"/> selects for the grain's string key over the
/// runtime's current compatible-silo set. Because <see cref="HashRing.Owner"/> is a pure function of
/// (key, silo set), a dispatcher reading the same membership view can predict this placement without
/// activating the grain (see <see cref="ISiloOwnership"/>).
/// </summary>
public sealed class ConsistentHashPlacementDirector : IPlacementDirector
{
    /// <inheritdoc />
    public Task<SiloAddress> OnAddActivation(
        PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        var silos = context.GetCompatibleSilos(target);
        if (silos.Length == 0)
            throw new OrleansException("No compatible silos available for consistent-hash placement.");

        var key = target.GrainIdentity.Key.ToString() ?? string.Empty;
        return Task.FromResult(HashRing.Owner(key, silos));
    }
}
