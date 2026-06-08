using Orleans.Hosting;

namespace Spiceport.Grains;

/// <summary>Silo-builder wiring for the Spiceport consistent-hash placement.</summary>
public static class SiloBuilderExtensions
{
    /// <summary>
    /// Registers the <see cref="ConsistentHashPlacementDirector"/> for
    /// <see cref="ConsistentHashPlacementStrategy"/> so that grains marked with
    /// <see cref="ConsistentHashPlacementAttribute"/> (e.g. <see cref="CheckGrain"/>) activate on the
    /// silo chosen by the deterministic <see cref="HashRing"/> over the current membership view.
    /// </summary>
    public static ISiloBuilder AddConsistentHashPlacement(this ISiloBuilder siloBuilder)
    {
        ArgumentNullException.ThrowIfNull(siloBuilder);
        return siloBuilder.AddPlacementDirector<ConsistentHashPlacementStrategy, ConsistentHashPlacementDirector>();
    }
}
