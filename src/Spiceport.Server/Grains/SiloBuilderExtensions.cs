using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
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

    /// <summary>
    /// Applies <see cref="ActivationMemoOptions.CollectionAge"/> as <see cref="CheckGrain"/>'s
    /// class-specific idle-collection age (<see cref="GrainCollectionOptions.ClassSpecificCollectionAge"/>),
    /// so a warm activation — and hence its per-activation reply memo (stage (a) of
    /// "Activation-as-cache") — survives at least that long between calls.
    /// </summary>
    /// <remarks>
    /// Reads <see cref="ActivationMemoOptions"/> from DI lazily, via the Options pattern's
    /// dependent-<c>Configure</c> overload, so it does not matter whether this is called before or after
    /// <see cref="ServiceCollectionExtensions.AddSpiceportGrainServices"/> registers (or a caller
    /// overrides) <see cref="ActivationMemoOptions"/> — the value is resolved when Orleans actually
    /// builds <see cref="GrainCollectionOptions"/>, not at wiring time.
    /// </remarks>
    public static ISiloBuilder AddActivationMemoCollectionAge(this ISiloBuilder siloBuilder)
    {
        ArgumentNullException.ThrowIfNull(siloBuilder);
        siloBuilder.Services.AddOptions<GrainCollectionOptions>()
            .Configure<ActivationMemoOptions>((options, memo) =>
            {
                if (!memo.Enabled)
                    return;

                // GrainCollectionOptions rejects a ClassSpecificCollectionAge entry that does not exceed
                // CollectionQuantum (default 1 minute) at configuration-validation time; clamp up instead
                // of letting the silo fail to start over a too-small configured value.
                var floor = options.CollectionQuantum + TimeSpan.FromSeconds(1);
                var age = memo.CollectionAge < floor ? floor : memo.CollectionAge;
                options.ClassSpecificCollectionAge[typeof(CheckGrain).FullName!] = age;
            });
        return siloBuilder;
    }
}
