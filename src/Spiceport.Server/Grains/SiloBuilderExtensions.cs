using Microsoft.Extensions.DependencyInjection;
using Orleans.Configuration;
using Orleans.Hosting;

namespace Spiceport.Grains;

/// <summary>
/// Silo-builder wiring for the check-grain mesh. With sub-problem recursion crossing every grain
/// boundary (no in-process local-recurse shortcut) and the correctness of a check depending only on
/// grain identity, placement is never load-bearing: the grain directory's single-activation guarantee
/// is the whole router. The four graph grain families carry <see cref="GraphLocalityPlacementAttribute"/>,
/// whose director is by default an inert random pick — <see cref="AddGraphLocalityPlacement"/> is the
/// measured, deployment-level opt-in that turns it into a shard co-location hint
/// (<c>docs/graph-sharded-datastore.md</c> §5). Orleans' activation rebalancing remains a host-level
/// opt-in a deployment can layer on later; this library does not enable it.
/// </summary>
public static class SiloBuilderExtensions
{
    /// <summary>
    /// Registers the <see cref="GraphLocalityPlacement"/> strategy/director pair and sets
    /// <see cref="GraphPlacementOptions"/> for this silo. <see cref="ServiceCollectionExtensions.AddSpiceportGrainServices"/>
    /// already registers the pair with the default (OFF, inert) options, so calling this is only needed
    /// to OPT IN to shard co-location (<see cref="GraphPlacementOptions.CoLocateWithShards"/>) — a pure
    /// first-activation locality hint, gated on measurement; see <see cref="GraphLocalityPlacement"/>
    /// for why it is not the deleted hash ring.
    /// </summary>
    /// <param name="siloBuilder">The silo builder to add to.</param>
    /// <param name="options">
    /// The placement options to use; null registers the default (co-location OFF). Registration is
    /// last-wins, matching the options-override pattern of the other grain options types.
    /// </param>
    public static ISiloBuilder AddGraphLocalityPlacement(
        this ISiloBuilder siloBuilder, GraphPlacementOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(siloBuilder);
        siloBuilder.Services.AddPlacementDirector<GraphLocalityPlacement, GraphLocalityPlacementDirector>();
        siloBuilder.Services.AddSingleton(options ?? new GraphPlacementOptions());
        return siloBuilder;
    }

    /// <summary>
    /// Applies <see cref="ActivationMemoOptions.CollectionAge"/> as <see cref="CheckGrain"/>'s,
    /// <see cref="SubjectFrontierMemoOptions.CollectionAge"/> as <see cref="SubjectFrontierGrain"/>'s, and
    /// <see cref="MembershipWalkOptions.CollectionAge"/> as <see cref="MembershipWalkGrain"/>'s
    /// class-specific idle-collection age (<see cref="GrainCollectionOptions.ClassSpecificCollectionAge"/>),
    /// so a warm activation — and hence its per-activation reply memo (stage (a) of
    /// "Activation-as-cache") — survives at least that long between calls.
    /// </summary>
    /// <remarks>
    /// Reads each of <see cref="ActivationMemoOptions"/>, <see cref="SubjectFrontierMemoOptions"/> and
    /// <see cref="MembershipWalkOptions"/> from DI lazily, via <see cref="AddMemoCollectionAge{TOptions,TGrain}"/>'s
    /// dependent-<c>Configure</c> overload, so it does not matter whether this is called before or after
    /// <see cref="ServiceCollectionExtensions.AddSpiceportGrainServices"/> registers (or a caller overrides)
    /// any of the three options types — the values are resolved when Orleans actually builds
    /// <see cref="GrainCollectionOptions"/>, not at wiring time.
    /// </remarks>
    public static ISiloBuilder AddActivationMemoCollectionAge(this ISiloBuilder siloBuilder)
    {
        ArgumentNullException.ThrowIfNull(siloBuilder);
        siloBuilder.AddMemoCollectionAge<ActivationMemoOptions, CheckGrain>();
        siloBuilder.AddMemoCollectionAge<SubjectFrontierMemoOptions, SubjectFrontierGrain>();
        siloBuilder.AddMemoCollectionAge<MembershipWalkOptions, MembershipWalkGrain>();
        return siloBuilder;
    }

    /// <summary>
    /// Wires one <typeparamref name="TOptions"/>'s <see cref="MemoGrainOptions.CollectionAge"/> as
    /// <typeparamref name="TGrain"/>'s class-specific idle-collection age — the one shared clamp/skip rule
    /// behind all three <see cref="AddActivationMemoCollectionAge"/> registrations.
    /// </summary>
    private static void AddMemoCollectionAge<TOptions, TGrain>(this ISiloBuilder siloBuilder)
        where TOptions : MemoGrainOptions
    {
        siloBuilder.Services.AddOptions<GrainCollectionOptions>()
            .Configure<TOptions>((options, memo) =>
            {
                if (!memo.Enabled)
                    return;

                // GrainCollectionOptions rejects a ClassSpecificCollectionAge entry that does not exceed
                // CollectionQuantum (default 1 minute) at configuration-validation time; clamp up instead
                // of letting the silo fail to start over a too-small configured value.
                var floor = options.CollectionQuantum + TimeSpan.FromSeconds(1);
                var age = memo.CollectionAge < floor ? floor : memo.CollectionAge;
                options.ClassSpecificCollectionAge[typeof(TGrain).FullName!] = age;
            });
    }

}
