using Orleans.Runtime;

namespace Spiceport.Grains;

/// <summary>
/// Lets the dispatcher predict, without sending any message, which silo a grain key would activate on —
/// computed from the SAME membership view and the SAME <see cref="HashRing"/> that the
/// <see cref="ConsistentHashPlacementDirector"/> uses. This is what makes the local-recurse shortcut
/// possible: the dispatcher asks "is this sub-problem's owner me?" purely as a function of (key, silos).
/// </summary>
public interface ISiloOwnership
{
    /// <summary>This silo's own address.</summary>
    SiloAddress LocalSilo { get; }

    /// <summary>The current active silo set (the membership view used to build the hash ring).</summary>
    IReadOnlyList<SiloAddress> ActiveSilos { get; }

    /// <summary>The silo that <paramref name="grainKey"/> would activate on under consistent-hash placement.</summary>
    SiloAddress OwnerOf(string grainKey);

    /// <summary>True if <paramref name="grainKey"/>'s owner is this local silo.</summary>
    bool IsLocal(string grainKey);
}

/// <summary>
/// Production <see cref="ISiloOwnership"/> backed by Orleans membership: <see cref="ILocalSiloDetails"/>
/// for the local address and <see cref="ISiloStatusOracle"/> for the active silo set.
/// </summary>
/// <remarks>
/// The director uses <c>IPlacementContext.GetCompatibleSilos</c> (filtered by grain-type/interface
/// version). In this homogeneous deployment every silo runs the same <c>CheckGrain</c>, so the
/// compatible set equals the active set and this oracle's owner computation matches the director's. A
/// transient disagreement (membership churn between the two reads, or a heterogeneous-version cluster)
/// can only cause the same sub-problem to be computed in two places during churn; both populate the
/// shared cache identically against the same schema/datastore snapshot, so verdicts agree and the only
/// cost is a little duplicated work — never correctness — because the grain key fully determines identity
/// and the director remains authoritative for where the activation actually lands.
/// </remarks>
public sealed class SiloOwnership(
    ILocalSiloDetails localDetails,
    ISiloStatusOracle statusOracle) : ISiloOwnership
{
    private readonly ILocalSiloDetails _localDetails =
        localDetails ?? throw new ArgumentNullException(nameof(localDetails));
    private readonly ISiloStatusOracle _statusOracle =
        statusOracle ?? throw new ArgumentNullException(nameof(statusOracle));

    /// <inheritdoc />
    public SiloAddress LocalSilo => _localDetails.SiloAddress;

    /// <inheritdoc />
    public IReadOnlyList<SiloAddress> ActiveSilos =>
        // onlyActive: true — only silos eligible to host activations, matching the director's view.
        _statusOracle.GetApproximateSiloStatuses(onlyActive: true).Keys.ToArray();

    /// <inheritdoc />
    public SiloAddress OwnerOf(string grainKey) => HashRing.Owner(grainKey, ActiveSilos);

    /// <inheritdoc />
    public bool IsLocal(string grainKey) => OwnerOf(grainKey).Equals(LocalSilo);
}
