namespace Spiceport.Grains;

/// <summary>
/// Toggle for the graph co-placement director (<see cref="GraphLocalityPlacementDirector"/>).
/// Default ON: the enablement was gated on measurement (<c>docs/graph-sharded-datastore.md</c> §5/§7)
/// and the real-network rig's A/B decided it (<c>docs/scalability-program.md</c> §3.5) — on real
/// sockets, co-locating compute with its shard turns cross-silo hops into local calls for a
/// consistent, large latency/throughput win. Opting OUT remains a deployment override via
/// <see cref="SiloBuilderExtensions.AddGraphLocalityPlacement"/>.
/// </summary>
/// <remarks>
/// When false the director places exactly like Orleans' random placement (a uniform pick from the
/// compatible silos), so nothing about correctness, identity, or dedup changes either way — the grain
/// directory remains the single authority for where an activation lives once placed. The flag only
/// biases WHERE a first activation lands.
/// </remarks>
public sealed class GraphPlacementOptions
{
    /// <summary>
    /// When true, first activations of the graph compute/data grain families
    /// (<see cref="CheckGrain"/>, <see cref="GraphShardGrain"/>, <see cref="MembershipWalkGrain"/>,
    /// <see cref="SubjectFrontierGrain"/>) are steered onto the silo chosen by a stable hash of their
    /// locality key, so compute lands beside the shard holding its data.
    /// </summary>
    public bool CoLocateWithShards { get; init; } = true;
}
