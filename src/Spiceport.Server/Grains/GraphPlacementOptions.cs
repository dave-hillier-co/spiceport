namespace Spiceport.Grains;

/// <summary>
/// Toggle for the graph co-placement director (<see cref="GraphLocalityPlacementDirector"/>).
/// Default OFF: enabling co-placement is a deployment opt-in gated on measurement
/// (<c>docs/graph-sharded-datastore.md</c> §5/§7 — pure performance, per the
/// simplicity-over-performance stance).
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
    public bool CoLocateWithShards { get; init; }
}
