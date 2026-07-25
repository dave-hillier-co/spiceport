using Orleans.Placement;
using Orleans.Runtime;

namespace Spiceport.Grains;

/// <summary>
/// Placement strategy marker for the graph compute/data grain families
/// (<see cref="CheckGrain"/>, <see cref="GraphShardGrain"/>, <see cref="MembershipWalkGrain"/>,
/// <see cref="SubjectFrontierGrain"/>): first activations are directed by
/// <see cref="GraphLocalityPlacementDirector"/>, which — only when
/// <see cref="GraphPlacementOptions.CoLocateWithShards"/> is enabled — steers a grain onto the same
/// silo as the <see cref="GraphShardGrain"/> holding the data it reads (the co-placement step of
/// <c>docs/graph-sharded-datastore.md</c> §5).
/// </summary>
/// <remarks>
/// This is a LOCALITY HINT, not ownership, and it is NOT the deleted hash ring
/// (<c>docs/future-work.md</c> §1.4). The ring computed OWNERSHIP — which silo a sub-problem
/// belonged to, load-bearing for dedup — whereas this strategy only biases where the FIRST
/// activation of a grain lands; the Orleans grain directory remains the single authority for
/// identity and single-activation dedup. On membership change the director's modulus moves and
/// existing activations simply stay where the directory has them: locality degrades gracefully
/// (some compute lands one hop from its shard until idle collection rotates the keyspace),
/// correctness is untouched.
/// <para>
/// The strategy attaches per grain CLASS via <see cref="GraphLocalityPlacementAttribute"/>, which
/// Orleans cannot make conditional — so the attribute is unconditional on all four grain classes and
/// the on/off decision lives in the DIRECTOR (see <see cref="GraphLocalityPlacementDirector"/>):
/// when <see cref="GraphPlacementOptions.CoLocateWithShards"/> is false (the default) the director
/// mirrors Orleans' random placement, a uniform pick from the compatible silos.
/// </para>
/// </remarks>
[Immutable]
[Serializable]
[GenerateSerializer]
public sealed class GraphLocalityPlacement : PlacementStrategy;

/// <summary>
/// Marks a grain class as placed by <see cref="GraphLocalityPlacement"/> /
/// <see cref="GraphLocalityPlacementDirector"/>. See the strategy's remarks for why the attribute is
/// unconditional while the behavior is opt-in.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
public sealed class GraphLocalityPlacementAttribute : PlacementAttribute
{
    public GraphLocalityPlacementAttribute()
        : base(new GraphLocalityPlacement())
    {
    }
}
