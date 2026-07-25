using System.Collections.Immutable;
using Orleans.Runtime;
using Spiceport.Conformance.Tests;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// The co-placement director (<see cref="GraphLocalityPlacementDirector"/>,
/// <c>docs/graph-sharded-datastore.md</c> §5): the locality-key rule must name the SAME object from
/// every grain-key shape that reads that object's shard (that agreement IS the co-location property),
/// the hash must be stable across processes (every silo must compute the same silo index), and a
/// multi-silo cluster with co-location ENABLED must answer exactly like the default cluster — the
/// director is a first-activation locality hint, never a correctness mechanism.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class GraphLocalityPlacementTests
{
    [Fact]
    public void Locality_key_names_the_same_object_from_every_key_shape_that_reads_its_shard()
    {
        var resource = new ObjectAndRelation("document", "readme", "view");
        var subject = new ObjectAndRelation("group", "eng", "member");

        // CheckGrain (8 segments) and the forward shard of its resource (3 segments) must agree: the
        // check's first data read is that shard.
        var checkKey = GrainKey.Build(resource, subject, "42", "hash");
        var forwardShardKey = GraphShardGrainKey.Build(
            new Abstractions.GraphShardKeyWire(Abstractions.GraphShardDirection.Forward, "document", "readme"));
        Assert.Equal(
            GraphLocalityPlacementDirector.LocalityKey(forwardShardKey),
            GraphLocalityPlacementDirector.LocalityKey(checkKey));

        // MembershipWalkGrain (5 segments) and the REVERSE shard of its subject (3 segments) must
        // agree: the walk's one data read is that shard. Direction is ignored by design, so the
        // reverse shard also agrees with the forward shard of the same object.
        var walkKey = MembershipWalkKey.Build("group", "eng", "member", "42", "hash");
        var reverseShardKey = GraphShardGrainKey.Build(
            new Abstractions.GraphShardKeyWire(Abstractions.GraphShardDirection.Reverse, "group", "eng"));
        Assert.Equal(
            GraphLocalityPlacementDirector.LocalityKey(reverseShardKey),
            GraphLocalityPlacementDirector.LocalityKey(walkKey));

        // SubjectFrontierGrain (7 segments) co-locates by its RESOURCE (its key names no subject id;
        // its whole walk starts from the resource root's forward shard).
        var frontierKey = SubjectFrontierKey.Build(resource, "user", CoreConstants.Ellipsis, "42", "hash");
        Assert.Equal(
            GraphLocalityPlacementDirector.LocalityKey(checkKey),
            GraphLocalityPlacementDirector.LocalityKey(frontierKey));

        // Escaping keeps the segment-count dispatch honest: a literal '/' in an id must not change the
        // recognized shape, and the escaped locality key must still agree across key shapes.
        var trickyShardKey = GraphShardGrainKey.Build(
            new Abstractions.GraphShardKeyWire(Abstractions.GraphShardDirection.Forward, "doc", "a/b"));
        var trickyCheckKey = GrainKey.Build(
            new ObjectAndRelation("doc", "a/b", "view"), subject, "42", "hash");
        Assert.Equal(
            GraphLocalityPlacementDirector.LocalityKey(trickyShardKey),
            GraphLocalityPlacementDirector.LocalityKey(trickyCheckKey));

        // An unrecognized shape yields null (the director then falls back to a random pick).
        Assert.Null(GraphLocalityPlacementDirector.LocalityKey("just/two"));
    }

    [Fact]
    public void Fnv1a64_matches_the_published_test_vectors()
    {
        // The FNV-1a 64-bit reference vectors (over one byte per char, which for these ASCII inputs
        // equals the UTF-16 code-unit fold the shared helper uses). string.GetHashCode is per-process
        // randomized; these constants prove the replacement is process-independent. The helper is
        // shared with the datastore grain's durable key-index bucketing, so these vectors also pin
        // the bucket assignment as part of the durable layout.
        Assert.Equal(14695981039346656037UL, StableHash.Fnv1a64(""));
        Assert.Equal(0xaf63dc4c8601ec8cUL, StableHash.Fnv1a64("a"));
        Assert.Equal(0x85944171f73967e8UL, StableHash.Fnv1a64("foobar"));
    }

    [Fact]
    public async Task Co_location_on_places_each_check_grain_on_its_resources_forward_shard_silo()
    {
        var (stats, checkType, forwardShardType) = await RunRecursiveCorpusAndCollectStatistics();

        // Group CheckGrain activations by their RESOURCE object, and forward GraphShardGrain
        // activations by their object, parsing the grain keys out of the statistics' grain ids.
        var checkSilos = new Dictionary<(string Type, string Id), HashSet<Orleans.Runtime.SiloAddress>>();
        var shardSilo = new Dictionary<(string Type, string Id), Orleans.Runtime.SiloAddress>();
        foreach (var stat in stats)
        {
            var key = stat.GrainId.Key.ToString()!;
            if (stat.GrainId.Type == checkType)
            {
                var resource = GrainKey.Parse(key).Resource;
                checkSilos.TryAdd((resource.ObjectType, resource.ObjectId), []);
                checkSilos[(resource.ObjectType, resource.ObjectId)].Add(stat.SiloAddress);
            }
            else if (stat.GrainId.Type == forwardShardType)
            {
                var shard = GraphShardGrainKey.Parse(key);
                if (shard.Direction == GraphShardDirection.Forward)
                    shardSilo[(shard.ObjectType, shard.ObjectId)] = stat.SiloAddress;
            }
        }

        // The co-location property: every resource object that has BOTH a CheckGrain activation and a
        // forward shard activation has them on the SAME silo (objects on only one side are skipped).
        var coLocatedPairs = 0;
        var misplaced = new List<string>();
        foreach (var (obj, silos) in checkSilos)
        {
            if (!shardSilo.TryGetValue(obj, out var expected))
                continue;
            coLocatedPairs++;
            foreach (var silo in silos.Where(s => !s.Equals(expected)))
                misplaced.Add($"  {obj.Type}:{obj.Id}: CheckGrain on {silo}, forward shard on {expected}");
        }

        Assert.True(misplaced.Count == 0, $"CheckGrains not co-located with their resource's forward shard:\n{string.Join('\n', misplaced)}");

        // Non-vacuity: the recursive chain guarantees five such objects (someresource + the four
        // nested groups), so requiring at least three proves the assertion above compared something.
        Assert.True(coLocatedPairs >= 3, $"Expected at least 3 co-located CheckGrain/forward-shard pairs, saw {coLocatedPairs}.");

        // Same-run positive control: co-location must not be the hollow all-on-one-silo degeneration
        // (a default-placement heuristic concentrating everything on the caller silo would make every
        // pair trivially "co-located"). The five locality keys hash across all three mod-3 buckets, so
        // the shards observed by THIS run must span at least two silos for the pairing above to mean
        // anything.
        Assert.True(shardSilo.Values.Distinct().Count() >= 2,
            $"Forward shards all landed on one silo ({shardSilo.Values.Distinct().Count()} distinct) — the co-location assertion would be vacuous.");
    }

    [Fact]
    public async Task Co_location_on_still_spreads_shards_across_silos()
    {
        var (stats, _, forwardShardType) = await RunRecursiveCorpusAndCollectStatistics();

        // The five distinct objects' locality keys hash to all three of the mod-3 buckets (verified
        // against the same FNV-1a fold pinned above), so the shards must land on at least two silos —
        // the hash genuinely spreads; co-location is not a hollow all-on-one-silo degeneration.
        var shardHostSilos = stats
            .Where(s => s.GrainId.Type == forwardShardType
                && GraphShardGrainKey.Parse(s.GrainId.Key.ToString()!).Direction == GraphShardDirection.Forward)
            .Select(s => s.SiloAddress)
            .Distinct()
            .Count();

        Assert.True(shardHostSilos >= 2, $"Expected forward GraphShardGrain activations on at least 2 silos, saw {shardHostSilos}.");
    }

    [Fact]
    public async Task Multi_silo_cluster_with_co_location_disabled_answers_checks_correctly()
    {
        // The OFF smoke for the option plumbing: the flag defaults to false, the director then mirrors
        // random placement, and verdicts hold. The broader suites cover default placement extensively;
        // this pins the explicit coLocateWithShards: false path through MeshTestCluster.
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(
            RecursiveSchema, siloCount: 3, coLocateWithShards: false);

        await SeedRecursiveCorpus(cluster);

        var member = await cluster.Checker.Check(
            "srrr/resource", "someresource", "viewer",
            new ObjectAndRelation("srrr/user", "someguy", CoreConstants.Ellipsis), null);
        Assert.Equal(Membership.Member, member.Verdict);

        var nonMember = await cluster.Checker.Check(
            "srrr/resource", "someresource", "viewer",
            new ObjectAndRelation("srrr/user", "stranger", CoreConstants.Ellipsis), null);
        Assert.Equal(Membership.NotMember, nonMember.Verdict);
    }

    /// <summary>
    /// The shared ON-path workload: a 3-silo cluster with co-location enabled runs the
    /// <c>simplerecursive</c> corpus file's nested-group chain. The only member sits at the DEEPEST
    /// group, so both the positive and the negative check must walk the whole chain — a CheckGrain and
    /// a forward-shard read are guaranteed for all five objects, no short-circuit can skip one.
    /// Returns the cluster-wide detailed grain statistics plus the two grain types to filter by
    /// (resolved from live grain references, so no grain-type naming convention is assumed).
    /// </summary>
    private static async Task<(Orleans.Runtime.DetailedGrainStatistic[] Stats, GrainType CheckType, GrainType ShardType)>
        RunRecursiveCorpusAndCollectStatistics()
    {
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(
            RecursiveSchema, siloCount: 3, coLocateWithShards: true);

        await SeedRecursiveCorpus(cluster);

        var member = await cluster.Checker.Check(
            "srrr/resource", "someresource", "viewer",
            new ObjectAndRelation("srrr/user", "someguy", CoreConstants.Ellipsis), null);
        Assert.Equal(Membership.Member, member.Verdict);

        var nonMember = await cluster.Checker.Check(
            "srrr/resource", "someresource", "viewer",
            new ObjectAndRelation("srrr/user", "stranger", CoreConstants.Ellipsis), null);
        Assert.Equal(Membership.NotMember, nonMember.Verdict);

        var management = cluster.GrainFactory.GetGrain<IManagementGrain>(0);
        var stats = await management.GetDetailedGrainStatistics();

        var checkType = cluster.GrainFactory.GetGrain<ICheckGrain>("probe").GetGrainId().Type;
        var shardType = cluster.GrainFactory.GetGrain<IGraphShardGrain>("probe").GetGrainId().Type;
        return (stats, checkType, shardType);
    }

    /// <summary>The schema of <c>TestData/simplerecursive.yaml</c>, loaded fresh from the linked corpus file.</summary>
    private static string RecursiveSchema =>
        LoadRecursiveCorpus().SchemaText;

    private static ValidationFile LoadRecursiveCorpus()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "TestData", "simplerecursive.yaml");
        Assert.True(File.Exists(path), $"Linked corpus file missing from output: {path}");
        return ValidationFileLoader.LoadFromFile(path);
    }

    private static async Task SeedRecursiveCorpus(MeshTestCluster cluster)
    {
        var updates = LoadRecursiveCorpus().Relationships
            .Select(r => new RelationshipUpdate(r, UpdateOperation.Create))
            .ToImmutableList();
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(updates));
    }

    [Fact]
    public async Task Multi_silo_cluster_with_co_location_enabled_answers_checks_correctly()
    {
        // The ON-path smoke: same schema/data as the default-placement mesh tests, with the director
        // actively steering first activations. Verdicts must be identical to default placement because
        // placement is a pure locality hint.
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(
            """
            definition user {}

            definition group {
                relation direct_member: user
                relation parent: group
                permission member = direct_member + parent->member
            }
            """,
            siloCount: 3,
            coLocateWithShards: true);

        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(new[]
        {
            new RelationshipUpdate(
                Relationship.Create(
                    new ObjectAndRelation("group", "g0", "parent"),
                    new ObjectAndRelation("group", "g1", CoreConstants.Ellipsis)),
                UpdateOperation.Create),
            new RelationshipUpdate(
                Relationship.Create(
                    new ObjectAndRelation("group", "g1", "direct_member"),
                    new ObjectAndRelation("user", "u", CoreConstants.Ellipsis)),
                UpdateOperation.Create),
        }));

        var member = await cluster.Checker.Check(
            "group", "g0", "member", new ObjectAndRelation("user", "u", CoreConstants.Ellipsis), null);
        Assert.Equal(Membership.Member, member.Verdict);

        var nonMember = await cluster.Checker.Check(
            "group", "g0", "member", new ObjectAndRelation("user", "someone-else", CoreConstants.Ellipsis), null);
        Assert.Equal(Membership.NotMember, nonMember.Verdict);
    }
}
