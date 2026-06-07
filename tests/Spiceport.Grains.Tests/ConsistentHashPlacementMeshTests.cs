using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Proves the custom consistent-hash placement is REAL on a multi-silo cluster: a given grain key
/// deterministically activates on its hash-owner silo, and the dispatcher's own owner computation
/// (the <see cref="ISiloOwnership"/> oracle, reading the same membership view through the same
/// <c>HashRing</c> the placement director uses) agrees with where the grain actually activates.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class ConsistentHashPlacementMeshTests
{
    private const string Schema = """
        definition user {}

        definition document {
            relation viewer: user
            permission view = viewer
        }
        """;

    /// <summary>The ownership oracle from any silo's container (every silo sees the same membership).</summary>
    private static ISiloOwnership Ownership(MeshTestCluster cluster) =>
        cluster.Services.GetRequiredService<ISiloOwnership>();

    [Fact]
    public async Task MultiSilo_cluster_has_the_requested_number_of_silos()
    {
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(Schema, siloCount: 3);
        Assert.Equal(3, cluster.SiloCount);
        Assert.Equal(3, Ownership(cluster).ActiveSilos.Count);
    }

    [Fact]
    public async Task Grain_key_activates_on_its_hash_owner_silo_and_dispatcher_agrees()
    {
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(Schema, siloCount: 3);
        var ownership = Ownership(cluster);

        // Sweep many distinct keys so we span the ring and exercise every silo as an owner.
        var ownersSeen = new HashSet<string>();
        for (var i = 0; i < 60; i++)
        {
            var key = $"document/doc{i}/view/user/u{i}/.../rev1/schemahash/0";

            // The dispatcher's prediction (pure function of key + membership view).
            var predictedOwner = ownership.OwnerOf(key).ToParsableString();

            // Where the grain ACTUALLY activates (the director runs at activation time).
            var grain = cluster.GrainFactory.GetGrain<ICheckGrain>(key);
            var actualOwner = await grain.GetHostSilo();

            Assert.Equal(predictedOwner, actualOwner);
            ownersSeen.Add(actualOwner);
        }

        // Anti-hollow: placement must genuinely spread across silos, not collapse onto one.
        Assert.True(ownersSeen.Count > 1,
            $"Consistent-hash placement should spread keys across silos; saw only {ownersSeen.Count}.");
    }

    [Fact]
    public async Task Placement_is_deterministic_repeated_activation_lands_on_the_same_silo()
    {
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(Schema, siloCount: 3);
        const string key = "document/stable/view/user/alice/.../rev1/schemahash/0";

        var first = await cluster.GrainFactory.GetGrain<ICheckGrain>(key).GetHostSilo();
        var second = await cluster.GrainFactory.GetGrain<ICheckGrain>(key).GetHostSilo();
        var predicted = Ownership(cluster).OwnerOf(key).ToParsableString();

        Assert.Equal(first, second);
        Assert.Equal(predicted, first);
    }

    [Fact]
    public async Task Every_silo_computes_the_same_owner_for_a_key()
    {
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(Schema, siloCount: 3);
        const string key = "document/agree/view/user/bob/.../rev1/schemahash/0";

        // Owner computation must be identical regardless of which silo's oracle we ask — that is what
        // makes the dispatcher's local prediction trustworthy on every silo.
        var owners = cluster.AllSiloServices
            .Select(sp => sp.GetRequiredService<ISiloOwnership>().OwnerOf(key).ToParsableString())
            .Distinct()
            .ToArray();

        Assert.Single(owners);
    }

    [Fact]
    public async Task A_locally_owned_key_is_reported_local_by_its_owner_silo_only()
    {
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(Schema, siloCount: 3);
        const string key = "document/locality/view/user/carol/.../rev1/schemahash/0";

        var ownerAddr = Ownership(cluster).OwnerOf(key).ToParsableString();

        // Exactly one silo should consider this key local, and it must be the hash owner.
        var localCount = 0;
        string? localSilo = null;
        foreach (var sp in cluster.AllSiloServices)
        {
            var o = sp.GetRequiredService<ISiloOwnership>();
            if (o.IsLocal(key))
            {
                localCount++;
                localSilo = o.LocalSilo.ToParsableString();
            }
        }

        Assert.Equal(1, localCount);
        Assert.Equal(ownerAddr, localSilo);
    }
}
