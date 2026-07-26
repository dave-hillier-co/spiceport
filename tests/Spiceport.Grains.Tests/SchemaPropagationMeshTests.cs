using Microsoft.Extensions.DependencyInjection;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Proves the cross-silo schema-propagation fix: a <c>WriteSchema</c> commit lands on exactly one silo's
/// <c>RelationshipsGrain</c> activation, which swaps only THAT silo's <see cref="ISchemaProvider"/>
/// directly — every other silo must instead learn about it over the <c>LogWatchHub</c>
/// push/heartbeat channel (<see cref="Abstractions.IDatastoreWatcher.SchemaAdvanced"/> and the
/// heartbeat's stored-schema-hash diff). A real multi-process rig exposed this as a silent wrong-verdict
/// divergence — silos other than the writer kept serving the stale schema forever, with no error. The
/// in-process <see cref="MeshTestCluster.CreateMultiSiloAsync"/> reproduces it faithfully because each
/// silo in the <c>TestCluster</c> gets its own DI container (and hence its own
/// <c>MutableSchemaProvider</c> instance), exactly like separate processes.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class SchemaPropagationMeshTests
{
    private const string SeedSchema = """
        definition user {}

        definition doc {
          relation viewer: user
        }
        """;

    private const string UpdatedSchema = """
        definition user {}

        definition doc {
          relation viewer: user
          relation editor: user
        }
        """;

    private static readonly TimeSpan ConvergenceTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(100);

    private static string[] CurrentHashes(MeshTestCluster cluster) =>
        cluster.AllSiloServices
            .Select(sp => sp.GetRequiredService<ISchemaProvider>().Current.SchemaHash)
            .ToArray();

    /// <summary>
    /// Layer 2: every silo's live <see cref="ISchemaProvider"/> must converge on the new schema's hash
    /// after a <c>WriteSchema</c>, even though the RPC only ever touches ONE silo's activation directly.
    /// Before the fix, silos that never hosted the <c>RelationshipsGrain</c> activation for this call kept
    /// serving the seed hash forever (no error, no timeout — a silent wrong-verdict divergence).
    /// </summary>
    [Fact]
    public async Task WriteSchema_converges_every_silos_live_schema_provider()
    {
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(SeedSchema, siloCount: 3);

        var seedHashes = CurrentHashes(cluster);
        Assert.Single(seedHashes.Distinct()); // every silo starts on the identical embedded seed.
        var seedHash = seedHashes[0];

        await cluster.WriteSchema(UpdatedSchema);

        var deadline = DateTime.UtcNow + ConvergenceTimeout;
        string[] hashes;
        do
        {
            hashes = CurrentHashes(cluster);
            if (hashes.Distinct().Count() == 1 && hashes[0] != seedHash)
                break;
            await Task.Delay(PollInterval);
        }
        while (DateTime.UtcNow < deadline);

        Assert.True(
            hashes.Distinct().Count() == 1,
            $"expected every silo to converge on one schema hash within {ConvergenceTimeout}; saw: {string.Join(", ", hashes)}");
        Assert.NotEqual(seedHash, hashes[0]);
    }

    /// <summary>
    /// Layer 1 regression: once every silo's <see cref="SchemaResolver"/> has resolved the (structural)
    /// hash a dispatch grain key pins at least once, further checks must NOT keep paying a sequencer
    /// <c>ReadSchemaAt</c> hop. Before the fix, <c>SchemaResolver.CompileFetched</c> cached fetched bytes
    /// only under their STORED-bytes hash, never under the STRUCTURAL hash dispatch keys actually pin — so
    /// every single dispatch missed the cache and paid the hop, growing ~1:1 with check volume (measured
    /// at ~400 checks/s -> 3556 calls in 5s). This asserts the fixed shape: after a warm-up window, further
    /// checks add at most a small constant, never one-per-check.
    /// </summary>
    [Fact]
    public async Task ReadSchemaAt_stays_bounded_after_WriteSchema_and_warmup()
    {
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(SeedSchema, siloCount: 3);

        await cluster.WriteSchema(UpdatedSchema);

        var rel = Relationship.Create(
            new ObjectAndRelation("doc", "d1", "viewer"),
            new ObjectAndRelation("user", "u1", CoreConstants.Ellipsis));
        await cluster.Datastore.ReadWriteTx(
            tx => tx.WriteRelationships([new RelationshipUpdate(rel, UpdateOperation.Create)]));

        async Task RunChecks(int count)
        {
            for (var i = 0; i < count; i++)
            {
                var result = await cluster.Checker.Check(
                    "doc", "d1", "viewer",
                    new ObjectAndRelation("user", "u1", CoreConstants.Ellipsis),
                    caveatContext: null,
                    consistency: ConsistencyRequirement.FullyConsistent);
                Assert.Equal(Membership.Member, result.Verdict);
            }
        }

        // Warm-up: enough checks that every silo's SchemaResolver has had the chance to miss-once and
        // cache the new hash (a fresh keyspace after the schema change).
        await RunChecks(10);
        var baseline = cluster.SequencerMetricsSnapshot().ReadSchemaAt;

        const int additionalChecks = 60;
        await RunChecks(additionalChecks);
        var grew = cluster.SequencerMetricsSnapshot().ReadSchemaAt - baseline;

        Assert.True(
            grew <= 5,
            $"expected ReadSchemaAt to stay bounded after warm-up (a per-hash-per-silo miss, not " +
            $"per-check), but it grew by {grew} over {additionalChecks} additional checks — the Layer-1 " +
            $"dual-hash-key regression.");
    }
}
