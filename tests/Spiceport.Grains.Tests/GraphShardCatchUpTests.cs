using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// A shard's catch-up is single-flight: readers that arrive together at a revision the shard has not
/// yet folded share one pull from the sequencer's log rather than each pulling it.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class GraphShardCatchUpTests
{
    private const string Schema = """
        definition user {}

        definition doc {
          relation viewer: user
        }
        """;

    private static Task Write(MeshTestCluster cluster, string subject) =>
        cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(
        [
            new RelationshipUpdate(
                Relationship.Create(
                    new ObjectAndRelation("doc", "d1", "viewer"),
                    new ObjectAndRelation("user", subject, CoreConstants.Ellipsis)),
                UpdateOperation.Touch),
        ]));

    [Fact]
    public async Task Concurrent_readers_share_one_catch_up()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var shard = cluster.GrainFactory.GetGrain<IGraphShardGrain>(
            GraphShardGrainKey.Build(GraphShardKeyWire.ForResource("doc", "d1")));

        await Write(cluster, "u0");
        var warm = await cluster.Datastore.HeadRevision();
        await shard.RowsAt(((TimestampRevision)warm.Revision).TimestampNanosSinceEpoch, filter: null, CancellationToken.None);

        for (var i = 1; i <= 5; i++)
            await Write(cluster, $"u{i}");
        var head = ((TimestampRevision)(await cluster.Datastore.HeadRevision()).Revision).TimestampNanosSinceEpoch;

        var before = cluster.SequencerMetricsSnapshot().ReadFrom;
        var replies = await Task.WhenAll(Enumerable.Range(0, 20)
            .Select(_ => shard.RowsAt(head, filter: null, CancellationToken.None)));
        var pulls = cluster.SequencerMetricsSnapshot().ReadFrom - before;

        Assert.All(replies, r => Assert.Equal(6, r.Rows.Count));
        Assert.Equal(1, pulls);
    }
}
