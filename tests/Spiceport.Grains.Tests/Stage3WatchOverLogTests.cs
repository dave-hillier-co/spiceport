using System.Diagnostics;
using System.Text;
using Spiceport.Core;
using Spiceport.Datastore;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Stage-3 gates for the log-driven Watch changefeed: the datastore <see cref="IDatastore.Watch"/> now tails
/// the event log (shipping only the per-revision diff) and parks on a per-silo signal rather than a per-stream
/// 50ms full-state poll. Proves a schema-only commit still surfaces a schema-change + checkpoint (no phantom
/// relationship change), and that a write is observed promptly (event-driven, not a fixed poll delay) thanks
/// to the local commit pulsing the watcher directly.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class Stage3WatchOverLogTests
{
    private const string Schema = """
        definition user {}
        definition document {
            relation viewer: user
            permission view = viewer
        }
        """;

    /// <summary>
    /// A schema-only commit (no relationship change) is replayed as a RevisionChange carrying SchemaChanged
    /// (and no relationship updates), followed by a checkpoint — the feed rides the revision even though no
    /// relationship matched. Uses the deterministic resume-from-pre-write-cursor path (no timing).
    /// </summary>
    [Fact]
    public async Task Watch_schema_only_commit_emits_schema_change_then_checkpoint()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var ds = cluster.Datastore;

        var preWrite = (await ds.HeadRevision(CancellationToken.None)).Revision;

        // A schema-only commit: rewrite the unified schema, touching no relationships.
        await ds.ReadWriteTx(tx => tx.WriteStoredSchema(
            Encoding.UTF8.GetBytes(Schema + "\ndefinition folder {}")));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var changes = await Collect(
            ds.Watch(preWrite, new WatchOptions(WatchContent.All | WatchContent.Checkpoints), cts.Token),
            count: 2, cts);

        var content = changes[0];
        Assert.False(content.IsCheckpoint);
        Assert.True(content.SchemaChanged);
        Assert.Empty(content.RelationshipChanges);

        var checkpoint = changes[1];
        Assert.True(checkpoint.IsCheckpoint);
        Assert.Empty(checkpoint.RelationshipChanges);
        Assert.Equal(content.Revision, checkpoint.Revision);
    }

    /// <summary>
    /// Tailing from head, a relationship write committed on the same silo is observed promptly — the commit
    /// pulses the per-silo watcher directly, so it does not wait on a poll tick. A generous bound proves the
    /// feed is event-driven (it is nowhere near the 20s cancellation fallback), not flaky on CI jitter.
    /// </summary>
    [Fact]
    public async Task Watch_observes_a_write_promptly_without_polling()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var ds = cluster.Datastore;

        var head = (await ds.HeadRevision(CancellationToken.None)).Revision;

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var first = new TaskCompletionSource<RevisionChange>(TaskCreationOptions.RunContinuationsAsynchronously);
        var consume = Task.Run(async () =>
        {
            await foreach (var change in ds.Watch(head, new WatchOptions(WatchContent.Relationships), cts.Token))
            {
                first.TrySetResult(change);
                break;
            }
        });

        // Give the stream a moment to start tailing, then commit and time the observation.
        await Task.Delay(100, cts.Token);
        var sw = Stopwatch.StartNew();
        await ds.ReadWriteTx(tx => tx.WriteRelationships(new[]
        {
            new RelationshipUpdate(
                Relationship.Create(new ObjectAndRelation("document", "doc1", "viewer"),
                    new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis)),
                UpdateOperation.Create),
        }));

        var observed = await first.Task.WaitAsync(TimeSpan.FromSeconds(10), cts.Token);
        sw.Stop();
        await cts.CancelAsync();
        try { await consume; } catch (OperationCanceledException) { }

        Assert.Single(observed.RelationshipChanges);
        Assert.Equal("doc1", observed.RelationshipChanges[0].Relationship.Resource.ObjectId);
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(1), $"observed after {sw.ElapsedMilliseconds}ms (expected event-driven, sub-second)");
    }

    /// <summary>
    /// Clean-teardown gate for the DI-owned hub (adapted from the retired GrainService lifecycle test —
    /// the invariant transfers): the per-silo <see cref="LogWatchHub"/> is an <see cref="IAsyncDisposable"/>
    /// DI singleton, so cluster/silo disposal must dispose it (a bounded, timeout-guarded unsubscribe from
    /// the DatastoreGrain's observer set) without hanging or throwing, even after a Watch stream started
    /// its observer subscription and heartbeat. (The DatastoreGrain's own watcher-registration expiry is a
    /// backstop against a leak either way; this test is about clean, bounded teardown.)
    /// </summary>
    [Fact]
    public async Task Cluster_shutdown_disposes_the_shared_hub_without_hanging_or_throwing()
    {
        var cluster = await MeshTestCluster.CreateAsync(Schema);

        // Exercise the hub: park a Watch stream so the hub's signal path (observer subscription +
        // heartbeat, lazily started by the stream's EnsureStarted) is genuinely live.
        var head = (await cluster.Datastore.HeadRevision()).Revision;
        using var watchCts = new CancellationTokenSource();
        var watchTask = Task.Run(async () =>
        {
            await foreach (var _ in cluster.Datastore.Watch(head, new WatchOptions(WatchContent.Relationships), watchCts.Token))
            {
            }
        });
        await Task.Delay(250);

        // Drain the consumer while the cluster is still alive (a background task left running past cluster
        // disposal would leak into and perturb later tests). The hub's observer subscription on the
        // datastore grain remains registered — container disposal, not this stream's lifetime, owns
        // tearing that down.
        await watchCts.CancelAsync();
        try { await watchTask; } catch (OperationCanceledException) { }

        var disposeTask = cluster.DisposeAsync().AsTask();
        var completed = await Task.WhenAny(disposeTask, Task.Delay(TimeSpan.FromSeconds(30)));
        Assert.Same(disposeTask, completed);
        await disposeTask; // rethrows if disposal itself faulted
    }

    private static async Task<List<RevisionChange>> Collect(
        IAsyncEnumerable<RevisionChange> stream, int count, CancellationTokenSource cts)
    {
        var list = new List<RevisionChange>();
        try
        {
            await foreach (var change in stream)
            {
                list.Add(change);
                if (list.Count >= count)
                {
                    await cts.CancelAsync();
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Cancellation is the normal stream terminator.
        }
        return list;
    }
}
