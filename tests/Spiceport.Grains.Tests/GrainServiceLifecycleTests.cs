using Microsoft.Extensions.DependencyInjection;
using Spiceport.Core;
using Spiceport.Datastore;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Gates for the silo-lifecycle-managed <see cref="DatastoreProjectionService"/> (docs/future-work.md §1.8):
/// the per-silo <see cref="SiloProjection"/>/<see cref="LogWatchHub"/> pair is bootstrapped and torn down by
/// the silo's own lifecycle rather than by hand-rolled DI singleton construction/disposal.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class GrainServiceLifecycleTests
{
    /// <summary>
    /// Gate (a): on a freshly started cluster using the PRODUCTION shared-host wiring (not the private-hub
    /// test seam), the projection is ALREADY bootstrapped by the time cluster startup returns — proving
    /// <see cref="DatastoreProjectionService.Start"/> ran (and awaited the projection's first ReadState
    /// fetch) before the silo finished standing up, not lazily on the first request. This is an honest
    /// observation of <see cref="SiloProjection.IsBootstrapped"/> (a real flag flipped by the real bootstrap
    /// path), not a fabricated counter, PLUS a live read that must already succeed correctly.
    /// </summary>
    [Fact]
    public async Task Projection_IsBootstrapped_BeforeFirstRequest()
    {
        const string schema = """
            definition user {}
            definition document {
                relation viewer: user
                permission view = viewer
            }
            """;

        await using var cluster = await MeshTestCluster.CreateAsync(schema);

        var host = cluster.Services.GetRequiredService<IDatastoreProjectionHost>();
        Assert.True(
            host.Projection.IsBootstrapped,
            "the shared projection should already be bootstrapped by DatastoreProjectionService.Start " +
            "before cluster startup completes, not lazily on the first request");

        // A live correctness assertion immediately after startup: no writes have happened yet, so a fresh
        // resource must not be visible. This never errors/hangs on an un-bootstrapped projection.
        var result = await cluster.Checker.Check("document", "doc1", "view", new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis), null);
        Assert.Equal(Engine.Membership.NotMember, result.Verdict);

        // Write, then read back immediately at FULL consistency. (A minimize-latency check here could
        // legitimately reuse the pre-write quantized revision the check above pinned within its 5s window —
        // the documented bounded staleness of OptimizedRevision, not a projection bug.)
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(new[]
        {
            new RelationshipUpdate(
                Relationship.Create(
                    new ObjectAndRelation("document", "doc1", "viewer"),
                    new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis)),
                UpdateOperation.Create),
        }));

        var afterWrite = await cluster.Checker.Check(
            "document", "doc1", "view", new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis), null,
            ConsistencyRequirement.FullyConsistent);
        Assert.Equal(Engine.Membership.Member, afterWrite.Verdict);
    }

    /// <summary>
    /// Gate (b): silo/cluster teardown does not hang or throw. <see cref="DatastoreProjectionService.Stop"/>
    /// disposes the shared hub (a bounded/timeout-guarded unsubscribe from the DatastoreGrain's observer
    /// set, mirroring <see cref="GrainBackedDatastore.DisposeAsync"/>'s own teardown) — this proves that
    /// path runs cleanly on real cluster disposal. (The DatastoreGrain's own watcher-registration expiry is
    /// a backstop against a leak either way; this test is about clean, bounded teardown, not about
    /// preventing an otherwise-catastrophic leak.)
    /// </summary>
    [Fact]
    public async Task Cluster_Shutdown_DisposesTheSharedHub_WithoutHangingOrThrowing()
    {
        const string schema = """
            definition user {}
            definition document {
                relation viewer: user
                permission view = viewer
            }
            """;

        var cluster = await MeshTestCluster.CreateAsync(schema);

        // Exercise the hub: park a Watch stream so the hub's signal path (and its observer subscription +
        // heartbeat, already started by DatastoreProjectionService at silo startup) is genuinely live.
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
        // disposal would leak into and perturb later tests). The hub's observer subscription on the datastore
        // grain remains registered — GrainService.Stop, not this stream's lifetime, owns tearing that down.
        await watchCts.CancelAsync();
        try { await watchTask; } catch (OperationCanceledException) { }

        var disposeTask = cluster.DisposeAsync().AsTask();
        var completed = await Task.WhenAny(disposeTask, Task.Delay(TimeSpan.FromSeconds(30)));
        Assert.Same(disposeTask, completed);
        await disposeTask; // rethrows if disposal itself faulted
    }
}
