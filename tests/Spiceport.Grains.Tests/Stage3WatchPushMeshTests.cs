using System.Diagnostics;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.TestingHost;
using Spiceport.Core;
using Spiceport.Datastore;
using Microsoft.Extensions.Configuration;
using Spiceport.Server.Hosting;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Gates for the PUSH-driven Watch feed: the per-silo <see cref="LogWatchHub"/> registers an
/// <see cref="Abstractions.IDatastoreWatcher"/> observer on the datastore grain, so a commit from anywhere
/// wakes parked streams by notification, with the hub's slow heartbeat only as the missed-push backstop.
/// Proves (a) PUSH: a commit through a DIFFERENT datastore instance (no local pulse) is observed far faster
/// than the fallback heartbeat could deliver it; (b) LIVENESS ACROSS REACTIVATION: deactivating the datastore
/// grain (which empties its in-memory observer set) never wedges a parked stream — the heartbeat resubscribes
/// and pulses the head it missed.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class Stage3WatchPushMeshTests
{
    /// <summary>
    /// Gate (a): the watcher's fallback heartbeat is set far beyond the assertion bound, and the commit goes
    /// through a SEPARATE GrainBackedDatastore instance (so the watcher's hub gets no same-instance pulse).
    /// An observation inside the bound can therefore only have arrived by observer push.
    /// </summary>
    [Fact]
    public async Task Commit_from_another_instance_wakes_a_parked_watch_by_push()
    {
        await using var scope = new Scope(await NewDatastoreClusterAsync());
        var gf = scope.Cluster.GrainFactory;

        await using var watcherHub = IsolatedWatchHub.Create(gf, heartbeatInterval: TimeSpan.FromSeconds(30));
        await using var committerHub = IsolatedWatchHub.Create(gf);
        var watcher = new GrainBackedDatastore(gf, watcherHub);
        var committer = new GrainBackedDatastore(gf, committerHub);

        var head = (await watcher.HeadRevision()).Revision;

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var first = new TaskCompletionSource<RevisionChange>(TaskCreationOptions.RunContinuationsAsynchronously);
        var consume = Task.Run(async () =>
        {
            await foreach (var change in watcher.Watch(head, new WatchOptions(WatchContent.Relationships), cts.Token))
            {
                first.TrySetResult(change);
                break;
            }
        });

        // Let the stream park and the hub's first heartbeat register the observer, then commit and time it.
        await Task.Delay(500, cts.Token);
        var sw = Stopwatch.StartNew();
        await committer.ReadWriteTx(tx => tx.WriteRelationships(new[] { Create("doc1", "alice") }));

        var observed = await first.Task.WaitAsync(TimeSpan.FromSeconds(10), cts.Token);
        sw.Stop();
        await cts.CancelAsync();
        try { await consume; } catch (OperationCanceledException) { }

        Assert.Single(observed.RelationshipChanges);
        Assert.Equal("doc1", observed.RelationshipChanges[0].Relationship.Resource.ObjectId);
        Assert.True(
            sw.Elapsed < TimeSpan.FromSeconds(5),
            $"observed after {sw.ElapsedMilliseconds}ms — beyond push latency, and the 30s fallback cannot have delivered it");
    }

    /// <summary>
    /// Gate (b): force-deactivating the datastore grain empties its in-memory observer set; the next commit
    /// (again via a separate instance, so no local pulse) reactivates it with NO watchers registered. The
    /// parked stream must still observe the change: the hub's heartbeat resubscribes and pulses the returned
    /// head. This is the liveness guarantee that makes best-effort push safe.
    /// </summary>
    [Fact]
    public async Task Grain_reactivation_never_wedges_a_parked_stream()
    {
        await using var scope = new Scope(await NewDatastoreClusterAsync());
        var gf = scope.Cluster.GrainFactory;

        await using var watcherHub = IsolatedWatchHub.Create(gf);
        await using var committerHub = IsolatedWatchHub.Create(gf);
        var watcher = new GrainBackedDatastore(gf, watcherHub);
        var committer = new GrainBackedDatastore(gf, committerHub);

        var head = (await watcher.HeadRevision()).Revision;

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var first = new TaskCompletionSource<RevisionChange>(TaskCreationOptions.RunContinuationsAsynchronously);
        var consume = Task.Run(async () =>
        {
            await foreach (var change in watcher.Watch(head, new WatchOptions(WatchContent.Relationships), cts.Token))
            {
                first.TrySetResult(change);
                break;
            }
        });

        // Park the stream, then drop every activation (and with it the grain's observer registrations).
        await Task.Delay(500, cts.Token);
        await gf.GetGrain<IManagementGrain>(0).ForceActivationCollection(TimeSpan.Zero);

        await committer.ReadWriteTx(tx => tx.WriteRelationships(new[] { Create("doc2", "bob") }));

        var observed = await first.Task.WaitAsync(TimeSpan.FromSeconds(15), cts.Token);
        await cts.CancelAsync();
        try { await consume; } catch (OperationCanceledException) { }

        Assert.Single(observed.RelationshipChanges);
        Assert.Equal("doc2", observed.RelationshipChanges[0].Relationship.Resource.ObjectId);
    }

    // --- helpers ---

    private static RelationshipUpdate Create(string rid, string sid) => new(
        Relationship.Create(
            new ObjectAndRelation("document", rid, "viewer"),
            new ObjectAndRelation("user", sid, CoreConstants.Ellipsis)),
        UpdateOperation.Create);

    private static async Task<TestCluster> NewDatastoreClusterAsync()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<DatastoreSiloConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        return cluster;
    }

    private sealed class DatastoreSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder b)
        {
            // The PRODUCTION "datastore" registration (in-memory branch): forces the binary grain-storage
            // serializer. The provider's Newtonsoft-JSON default cannot round-trip the meta state's
            // ImmutableDictionary key index (and silently corrupts boxed-JsonElement caveat context) —
            // see AddDatastoreGrainStorage.
            b.AddDatastoreGrainStorage(new ConfigurationBuilder().Build());
            b.AddCustomStorageBasedLogConsistencyProvider("CustomStorage");
        }
    }

    private sealed class Scope(TestCluster cluster) : IAsyncDisposable
    {
        public TestCluster Cluster { get; } = cluster;
        public async ValueTask DisposeAsync() => await Cluster.DisposeAsync();
    }
}
