using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.TestingHost;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;
using Microsoft.Extensions.Configuration;
using Spiceport.Server.Hosting;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Grain/mesh-level gates for reminder-driven MVCC garbage collection, driven through a real
/// Orleans <see cref="TestCluster"/> (the actual <see cref="DatastoreGrain"/>, not a fake).
/// <see cref="GcSiloConfigurator"/> uses <see cref="DatastoreGcOptions.Window"/> = <see cref="TimeSpan.Zero"/>,
/// which makes <see cref="IDatastoreGrain.RunGc"/> fully deterministic for collection/floor-rejection gates:
/// the computed floor is <c>min(head, now) == head</c> (revisions never exceed "now"), so ONE <c>RunGc()</c>
/// call collects everything dead as of the current head. Gates that instead need a cursor minted moments
/// ago to SURVIVE a GC run (the Watch-continuity risk case) use <see cref="RealWindowSiloConfigurator"/>'s
/// realistic multi-hour window instead, where the floor stays far in the past.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class DatastoreGcMeshTests
{
    private static Relationship Rel(string rid, string sid) =>
        Relationship.Create(new ObjectAndRelation("doc", rid, "viewer"), new ObjectAndRelation("user", sid, CoreConstants.Ellipsis));

    private static RelationshipUpdate Create(string rid, string sid) => new(Rel(rid, sid), UpdateOperation.Create);
    private static RelationshipUpdate Delete(string rid, string sid) => new(Rel(rid, sid), UpdateOperation.Delete);

    private static IDatastoreGrain Grain(TestCluster cluster) =>
        cluster.GrainFactory.GetGrain<IDatastoreGrain>(IDatastoreGrain.Key);

    private static async Task<SortedSet<string>> LiveIds(IDatastoreReader reader)
    {
        var set = new SortedSet<string>();
        await foreach (var r in reader.QueryRelationships(new RelationshipsFilter()))
            set.Add($"{r.Resource.ObjectId}:{r.Subject.ObjectId}");
        return set;
    }

    /// <summary>Aggressive, deterministic GC (Window=Zero) with the reminder wired but not relied upon
    /// (tests call <c>RunGc()</c> directly rather than waiting for the reminder to fire).</summary>
    private sealed class GcSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder b)
        {
            // The PRODUCTION "datastore" registration (in-memory branch): forces the binary grain-storage
            // serializer. The provider's Newtonsoft-JSON default cannot round-trip the meta state's
            // ImmutableDictionary key index (and silently corrupts boxed-JsonElement caveat context) —
            // see AddDatastoreGrainStorage.
            b.AddDatastoreGrainStorage(new ConfigurationBuilder().Build());
            b.AddCustomStorageBasedLogConsistencyProvider("CustomStorage");
            b.UseInMemoryReminderService();
            b.ConfigureServices(services => services.AddSingleton<IOptions<DatastoreGcOptions>>(
                Options.Create(new DatastoreGcOptions
                {
                    Window = TimeSpan.Zero,
                    ReminderEnabled = true,
                    ReminderPeriod = DatastoreGcOptions.MinimumReminderPeriod,
                })));
        }
    }

    /// <summary>Same as <see cref="GcSiloConfigurator"/> but with NO reminder service registered at all —
    /// proves activation still succeeds (the try/catch gate in <c>DatastoreGrain.OnActivateAsync</c>).</summary>
    private sealed class GcWithoutReminderServiceSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder b)
        {
            // The PRODUCTION "datastore" registration (in-memory branch): forces the binary grain-storage
            // serializer. The provider's Newtonsoft-JSON default cannot round-trip the meta state's
            // ImmutableDictionary key index (and silently corrupts boxed-JsonElement caveat context) —
            // see AddDatastoreGrainStorage.
            b.AddDatastoreGrainStorage(new ConfigurationBuilder().Build());
            b.AddCustomStorageBasedLogConsistencyProvider("CustomStorage");
            b.ConfigureServices(services => services.AddSingleton<IOptions<DatastoreGcOptions>>(
                Options.Create(new DatastoreGcOptions { Window = TimeSpan.Zero })));
        }
    }

    private static Task<TestCluster> NewClusterAsync<TConfigurator>() where TConfigurator : ISiloConfigurator, new()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<TConfigurator>();
        var cluster = builder.Build();
        return DeployAsync(cluster);
    }

    private static async Task<TestCluster> DeployAsync(TestCluster cluster)
    {
        await cluster.DeployAsync();
        return cluster;
    }

    private sealed class Scope(TestCluster cluster) : IAsyncDisposable
    {
        public TestCluster Cluster { get; } = cluster;
        public async ValueTask DisposeAsync() => await Cluster.DisposeAsync();
    }

    [Fact]
    public async Task RunGc_appends_a_log_event_advances_head_and_collects_dead_rows()
    {
        await using var scope = new Scope(await NewClusterAsync<GcSiloConfigurator>());
        await using var hub = IsolatedWatchHub.Create(scope.Cluster.GrainFactory);
        IDatastore ds = new GrainBackedDatastore(scope.Cluster.GrainFactory, hub);
        var grain = Grain(scope.Cluster);

        await ds.ReadWriteTx(tx => tx.WriteRelationships(new[] { Create("a", "alice"), Create("b", "bob") }));
        await ds.ReadWriteTx(tx => tx.WriteRelationships(new[] { Delete("a", "alice") })); // "a" now dead

        var headBefore = (await grain.GetHead()).Head;

        var floor = await grain.RunGc();

        Assert.NotNull(floor);
        var head = await grain.GetHead();
        Assert.True(head.Head > headBefore);
        Assert.Equal(floor, head.GcFloor);

        var state = await grain.ReadState();
        Assert.Equal(floor, state.GcFloor);
        Assert.DoesNotContain(state.Relationships, r => r.Relationship.ResourceId == "a"); // dead row collected
        Assert.Contains(state.Relationships, r => r.Relationship.ResourceId == "b");        // live row kept
    }

    /// <summary>
    /// The no-op guard (<c>floor &lt;= currentFloor</c> =&gt; return null, no event appended, head
    /// unchanged): with a retention window LONGER than elapsed Unix-epoch time, <c>now - window</c>
    /// underflows below the initial floor of 0 on every call, so <c>RunGc</c> never actually has anything
    /// new to collect. (A realistic window can never reach this branch on a live cluster — see the other
    /// tests in this file — because RunGc's own GC event keeps minting a fresh head, which itself becomes
    /// the next call's floor candidate. This deliberately-oversized window isolates the guard itself.)
    /// </summary>
    [Fact]
    public async Task RunGc_is_a_no_op_when_the_window_exceeds_elapsed_epoch_time()
    {
        await using var scope = new Scope(await NewClusterAsync<HugeWindowSiloConfigurator>());
        await using var hub = IsolatedWatchHub.Create(scope.Cluster.GrainFactory);
        IDatastore ds = new GrainBackedDatastore(scope.Cluster.GrainFactory, hub);
        var grain = Grain(scope.Cluster);

        await ds.ReadWriteTx(tx => tx.WriteRelationships(new[] { Create("a", "alice") }));
        await ds.ReadWriteTx(tx => tx.WriteRelationships(new[] { Delete("a", "alice") }));

        var headBefore = await grain.GetHead();

        var result = await grain.RunGc();

        Assert.Null(result);
        var headAfter = await grain.GetHead();
        Assert.Equal(headBefore.Head, headAfter.Head);   // no event was appended
        Assert.Equal(0, headAfter.GcFloor);
    }

    private sealed class HugeWindowSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder b)
        {
            // The PRODUCTION "datastore" registration (in-memory branch): forces the binary grain-storage
            // serializer. The provider's Newtonsoft-JSON default cannot round-trip the meta state's
            // ImmutableDictionary key index (and silently corrupts boxed-JsonElement caveat context) —
            // see AddDatastoreGrainStorage.
            b.AddDatastoreGrainStorage(new ConfigurationBuilder().Build());
            b.AddCustomStorageBasedLogConsistencyProvider("CustomStorage");
            b.ConfigureServices(services => services.AddSingleton<IOptions<DatastoreGcOptions>>(
                // Comfortably longer than elapsed Unix-epoch time (~56 years), safely under the long-range
                // overflow the ms->nanos conversion could hit at TimeSpan.MaxValue.
                Options.Create(new DatastoreGcOptions { Window = TimeSpan.FromDays(365 * 200), ReminderEnabled = false })));
        }
    }

    /// <summary>A realistic (multi-hour) retention window: RunGc still flows a GC event through the log,
    /// but the floor never approaches "just now", so a cursor minted moments ago stays valid.</summary>
    private sealed class RealWindowSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder b)
        {
            // The PRODUCTION "datastore" registration (in-memory branch): forces the binary grain-storage
            // serializer. The provider's Newtonsoft-JSON default cannot round-trip the meta state's
            // ImmutableDictionary key index (and silently corrupts boxed-JsonElement caveat context) —
            // see AddDatastoreGrainStorage.
            b.AddDatastoreGrainStorage(new ConfigurationBuilder().Build());
            b.AddCustomStorageBasedLogConsistencyProvider("CustomStorage");
            b.ConfigureServices(services => services.AddSingleton<IOptions<DatastoreGcOptions>>(
                Options.Create(new DatastoreGcOptions { Window = TimeSpan.FromHours(24), ReminderEnabled = false })));
        }
    }

    [Fact]
    public async Task Second_datastore_instance_observes_gc_results_through_the_grain()
    {
        await using var scope = new Scope(await NewClusterAsync<GcSiloConfigurator>());
        var gf = scope.Cluster.GrainFactory;
        // Two ISOLATED instances: "reader" can only observe rows (and their collection) through the
        // singleton grain's state, never by sharing "writer"'s in-memory transaction state.
        await using var writerHub = IsolatedWatchHub.Create(gf);
        await using var readerHub = IsolatedWatchHub.Create(gf);
        IDatastore writer = new GrainBackedDatastore(gf, writerHub);
        IDatastore reader = new GrainBackedDatastore(gf, readerHub); // a SEPARATE facade instance
        var grain = Grain(scope.Cluster);

        var rev1 = await writer.ReadWriteTx(tx => tx.WriteRelationships(new[] { Create("a", "alice") }));

        // A pre-GC reader on the second instance sees the row live at its revision.
        var before = await LiveIds(reader.SnapshotReader(rev1));
        Assert.Contains("a:alice", before);

        await writer.ReadWriteTx(tx => tx.WriteRelationships(new[] { Delete("a", "alice") }));
        var floor = await grain.RunGc();
        Assert.NotNull(floor);

        var head = await writer.HeadRevision();
        var after = await LiveIds(reader.SnapshotReader(head.Revision));
        Assert.DoesNotContain("a:alice", after);
    }

    [Fact]
    public async Task Reader_pinned_below_the_floor_is_rejected_on_first_read()
    {
        // The floor rejection is the MvccSnapshotReader ctor guard over the state fetched from the
        // sequencer on the reader's FIRST query (see GrainBackedDatastore.SnapshotReader). That state
        // always carries the sequencer's current GcFloor, so a below-floor pin is rejected immediately —
        // there is no deferred-error window (that bounded-lag contract belonged to the retired per-silo
        // projection; the per-shard analogue is pinned by ShardedReaderEquivalenceTests'
        // Gc_Floor_Is_Enforced_Through_The_Shard_Grain).
        await using var scope = new Scope(await NewClusterAsync<GcSiloConfigurator>());
        await using var hub = IsolatedWatchHub.Create(scope.Cluster.GrainFactory);
        IDatastore ds = new GrainBackedDatastore(scope.Cluster.GrainFactory, hub);
        var grain = Grain(scope.Cluster);

        var oldHead = await ds.HeadRevision(); // strictly before the write below, so below the post-GC floor
        await ds.ReadWriteTx(tx => tx.WriteRelationships(new[] { Create("a", "alice") }));

        var floor = await grain.RunGc();
        Assert.NotNull(floor);
        Assert.True(floor > ((TimestampRevision)oldHead.Revision).TimestampNanosSinceEpoch);

        var reader = ds.SnapshotReader(oldHead.Revision);
        await Assert.ThrowsAsync<RevisionNotFoundException>(async () =>
        {
            await foreach (var _ in reader.QueryRelationships(new RelationshipsFilter()))
            {
            }
        });

        // A reader pinned AT (or above) the floor still serves: the retained rows are intact.
        var head = await ds.HeadRevision();
        var live = await LiveIds(ds.SnapshotReader(head.Revision));
        Assert.Contains("a:alice", live);
    }

    [Fact]
    public async Task Watch_parked_before_a_gc_event_keeps_working_and_the_gc_event_itself_emits_nothing()
    {
        // A REAL (multi-hour) window: RunGc still mints a GC LogEvent (advancing the head, exercising the
        // Watch-over-a-content-free-event path), but its floor stays far in the past, so the cursor parked
        // moments ago is never retroactively invalidated (unlike the aggressive Window=Zero clusters used
        // to test the floor-rejection behavior itself, below).
        await using var scope = new Scope(await NewClusterAsync<RealWindowSiloConfigurator>());
        var gf = scope.Cluster.GrainFactory;
        await using var watcherHub = IsolatedWatchHub.Create(gf);
        await using var committerHub = IsolatedWatchHub.Create(gf);
        var watcher = new GrainBackedDatastore(gf, watcherHub);
        var committer = new GrainBackedDatastore(gf, committerHub);
        var grain = Grain(scope.Cluster);

        var head = (await watcher.HeadRevision()).Revision;

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var first = new TaskCompletionSource<RevisionChange>(TaskCreationOptions.RunContinuationsAsynchronously);
        var consume = Task.Run(async () =>
        {
            try
            {
                await foreach (var change in watcher.Watch(head, new WatchOptions(WatchContent.Relationships), cts.Token))
                {
                    first.TrySetResult(change);
                    break;
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                first.TrySetException(ex);
            }
        });

        // Let the stream park (and the hub's heartbeat register the observer), THEN run GC. A GC event
        // carries no relationship content, so it must not surface as a Watch item on its own.
        await Task.Delay(1200, cts.Token);
        var floor = await grain.RunGc();
        Assert.NotNull(floor);

        // A commit made AFTER the GC event must still be delivered to the consumer parked before it.
        await committer.ReadWriteTx(tx => tx.WriteRelationships(new[] { Create("z", "zed") }));

        var observed = await first.Task.WaitAsync(TimeSpan.FromSeconds(20), cts.Token);
        await cts.CancelAsync();
        try { await consume; } catch (OperationCanceledException) { }

        var change = Assert.Single(observed.RelationshipChanges);
        Assert.Equal("z", change.Relationship.Resource.ObjectId);
    }

    [Fact]
    public async Task New_watch_from_a_cursor_below_the_floor_throws()
    {
        await using var scope = new Scope(await NewClusterAsync<GcSiloConfigurator>());
        await using var hub = IsolatedWatchHub.Create(scope.Cluster.GrainFactory);
        IDatastore ds = new GrainBackedDatastore(scope.Cluster.GrainFactory, hub);
        var grain = Grain(scope.Cluster);

        var oldHead = await ds.HeadRevision();
        await ds.ReadWriteTx(tx => tx.WriteRelationships(new[] { Create("a", "alice") }));

        var floor = await grain.RunGc();
        Assert.NotNull(floor);

        await Assert.ThrowsAsync<RevisionNotFoundException>(async () =>
        {
            await foreach (var _ in ds.Watch(oldHead.Revision, new WatchOptions(WatchContent.Relationships)))
                break;
        });
    }

    /// <summary>A short (millisecond) nominal window, reminder disabled — so <c>RunGc</c> is never invoked
    /// and the REAL <c>GcFloor</c> stays 0 for the whole test. Any rejection observed here can only come
    /// from <see cref="GrainBackedDatastore"/>'s own nominal-window check, which is the regression surface
    /// for the bug where that check was hardcoded to 24h independent of <see cref="DatastoreGcOptions"/>.</summary>
    private sealed class ShortWindowSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder b)
        {
            // The PRODUCTION "datastore" registration (in-memory branch): forces the binary grain-storage
            // serializer. The provider's Newtonsoft-JSON default cannot round-trip the meta state's
            // ImmutableDictionary key index (and silently corrupts boxed-JsonElement caveat context) —
            // see AddDatastoreGrainStorage.
            b.AddDatastoreGrainStorage(new ConfigurationBuilder().Build());
            b.AddCustomStorageBasedLogConsistencyProvider("CustomStorage");
            b.ConfigureServices(services => services.AddSingleton<IOptions<DatastoreGcOptions>>(
                Options.Create(new DatastoreGcOptions
                {
                    Window = TimeSpan.FromMilliseconds(50),
                    ReminderEnabled = false,
                })));
        }
    }

    /// <summary>
    /// Regression test for the bug where <see cref="GrainBackedDatastore"/> AND'd the real <c>GcFloor</c>
    /// with its OWN hardcoded 24h nominal window, completely independent of the configured
    /// <see cref="DatastoreGcOptions.Window"/>. Wiring <see cref="GrainBackedDatastore"/> with the SAME
    /// <see cref="IOptions{TOptions}"/> the grain silo is configured with (as production now does — see
    /// <c>Spiceport.Api</c>/<c>Spiceport.Silo</c>'s <c>Program.cs</c>) means a 50ms window rejects a
    /// revision once the HEAD has moved more than 50ms past it, EVEN THOUGH the real <c>GcFloor</c> is
    /// still 0 (no reminder, no manual <c>RunGc</c> ever ran) — proving the nominal window actually tracks
    /// the configured value rather than a stale, independently-hardcoded default. (The nominal-window bound
    /// is checked against the current HEAD, not wall-clock "now", so the head must actually advance past
    /// the window for the check to bite — a second write after the delay does that.)
    /// </summary>
    [Fact]
    public async Task CheckRevision_and_Watch_honor_the_configured_window_not_a_hardcoded_default()
    {
        await using var scope = new Scope(await NewClusterAsync<ShortWindowSiloConfigurator>());
        var gf = scope.Cluster.GrainFactory;
        var services = ((InProcessSiloHandle)scope.Cluster.Primary!).SiloHost.Services;
        var gcOptions = services.GetRequiredService<IOptions<DatastoreGcOptions>>();
        var grain = Grain(scope.Cluster);

        await using var hub = IsolatedWatchHub.Create(gf);
        IDatastore ds = new GrainBackedDatastore(gf, hub, gcOptions: gcOptions);

        var revision = await ds.ReadWriteTx(tx => tx.WriteRelationships(new[] { Create("a", "alice") }));

        // Immediately: within the 50ms window, so both checks pass.
        Assert.True(await ds.CheckRevision(revision));

        await Task.Delay(TimeSpan.FromMilliseconds(300));

        // Advance the head with a second write, minted from the current (300ms-later) wall clock, so the
        // nominal window's "head - window" bound moves past the first revision. The real GcFloor never
        // moved (no reminder, no manual RunGc) — the data is still fully present — but the nominal window
        // has now elapsed relative to head, so CheckRevision must reject the stale revision and Watch must
        // refuse to resume from it, exactly as it would for ReferenceDatastore/SpiceDB API-parity.
        await ds.ReadWriteTx(tx => tx.WriteRelationships(new[] { Create("b", "bob") }));

        Assert.Equal(0, (await grain.GetHead()).GcFloor);
        Assert.False(await ds.CheckRevision(revision));
        await Assert.ThrowsAsync<RevisionNotFoundException>(async () =>
        {
            await foreach (var _ in ds.Watch(revision, new WatchOptions(WatchContent.Relationships)))
                break;
        });
    }

    [Fact]
    public async Task Reminder_is_registered_after_activation()
    {
        await using var scope = new Scope(await NewClusterAsync<GcSiloConfigurator>());
        var grain = Grain(scope.Cluster);

        // Force activation (any call does), then inspect the reminder table directly — the reminder
        // registration itself is not observable through the grain's own public surface.
        _ = await grain.GetHead();

        var services = ((InProcessSiloHandle)scope.Cluster.Primary!).SiloHost.Services;
        var reminderTable = services.GetRequiredService<IReminderTable>();
        var grainId = ((GrainReference)grain).GrainId;

        var entry = await reminderTable.ReadRow(grainId, "mvcc-gc");

        Assert.NotNull(entry);
        Assert.Equal("mvcc-gc", entry.ReminderName);
        Assert.Equal(DatastoreGcOptions.MinimumReminderPeriod, entry.Period);
    }

    [Fact]
    public async Task Activation_without_a_reminder_service_does_not_throw()
    {
        // The try/catch gate in DatastoreGrain.OnActivateAsync: no reminder service is configured on this
        // cluster at all, yet the grain must still activate and serve calls normally.
        await using var scope = new Scope(await NewClusterAsync<GcWithoutReminderServiceSiloConfigurator>());
        await using var hub = IsolatedWatchHub.Create(scope.Cluster.GrainFactory);
        IDatastore ds = new GrainBackedDatastore(scope.Cluster.GrainFactory, hub);
        var grain = Grain(scope.Cluster);

        await ds.ReadWriteTx(tx => tx.WriteRelationships(new[] { Create("a", "alice") }));
        var floor = await grain.RunGc(); // RunGc itself remains fully usable without the reminder service
        Assert.NotNull(floor);
    }
}
