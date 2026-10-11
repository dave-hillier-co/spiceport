using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Storage;
using Orleans.TestingHost;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Grain-level gates for interleaving pure reads (<see cref="IDatastoreGrain.GetHead"/>,
/// <see cref="IDatastoreGrain.ReadState"/>, <see cref="IDatastoreLog.ReadFrom"/>) past an in-flight
/// <see cref="IDatastoreGrain.Commit"/> on the cluster-singleton <see cref="DatastoreGrain"/>. Drives
/// the REAL grain through an Orleans <see cref="TestCluster"/>, with the keyed "datastore" storage provider
/// swapped for <see cref="PausableGrainStorage"/> so the sequencer's state write (the commit point) can
/// be parked mid-flight while a read is issued concurrently, and so a write can be made to fail once.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public sealed class DatastoreInterleavedReadTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

    private static Relationship Rel(string rid, string sid) =>
        Relationship.Create(new ObjectAndRelation("doc", rid, "viewer"), new ObjectAndRelation("user", sid, CoreConstants.Ellipsis));

    /// <summary>
    /// A compatibility-shape commit (ExpectedHead CAS, one resolved Touch, nothing else) — the same
    /// wire request <c>GrainBackedDatastore.ReadWriteTx</c> submits, so these gates exercise exactly
    /// the CAS-carrying write turn the production write path parks on.
    /// </summary>
    private static CommitRequest TouchCommit(string rid, string sid, long expectedHead) =>
        new(Array.Empty<CommitPreconditionWire>(),
            new[] { new RelationshipUpdateWire(RelationshipUpdateOpWire.Touch, WireConvert.ToWire(Rel(rid, sid))) },
            DeleteByFilter: null, SchemaBytes: null, ExpectedSchemaHash: null,
            Array.Empty<CounterDeltaWire>(), expectedHead);

    /// <summary>A declarative commit (no ExpectedHead) of one relationship update.</summary>
    private static CommitRequest UpdateCommit(RelationshipUpdateOpWire op, string rid, string sid) =>
        new(Array.Empty<CommitPreconditionWire>(),
            new[] { new RelationshipUpdateWire(op, WireConvert.ToWire(Rel(rid, sid))) },
            DeleteByFilter: null, SchemaBytes: null, ExpectedSchemaHash: null,
            Array.Empty<CounterDeltaWire>(), ExpectedHead: null);

    private static IDatastoreGrain Grain(TestCluster cluster) =>
        cluster.GrainFactory.GetGrain<IDatastoreGrain>(IDatastoreGrain.Key);

    private static async Task<TestCluster> NewClusterAsync()
    {
        Gate.Reset();
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<PausableSiloConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        return cluster;
    }

    private sealed class Scope(TestCluster cluster) : IAsyncDisposable
    {
        public TestCluster Cluster { get; } = cluster;
        public async ValueTask DisposeAsync() => await Cluster.DisposeAsync();
    }

    /// <summary>
    /// Process-wide gate state consulted by <see cref="PausableGrainStorage"/>. Safe as static state because
    /// every cluster-using test class (including this one, via <see cref="MeshClusterCollection"/>) is
    /// serialized by xUnit — never two of these tests run concurrently.
    /// </summary>
    private static class Gate
    {
        public static TaskCompletionSource<bool>? HeadWriteBlock;
        public static TaskCompletionSource<bool>? HeadWriteParked;
        public static volatile bool ThrowOnceOnHeadWrite;
        public static volatile bool ThrowOnceOnShardWrite;
        public static int DatastoreWrites;

        public static void Reset()
        {
            DatastoreWrites = 0;
            HeadWriteBlock = null;
            HeadWriteParked = null;
            ThrowOnceOnHeadWrite = false;
            ThrowOnceOnShardWrite = false;
        }
    }

    /// <summary>
    /// Wraps the in-memory grain-storage provider so a test can park the sequencer's state write — its
    /// commit point — mid-flight (an await that only resumes once the test releases the
    /// <see cref="Gate"/> TCS), or make the next sequencer or shard-projection write throw once. Reads
    /// (<c>ReadStateAsync</c>) are never gated — only the write path is under test here.
    /// </summary>
    private sealed class PausableGrainStorage(IGrainStorage inner) : IGrainStorage
    {
        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            inner.ReadStateAsync(stateName, grainId, grainState);

        public async Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            var grainType = grainId.Type.ToString()!;
            if (grainType == "graphshard" && Gate.ThrowOnceOnShardWrite)
            {
                Gate.ThrowOnceOnShardWrite = false;
                throw new InvalidOperationException("injected shard-projection write failure (test seam)");
            }

            if (grainType == "datastore")
            {
                Interlocked.Increment(ref Gate.DatastoreWrites);
                if (Gate.ThrowOnceOnHeadWrite)
                {
                    Gate.ThrowOnceOnHeadWrite = false;
                    throw new InvalidOperationException("injected head-write failure (test seam)");
                }

                if (Gate.HeadWriteBlock is { } headGate)
                {
                    Gate.HeadWriteParked?.TrySetResult(true);
                    await headGate.Task;
                }
            }

            await inner.WriteStateAsync(stateName, grainId, grainState);
        }

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            inner.ClearStateAsync(stateName, grainId, grainState);
    }

    private sealed class PausableSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder b)
        {
            // The pausable wrapper must sit over a provider with the binary grain-storage serializer
            // (production parity — the memory provider's Newtonsoft-JSON default cannot round-trip the
            // journaled state's immutable collections; see AddDatastoreGrainStorage).
            b.AddMemoryGrainStorage("datastore-inner", optionsBuilder =>
                optionsBuilder.Configure<Serializer>((options, serializer) =>
                    options.GrainStorageSerializer = new OrleansGrainStorageSerializer(serializer)));
            b.AddStateStorageBasedLogConsistencyProvider("StateStorage");
            // The sequencer reads and projects through the shard grains, whose placement strategy needs
            // its director wherever they can activate.
            b.AddGraphLocalityPlacement();
            b.ConfigureServices(services => services.AddKeyedSingleton<IGrainStorage>(
                "datastore",
                (sp, _) => new PausableGrainStorage(sp.GetRequiredKeyedService<IGrainStorage>("datastore-inner"))));
        }
    }

    /// <summary>
    /// Gate 1: with a write parked at the sequencer's state write (the commit point), <c>GetHead</c> and
    /// <c>ReadFrom</c> must still complete promptly — proving the pure reads interleave past the blocking
    /// write turn rather than queuing behind it on the non-reentrant activation. MUST FAIL on the
    /// unmodified grain (reads queue behind the parked write and the assertion times out) — verified red
    /// before adding <c>[AlwaysInterleave]</c>, green after.
    /// </summary>
    [Fact]
    public async Task Reads_complete_while_a_write_is_parked()
    {
        await using var scope = new Scope(await NewClusterAsync());
        var grain = Grain(scope.Cluster);
        var oldHead = (await grain.GetHead()).Head;

        Gate.HeadWriteBlock = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        Gate.HeadWriteParked = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var appendTask = grain.Commit(TouchCommit("a", "alice", oldHead));
        try
        {
            await Gate.HeadWriteParked.Task.WaitAsync(Timeout);

            var headTask = grain.GetHead();
            var readTask = grain.ReadFrom(oldHead, -1);
            var combined = Task.WhenAll((Task)headTask, readTask);
            var winner = await Task.WhenAny(combined, Task.Delay(Timeout));

            Assert.True(
                ReferenceEquals(winner, combined),
                "GetHead/ReadFrom did not complete while the head write was parked — reads are still queuing behind the write.");
        }
        finally
        {
            Gate.HeadWriteBlock.TrySetResult(true);
            await appendTask;
        }
    }

    /// <summary>
    /// Gate 2: a write parked mid-flight must never be observable by an interleaved reader. Once the
    /// write completes, the event and the advanced head become visible together (one consistent
    /// publication: both come from the one confirmed state).
    /// </summary>
    [Fact]
    public async Task Parked_write_is_invisible_until_the_head_commit()
    {
        await using var scope = new Scope(await NewClusterAsync());
        var grain = Grain(scope.Cluster);
        var oldHead = (await grain.GetHead()).Head;

        // Park at the sequencer's state write itself.
        Gate.HeadWriteBlock = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        Gate.HeadWriteParked = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var append1 = grain.Commit(TouchCommit("a", "alice", oldHead));
        long? newHead1;
        try
        {
            await Gate.HeadWriteParked.Task.WaitAsync(Timeout);

            var segmentDuringPark = await grain.ReadFrom(oldHead, -1).WaitAsync(Timeout);
            Assert.Empty(segmentDuringPark.Events);
            Assert.Equal(oldHead, segmentDuringPark.HeadRevision);

            var headDuringPark = await grain.GetHead().WaitAsync(Timeout);
            Assert.Equal(oldHead, headDuringPark.Head);
        }
        finally
        {
            Gate.HeadWriteBlock.TrySetResult(true);
            newHead1 = (await append1).Revision;
        }

        Assert.NotNull(newHead1);
        var segmentAfterCommit = await grain.ReadFrom(oldHead, -1);
        var committedEvent = Assert.Single(segmentAfterCommit.Events);
        Assert.Equal(newHead1, committedEvent.Revision);
        Assert.Equal(newHead1, segmentAfterCommit.HeadRevision);
    }

    /// <summary>
    /// Gate 3: a commit whose state write fails transiently must never leak a spurious event, and the
    /// log-consistency adaptor's internal retry RECOVERS the commit: the caller's <c>Commit</c>
    /// completes with the minted revision. This pins three things at once: the commit succeeds despite
    /// the one-shot fault, exactly ONE event surfaces, and the datastore is writable afterwards.
    /// </summary>
    [Fact]
    public async Task Failed_head_write_recovers_without_leaking_or_duplicating_events()
    {
        await using var scope = new Scope(await NewClusterAsync());
        var grain = Grain(scope.Cluster);
        var oldHead = (await grain.GetHead()).Head;

        Gate.ThrowOnceOnHeadWrite = true;
        var reply = await grain.Commit(TouchCommit("a", "alice", oldHead)).WaitAsync(Timeout);

        Assert.Null(reply.Failure);
        Assert.NotNull(reply.Revision);

        var head = await grain.GetHead().WaitAsync(Timeout);
        Assert.Equal(reply.Revision, head.Head);
        var segment = await grain.ReadFrom(oldHead, -1).WaitAsync(Timeout);
        var committed = Assert.Single(segment.Events);
        Assert.Equal(reply.Revision, committed.Revision);

        // Writable again afterwards — the half of the contract the wedged-retry era could not assert.
        var reply2 = await grain.Commit(TouchCommit("b", "bob", head.Head)).WaitAsync(Timeout);
        Assert.Null(reply2.Failure);
        Assert.NotNull(reply2.Revision);
        Assert.True(reply2.Revision > reply.Revision);
    }

    /// <summary>
    /// Gate 3b: a shard-projection write that fails at the FLUSH BOUNDARY (the 64th commit) must not fail
    /// the commit that triggered the flush — that commit is already journaled — and must lose nothing:
    /// the events stay pending (reads fold them over the partial projection), and the next flush replays
    /// them, idempotently for the shards that did persist. After a forced deactivation every key is
    /// still served from the projection.
    /// </summary>
    [Fact]
    public async Task Failed_projection_write_at_a_flush_boundary_keeps_events_pending_until_a_later_flush()
    {
        await using var scope = new Scope(await NewClusterAsync());
        var grain = Grain(scope.Cluster);
        var head = (await grain.GetHead()).Head;

        // 63 commits — the next commit fills the pending tail to the flush interval.
        for (var i = 0; i < 63; i++)
        {
            var reply = await grain.Commit(TouchCommit($"k{i}", $"u{i}", head));
            Assert.Null(reply.Failure);
            head = reply.Revision!.Value;
        }

        Gate.ThrowOnceOnShardWrite = true;
        var boundary = await grain.Commit(TouchCommit("k63", "u63", head)).WaitAsync(Timeout);
        Assert.Null(boundary.Failure);
        Assert.NotNull(boundary.Revision);
        Assert.False(Gate.ThrowOnceOnShardWrite); // the fault really fired

        // Nothing projected yet: the tail is still served from the pending state.
        var tail = await grain.ReadFrom(head - 1, -1);
        Assert.Contains(tail.Events, e => e.Revision == boundary.Revision);
        for (var i = 0; i < 64; i++)
        {
            var shard = await grain.ReadShard(GraphShardKeyWire.ForResource("doc", $"k{i}"));
            Assert.Equal($"u{i}", Assert.Single(shard.Rows, r => r.DeletedRevision is null).Relationship.SubjectId);
        }

        // A later flush replays the whole tail; then recovery reads every key back from projections.
        await grain.FlushProjection();
        var management = scope.Cluster.GrainFactory.GetGrain<IManagementGrain>(0);
        await management.ForceActivationCollection(TimeSpan.Zero);

        var headAfter = await grain.GetHead();
        Assert.Equal(boundary.Revision, headAfter.Head);
        for (var i = 0; i < 64; i++)
        {
            var projected = await scope.Cluster.GrainFactory
                .GetGrain<IGraphShardGrain>(GraphShardGrainKey.Build(GraphShardKeyWire.ForResource("doc", $"k{i}")))
                .ReadProjected();
            var live = Assert.Single(projected.Rows, r => r.DeletedRevision is null);
            Assert.Equal($"u{i}", live.Relationship.SubjectId);
        }
    }

    /// <summary>
    /// Gate 4: the CAS remains exact under concurrent writers even while reads hammer the activation
    /// concurrently — exactly one of two same-<c>expectedHead</c> commits succeeds.
    /// </summary>
    [Fact]
    public async Task Cas_still_exact_under_concurrent_writers()
    {
        await using var scope = new Scope(await NewClusterAsync());
        var grain = Grain(scope.Cluster);
        var head = (await grain.GetHead()).Head;

        using var cts = new CancellationTokenSource();
        var hammer = Task.Run(async () =>
        {
            while (!cts.IsCancellationRequested)
                await grain.GetHead();
        });

        var t1 = grain.Commit(TouchCommit("a", "alice", head));
        var t2 = grain.Commit(TouchCommit("b", "bob", head));
        var results = await Task.WhenAll(t1, t2).WaitAsync(Timeout);

        await cts.CancelAsync();
        await hammer;

        // Exactly one same-expectedHead commit wins; the loser is rejected as reply data (HeadMoved),
        // the same CAS invariant the retired two-step append surfaced as a null revision.
        Assert.Single(results, r => r.Revision is not null);
        Assert.Single(results, r => r.Failure is { Kind: CommitFailureKind.HeadMoved });
    }

    /// <summary>
    /// Group commit: commits that arrive while a write is parked are not queued behind it one write
    /// each. They are evaluated in order and appended together, so the sequencer's write count is two
    /// (the parked write, then one for the whole batch) rather than one per commit, and every commit
    /// still gets its own revision.
    /// </summary>
    [Fact]
    public async Task Commits_arriving_during_a_write_share_one_write()
    {
        await using var scope = new Scope(await NewClusterAsync());
        var grain = Grain(scope.Cluster);
        var oldHead = (await grain.GetHead()).Head;
        var writesBefore = Gate.DatastoreWrites;

        Gate.HeadWriteBlock = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        Gate.HeadWriteParked = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = grain.Commit(UpdateCommit(RelationshipUpdateOpWire.Touch, "first", "alice"));
        Task<CommitReply>[] batched;
        try
        {
            await Gate.HeadWriteParked.Task.WaitAsync(Timeout);
            Gate.HeadWriteParked = null;
            batched = Enumerable.Range(0, 5)
                .Select(i => grain.Commit(UpdateCommit(RelationshipUpdateOpWire.Touch, $"b{i}", "bob")))
                .ToArray();
            // Let every batched call reach the activation before the parked write resumes.
            await Task.Delay(TimeSpan.FromMilliseconds(500));
        }
        finally
        {
            Gate.HeadWriteBlock.TrySetResult(true);
            Gate.HeadWriteBlock = null;
        }

        var firstReply = await first.WaitAsync(Timeout);
        var replies = await Task.WhenAll(batched).WaitAsync(Timeout);

        Assert.Null(firstReply.Failure);
        Assert.All(replies, r => Assert.Null(r.Failure));
        var revisions = replies.Select(r => r.Revision!.Value).ToArray();
        Assert.Equal(revisions.Length, revisions.Distinct().Count());
        Assert.All(revisions, r => Assert.True(r > firstReply.Revision));
        Assert.Equal(writesBefore + 2, Gate.DatastoreWrites);

        var segment = await grain.ReadFrom(oldHead, -1);
        Assert.Equal(6, segment.Events.Count);
        Assert.Equal(revisions.Max(), segment.HeadRevision);
    }

    /// <summary>
    /// A commit evaluated later in a batch sees the commits before it in the same batch: two Creates of
    /// one relationship batched together still conflict, exactly one winning.
    /// </summary>
    [Fact]
    public async Task A_batched_commit_sees_the_commits_before_it_in_its_batch()
    {
        await using var scope = new Scope(await NewClusterAsync());
        var grain = Grain(scope.Cluster);

        Gate.HeadWriteBlock = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        Gate.HeadWriteParked = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = grain.Commit(UpdateCommit(RelationshipUpdateOpWire.Touch, "first", "alice"));
        Task<CommitReply>[] creates;
        try
        {
            await Gate.HeadWriteParked.Task.WaitAsync(Timeout);
            Gate.HeadWriteParked = null;
            creates = new[]
            {
                grain.Commit(UpdateCommit(RelationshipUpdateOpWire.Create, "x", "carol")),
                grain.Commit(UpdateCommit(RelationshipUpdateOpWire.Create, "x", "carol")),
            };
            await Task.Delay(TimeSpan.FromMilliseconds(500));
        }
        finally
        {
            Gate.HeadWriteBlock.TrySetResult(true);
            Gate.HeadWriteBlock = null;
        }

        await first.WaitAsync(Timeout);
        var replies = await Task.WhenAll(creates).WaitAsync(Timeout);
        Assert.Single(replies, r => r.Revision is not null);
        Assert.Single(replies, r => r.Failure is { Kind: CommitFailureKind.CreateAlreadyExists });
    }

    /// <summary>
    /// Writes run in arrival order through one queue: a GC round issued while a write is parked, and a
    /// commit issued after it, append in that order. The GC neither waits for every commit in flight to
    /// finish nor lets a later commit overtake it.
    /// </summary>
    [Fact]
    public async Task A_gc_round_queues_with_commits_in_arrival_order()
    {
        await using var scope = new Scope(await NewClusterAsync());
        var grain = Grain(scope.Cluster);
        var oldHead = (await grain.GetHead()).Head;

        Gate.HeadWriteBlock = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        Gate.HeadWriteParked = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = grain.Commit(UpdateCommit(RelationshipUpdateOpWire.Touch, "first", "alice"));
        Task<long?> gc;
        Task<CommitReply> after;
        try
        {
            await Gate.HeadWriteParked.Task.WaitAsync(Timeout);
            Gate.HeadWriteParked = null;
            gc = grain.RunGc();
            await Task.Delay(TimeSpan.FromMilliseconds(200));
            after = grain.Commit(UpdateCommit(RelationshipUpdateOpWire.Touch, "after", "bob"));
            await Task.Delay(TimeSpan.FromMilliseconds(500));
        }
        finally
        {
            Gate.HeadWriteBlock.TrySetResult(true);
            Gate.HeadWriteBlock = null;
        }

        await first.WaitAsync(Timeout);
        Assert.NotNull(await gc.WaitAsync(Timeout));
        Assert.Null((await after.WaitAsync(Timeout)).Failure);

        var segment = await grain.ReadFrom(oldHead, -1);
        Assert.Collection(
            segment.Events,
            e => Assert.Equal("first", Assert.Single(e.RelationshipChanges).Relationship.ResourceId),
            e => Assert.NotNull(e.GcFloor),
            e => Assert.Equal("after", Assert.Single(e.RelationshipChanges).Relationship.ResourceId));
    }

    /// <summary>
    /// A write waiting its turn does not hold the activation: with a commit's write parked and a GC round
    /// issued behind it, a non-interleaving call (<see cref="IDatastoreGrain.SubscribeWatch"/>, the watch
    /// hubs' heartbeat) still completes. Writes wait in the sequencer's own queue; a GC round waiting in
    /// the activation as its blocking request would hold every non-interleaving call behind the write.
    /// </summary>
    [Fact]
    public async Task A_queued_gc_round_does_not_block_the_watch_heartbeat()
    {
        await using var scope = new Scope(await NewClusterAsync());
        var grain = Grain(scope.Cluster);
        await grain.GetHead(); // activate first, so the parked write below is the commit's, not the seed's
        var watcher = scope.Cluster.GrainFactory.CreateObjectReference<IDatastoreWatcher>(new NullWatcher());

        Gate.HeadWriteBlock = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        Gate.HeadWriteParked = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = grain.Commit(UpdateCommit(RelationshipUpdateOpWire.Touch, "first", "alice"));
        Task<long?> gc;
        try
        {
            await Gate.HeadWriteParked.Task.WaitAsync(Timeout);
            Gate.HeadWriteParked = null;
            gc = grain.RunGc();
            await Task.Delay(TimeSpan.FromMilliseconds(200));

            var subscribe = grain.SubscribeWatch(watcher);
            var winner = await Task.WhenAny(subscribe, Task.Delay(Timeout));
            Assert.True(
                ReferenceEquals(winner, subscribe),
                "SubscribeWatch did not complete while a GC round waited behind a parked write.");
        }
        finally
        {
            Gate.HeadWriteBlock.TrySetResult(true);
            Gate.HeadWriteBlock = null;
        }

        await first.WaitAsync(Timeout);
        await gc.WaitAsync(Timeout);
    }

    private sealed class NullWatcher : IDatastoreWatcher
    {
        public Task HeadAdvanced(long head) => Task.CompletedTask;
        public Task SchemaAdvanced(byte[] schemaBytes, string storedHash) => Task.CompletedTask;
    }
}
