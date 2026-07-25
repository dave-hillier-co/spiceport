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
/// swapped for <see cref="PausableGrainStorage"/> so a write can be parked mid-flight (at the <c>head</c>
/// write, or at a <c>log/{n}</c> write) while a read is issued concurrently.
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
        public static TaskCompletionSource<bool>? LogWriteBlock;
        public static TaskCompletionSource<bool>? LogWriteParked;
        public static volatile bool ThrowOnceOnHeadWrite;

        public static void Reset()
        {
            HeadWriteBlock = null;
            HeadWriteParked = null;
            LogWriteBlock = null;
            LogWriteParked = null;
            ThrowOnceOnHeadWrite = false;
        }
    }

    /// <summary>
    /// Wraps the in-memory grain-storage provider so a test can park a <c>head</c> or <c>log/{n}</c> write
    /// mid-flight (an await that only resumes once the test releases the corresponding <see cref="Gate"/>
    /// TCS), or make the next <c>head</c> write throw once. Reads (<c>ReadStateAsync</c>) are never gated —
    /// only the write path is under test here.
    /// </summary>
    private sealed class PausableGrainStorage(IGrainStorage inner) : IGrainStorage
    {
        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
            inner.ReadStateAsync(stateName, grainId, grainState);

        public async Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            if (stateName == "head")
            {
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
            else if (stateName.StartsWith("log/", StringComparison.Ordinal) && Gate.LogWriteBlock is { } logGate)
            {
                Gate.LogWriteParked?.TrySetResult(true);
                await logGate.Task;
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
            // meta state's ImmutableDictionary key index; see AddDatastoreGrainStorage).
            b.AddMemoryGrainStorage("datastore-inner", optionsBuilder =>
                optionsBuilder.Configure<Serializer>((options, serializer) =>
                    options.GrainStorageSerializer = new OrleansGrainStorageSerializer(serializer)));
            b.AddCustomStorageBasedLogConsistencyProvider("CustomStorage");
            b.ConfigureServices(services => services.AddKeyedSingleton<IGrainStorage>(
                "datastore",
                (sp, _) => new PausableGrainStorage(sp.GetRequiredKeyedService<IGrainStorage>("datastore-inner"))));
        }
    }

    /// <summary>
    /// Gate 1: with a write parked at the <c>head</c> storage write (the commit point), <c>GetHead</c> and
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
    /// Gate 2: a write parked mid-flight must never be observable by an interleaved reader — neither at the
    /// <c>head</c> write (log entries already durable) nor at a <c>log/{n}</c> write (nothing durable yet
    /// for this commit). Once the write completes, the event and the advanced head become visible together
    /// (one consistent publication).
    /// </summary>
    [Fact]
    public async Task Parked_write_is_invisible_until_the_head_commit()
    {
        await using var scope = new Scope(await NewClusterAsync());
        var grain = Grain(scope.Cluster);
        var oldHead = (await grain.GetHead()).Head;

        // Phase 1: park at the head write itself.
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

        // Phase 2: repeat, this time parking at the log-entry write (nothing durable for this commit yet).
        Gate.HeadWriteBlock = null;
        Gate.HeadWriteParked = null;
        Gate.LogWriteBlock = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        Gate.LogWriteParked = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var append2 = grain.Commit(TouchCommit("b", "bob", newHead1!.Value));
        long? newHead2;
        try
        {
            await Gate.LogWriteParked.Task.WaitAsync(Timeout);

            var segmentDuringLogPark = await grain.ReadFrom(newHead1.Value, -1).WaitAsync(Timeout);
            Assert.Empty(segmentDuringLogPark.Events);

            var headDuringLogPark = await grain.GetHead().WaitAsync(Timeout);
            Assert.Equal(newHead1, headDuringLogPark.Head);
        }
        finally
        {
            Gate.LogWriteBlock.TrySetResult(true);
            newHead2 = (await append2).Revision;
        }

        Assert.NotNull(newHead2);
        var segmentFinal = await grain.ReadFrom(newHead1.Value, -1);
        var finalEvent = Assert.Single(segmentFinal.Events);
        Assert.Equal(newHead2, finalEvent.Revision);
        Assert.Equal(newHead2, segmentFinal.HeadRevision);
    }

    /// <summary>
    /// Gate 3: a commit whose <c>head</c> write fails transiently must never leak a spurious event — and,
    /// because the log-row writes tolerate overwriting their own orphan (the write-first, ETag-tolerant-
    /// fallback discipline), the storage adaptor's internal retry now RECOVERS the commit: it re-writes
    /// the same log entry over the orphan, retries the head write, and the caller's <c>Commit</c>
    /// completes with the minted revision. This pins three things at once: the commit succeeds despite
    /// the one-shot fault, exactly ONE event surfaces (the orphan overwrite duplicated nothing), and the
    /// datastore is writable afterwards. An earlier version of this gate pinned the pre-recovery
    /// behavior instead — the retry wedged forever on its own write-once orphan, so it could only assert
    /// the stuck-but-not-leaking half of this contract and had to document the wedge as a known wart.
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
    /// Gate 3b: the same one-shot head-write fault as Gate 3, landed exactly ON a FLUSH BOUNDARY (the
    /// 64th commit). The failed attempt has already written its shard rows, the rotated index-bucket
    /// pair, the delta row and the meta row — all orphans at versioned names — and the adaptor's retry
    /// re-runs the SAME boundary: same log slot, same flush version, same rotated bucket, overwriting
    /// its own orphans via the read-then-write discipline. Pins that the boundary commit succeeds and
    /// that the index the retried flush committed is CORRECT: after a forced deactivation, recovery
    /// reconstructs the store THROUGH that index, and every pre-boundary key must still be served (a
    /// referenced-but-wrong bucket/delta/shard row would lose keys or fail the recovery loudly).
    /// </summary>
    [Fact]
    public async Task Failed_head_write_at_a_flush_boundary_retries_the_same_bucket_and_commits_a_correct_index()
    {
        await using var scope = new Scope(await NewClusterAsync());
        var grain = Grain(scope.Cluster);
        var head = (await grain.GetHead()).Head;

        // 63 commits bring the contiguous log to version 63 — the next commit is the flush boundary.
        for (var i = 0; i < 63; i++)
        {
            var reply = await grain.Commit(TouchCommit($"k{i}", $"u{i}", head));
            Assert.Null(reply.Failure);
            head = reply.Revision!.Value;
        }

        Gate.ThrowOnceOnHeadWrite = true;
        var boundary = await grain.Commit(TouchCommit("k63", "u63", head)).WaitAsync(Timeout);
        Assert.Null(boundary.Failure);
        Assert.NotNull(boundary.Revision);

        // The flush really committed on the retry: the durable head names the boundary as its flush.
        var storage = ((InProcessSiloHandle)scope.Cluster.Primary!).SiloHost.Services
            .GetRequiredKeyedService<IGrainStorage>("datastore");
        var grainId = ((GrainReference)grain).GrainId;
        var headRow = new GrainState<LogHeadEntry>();
        await storage.ReadStateAsync("head", grainId, headRow);
        Assert.True(headRow.RecordExists);
        Assert.Equal(64, headRow.State!.LogVersion);
        Assert.Equal(64, headRow.State.SnapshotVersion);

        // Recovery THROUGH the retried flush's index: drop the activation and re-read every key.
        var management = scope.Cluster.GrainFactory.GetGrain<IManagementGrain>(0);
        await management.ForceActivationCollection(TimeSpan.Zero);

        var headAfter = await grain.GetHead();
        Assert.Equal(boundary.Revision, headAfter.Head);
        for (var i = 0; i < 64; i++)
        {
            var shard = await grain.ReadShard(GraphShardKeyWire.ForResource("doc", $"k{i}"));
            var live = Assert.Single(shard.Rows, r => r.DeletedRevision is null);
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
}
