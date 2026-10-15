using System.Reflection;
using System.Text;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// The <see cref="LogWatchHub"/> schema-apply dedup: a stored hash is recorded as applied only once the
/// apply has succeeded, so a transient apply failure is retried by the next push (or heartbeat) carrying
/// the same hash instead of pinning the silo on its stale schema. Pure unit tests: the constructor never
/// touches the grain or the grain factory, and <see cref="LogWatchHub.SchemaAdvanced"/> only drives the
/// apply callback.
/// </summary>
public class LogWatchHubSchemaApplyTests
{
    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    [Fact]
    public async Task Failed_apply_is_retried_on_next_push_of_same_hash()
    {
        var invocations = 0;
        var applied = new List<string>();
        var hub = new LogWatchHub(grain: null!, grainFactory: null!, applySchema: text =>
        {
            invocations++;
            if (invocations == 1)
                throw new InvalidOperationException("transient provider failure");
            applied.Add(text);
        });

        await hub.SchemaAdvanced(Bytes("schema-A"), "h1");
        await hub.SchemaAdvanced(Bytes("schema-A"), "h1");

        Assert.Equal(2, invocations);
        Assert.Equal(["schema-A"], applied);
    }

    [Fact]
    public async Task Successful_apply_is_not_repeated_for_same_hash()
    {
        var invocations = 0;
        var hub = new LogWatchHub(grain: null!, grainFactory: null!, applySchema: _ => invocations++);

        await hub.SchemaAdvanced(Bytes("schema-A"), "h1");
        await hub.SchemaAdvanced(Bytes("schema-A"), "h1");

        Assert.Equal(1, invocations);
    }

    /// <summary>
    /// The heartbeat backstop: a failed apply leaves the hash unrecorded, so the next heartbeat whose
    /// <see cref="IDatastoreGrain.SubscribeWatch"/> reply carries the same stored hash fetches the bytes
    /// again via <see cref="IDatastoreGrain.ReadSchemaAt"/> and retries the apply.
    /// </summary>
    [Fact]
    public async Task Failed_apply_is_retried_by_next_heartbeat_reporting_same_hash()
    {
        var grain = new HeartbeatFakeGrain(head: 7, schemaHash: "h1", schemaBytes: Bytes("schema-A"));
        var invocations = 0;
        var applied = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var hub = new LogWatchHub(
            grain, ObserverOnlyGrainFactory.Create(), heartbeatInterval: TimeSpan.FromMilliseconds(10),
            applySchema: text =>
            {
                if (Interlocked.Increment(ref invocations) == 1)
                    throw new InvalidOperationException("transient provider failure");
                applied.TrySetResult(text);
            });

        hub.EnsureStarted();

        Assert.Equal("schema-A", await applied.Task.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.True(grain.ReadSchemaAtCalls >= 2, $"expected a second ReadSchemaAt, saw {grain.ReadSchemaAtCalls}");
    }

    /// <summary>
    /// An <see cref="IDatastoreGrain"/> whose heartbeat reply always reports the same head and stored schema
    /// hash, and whose <see cref="ReadSchemaAt"/> returns the matching bytes. Everything else is unused.
    /// </summary>
    private sealed class HeartbeatFakeGrain(long head, string schemaHash, byte[] schemaBytes) : IDatastoreGrain
    {
        private int _readSchemaAtCalls;

        public int ReadSchemaAtCalls => Volatile.Read(ref _readSchemaAtCalls);

        public Task<DatastoreHeadWire> SubscribeWatch(IDatastoreWatcher watcher) =>
            Task.FromResult(new DatastoreHeadWire(head, schemaHash));

        public Task UnsubscribeWatch(IDatastoreWatcher watcher) => Task.CompletedTask;

        public Task<byte[]?> ReadSchemaAt(long revision)
        {
            Interlocked.Increment(ref _readSchemaAtCalls);
            return Task.FromResult<byte[]?>(schemaBytes);
        }

        public Task<DatastoreHeadWire> GetHead() => Task.FromResult(new DatastoreHeadWire(head, schemaHash));
        public Task<DatastoreGrainState> ReadState() => throw new NotSupportedException();
        public Task<GraphShardState> ReadShard(GraphShardKeyWire key) => throw new NotSupportedException();
        public Task<ShardTailWire> ReadShardTail(GraphShardKeyWire key) => throw new NotSupportedException();
        public Task FlushProjection() => throw new NotSupportedException();
        public Task<CommitReply> Commit(CommitRequest request) => throw new NotSupportedException();
        public Task<LogSegment> ReadFrom(long afterRevision, int maxCount) => throw new NotSupportedException();
        public Task<long?> RunGc() => throw new NotSupportedException();
    }

    /// <summary>
    /// An <see cref="IGrainFactory"/> that supports only the observer-reference calls the hub makes: the
    /// "reference" is the observer itself (in-process, no runtime), and deletion is a no-op.
    /// </summary>
    public class ObserverOnlyGrainFactory : DispatchProxy
    {
        public static IGrainFactory Create() => Create<IGrainFactory, ObserverOnlyGrainFactory>();

        protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) => targetMethod?.Name switch
        {
            nameof(IGrainFactory.CreateObjectReference) => args![0],
            nameof(IGrainFactory.DeleteObjectReference) => null,
            _ => throw new NotSupportedException(targetMethod?.Name),
        };
    }
}
