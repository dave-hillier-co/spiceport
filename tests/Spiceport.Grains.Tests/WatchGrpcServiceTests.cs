using System.Threading.Channels;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Spiceport.Api;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains;
using Spiceport.Protos;
using RelationshipUpdate = Spiceport.Protos.RelationshipUpdate;
using Relationship = Spiceport.Protos.Relationship;
using WireRelationship = Spiceport.Grains.Abstractions.RelationshipWire;
using WireUpdate = Spiceport.Grains.Abstractions.RelationshipUpdateWire;
using WireOp = Spiceport.Grains.Abstractions.RelationshipUpdateOpWire;
using WireWriteArgs = Spiceport.Grains.Abstractions.WriteRelationshipsArgs;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Drives the server-streaming <see cref="WatchGrpcService"/> IN-PROCESS (no Kestrel): a fake
/// <see cref="IServerStreamWriter{T}"/> collects emitted responses and a <see cref="CancellationToken"/>
/// stops the stream. Verifies: tailing from head sees a subsequent write with its token; resuming from a
/// pre-write token replays the write exactly once; cancellation terminates the stream.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class WatchGrpcServiceTests
{
    private const string Schema = """
        definition user {}
        definition document {
            relation viewer: user
            permission view = viewer
        }
        """;

    private static WatchGrpcService Service(MeshTestCluster cluster) =>
        new(cluster.Datastore, cluster.Services.GetRequiredService<ISchemaProvider>());

    private static Task WriteViewer(MeshTestCluster cluster, string doc, string user)
    {
        var update = new WireUpdate(
            WireOp.Touch,
            new WireRelationship("document", doc, "viewer", "user", user, "...", null, null, null));
        return cluster.Relationships.WriteRelationships(new WireWriteArgs(new List<WireUpdate> { update }));
    }

    [Fact]
    public async Task Watch_from_head_sees_subsequent_write_with_token()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new CollectingStreamWriter<WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);

        // No start cursor -> tail from head (future writes only).
        var watchTask = service.Watch(new WatchRequest(), writer, ctx);

        await WriteViewer(cluster, "doc1", "alice");

        var response = await writer.WaitForNext(TimeSpan.FromSeconds(10), watchTask);
        cts.Cancel();
        await watchTask;

        Assert.False(string.IsNullOrEmpty(response.ChangesThrough.Token));
        var update = Assert.Single(response.Updates);
        Assert.Equal(RelationshipUpdate.Types.Operation.Touch, update.Operation);
        Assert.Equal("doc1", update.Relationship.Resource.ObjectId);
        Assert.Equal("alice", update.Relationship.Subject.Object.ObjectId);
    }

    [Fact]
    public async Task Watch_resumes_from_pre_write_token_and_replays_the_write()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        // Capture a token at head BEFORE the write.
        var preWrite = await cluster.Datastore.HeadRevision(CancellationToken.None);
        var datastoreId = await cluster.Datastore.GetUniqueId(CancellationToken.None);
        var startToken = ZedTokens.FromRevision(preWrite.Revision, preWrite.SchemaHash, datastoreId);

        // Write AFTER capturing the cursor.
        await WriteViewer(cluster, "doc2", "bob");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new CollectingStreamWriter<WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);

        var request = new WatchRequest { OptionalStartCursor = new Spiceport.Protos.ZedToken { Token = startToken.Token } };
        var watchTask = service.Watch(request, writer, ctx);

        // The resumed stream must replay the already-committed write.
        var response = await writer.WaitForNext(TimeSpan.FromSeconds(10));
        cts.Cancel();
        await watchTask;

        var update = Assert.Single(response.Updates);
        Assert.Equal("doc2", update.Relationship.Resource.ObjectId);
        Assert.Equal("bob", update.Relationship.Subject.Object.ObjectId);
    }

    [Fact]
    public async Task Watch_cancellation_stops_the_stream()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        using var cts = new CancellationTokenSource();
        var writer = new CollectingStreamWriter<WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);

        var watchTask = service.Watch(new WatchRequest(), writer, ctx);

        // No writes -> the stream is parked waiting. Cancelling must complete the task promptly.
        cts.Cancel();
        await watchTask.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.True(watchTask.IsCompletedSuccessfully);
        Assert.Empty(writer.Collected);
    }

    /// <summary>A fake server stream writer that records responses and signals each arrival.</summary>
    private sealed class CollectingStreamWriter<T> : IServerStreamWriter<T>
    {
        private readonly Channel<T> _channel = Channel.CreateUnbounded<T>();
        public List<T> Collected { get; } = [];

        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(T message)
        {
            Collected.Add(message);
            _channel.Writer.TryWrite(message);
            return Task.CompletedTask;
        }

        public async Task<T> WaitForNext(TimeSpan timeout, Task? watchTask = null)
        {
            using var cts = new CancellationTokenSource(timeout);
            var read = _channel.Reader.ReadAsync(cts.Token).AsTask();
            if (watchTask is not null)
            {
                var done = await Task.WhenAny(read, watchTask);
                if (done == watchTask)
                    await watchTask; // surface any fault from the watch stream
            }
            return await read;
        }
    }

    private sealed class FakeServerCallContext(CancellationToken cancellationToken) : ServerCallContext
    {
        protected override string MethodCore => string.Empty;
        protected override string HostCore => string.Empty;
        protected override string PeerCore => string.Empty;
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override Metadata RequestHeadersCore => [];
        protected override CancellationToken CancellationTokenCore => cancellationToken;
        protected override Metadata ResponseTrailersCore => [];
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => new(string.Empty, new Dictionary<string, List<AuthProperty>>());

        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) =>
            throw new NotSupportedException();

        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => Task.CompletedTask;
    }
}
