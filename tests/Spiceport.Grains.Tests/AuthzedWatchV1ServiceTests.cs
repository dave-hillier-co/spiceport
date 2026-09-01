using System.Threading.Channels;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Spiceport.Api;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains;
using V1 = Authzed.Api.V1;
using WireRelationship = Spiceport.Grains.Abstractions.RelationshipWire;
using WireUpdate = Spiceport.Grains.Abstractions.RelationshipUpdateWire;
using WireOp = Spiceport.Grains.Abstractions.RelationshipUpdateOpWire;
using WireWriteArgs = Spiceport.Grains.Abstractions.WriteRelationshipsArgs;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Drives the server-streaming <c>authzed.api.v1</c> <see cref="AuthzedWatchV1Service"/> IN-PROCESS (no
/// Kestrel). Verifies: tailing from head sees a subsequent write with its changes_through token; resuming
/// from a pre-write token replays the write exactly once; cancellation terminates the stream cleanly; the
/// optional_object_types filter excludes updates of other resource types.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class AuthzedWatchV1ServiceTests
{
    private const string Schema = """
        definition user {}
        definition document {
            relation viewer: user
            permission view = viewer
        }
        definition folder {
            relation viewer: user
            permission view = viewer
        }
        """;

    private static AuthzedWatchV1Service Service(MeshTestCluster cluster) =>
        new(cluster.Datastore, cluster.Services.GetRequiredService<ISchemaProvider>());

    private static Task WriteViewer(MeshTestCluster cluster, string type, string id, string user)
    {
        var update = new WireUpdate(
            WireOp.Touch,
            new WireRelationship(type, id, "viewer", "user", user, "...", null, null, null));
        return cluster.Relationships.WriteRelationships(new WireWriteArgs(new List<WireUpdate> { update }));
    }

    [Fact]
    public async Task Watch_from_head_sees_subsequent_write_with_token()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new CollectingStreamWriter<V1::WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);

        var watchTask = service.Watch(new V1::WatchRequest(), writer, ctx);

        await WriteViewer(cluster, "document", "doc1", "alice");

        var response = await writer.WaitForNext(TimeSpan.FromSeconds(10), watchTask);
        cts.Cancel();
        await watchTask;

        Assert.False(string.IsNullOrEmpty(response.ChangesThrough.Token));
        var update = Assert.Single(response.Updates);
        Assert.Equal(V1::RelationshipUpdate.Types.Operation.Touch, update.Operation);
        Assert.Equal("doc1", update.Relationship.Resource.ObjectId);
        Assert.Equal("alice", update.Relationship.Subject.Object.ObjectId);
    }

    [Fact]
    public async Task Watch_resumes_from_pre_write_token_and_replays_the_write()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        var preWrite = await cluster.Datastore.HeadRevision(CancellationToken.None);
        var datastoreId = await cluster.Datastore.GetUniqueId(CancellationToken.None);
        var startToken = ZedTokens.FromRevision(preWrite.Revision, preWrite.SchemaHash, datastoreId);

        await WriteViewer(cluster, "document", "doc2", "bob");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new CollectingStreamWriter<V1::WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);

        var request = new V1::WatchRequest { OptionalStartCursor = new V1::ZedToken { Token = startToken.Token } };
        var watchTask = service.Watch(request, writer, ctx);

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
        var writer = new CollectingStreamWriter<V1::WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);

        var watchTask = service.Watch(new V1::WatchRequest(), writer, ctx);

        cts.Cancel();
        await watchTask.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.True(watchTask.IsCompletedSuccessfully);
        Assert.Empty(writer.Collected);
    }

    [Fact]
    public async Task Watch_object_type_filter_excludes_other_types()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new CollectingStreamWriter<V1::WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);

        var request = new V1::WatchRequest();
        request.OptionalObjectTypes.Add("document");
        var watchTask = service.Watch(request, writer, ctx);

        // A folder write must be filtered out; a document write must come through.
        await WriteViewer(cluster, "folder", "f1", "alice");
        await WriteViewer(cluster, "document", "doc3", "carol");

        var response = await writer.WaitForNext(TimeSpan.FromSeconds(10), watchTask);
        cts.Cancel();
        await watchTask;

        // The emitted response carries only the document update, never the folder update.
        var update = Assert.Single(response.Updates);
        Assert.Equal("document", update.Relationship.Resource.ObjectType);
        Assert.Equal("doc3", update.Relationship.Resource.ObjectId);

        Assert.All(writer.Collected,
            r => Assert.All(r.Updates, u => Assert.Equal("document", u.Relationship.Resource.ObjectType)));
    }

    [Fact]
    public async Task Watch_with_checkpoints_emits_a_checkpoint_response_after_a_change()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new CollectingStreamWriter<V1::WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);

        var request = new V1::WatchRequest();
        request.OptionalUpdateKinds.Add(V1::WatchKind.IncludeRelationshipUpdates);
        request.OptionalUpdateKinds.Add(V1::WatchKind.IncludeCheckpoints);
        var watchTask = service.Watch(request, writer, ctx);

        await WriteViewer(cluster, "document", "doc1", "alice");

        // First the content response, then the checkpoint marking the revision progressed through.
        var content = await writer.WaitForNext(TimeSpan.FromSeconds(10), watchTask);
        var checkpoint = await writer.WaitForNext(TimeSpan.FromSeconds(10), watchTask);
        cts.Cancel();
        await watchTask;

        Assert.False(content.IsCheckpoint);
        Assert.Single(content.Updates);
        Assert.True(checkpoint.IsCheckpoint);
        Assert.Empty(checkpoint.Updates);
        Assert.False(string.IsNullOrEmpty(checkpoint.ChangesThrough.Token));
    }

    [Fact]
    public async Task Watch_with_checkpoints_emits_checkpoint_even_when_filter_excludes_the_change()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new CollectingStreamWriter<V1::WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);

        var request = new V1::WatchRequest();
        request.OptionalObjectTypes.Add("document");
        request.OptionalUpdateKinds.Add(V1::WatchKind.IncludeRelationshipUpdates);
        request.OptionalUpdateKinds.Add(V1::WatchKind.IncludeCheckpoints);
        var watchTask = service.Watch(request, writer, ctx);

        // A folder write is filtered out of content, but the checkpoint must still surface so the
        // consumer sees the revision advanced (liveness for a filtered subset).
        await WriteViewer(cluster, "folder", "f1", "alice");

        var first = await writer.WaitForNext(TimeSpan.FromSeconds(10), watchTask);
        cts.Cancel();
        await watchTask;

        Assert.True(first.IsCheckpoint);
        Assert.Empty(first.Updates);
    }

    [Fact]
    public async Task Watch_with_checkpoints_only_still_emits_checkpoints_with_no_content()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new CollectingStreamWriter<V1::WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);

        // Requesting checkpoints ONLY (no IncludeRelationshipUpdates, no IncludeSchemaUpdates): the
        // resulting WatchContent has just the Checkpoints bit set. Content selection is additive in
        // name only here — checkpoint emission is driven by commit activity, not by the content mask
        // matching anything, so this must still see a checkpoint (and never relationship content).
        var request = new V1::WatchRequest();
        request.OptionalUpdateKinds.Add(V1::WatchKind.IncludeCheckpoints);
        var watchTask = service.Watch(request, writer, ctx);

        await WriteViewer(cluster, "document", "doc1", "alice");

        var response = await writer.WaitForNext(TimeSpan.FromSeconds(10), watchTask);
        cts.Cancel();
        await watchTask;

        Assert.True(response.IsCheckpoint);
        Assert.Empty(response.Updates);
        Assert.False(string.IsNullOrEmpty(response.ChangesThrough.Token));
    }

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
                    await watchTask;
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
