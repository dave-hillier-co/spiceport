using Grpc.Core;
using Spiceport.Api;
using Spiceport.Core;
using Spiceport.Grains.Abstractions;
using Spiceport.Protos;
using WireRelationship = Spiceport.Grains.Abstractions.RelationshipWire;
using WireUpdate = Spiceport.Grains.Abstractions.RelationshipUpdateWire;
using WireOp = Spiceport.Grains.Abstractions.RelationshipUpdateOpWire;
using WireWriteArgs = Spiceport.Grains.Abstractions.WriteRelationshipsArgs;
using ProtoRelationship = Spiceport.Protos.Relationship;
using ProtoObjectReference = Spiceport.Protos.ObjectReference;
using ProtoSubjectReference = Spiceport.Protos.SubjectReference;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Drives the streaming <see cref="BulkGrpcService"/> IN-PROCESS (no Kestrel): a fake
/// <see cref="IAsyncStreamReader{T}"/> feeds the client-streaming import; a collecting
/// <see cref="IServerStreamWriter{T}"/> drains the server-streaming export; a fake
/// <see cref="ServerCallContext"/> carries the cancellation token. Verifies: import across multiple
/// batches returns the total count; export pages back the exact same set (round-trip); export resumes
/// from a mid-stream cursor; an export pinned at a snapshot does not see later writes.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class BulkGrpcServiceTests
{
    private const string Schema = """
        definition user {}
        definition document {
            relation viewer: user
            permission view = viewer
        }
        """;

    private static BulkGrpcService Service(MeshTestCluster cluster) =>
        new(cluster.GrainFactory, cluster.RelationshipReads);

    private static ProtoRelationship Viewer(string doc, string user) => new()
    {
        Resource = new ProtoObjectReference { ObjectType = "document", ObjectId = doc },
        ResourceRelation = "viewer",
        Subject = new ProtoSubjectReference
        {
            Object = new ProtoObjectReference { ObjectType = "user", ObjectId = user },
        },
    };

    private static string Key(ProtoRelationship r) =>
        $"{r.Resource.ObjectId}#{r.Subject.Object.ObjectId}";

    [Fact]
    public async Task Import_across_batches_returns_total_count()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        // 300 relationships fed as three 100-row batches.
        var batches = new List<ImportBulkRelationshipsRequest>();
        for (var b = 0; b < 3; b++)
        {
            var req = new ImportBulkRelationshipsRequest();
            for (var i = 0; i < 100; i++)
                req.Relationships.Add(Viewer($"doc{b * 100 + i:D4}", "alice"));
            batches.Add(req);
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var reader = new FakeStreamReader<ImportBulkRelationshipsRequest>(batches);
        var ctx = new FakeServerCallContext(cts.Token);

        var response = await service.ImportBulkRelationships(reader, ctx);

        Assert.Equal(300UL, response.NumLoaded);
        Assert.NotNull(response.LoadedAt);
        Assert.False(string.IsNullOrEmpty(response.LoadedAt.Token));
    }

    [Fact]
    public async Task Export_round_trips_the_imported_set()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        var imported = new List<ProtoRelationship>();
        for (var i = 0; i < 250; i++)
            imported.Add(Viewer($"doc{i:D4}", "alice"));

        var importReq = new ImportBulkRelationshipsRequest();
        importReq.Relationships.AddRange(imported);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var importReader = new FakeStreamReader<ImportBulkRelationshipsRequest>([importReq]);
        var importResp = await service.ImportBulkRelationships(importReader, new FakeServerCallContext(cts.Token));
        Assert.Equal(250UL, importResp.NumLoaded);

        // Export with a small page size so multiple pages are exercised.
        var writer = new CollectingStreamWriter<ExportBulkRelationshipsResponse>();
        await service.ExportBulkRelationships(
            new ExportBulkRelationshipsRequest { OptionalLimit = 60 },
            writer,
            new FakeServerCallContext(cts.Token));

        var exported = writer.Collected.SelectMany(p => p.Relationships).ToList();
        Assert.Equal(imported.Count, exported.Count);
        Assert.Equal(
            imported.Select(Key).OrderBy(k => k, StringComparer.Ordinal),
            exported.Select(Key).OrderBy(k => k, StringComparer.Ordinal));

        // More than one page emitted at this page size.
        Assert.True(writer.Collected.Count > 1);
    }

    [Fact]
    public async Task Export_resumes_from_a_mid_stream_cursor()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        var importReq = new ImportBulkRelationshipsRequest();
        for (var i = 0; i < 100; i++)
            importReq.Relationships.Add(Viewer($"doc{i:D4}", "alice"));
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await service.ImportBulkRelationships(
            new FakeStreamReader<ImportBulkRelationshipsRequest>([importReq]),
            new FakeServerCallContext(cts.Token));

        // First page (size 40): grab its after_cursor. A per-call CTS cancels after the first page so the
        // server loop stops, simulating a client that reads one page then disconnects.
        var firstPage = await FirstPage(service, new ExportBulkRelationshipsRequest { OptionalLimit = 40 });
        Assert.Equal(40, firstPage.Relationships.Count);
        Assert.False(string.IsNullOrEmpty(firstPage.AfterCursor));

        // Resume from that cursor: the remaining 60 must come back, with no overlap.
        var resumeWriter = new CollectingStreamWriter<ExportBulkRelationshipsResponse>();
        await service.ExportBulkRelationships(
            new ExportBulkRelationshipsRequest { OptionalLimit = 40, OptionalCursor = firstPage.AfterCursor },
            resumeWriter,
            new FakeServerCallContext(cts.Token));

        var firstKeys = firstPage.Relationships.Select(Key).ToHashSet();
        var resumed = resumeWriter.Collected.SelectMany(p => p.Relationships).Select(Key).ToList();
        Assert.Equal(60, resumed.Count);
        Assert.DoesNotContain(resumed, firstKeys.Contains); // strictly after the cursor, no overlap
    }

    [Fact]
    public async Task Export_pinned_snapshot_does_not_see_later_writes()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        var importReq = new ImportBulkRelationshipsRequest();
        for (var i = 0; i < 100; i++)
            importReq.Relationships.Add(Viewer($"doc{i:D4}", "alice"));
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await service.ImportBulkRelationships(
            new FakeStreamReader<ImportBulkRelationshipsRequest>([importReq]),
            new FakeServerCallContext(cts.Token));

        // Start the export and read ONLY the first page (pins the snapshot at this revision).
        var pinnedCursor = (await FirstPage(service, new ExportBulkRelationshipsRequest { OptionalLimit = 40 })).AfterCursor;

        // Write MORE relationships AFTER the snapshot was pinned.
        var update = new WireUpdate(
            WireOp.Touch,
            new WireRelationship("document", "doc9999", "viewer", "user", "zzz", "...", null, null, null));
        await cluster.Relationships.WriteRelationships(new WireWriteArgs(new List<WireUpdate> { update }));

        // Resume the pinned export to exhaustion: it must total exactly 100 (40 + 60), never 101.
        var rest = new CollectingStreamWriter<ExportBulkRelationshipsResponse>();
        await service.ExportBulkRelationships(
            new ExportBulkRelationshipsRequest { OptionalLimit = 40, OptionalCursor = pinnedCursor },
            rest,
            new FakeServerCallContext(cts.Token));

        var restKeys = rest.Collected.SelectMany(p => p.Relationships).Select(Key).ToList();
        Assert.Equal(60, restKeys.Count);
        Assert.DoesNotContain("doc9999#zzz", restKeys);
    }

    /// <summary>
    /// Reads exactly one export page: a per-call CTS is cancelled the moment the first page is written,
    /// so the server's "keep paging while a cursor is returned" loop exits after one page — modelling a
    /// client that reads a single page then disconnects.
    /// </summary>
    private static async Task<ExportBulkRelationshipsResponse> FirstPage(
        BulkGrpcService service, ExportBulkRelationshipsRequest request)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new CancelAfterFirstStreamWriter<ExportBulkRelationshipsResponse>(cts);
        await service.ExportBulkRelationships(request, writer, new FakeServerCallContext(cts.Token));
        return writer.Collected[0];
    }

    /// <summary>A writer that records responses and cancels the supplied source after the first write.</summary>
    private sealed class CancelAfterFirstStreamWriter<T>(CancellationTokenSource cts) : IServerStreamWriter<T>
    {
        public List<T> Collected { get; } = [];
        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(T message)
        {
            Collected.Add(message);
            cts.Cancel();
            return Task.CompletedTask;
        }
    }

    /// <summary>A fake client-stream reader over a fixed list of messages.</summary>
    private sealed class FakeStreamReader<T>(IReadOnlyList<T> messages) : IAsyncStreamReader<T>
    {
        private int _index = -1;
        public T Current => messages[_index];

        public Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _index++;
            return Task.FromResult(_index < messages.Count);
        }
    }

    /// <summary>A server stream writer that records every response.</summary>
    private sealed class CollectingStreamWriter<T> : IServerStreamWriter<T>
    {
        public List<T> Collected { get; } = [];
        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(T message)
        {
            Collected.Add(message);
            return Task.CompletedTask;
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
