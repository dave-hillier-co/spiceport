using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Spiceport.Api;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine.Tests;
using Spiceport.Grains;
using Spiceport.Grains.Tests;
using V1 = Authzed.Api.V1;

namespace Spiceport.Differential.Tests;

/// <summary>
/// Directed differential gate for issue #35: <c>ImportBulkRelationships</c> applies CREATE semantics.
/// Runs the SAME import streams against a real <c>authzed/spicedb</c> container and against Spiceport's
/// in-process <see cref="AuthzedPermissionsV1Service"/> and asserts both agree.
/// </summary>
/// <remarks>
/// Real SpiceDB (authzed/spicedb v1.49.2), empirically observed by driving the client-streaming
/// <c>ImportBulkRelationships</c> directly over gRPC:
/// <list type="bullet">
/// <item>a duplicate row within one streamed batch -&gt; <c>AlreadyExists</c>: "could not CREATE
/// relationship `document:d1#viewer@user:alice`, as it already existed. If this is persistent, please
/// switch to TOUCH operations or specify a precondition".</item>
/// <item>a duplicate across two batches of the same stream -&gt; the same <c>AlreadyExists</c>, and the
/// failed import is atomic across the WHOLE stream: afterwards ReadRelationships shows ZERO rows —
/// including the clean rows of the batch that streamed before the duplicate.</item>
/// <item>a row already stored by an earlier WriteRelationships -&gt; the same <c>AlreadyExists</c>; only
/// the pre-existing row remains, nothing from the failed stream applies.</item>
/// <item>a clean import of 3 distinct rows across two batches -&gt; success, <c>NumLoaded = 3</c>.</item>
/// </list>
/// </remarks>
[Collection(SpiceDbCollection.Name)]
public sealed class ImportBulkRelationshipsDifferentialTests
{
    private const string Schema = """
        definition user {}

        definition document {
            relation viewer: user
            permission view = viewer
        }
        """;

    private readonly SpiceDbContainerFixture _spiceDb;

    public ImportBulkRelationshipsDifferentialTests(SpiceDbContainerFixture spiceDb) => _spiceDb = spiceDb;

    [SkippableFact]
    public async Task Duplicate_across_stream_batches_fails_already_exists_and_applies_nothing_on_both_systems()
    {
        Skip.IfNot(_spiceDb.Available, _spiceDb.SkipReason ?? "Docker/SpiceDB container unavailable");

        using var spiceDbClient = new SpiceDbGrpcClient(_spiceDb.Address, _spiceDb.PreSharedKey);
        await SpiceDbReset.ResetAsync(spiceDbClient);
        await spiceDbClient.WriteSchemaAsync(new V1::WriteSchemaRequest { Schema = Schema });

        // Batch 1 is clean; the duplicate arrives in batch 2 — exercising both the AlreadyExists verdict
        // and whole-stream atomicity (batch 1's clean rows must not survive the failed import).
        V1::ImportBulkRelationshipsRequest[] Batches() =>
        [
            Batch(Rel("d1", "alice"), Rel("d2", "bob")),
            Batch(Rel("d1", "alice")),
        ];

        var spiceDbEx = await Assert.ThrowsAsync<RpcException>(
            () => spiceDbClient.ImportBulkRelationshipsAsync(Batches()));
        Assert.Equal(StatusCode.AlreadyExists, spiceDbEx.StatusCode);
        Assert.Contains("could not CREATE relationship", spiceDbEx.Status.Detail);
        Assert.Contains("document:d1#viewer@user:alice", spiceDbEx.Status.Detail);

        var spiceDbRows = await spiceDbClient.ReadRelationshipsAsync(new V1::ReadRelationshipsRequest
        {
            Consistency = new V1::Consistency { FullyConsistent = true },
            RelationshipFilter = new V1::RelationshipFilter { ResourceType = "document" },
        });
        Assert.Empty(spiceDbRows);

        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        var spiceportEx = await Assert.ThrowsAsync<RpcException>(
            () => service.ImportBulkRelationships(
                new FakeAsyncStreamReader<V1::ImportBulkRelationshipsRequest>(Batches()),
                FakeServerCallContext.Default));
        Assert.Equal(StatusCode.AlreadyExists, spiceportEx.StatusCode);
        Assert.Contains("could not CREATE relationship", spiceportEx.Status.Detail);
        Assert.Contains("document:d1#viewer@user:alice", spiceportEx.Status.Detail);

        // Same whole-stream atomicity: batch 1's clean row is not visible after the failed import.
        var check = await service.CheckPermission(new V1::CheckPermissionRequest
        {
            Consistency = new V1::Consistency { FullyConsistent = true },
            Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "d2" },
            Permission = "view",
            Subject = Subject("bob"),
        }, FakeServerCallContext.Default);
        Assert.Equal(V1::CheckPermissionResponse.Types.Permissionship.NoPermission, check.Permissionship);
    }

    [SkippableFact]
    public async Task Duplicate_within_single_batch_fails_already_exists_on_both_systems()
    {
        Skip.IfNot(_spiceDb.Available, _spiceDb.SkipReason ?? "Docker/SpiceDB container unavailable");

        using var spiceDbClient = new SpiceDbGrpcClient(_spiceDb.Address, _spiceDb.PreSharedKey);
        await SpiceDbReset.ResetAsync(spiceDbClient);
        await spiceDbClient.WriteSchemaAsync(new V1::WriteSchemaRequest { Schema = Schema });

        // The duplicate sits INSIDE one batch — the remarks' observation (a), distinct from the
        // cross-batch shape above, since SpiceDB could in principle pre-validate a single batch.
        V1::ImportBulkRelationshipsRequest[] Batches() =>
        [
            Batch(Rel("d1", "alice"), Rel("d1", "alice")),
        ];

        var spiceDbEx = await Assert.ThrowsAsync<RpcException>(
            () => spiceDbClient.ImportBulkRelationshipsAsync(Batches()));
        Assert.Equal(StatusCode.AlreadyExists, spiceDbEx.StatusCode);
        Assert.Contains("could not CREATE relationship", spiceDbEx.Status.Detail);

        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var spiceportEx = await Assert.ThrowsAsync<RpcException>(
            () => Service(cluster).ImportBulkRelationships(
                new FakeAsyncStreamReader<V1::ImportBulkRelationshipsRequest>(Batches()),
                FakeServerCallContext.Default));
        Assert.Equal(StatusCode.AlreadyExists, spiceportEx.StatusCode);
        Assert.Contains("could not CREATE relationship", spiceportEx.Status.Detail);
    }

    [SkippableFact]
    public async Task Clean_import_succeeds_with_matching_loaded_count_on_both_systems()
    {
        Skip.IfNot(_spiceDb.Available, _spiceDb.SkipReason ?? "Docker/SpiceDB container unavailable");

        using var spiceDbClient = new SpiceDbGrpcClient(_spiceDb.Address, _spiceDb.PreSharedKey);
        await SpiceDbReset.ResetAsync(spiceDbClient);
        await spiceDbClient.WriteSchemaAsync(new V1::WriteSchemaRequest { Schema = Schema });

        V1::ImportBulkRelationshipsRequest[] Batches() =>
        [
            Batch(Rel("d1", "alice"), Rel("d2", "bob")),
            Batch(Rel("d3", "carol")),
        ];

        var spiceDbResp = await spiceDbClient.ImportBulkRelationshipsAsync(Batches());
        Assert.Equal(3ul, spiceDbResp.NumLoaded);

        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var spiceportResp = await Service(cluster).ImportBulkRelationships(
            new FakeAsyncStreamReader<V1::ImportBulkRelationshipsRequest>(Batches()),
            FakeServerCallContext.Default);
        Assert.Equal(spiceDbResp.NumLoaded, spiceportResp.NumLoaded);
    }

    private static AuthzedPermissionsV1Service Service(MeshTestCluster cluster) => new(
        cluster.Services.GetRequiredService<IPermissionChecker>(), cluster.GrainFactory,
        cluster.ReverseOps, cluster.RelationshipReads,
        cluster.Services.GetRequiredService<ISchemaProvider>());

    private static V1::Relationship Rel(string doc, string user) => new()
    {
        Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = doc },
        Relation = "viewer",
        Subject = Subject(user),
    };

    private static V1::SubjectReference Subject(string user) => new()
    {
        Object = new V1::ObjectReference { ObjectType = "user", ObjectId = user },
    };

    private static V1::ImportBulkRelationshipsRequest Batch(params V1::Relationship[] rels)
    {
        var batch = new V1::ImportBulkRelationshipsRequest();
        batch.Relationships.AddRange(rels);
        return batch;
    }

    private sealed class FakeAsyncStreamReader<T>(IReadOnlyList<T> messages) : IAsyncStreamReader<T>
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

    private sealed class FakeServerCallContext(CancellationToken cancellationToken) : ServerCallContext
    {
        public static FakeServerCallContext Default { get; } = new(CancellationToken.None);

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
