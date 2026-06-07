using Grpc.Core;
using Spiceport.Api;
using Spiceport.Grains.Abstractions;
using V1 = Authzed.Api.V1;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Drives the <c>authzed.api.v1</c> <see cref="AuthzedExperimentalV1Service"/> IN-PROCESS over the in-process
/// cluster's grain mesh (in-memory datastore). Verifies the on-demand relationship-counter RPCs: register +
/// count returns the matching count with a non-empty read-at token; the count tracks subsequent matching /
/// non-matching writes; unregister then count is FAILED_PRECONDITION; re-registering an existing name is
/// FAILED_PRECONDITION. (Postgres MVCC/count behaviour is covered by the datastore Testcontainers suite.)
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class AuthzedExperimentalV1ServiceTests
{
    private const string Schema = """
        definition user {}
        definition document {
            relation viewer: user
            permission view = viewer
        }
        """;

    private static AuthzedExperimentalV1Service Service(MeshTestCluster cluster) =>
        new(cluster.GrainFactory, cluster.SchemaProvider);

    private static V1::RelationshipFilter DocumentViewerFilter() => new()
    {
        ResourceType = "document",
        OptionalRelation = "viewer",
    };

    private static Task WriteViewer(MeshTestCluster cluster, string document, string user) =>
        cluster.Relationships.WriteRelationships(new WriteRelationshipsArgs(
            new List<RelationshipUpdateWire>
            {
                new(RelationshipUpdateOpWire.Touch,
                    new RelationshipWire("document", document, "viewer", "user", user, "...", null, null, null)),
            }));

    [Fact]
    public async Task Register_then_Count_returns_matching_count_with_read_at_token()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        await WriteViewer(cluster, "readme", "alice");
        await WriteViewer(cluster, "readme", "bob");

        await service.ExperimentalRegisterRelationshipCounter(
            new V1::ExperimentalRegisterRelationshipCounterRequest
            {
                Name = "doc_viewers",
                RelationshipFilter = DocumentViewerFilter(),
            },
            FakeServerCallContext.Default);

        var resp = await service.ExperimentalCountRelationships(
            new V1::ExperimentalCountRelationshipsRequest { Name = "doc_viewers" },
            FakeServerCallContext.Default);

        Assert.Equal(
            V1::ExperimentalCountRelationshipsResponse.CounterResultOneofCase.ReadCounterValue,
            resp.CounterResultCase);
        Assert.Equal(2UL, resp.ReadCounterValue.RelationshipCount);
        Assert.False(string.IsNullOrEmpty(resp.ReadCounterValue.ReadAt?.Token));
    }

    [Fact]
    public async Task Count_tracks_matching_and_non_matching_writes()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        await WriteViewer(cluster, "readme", "alice");

        await service.ExperimentalRegisterRelationshipCounter(
            new V1::ExperimentalRegisterRelationshipCounterRequest
            {
                Name = "doc_viewers",
                RelationshipFilter = DocumentViewerFilter(),
            },
            FakeServerCallContext.Default);

        var first = await service.ExperimentalCountRelationships(
            new V1::ExperimentalCountRelationshipsRequest { Name = "doc_viewers" }, FakeServerCallContext.Default);
        Assert.Equal(1UL, first.ReadCounterValue.RelationshipCount);

        // A matching write bumps the count.
        await WriteViewer(cluster, "guide", "carol");
        var second = await service.ExperimentalCountRelationships(
            new V1::ExperimentalCountRelationshipsRequest { Name = "doc_viewers" }, FakeServerCallContext.Default);
        Assert.Equal(2UL, second.ReadCounterValue.RelationshipCount);

        // A non-matching write (different resource type) does not.
        await cluster.Relationships.WriteRelationships(new WriteRelationshipsArgs(
            new List<RelationshipUpdateWire>
            {
                new(RelationshipUpdateOpWire.Touch,
                    new RelationshipWire("user", "alice", "...", "user", "carol", "...", null, null, null)),
            }));
        var third = await service.ExperimentalCountRelationships(
            new V1::ExperimentalCountRelationshipsRequest { Name = "doc_viewers" }, FakeServerCallContext.Default);
        Assert.Equal(2UL, third.ReadCounterValue.RelationshipCount);
    }

    [Fact]
    public async Task Unregister_then_Count_is_failed_precondition()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        await service.ExperimentalRegisterRelationshipCounter(
            new V1::ExperimentalRegisterRelationshipCounterRequest
            {
                Name = "doc_viewers",
                RelationshipFilter = DocumentViewerFilter(),
            },
            FakeServerCallContext.Default);

        await service.ExperimentalUnregisterRelationshipCounter(
            new V1::ExperimentalUnregisterRelationshipCounterRequest { Name = "doc_viewers" },
            FakeServerCallContext.Default);

        var ex = await Assert.ThrowsAsync<RpcException>(() => service.ExperimentalCountRelationships(
            new V1::ExperimentalCountRelationshipsRequest { Name = "doc_viewers" }, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
    }

    [Fact]
    public async Task Unregister_unknown_counter_is_failed_precondition()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        var ex = await Assert.ThrowsAsync<RpcException>(() => service.ExperimentalUnregisterRelationshipCounter(
            new V1::ExperimentalUnregisterRelationshipCounterRequest { Name = "nope" }, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
    }

    [Fact]
    public async Task Register_existing_name_is_failed_precondition()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        var request = new V1::ExperimentalRegisterRelationshipCounterRequest
        {
            Name = "doc_viewers",
            RelationshipFilter = DocumentViewerFilter(),
        };
        await service.ExperimentalRegisterRelationshipCounter(request, FakeServerCallContext.Default);

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            service.ExperimentalRegisterRelationshipCounter(request, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
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
