using Grpc.Core;
using Spiceport.Api;
using V1 = Authzed.Api.V1;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Drives the <c>authzed.api.v1</c> <see cref="AuthzedSchemaV1Service"/> IN-PROCESS over the in-process
/// cluster's grain mesh. Verifies: write-then-read round-trips the schema text; invalid schema text maps to
/// INVALID_ARGUMENT; an orphaning change maps to FAILED_PRECONDITION; reading a never-written schema maps
/// to NOT_FOUND.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class AuthzedSchemaV1ServiceTests
{
    private const string Schema = """
        definition user {}
        definition document {
            relation viewer: user
            permission view = viewer
        }
        """;

    // A cluster needs a compiled schema to start, so the "no schema" case starts from an empty schema text.
    private const string EmptySchema = "";

    private static AuthzedSchemaV1Service Service(MeshTestCluster cluster) => new(cluster.GrainFactory);

    [Fact]
    public async Task WriteSchema_then_ReadSchema_round_trips_text()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);

        await service.WriteSchema(new V1::WriteSchemaRequest { Schema = Schema }, FakeServerCallContext.Default);

        var read = await service.ReadSchema(new V1::ReadSchemaRequest(), FakeServerCallContext.Default);
        Assert.Contains("definition document", read.SchemaText);
        Assert.Contains("permission view", read.SchemaText);
    }

    [Fact]
    public async Task WriteSchema_invalid_text_is_invalid_argument()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);

        var ex = await Assert.ThrowsAsync<RpcException>(() => service.WriteSchema(
            new V1::WriteSchemaRequest { Schema = "this is not a valid schema {{{" },
            FakeServerCallContext.Default));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
    }

    [Fact]
    public async Task WriteSchema_orphaning_change_is_failed_precondition()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);

        await service.WriteSchema(new V1::WriteSchemaRequest { Schema = Schema }, FakeServerCallContext.Default);

        // Write a viewer relationship, then attempt to drop the viewer relation that backs it.
        await cluster.Relationships.WriteRelationships(new Abstractions.WriteRelationshipsArgs(
            new List<Abstractions.RelationshipUpdateWire>
            {
                new(Abstractions.RelationshipUpdateOpWire.Touch,
                    new Abstractions.RelationshipWire(
                        "document", "readme", "viewer", "user", "alice", "...", null, null, null)),
            }));

        var orphaning = """
            definition user {}
            definition document {
                permission view = nil
            }
            """;

        var ex = await Assert.ThrowsAsync<RpcException>(() => service.WriteSchema(
            new V1::WriteSchemaRequest { Schema = orphaning }, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
    }

    [Fact]
    public async Task ReadSchema_on_fresh_cluster_is_not_found()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            service.ReadSchema(new V1::ReadSchemaRequest(), FakeServerCallContext.Default));
        Assert.Equal(StatusCode.NotFound, ex.StatusCode);
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
