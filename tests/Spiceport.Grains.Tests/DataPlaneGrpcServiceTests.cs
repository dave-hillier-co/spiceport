using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Spiceport.Api;
using Spiceport.Core;
using Spiceport.Protos;
using RelationshipUpdate = Spiceport.Protos.RelationshipUpdate;
using Relationship = Spiceport.Protos.Relationship;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Drives the full data-plane lifecycle THROUGH the gRPC <see cref="PermissionsGrpcService"/> IN-PROCESS
/// (no Kestrel listener): the service type is instantiated directly with the in-process
/// <see cref="Orleans.TestingHost.TestCluster"/>'s grain factory and the silo's
/// <see cref="IPermissionChecker"/>, then its proto RPCs (WriteSchema / WriteRelationships / CheckPermission
/// / ReadRelationships / DeleteRelationships) are invoked and their proto responses asserted. This verifies
/// the proto &lt;-&gt; grain &lt;-&gt; datastore mapping end to end across the real grain mesh.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class DataPlaneGrpcServiceTests
{
    private const string EmptySchema = "definition user {}";

    private const string ViewerOnlySchema = """
        definition user {}

        definition document {
            relation viewer: user
            relation editor: user
            permission view = viewer
        }
        """;

    private const string ViewerOrEditorSchema = """
        definition user {}

        definition document {
            relation viewer: user
            relation editor: user
            permission view = viewer + editor
        }
        """;

    private static PermissionsGrpcService Service(MeshTestCluster cluster) =>
        new(cluster.Services.GetRequiredService<IPermissionChecker>(), cluster.GrainFactory);

    private static RelationshipUpdate Touch(string res, string rel, string subj) => new()
    {
        Operation = RelationshipUpdate.Types.Operation.Touch,
        Relationship = new Relationship
        {
            Resource = new ObjectReference { ObjectType = "document", ObjectId = res },
            ResourceRelation = rel,
            Subject = new SubjectReference
            {
                Object = new ObjectReference { ObjectType = "user", ObjectId = subj },
            },
        },
    };

    private static CheckPermissionRequest CheckOf(string res, string perm, string subj) => new()
    {
        Resource = new ObjectReference { ObjectType = "document", ObjectId = res },
        Permission = perm,
        Subject = new SubjectReference
        {
            Object = new ObjectReference { ObjectType = "user", ObjectId = subj },
        },
    };

    [Fact]
    public async Task FullLifecycle_WriteSchema_Write_Check_Delete_Check_Read_via_grpc_service()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);

        // 1. WriteSchema over the gRPC surface.
        var schemaResp = await service.WriteSchema(
            new WriteSchemaRequest { Schema = ViewerOrEditorSchema }, FakeContext.Instance);
        Assert.False(string.IsNullOrEmpty(schemaResp.WrittenAt.Token));

        // ReadSchema round-trips the source text.
        var readSchema = await service.ReadSchema(new ReadSchemaRequest(), FakeContext.Instance);
        Assert.Equal(ViewerOrEditorSchema, readSchema.SchemaText);

        // 2. WriteRelationships over the gRPC surface.
        var writeReq = new WriteRelationshipsRequest();
        writeReq.Updates.Add(Touch("readme", "viewer", "alice"));
        var writeResp = await service.WriteRelationships(writeReq, FakeContext.Instance);
        Assert.False(string.IsNullOrEmpty(writeResp.WrittenAt.Token));

        // 3. CheckPermission -> HasPermission. Use full consistency to observe the relationship just
        // written above: a default (minimize-latency) check resolves to the optimized revision, a real
        // but window-stable cached head (matching SpiceDB's CachedOptimizedRevisions); the earlier
        // ReadSchema opened that window before this write committed, so a minimize-latency check could
        // still read the pre-write head. Read-your-writes is expressed via full consistency (or the
        // write's returned token).
        var checkReq = CheckOf("readme", "view", "alice");
        checkReq.Consistency = new Consistency { FullyConsistent = true };
        var checkAfterWrite = await service.CheckPermission(checkReq, FakeContext.Instance);
        Assert.Equal(
            CheckPermissionResponse.Types.Permissionship.HasPermission,
            checkAfterWrite.Permissionship);

        // ReadRelationships returns what was written (full consistency: read-your-writes within the
        // current quantization window, where the minimize-latency optimized revision may lag head).
        var readResp = await service.ReadRelationships(
            new ReadRelationshipsRequest
            {
                Filter = new RelationshipFilter { ResourceType = "document", OptionalResourceRelation = "viewer" },
                Consistency = new Consistency { FullyConsistent = true },
            },
            FakeContext.Instance);
        Assert.Single(readResp.Relationships);
        var r = readResp.Relationships[0];
        Assert.Equal("readme", r.Resource.ObjectId);
        Assert.Equal("viewer", r.ResourceRelation);
        Assert.Equal("alice", r.Subject.Object.ObjectId);

        // 4. DeleteRelationships over the gRPC surface.
        var delResp = await service.DeleteRelationships(
            new DeleteRelationshipsRequest
            {
                Filter = new RelationshipFilter { ResourceType = "document", OptionalResourceRelation = "viewer" },
            },
            FakeContext.Instance);
        Assert.Equal(1UL, delResp.DeletedCount);

        // ReadRelationships now empty. Ask for full consistency so the read reflects the just-committed
        // delete: a default (minimize-latency) read resolves to the optimized revision — a real but
        // window-stable cached head (matching SpiceDB's CachedOptimizedRevisions) — which need not yet
        // include a mutation that landed within the current quantization window.
        var readAfterDelete = await service.ReadRelationships(
            new ReadRelationshipsRequest
            {
                Filter = new RelationshipFilter { ResourceType = "document", OptionalResourceRelation = "viewer" },
                Consistency = new Consistency { FullyConsistent = true },
            },
            FakeContext.Instance);
        Assert.Empty(readAfterDelete.Relationships);
    }

    [Fact]
    public async Task SchemaChange_flips_grpc_Check_outcome_across_dispatch_cache()
    {
        // Schema A (view = viewer). Bob is editor (not viewer) -> NoPermission, cached under hash(A).
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerOnlySchema);
        var service = Service(cluster);

        var writeReq = new WriteRelationshipsRequest();
        writeReq.Updates.Add(Touch("readme", "editor", "bob"));
        await service.WriteRelationships(writeReq, FakeContext.Instance);

        var before = await service.CheckPermission(CheckOf("readme", "view", "bob"), FakeContext.Instance);
        Assert.Equal(
            CheckPermissionResponse.Types.Permissionship.NoPermission, before.Permissionship);

        // Schema B (view = viewer + editor): swap the live schema -> new schema hash.
        await service.WriteSchema(new WriteSchemaRequest { Schema = ViewerOrEditorSchema }, FakeContext.Instance);

        // Same check must now be HasPermission: every new cache/grain key is prefixed with hash(B),
        // so the stale hash(A) NoPermission entry can never be served.
        var after = await service.CheckPermission(CheckOf("readme", "view", "bob"), FakeContext.Instance);
        Assert.Equal(
            CheckPermissionResponse.Types.Permissionship.HasPermission, after.Permissionship);
    }

    [Fact]
    public async Task WriteSchema_invalid_maps_to_InvalidArgument_over_grpc()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerOrEditorSchema);
        var service = Service(cluster);

        var ex = await Assert.ThrowsAsync<RpcException>(
            () => service.WriteSchema(
                new WriteSchemaRequest { Schema = "definition document { relation" }, FakeContext.Instance));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
    }

    /// <summary>A no-op <see cref="ServerCallContext"/>; the service methods only read CancellationToken.</summary>
    private sealed class FakeContext : ServerCallContext
    {
        public static readonly FakeContext Instance = new();

        protected override string MethodCore => string.Empty;
        protected override string HostCore => string.Empty;
        protected override string PeerCore => string.Empty;
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override Metadata RequestHeadersCore => [];
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override Metadata ResponseTrailersCore => [];
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => new(string.Empty, new Dictionary<string, List<AuthProperty>>());

        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) =>
            throw new NotSupportedException();

        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) =>
            Task.CompletedTask;
    }
}
