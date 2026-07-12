using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Spiceport.Api;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Protos;
using CoreRelationship = Spiceport.Core.Relationship;
using CoreContextualizedCaveat = Spiceport.Core.ContextualizedCaveat;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Drives <c>BatchCheckPermission</c> THROUGH the gRPC <see cref="PermissionsGrpcService"/> IN-PROCESS
/// (no Kestrel): the service is instantiated with the in-process cluster's grain factory and
/// <see cref="IPermissionChecker"/>, then the proto RPC is invoked and its response asserted. Verifies
/// pairs are index-aligned to the request, per-item permissionship/missing-fields map correctly
/// (including a caveated item whose per-item context collapses over a shared cached branch), and there is
/// ONE batch-level <c>checked_at</c> token.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class BatchCheckGrpcServiceTests
{
    private const string SchemaText = """
        definition user {}

        caveat allow_region(region string) {
            region == "eu"
        }

        definition group {
            relation member: user
        }

        definition document {
            relation parent: group
            relation restricted: group with allow_region
            permission view = parent->member
            permission restricted_view = restricted->member
        }
        """;

    private static PermissionsGrpcService Service(MeshTestCluster cluster) =>
        new(cluster.Services.GetRequiredService<IPermissionChecker>(), cluster.GrainFactory, cluster.ReverseOps, cluster.RelationshipReads);

    private static BatchCheckPermissionRequestItem Item(
        string doc, string permission, string subject, Struct? context = null)
    {
        var it = new BatchCheckPermissionRequestItem
        {
            Resource = new ObjectReference { ObjectType = "document", ObjectId = doc },
            Permission = permission,
            Subject = new SubjectReference
            {
                Object = new ObjectReference { ObjectType = "user", ObjectId = subject },
            },
        };
        if (context is not null)
        {
            it.Context = context;
        }

        return it;
    }

    [Fact]
    public async Task BatchCheckPermission_returns_index_aligned_pairs_with_one_checked_at()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(SchemaText);
        var service = Service(cluster);

        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(
        [
            new(CoreRelationship.Create(
                new ObjectAndRelation("group", "eng", "member"),
                new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis)), UpdateOperation.Touch),
            new(CoreRelationship.Create(
                new ObjectAndRelation("document", "doc1", "parent"),
                new ObjectAndRelation("group", "eng", CoreConstants.Ellipsis)), UpdateOperation.Touch),
            new(CoreRelationship.Create(
                new ObjectAndRelation("document", "doc0", "restricted"),
                new ObjectAndRelation("group", "eng", CoreConstants.Ellipsis),
                new CoreContextualizedCaveat("allow_region")), UpdateOperation.Touch),
        ]));

        var euContext = new Struct { Fields = { ["region"] = Value.ForString("eu") } };

        var request = new BatchCheckPermissionRequest();
        request.Items.Add(Item("doc1", "view", "alice"));                 // 0: HasPermission
        request.Items.Add(Item("doc1", "view", "bob"));                   // 1: NoPermission
        request.Items.Add(Item("doc0", "restricted_view", "alice"));      // 2: Conditional (no context)
        request.Items.Add(Item("doc0", "restricted_view", "alice", euContext)); // 3: HasPermission

        var resp = await service.BatchCheckPermission(request, FakeBatchContext.Instance);

        // Pairs index-aligned to request order, request echoed back.
        Assert.Equal(request.Items.Count, resp.Pairs.Count);
        for (var i = 0; i < request.Items.Count; i++)
        {
            Assert.Equal(request.Items[i], resp.Pairs[i].Request);
            Assert.Equal(BatchCheckPermissionPair.ResponseOneofCase.Item, resp.Pairs[i].ResponseCase);
        }

        Assert.Equal(
            CheckPermissionResponse.Types.Permissionship.HasPermission, resp.Pairs[0].Item.Permissionship);
        Assert.Equal(
            CheckPermissionResponse.Types.Permissionship.NoPermission, resp.Pairs[1].Item.Permissionship);
        Assert.Equal(
            CheckPermissionResponse.Types.Permissionship.ConditionalPermission,
            resp.Pairs[2].Item.Permissionship);
        Assert.Contains("region", resp.Pairs[2].Item.PartialCaveatMissingFields);
        Assert.Equal(
            CheckPermissionResponse.Types.Permissionship.HasPermission, resp.Pairs[3].Item.Permissionship);

        // ONE batch-level checked_at token.
        Assert.NotNull(resp.CheckedAt);
        Assert.False(string.IsNullOrEmpty(resp.CheckedAt.Token));
    }

    /// <summary>A no-op <see cref="ServerCallContext"/>; the service only reads CancellationToken.</summary>
    private sealed class FakeBatchContext : ServerCallContext
    {
        public static readonly FakeBatchContext Instance = new();

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
