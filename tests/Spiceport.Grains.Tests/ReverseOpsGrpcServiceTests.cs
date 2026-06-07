using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Spiceport.Api;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Protos;
using Relationship = Spiceport.Core.Relationship;
using RelationshipUpdate = Spiceport.Core.RelationshipUpdate;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Drives the gRPC <see cref="PermissionsGrpcService"/> IN-PROCESS (no Kestrel listener): the service
/// type is instantiated directly with the in-process <see cref="TestCluster"/>'s grain factory and the
/// silo's <see cref="IPermissionChecker"/>, then its reverse / tree RPC methods are invoked and their
/// proto responses asserted. This verifies the proto &lt;-&gt; grain mapping end to end.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class ReverseOpsGrpcServiceTests
{
    private const string SchemaText = """
        definition user {}

        definition document {
            relation viewer: user
            relation editor: user
            permission view = viewer + editor
        }
        """;

    private static async Task SeedAsync(IDatastore datastore, params (string res, string rel, string subj)[] tuples)
    {
        var updates = tuples
            .Select(t => new RelationshipUpdate(
                Relationship.Create(
                    new ObjectAndRelation("document", t.res, t.rel),
                    new ObjectAndRelation("user", t.subj, CoreConstants.Ellipsis)),
                UpdateOperation.Touch))
            .ToList();
        await datastore.ReadWriteTx(tx => tx.WriteRelationships(updates));
    }

    private static PermissionsGrpcService Service(MeshTestCluster cluster) =>
        new(cluster.Services.GetRequiredService<IPermissionChecker>(), cluster.GrainFactory);

    [Fact]
    public async Task ExpandPermissionTree_Rpc_Returns_Union_Tree()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(SchemaText);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"), ("readme", "editor", "bob"));

        var resp = await Service(cluster).ExpandPermissionTree(
            new ExpandPermissionTreeRequest
            {
                Resource = new ObjectReference { ObjectType = "document", ObjectId = "readme" },
                Permission = "view",
                Mode = ExpandPermissionTreeRequest.Types.ExpandMode.Shallow,
            },
            FakeContext.Instance);

        Assert.NotNull(resp.TreeRoot);
        Assert.Equal(PermissionTreeNode.NodeOneofCase.SetOp, resp.TreeRoot.NodeCase);
        Assert.Equal(
            PermissionTreeNode.Types.SetOpNode.Types.Operation.Union,
            resp.TreeRoot.SetOp.Operation);
    }

    [Fact]
    public async Task LookupSubjects_Rpc_Returns_Holders()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(SchemaText);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"), ("readme", "editor", "bob"));

        var resp = await Service(cluster).LookupSubjects(
            new LookupSubjectsRequest
            {
                Resource = new ObjectReference { ObjectType = "document", ObjectId = "readme" },
                Permission = "view",
                SubjectObjectType = "user",
            },
            FakeContext.Instance);

        var ids = resp.Subjects.Select(s => s.SubjectObjectId).ToHashSet();
        Assert.Equal(new HashSet<string> { "alice", "bob" }, ids);
        Assert.All(resp.Subjects, s =>
            Assert.Equal(Permissionship.Types.Kind.HasPermission, s.Permissionship.Kind));
    }

    [Fact]
    public async Task LookupResources_Rpc_Returns_Reachable_Resources()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(SchemaText);
        await SeedAsync(cluster.Datastore,
            ("readme", "viewer", "alice"),
            ("design", "editor", "alice"),
            ("secret", "viewer", "bob"));

        var resp = await Service(cluster).LookupResources(
            new LookupResourcesRequest
            {
                ResourceObjectType = "document",
                Permission = "view",
                Subject = new SubjectReference
                {
                    Object = new ObjectReference { ObjectType = "user", ObjectId = "alice" },
                },
            },
            FakeContext.Instance);

        var ids = resp.Resources.Select(r => r.ResourceObjectId).ToHashSet();
        Assert.Equal(new HashSet<string> { "readme", "design" }, ids);
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
