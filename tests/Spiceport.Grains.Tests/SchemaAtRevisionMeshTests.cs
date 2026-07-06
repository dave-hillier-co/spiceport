using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Spiceport.Api;
using Spiceport.Core;
using Spiceport.Grains.Abstractions;
using Spiceport.Protos;
using ZedToken = Spiceport.Protos.ZedToken;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Proves that a permission check evaluates under the schema PERSISTED AT THE PINNED REVISION, not the
/// silo-local ambient <see cref="ISchemaProvider.Current"/>. This is the multi-silo correctness property
/// behind the schema@revision dispatch: the schema bytes are folded into the datastore log on every silo,
/// so a <see cref="CheckGrain"/> resolves the schema its grain key names from the log rather than trusting
/// whichever <c>WriteSchema</c> happened to land on its own silo. A single-process <see cref="MeshTestCluster"/>
/// cannot manufacture cross-silo divergence directly, but the SAME code path is exercised by pinning an OLD
/// revision whose persisted schema differs from the current one: before this change the grain used the
/// (current) ambient schema and returned the wrong verdict for an at-exact historical read.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class SchemaAtRevisionMeshTests
{
    private const string EmptySchema = "definition user {}";

    // view is granted to BOTH viewer and editor.
    private const string ViewerOrEditorSchema = """
        definition user {}

        definition document {
            relation viewer: user
            relation editor: user
            permission view = viewer + editor
        }
        """;

    // view is granted to viewer ONLY (an editor no longer has view).
    private const string ViewerOnlySchema = """
        definition user {}

        definition document {
            relation viewer: user
            relation editor: user
            permission view = viewer
        }
        """;

    private static PermissionsGrpcService Service(MeshTestCluster cluster) =>
        new(cluster.Services.GetRequiredService<IPermissionChecker>(), cluster.GrainFactory);

    private static RelationshipUpdateWire TouchEditor(string res, string subj) =>
        new(RelationshipUpdateOpWire.Touch,
            new RelationshipWire("document", res, "editor", "user", subj, CoreConstants.Ellipsis, null, null, null));

    private static CheckPermissionRequest CheckView(string res, string subj, Consistency consistency) =>
        new()
        {
            Resource = new ObjectReference { ObjectType = "document", ObjectId = res },
            Permission = "view",
            Subject = new SubjectReference { Object = new ObjectReference { ObjectType = "user", ObjectId = subj } },
            Consistency = consistency,
        };

    [Fact]
    public async Task Check_at_exact_old_token_uses_the_schema_persisted_at_that_revision()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);

        // 1. Install `view = viewer + editor` and make alice an EDITOR of readme. The write's token pins a
        //    revision at which the persisted schema is ViewerOrEditor AND the editor edge is visible.
        await cluster.WriteSchema(ViewerOrEditorSchema);
        var writeReply = await cluster.Relationships.WriteRelationships(
            new WriteRelationshipsArgs(new List<RelationshipUpdateWire> { TouchEditor("readme", "alice") }));
        var oldToken = new ZedToken { Token = writeReply.WrittenAtToken };

        // Sanity: at that revision alice (an editor) HAS view under ViewerOrEditor.
        var atOldBefore = await service.CheckPermission(
            CheckView("readme", "alice", new Consistency { AtExactSnapshot = oldToken }), FakeContext.Instance);
        Assert.Equal(CheckPermissionResponse.Types.Permissionship.HasPermission, atOldBefore.Permissionship);

        // 2. Narrow the persisted schema to `view = viewer` only. This advances the head; the ambient current
        //    schema on the silo is now ViewerOnly.
        await cluster.WriteSchema(ViewerOnlySchema);

        // At HEAD, alice (still only an editor) no longer has view — confirms the narrow took effect.
        var atHead = await service.CheckPermission(
            CheckView("readme", "alice", new Consistency { FullyConsistent = true }), FakeContext.Instance);
        Assert.Equal(CheckPermissionResponse.Types.Permissionship.NoPermission, atHead.Permissionship);

        // 3. THE PROPERTY: a check pinned to the OLD token must evaluate under the schema persisted at that
        //    revision (ViewerOrEditor), so alice — an editor there — still HAS view. Evaluating under the
        //    ambient current schema (ViewerOnly) would wrongly return NoPermission.
        var atOldAfter = await service.CheckPermission(
            CheckView("readme", "alice", new Consistency { AtExactSnapshot = oldToken }), FakeContext.Instance);
        Assert.Equal(CheckPermissionResponse.Types.Permissionship.HasPermission, atOldAfter.Permissionship);
    }

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
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => null!;
        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => Task.CompletedTask;
    }
}
