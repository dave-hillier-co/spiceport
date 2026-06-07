using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Spiceport.Api;
using Spiceport.Protos;
using RelationshipUpdate = Spiceport.Protos.RelationshipUpdate;
using Relationship = Spiceport.Protos.Relationship;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Write-safety verification driven THROUGH the gRPC <see cref="PermissionsGrpcService"/> in-process
/// (no Kestrel): WriteRelationships / DeleteRelationships preconditions (MUST_MATCH / MUST_NOT_MATCH,
/// checked atomically inside the write tx — satisfied commits, violated rejects with
/// FailedPrecondition and nothing changes) and WriteSchema change validation (removing a still-
/// referenced definition / relation / allowed subject type is rejected; removing an unreferenced one
/// is allowed).
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class WriteSafetyGrpcServiceTests
{
    private const string Schema = """
        definition user {}
        definition group {
            relation member: user
        }
        definition document {
            relation viewer: user | group#member
            relation editor: user
            permission view = viewer + editor
        }
        """;

    private static PermissionsGrpcService Service(MeshTestCluster cluster) =>
        new(cluster.Services.GetRequiredService<IPermissionChecker>(), cluster.GrainFactory);

    private static RelationshipUpdate Touch(string resType, string res, string rel, string subjType, string subj, string subjRel = "") => new()
    {
        Operation = RelationshipUpdate.Types.Operation.Touch,
        Relationship = new Relationship
        {
            Resource = new ObjectReference { ObjectType = resType, ObjectId = res },
            ResourceRelation = rel,
            Subject = new SubjectReference
            {
                Object = new ObjectReference { ObjectType = subjType, ObjectId = subj },
                OptionalRelation = subjRel,
            },
        },
    };

    private static async Task Seed(PermissionsGrpcService svc, params RelationshipUpdate[] updates)
    {
        var req = new WriteRelationshipsRequest();
        req.Updates.AddRange(updates);
        await svc.WriteRelationships(req, FakeContext.Instance);
    }

    private static RelationshipFilter DocFilter(string rel) =>
        new() { ResourceType = "document", OptionalResourceRelation = rel };

    // ---- A) Preconditions ----

    [Fact]
    public async Task MustNotMatch_satisfied_commits_the_write()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var svc = Service(cluster);

        // Precondition: editor#bob must NOT already exist (it does not) -> write proceeds.
        var req = new WriteRelationshipsRequest();
        req.Updates.Add(Touch("document", "readme", "viewer", "user", "alice"));
        req.OptionalPreconditions.Add(new Precondition
        {
            Operation = Precondition.Types.Operation.MustNotMatch,
            Filter = DocFilter("editor"),
        });

        var resp = await svc.WriteRelationships(req, FakeContext.Instance);
        Assert.False(string.IsNullOrEmpty(resp.WrittenAt.Token));

        var read = await svc.ReadRelationships(
            new ReadRelationshipsRequest { Filter = DocFilter("viewer") }, FakeContext.Instance);
        Assert.Single(read.Relationships);
    }

    [Fact]
    public async Task MustNotMatch_violated_rejects_and_nothing_changes()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var svc = Service(cluster);

        await Seed(svc, Touch("document", "readme", "editor", "user", "bob"));

        // Precondition: editor#* must NOT exist (it does) -> reject; the new viewer must NOT be written.
        var req = new WriteRelationshipsRequest();
        req.Updates.Add(Touch("document", "readme", "viewer", "user", "alice"));
        req.OptionalPreconditions.Add(new Precondition
        {
            Operation = Precondition.Types.Operation.MustNotMatch,
            Filter = DocFilter("editor"),
        });

        var ex = await Assert.ThrowsAsync<RpcException>(() => svc.WriteRelationships(req, FakeContext.Instance));
        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);

        // Nothing committed: the viewer relationship is absent, the editor one is untouched.
        var viewers = await svc.ReadRelationships(
            new ReadRelationshipsRequest { Filter = DocFilter("viewer") }, FakeContext.Instance);
        Assert.Empty(viewers.Relationships);
        var editors = await svc.ReadRelationships(
            new ReadRelationshipsRequest { Filter = DocFilter("editor") }, FakeContext.Instance);
        Assert.Single(editors.Relationships);
    }

    [Fact]
    public async Task MustMatch_satisfied_commits_the_write()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var svc = Service(cluster);

        await Seed(svc, Touch("document", "readme", "editor", "user", "bob"));

        var req = new WriteRelationshipsRequest();
        req.Updates.Add(Touch("document", "readme", "viewer", "user", "alice"));
        req.OptionalPreconditions.Add(new Precondition
        {
            Operation = Precondition.Types.Operation.MustMatch,
            Filter = DocFilter("editor"),
        });

        var resp = await svc.WriteRelationships(req, FakeContext.Instance);
        Assert.False(string.IsNullOrEmpty(resp.WrittenAt.Token));
    }

    [Fact]
    public async Task MustMatch_violated_rejects_and_nothing_changes()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var svc = Service(cluster);

        // Precondition: editor must MATCH (none exists) -> reject.
        var req = new WriteRelationshipsRequest();
        req.Updates.Add(Touch("document", "readme", "viewer", "user", "alice"));
        req.OptionalPreconditions.Add(new Precondition
        {
            Operation = Precondition.Types.Operation.MustMatch,
            Filter = DocFilter("editor"),
        });

        var ex = await Assert.ThrowsAsync<RpcException>(() => svc.WriteRelationships(req, FakeContext.Instance));
        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);

        var viewers = await svc.ReadRelationships(
            new ReadRelationshipsRequest { Filter = DocFilter("viewer") }, FakeContext.Instance);
        Assert.Empty(viewers.Relationships);
    }

    [Fact]
    public async Task Delete_precondition_violated_rejects_and_deletes_nothing()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var svc = Service(cluster);

        await Seed(svc, Touch("document", "readme", "viewer", "user", "alice"));

        // Delete all viewers, but only if NO editor exists; seed an editor so the precondition fails.
        await Seed(svc, Touch("document", "readme", "editor", "user", "bob"));

        var del = new DeleteRelationshipsRequest { Filter = DocFilter("viewer") };
        del.OptionalPreconditions.Add(new Precondition
        {
            Operation = Precondition.Types.Operation.MustNotMatch,
            Filter = DocFilter("editor"),
        });

        var ex = await Assert.ThrowsAsync<RpcException>(() => svc.DeleteRelationships(del, FakeContext.Instance));
        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);

        // Nothing deleted.
        var viewers = await svc.ReadRelationships(
            new ReadRelationshipsRequest { Filter = DocFilter("viewer") }, FakeContext.Instance);
        Assert.Single(viewers.Relationships);
    }

    [Fact]
    public async Task Delete_precondition_satisfied_commits_the_delete()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var svc = Service(cluster);

        await Seed(svc, Touch("document", "readme", "viewer", "user", "alice"));

        var del = new DeleteRelationshipsRequest { Filter = DocFilter("viewer") };
        del.OptionalPreconditions.Add(new Precondition
        {
            Operation = Precondition.Types.Operation.MustNotMatch,
            Filter = DocFilter("editor"),
        });

        var resp = await svc.DeleteRelationships(del, FakeContext.Instance);
        Assert.Equal(1UL, resp.DeletedCount);
    }

    // ---- B) Schema change validation ----

    [Fact]
    public async Task RemoveDefinition_still_referenced_is_rejected()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var svc = Service(cluster);

        await Seed(svc, Touch("document", "readme", "viewer", "user", "alice"));

        // Drop the `document` definition while a relationship is written under it.
        const string withoutDocument = """
            definition user {}
            definition group {
                relation member: user
            }
            """;

        var ex = await Assert.ThrowsAsync<RpcException>(
            () => svc.WriteSchema(new WriteSchemaRequest { Schema = withoutDocument }, FakeContext.Instance));
        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
        Assert.Contains("document", ex.Status.Detail);

        // Live schema unchanged: the document relationship is still readable.
        var read = await svc.ReadRelationships(
            new ReadRelationshipsRequest { Filter = DocFilter("viewer") }, FakeContext.Instance);
        Assert.Single(read.Relationships);
    }

    [Fact]
    public async Task RemoveRelation_still_referenced_is_rejected()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var svc = Service(cluster);

        await Seed(svc, Touch("document", "readme", "viewer", "user", "alice"));

        // Drop the `viewer` relation while it is written.
        const string withoutViewer = """
            definition user {}
            definition group {
                relation member: user
            }
            definition document {
                relation editor: user
                permission view = editor
            }
            """;

        var ex = await Assert.ThrowsAsync<RpcException>(
            () => svc.WriteSchema(new WriteSchemaRequest { Schema = withoutViewer }, FakeContext.Instance));
        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
        Assert.Contains("viewer", ex.Status.Detail);
    }

    [Fact]
    public async Task RemoveRelation_referenced_as_subject_is_rejected()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var svc = Service(cluster);

        // document:readme#viewer @ group:eng#member  -> group#member is referenced as a subject.
        await Seed(svc, Touch("document", "readme", "viewer", "group", "eng", "member"));

        // Drop group#member: it is referenced on the subject side of the viewer relationship.
        const string withoutGroupMember = """
            definition user {}
            definition group {}
            definition document {
                relation viewer: user | group#member
                relation editor: user
                permission view = viewer + editor
            }
            """;

        var ex = await Assert.ThrowsAsync<RpcException>(
            () => svc.WriteSchema(new WriteSchemaRequest { Schema = withoutGroupMember }, FakeContext.Instance));
        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
        Assert.Contains("member", ex.Status.Detail);
    }

    [Fact]
    public async Task RemoveAllowedSubjectType_still_referenced_is_rejected()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var svc = Service(cluster);

        await Seed(svc, Touch("document", "readme", "viewer", "user", "alice"));

        // Keep `viewer` but remove `user` from its allowed subject types.
        const string viewerWithoutUser = """
            definition user {}
            definition group {
                relation member: user
            }
            definition document {
                relation viewer: group#member
                relation editor: user
                permission view = viewer + editor
            }
            """;

        var ex = await Assert.ThrowsAsync<RpcException>(
            () => svc.WriteSchema(new WriteSchemaRequest { Schema = viewerWithoutUser }, FakeContext.Instance));
        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
    }

    [Fact]
    public async Task RemoveUnreferencedDefinition_is_allowed()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var svc = Service(cluster);

        // Only document relationships exist; `group` has none, so dropping it is safe.
        await Seed(svc, Touch("document", "readme", "viewer", "user", "alice"));

        const string withoutGroup = """
            definition user {}
            definition document {
                relation viewer: user
                relation editor: user
                permission view = viewer + editor
            }
            """;

        var resp = await svc.WriteSchema(new WriteSchemaRequest { Schema = withoutGroup }, FakeContext.Instance);
        Assert.False(string.IsNullOrEmpty(resp.WrittenAt.Token));

        var read = await svc.ReadSchema(new ReadSchemaRequest(), FakeContext.Instance);
        Assert.Equal(withoutGroup, read.SchemaText);
    }

    [Fact]
    public async Task RemoveUnreferencedRelation_is_allowed()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var svc = Service(cluster);

        // Only viewer is written; editor has no relationships, so dropping editor is safe.
        await Seed(svc, Touch("document", "readme", "viewer", "user", "alice"));

        const string withoutEditor = """
            definition user {}
            definition group {
                relation member: user
            }
            definition document {
                relation viewer: user | group#member
                permission view = viewer
            }
            """;

        var resp = await svc.WriteSchema(new WriteSchemaRequest { Schema = withoutEditor }, FakeContext.Instance);
        Assert.False(string.IsNullOrEmpty(resp.WrittenAt.Token));
    }

    [Fact]
    public async Task PermissionOnlyChange_is_always_allowed()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var svc = Service(cluster);

        await Seed(svc, Touch("document", "readme", "viewer", "user", "alice"));

        // Change only the permission expression; relations are untouched.
        const string permChanged = """
            definition user {}
            definition group {
                relation member: user
            }
            definition document {
                relation viewer: user | group#member
                relation editor: user
                permission view = editor + viewer
            }
            """;

        var resp = await svc.WriteSchema(new WriteSchemaRequest { Schema = permChanged }, FakeContext.Instance);
        Assert.False(string.IsNullOrEmpty(resp.WrittenAt.Token));
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
