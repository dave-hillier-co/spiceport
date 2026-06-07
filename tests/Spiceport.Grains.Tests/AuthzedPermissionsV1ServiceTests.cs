using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Spiceport.Api;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains;
using V1 = Authzed.Api.V1;
using CoreRelationship = Spiceport.Core.Relationship;
using CoreRelationshipUpdate = Spiceport.Core.RelationshipUpdate;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Drives the <c>authzed.api.v1</c> <see cref="AuthzedPermissionsV1Service"/> IN-PROCESS (no Kestrel):
/// the service is instantiated with the in-process cluster's <see cref="IPermissionChecker"/> and grain
/// factory; server-streaming RPCs are drained through a fake <see cref="IServerStreamWriter{T}"/>. Verifies
/// the v1 proto &lt;-&gt; grain mapping: permissionship + partial-caveat-info, preconditions, the nested v1
/// relationship filter, internal cursor paging, and the modern + deprecated lookup-subject fields.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class AuthzedPermissionsV1ServiceTests
{
    private const string Schema = """
        definition user {}

        caveat over_limit(limit int, requested int) {
            requested <= limit
        }

        definition document {
            relation viewer: user | user with over_limit
            relation editor: user
            permission view = viewer + editor
        }
        """;

    private static AuthzedPermissionsV1Service Service(MeshTestCluster cluster) =>
        new(cluster.Services.GetRequiredService<IPermissionChecker>(), cluster.GrainFactory,
            cluster.Services.GetRequiredService<ISchemaProvider>());

    private static async Task SeedAsync(IDatastore datastore, params (string res, string rel, string subj)[] tuples)
    {
        var updates = tuples
            .Select(t => new CoreRelationshipUpdate(
                CoreRelationship.Create(
                    new ObjectAndRelation("document", t.res, t.rel),
                    new ObjectAndRelation("user", t.subj, CoreConstants.Ellipsis)),
                UpdateOperation.Touch))
            .ToList();
        await datastore.ReadWriteTx(tx => tx.WriteRelationships(updates));
    }

    private static async Task SeedCaveatedViewerAsync(IDatastore datastore, string res, string subj)
    {
        var rel = CoreRelationship.Create(
            new ObjectAndRelation("document", res, "viewer"),
            new ObjectAndRelation("user", subj, CoreConstants.Ellipsis),
            new ContextualizedCaveat("over_limit", null));
        await datastore.ReadWriteTx(tx => tx.WriteRelationships(
            new List<CoreRelationshipUpdate> { new(rel, UpdateOperation.Touch) }));
    }

    private static V1::SubjectReference UserSubject(string id) => new()
    {
        Object = new V1::ObjectReference { ObjectType = "user", ObjectId = id },
    };

    [Fact]
    public async Task CheckPermission_member_has_permission()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"));

        var resp = await Service(cluster).CheckPermission(new V1::CheckPermissionRequest
        {
            Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
            Permission = "view",
            Subject = UserSubject("alice"),
        }, FakeServerCallContext.Default);

        Assert.Equal(V1::CheckPermissionResponse.Types.Permissionship.HasPermission, resp.Permissionship);
        Assert.False(string.IsNullOrEmpty(resp.CheckedAt.Token));
        Assert.Null(resp.PartialCaveatInfo);
    }

    [Fact]
    public async Task CheckPermission_non_member_no_permission()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"));

        var resp = await Service(cluster).CheckPermission(new V1::CheckPermissionRequest
        {
            Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
            Permission = "view",
            Subject = UserSubject("bob"),
        }, FakeServerCallContext.Default);

        Assert.Equal(V1::CheckPermissionResponse.Types.Permissionship.NoPermission, resp.Permissionship);
    }

    [Fact]
    public async Task CheckPermission_unknown_definition_is_failed_precondition()
    {
        // An unknown resource definition is a client schema/typo bug. SpiceDB validates up front with
        // CheckNamespaceAndRelations and returns FailedPrecondition, NOT a NO_PERMISSION verdict.
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).CheckPermission(
            new V1::CheckPermissionRequest
            {
                Resource = new V1::ObjectReference { ObjectType = "nonesuch", ObjectId = "readme" },
                Permission = "view",
                Subject = UserSubject("alice"),
            },
            FakeServerCallContext.Default));

        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
    }

    [Fact]
    public async Task CheckPermission_unknown_permission_is_failed_precondition()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).CheckPermission(
            new V1::CheckPermissionRequest
            {
                Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
                Permission = "no_such_permission",
                Subject = UserSubject("alice"),
            },
            FakeServerCallContext.Default));

        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
    }

    [Fact]
    public async Task CheckPermission_unknown_subject_definition_is_failed_precondition()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).CheckPermission(
            new V1::CheckPermissionRequest
            {
                Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
                Permission = "view",
                Subject = new V1::SubjectReference
                {
                    Object = new V1::ObjectReference { ObjectType = "ghost", ObjectId = "alice" },
                },
            },
            FakeServerCallContext.Default));

        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
    }

    [Fact]
    public async Task WriteRelationships_create_existing_is_already_exists()
    {
        // A CREATE on an already-existing relationship is a permanent duplicate-create: AlreadyExists,
        // distinct from a transient write-write serialization conflict (which maps to Aborted).
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        await Service(cluster).WriteRelationships(
            WriteReq(V1::RelationshipUpdate.Types.Operation.Create, "readme", "alice"),
            FakeServerCallContext.Default);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).WriteRelationships(
            WriteReq(V1::RelationshipUpdate.Types.Operation.Create, "readme", "alice"),
            FakeServerCallContext.Default));

        Assert.Equal(StatusCode.AlreadyExists, ex.StatusCode);
    }

    [Fact]
    public async Task CheckPermission_caveated_missing_context_populates_partial_caveat_info()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedCaveatedViewerAsync(cluster.Datastore, "readme", "alice");

        // No context supplied: the caveat cannot be fully evaluated -> conditional + missing fields.
        var resp = await Service(cluster).CheckPermission(new V1::CheckPermissionRequest
        {
            Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
            Permission = "view",
            Subject = UserSubject("alice"),
        }, FakeServerCallContext.Default);

        Assert.Equal(V1::CheckPermissionResponse.Types.Permissionship.ConditionalPermission, resp.Permissionship);
        Assert.NotNull(resp.PartialCaveatInfo);
        Assert.NotEmpty(resp.PartialCaveatInfo.MissingRequiredContext);
    }

    [Fact]
    public async Task CheckPermission_invalid_consistency_token_is_invalid_argument()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).CheckPermission(
            new V1::CheckPermissionRequest
            {
                Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
                Permission = "view",
                Subject = UserSubject("alice"),
                Consistency = new V1::Consistency
                {
                    AtExactSnapshot = new V1::ZedToken { Token = "not-a-real-token" },
                },
            },
            FakeServerCallContext.Default));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
    }

    [Fact]
    public async Task WriteRelationships_writes_and_returns_token()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var resp = await Service(cluster).WriteRelationships(WriteReq(
            V1::RelationshipUpdate.Types.Operation.Touch, "readme", "alice"), FakeServerCallContext.Default);

        Assert.False(string.IsNullOrEmpty(resp.WrittenAt.Token));

        var check = await Service(cluster).CheckPermission(new V1::CheckPermissionRequest
        {
            Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
            Permission = "view",
            Subject = UserSubject("alice"),
        }, FakeServerCallContext.Default);
        Assert.Equal(V1::CheckPermissionResponse.Types.Permissionship.HasPermission, check.Permissionship);
    }

    [Fact]
    public async Task WriteRelationships_unspecified_operation_is_invalid_argument()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).WriteRelationships(
            WriteReq(V1::RelationshipUpdate.Types.Operation.Unspecified, "readme", "alice"),
            FakeServerCallContext.Default));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
    }

    [Fact]
    public async Task WriteRelationships_precondition_must_match_success_and_failure()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"));

        // MUST_MATCH on an existing relationship -> success.
        var ok = WriteReq(V1::RelationshipUpdate.Types.Operation.Touch, "readme", "bob");
        ok.OptionalPreconditions.Add(new V1::Precondition
        {
            Operation = V1::Precondition.Types.Operation.MustMatch,
            Filter = new V1::RelationshipFilter { ResourceType = "document", OptionalResourceId = "readme" },
        });
        var resp = await Service(cluster).WriteRelationships(ok, FakeServerCallContext.Default);
        Assert.False(string.IsNullOrEmpty(resp.WrittenAt.Token));

        // MUST_MATCH on a non-existent resource -> FailedPrecondition.
        var bad = WriteReq(V1::RelationshipUpdate.Types.Operation.Touch, "other", "carol");
        bad.OptionalPreconditions.Add(new V1::Precondition
        {
            Operation = V1::Precondition.Types.Operation.MustMatch,
            Filter = new V1::RelationshipFilter { ResourceType = "document", OptionalResourceId = "missing" },
        });
        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            Service(cluster).WriteRelationships(bad, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
    }

    [Fact]
    public async Task WriteRelationships_precondition_must_not_match_failure()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"));

        var req = WriteReq(V1::RelationshipUpdate.Types.Operation.Touch, "readme", "bob");
        req.OptionalPreconditions.Add(new V1::Precondition
        {
            Operation = V1::Precondition.Types.Operation.MustNotMatch,
            Filter = new V1::RelationshipFilter { ResourceType = "document", OptionalResourceId = "readme" },
        });

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            Service(cluster).WriteRelationships(req, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
    }

    [Fact]
    public async Task ReadRelationships_streams_all_with_read_at_and_nested_subject_filter()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore,
            ("readme", "viewer", "alice"),
            ("readme", "viewer", "bob"),
            ("readme", "editor", "carol"));

        // Resource filter only: all three viewer/editor relationships on readme.
        var allWriter = new CollectingStreamWriter<V1::ReadRelationshipsResponse>();
        await Service(cluster).ReadRelationships(new V1::ReadRelationshipsRequest
        {
            RelationshipFilter = new V1::RelationshipFilter { ResourceType = "document", OptionalResourceId = "readme" },
        }, allWriter, FakeServerCallContext.Default);

        Assert.Equal(3, allWriter.Collected.Count);
        Assert.All(allWriter.Collected, r => Assert.False(string.IsNullOrEmpty(r.ReadAt.Token)));

        // Nested subject filter narrows to a single subject id.
        var narrowWriter = new CollectingStreamWriter<V1::ReadRelationshipsResponse>();
        await Service(cluster).ReadRelationships(new V1::ReadRelationshipsRequest
        {
            RelationshipFilter = new V1::RelationshipFilter
            {
                ResourceType = "document",
                OptionalResourceId = "readme",
                OptionalRelation = "viewer",
                OptionalSubjectFilter = new V1::SubjectFilter
                {
                    SubjectType = "user",
                    OptionalSubjectId = "alice",
                },
            },
        }, narrowWriter, FakeServerCallContext.Default);

        var only = Assert.Single(narrowWriter.Collected);
        Assert.Equal("alice", only.Relationship.Subject.Object.ObjectId);
        Assert.Equal("viewer", only.Relationship.Relation);
    }

    [Fact]
    public async Task DeleteRelationships_deletes_matching_and_returns_token()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"), ("readme", "viewer", "bob"));

        var resp = await Service(cluster).DeleteRelationships(new V1::DeleteRelationshipsRequest
        {
            RelationshipFilter = new V1::RelationshipFilter { ResourceType = "document", OptionalResourceId = "readme" },
        }, FakeServerCallContext.Default);

        Assert.False(string.IsNullOrEmpty(resp.DeletedAt.Token));

        var after = new CollectingStreamWriter<V1::ReadRelationshipsResponse>();
        await Service(cluster).ReadRelationships(new V1::ReadRelationshipsRequest
        {
            RelationshipFilter = new V1::RelationshipFilter { ResourceType = "document", OptionalResourceId = "readme" },
        }, after, FakeServerCallContext.Default);
        Assert.Empty(after.Collected);
    }

    [Fact]
    public async Task DeleteRelationships_precondition_failure_is_failed_precondition()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"));

        var req = new V1::DeleteRelationshipsRequest
        {
            RelationshipFilter = new V1::RelationshipFilter { ResourceType = "document", OptionalResourceId = "readme" },
        };
        req.OptionalPreconditions.Add(new V1::Precondition
        {
            Operation = V1::Precondition.Types.Operation.MustNotMatch,
            Filter = new V1::RelationshipFilter { ResourceType = "document", OptionalResourceId = "readme" },
        });

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            Service(cluster).DeleteRelationships(req, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
    }

    [Fact]
    public async Task LookupResources_streams_accessible_resources()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore,
            ("doc1", "viewer", "alice"),
            ("doc2", "editor", "alice"),
            ("doc3", "viewer", "bob"));

        var writer = new CollectingStreamWriter<V1::LookupResourcesResponse>();
        await Service(cluster).LookupResources(new V1::LookupResourcesRequest
        {
            ResourceObjectType = "document",
            Permission = "view",
            Subject = UserSubject("alice"),
        }, writer, FakeServerCallContext.Default);

        var ids = writer.Collected.Select(r => r.ResourceObjectId).OrderBy(x => x, StringComparer.Ordinal).ToList();
        Assert.Equal(new[] { "doc1", "doc2" }, ids);
        Assert.All(writer.Collected, r =>
            Assert.Equal(V1::LookupPermissionship.HasPermission, r.Permissionship));
        Assert.All(writer.Collected, r => Assert.False(string.IsNullOrEmpty(r.LookedUpAt.Token)));
    }

    [Fact]
    public async Task LookupSubjects_populates_modern_and_deprecated_fields()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"), ("readme", "editor", "bob"));

        var writer = new CollectingStreamWriter<V1::LookupSubjectsResponse>();
        await Service(cluster).LookupSubjects(new V1::LookupSubjectsRequest
        {
            Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
            Permission = "view",
            SubjectObjectType = "user",
        }, writer, FakeServerCallContext.Default);

        var ids = writer.Collected.Select(r => r.Subject.SubjectObjectId).OrderBy(x => x, StringComparer.Ordinal).ToList();
        Assert.Equal(new[] { "alice", "bob" }, ids);

        // Both the modern ResolvedSubject and the deprecated mirror fields are populated.
        Assert.All(writer.Collected, r =>
        {
            Assert.Equal(r.SubjectObjectId, r.Subject.SubjectObjectId);
            Assert.Equal(V1::LookupPermissionship.HasPermission, r.Subject.Permissionship);
            Assert.Equal(V1::LookupPermissionship.HasPermission, r.Permissionship);
        });
    }

    [Fact]
    public async Task ExpandPermissionTree_returns_union_tree_with_leaves()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"), ("readme", "editor", "bob"));

        var resp = await Service(cluster).ExpandPermissionTree(new V1::ExpandPermissionTreeRequest
        {
            Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
            Permission = "view",
        }, FakeServerCallContext.Default);

        Assert.NotNull(resp.TreeRoot);
        Assert.Equal("readme", resp.TreeRoot.ExpandedObject.ObjectId);
        Assert.Equal("view", resp.TreeRoot.ExpandedRelation);
        Assert.Equal(V1::PermissionRelationshipTree.TreeTypeOneofCase.Intermediate, resp.TreeRoot.TreeTypeCase);
        Assert.Equal(V1::AlgebraicSubjectSet.Types.Operation.Union, resp.TreeRoot.Intermediate.Operation);

        // Every concrete subject lands in some leaf's DirectSubjectSet.
        var subjectIds = CollectLeafSubjects(resp.TreeRoot).ToHashSet();
        Assert.Contains("alice", subjectIds);
        Assert.Contains("bob", subjectIds);
    }

    private static IEnumerable<string> CollectLeafSubjects(V1::PermissionRelationshipTree tree)
    {
        if (tree.TreeTypeCase == V1::PermissionRelationshipTree.TreeTypeOneofCase.Leaf)
        {
            foreach (var s in tree.Leaf.Subjects)
                yield return s.Object.ObjectId;
        }
        else if (tree.TreeTypeCase == V1::PermissionRelationshipTree.TreeTypeOneofCase.Intermediate)
        {
            foreach (var child in tree.Intermediate.Children)
                foreach (var id in CollectLeafSubjects(child))
                    yield return id;
        }
    }

    [Fact]
    public async Task CheckBulkPermissions_preserves_item_order_with_one_token()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"));

        var req = new V1::CheckBulkPermissionsRequest();
        req.Items.Add(BulkItem("readme", "view", "alice")); // member
        req.Items.Add(BulkItem("readme", "view", "bob"));   // non-member
        req.Items.Add(BulkItem("other", "view", "alice"));  // non-member (no rel)

        var resp = await Service(cluster).CheckBulkPermissions(req, FakeServerCallContext.Default);

        Assert.False(string.IsNullOrEmpty(resp.CheckedAt.Token));
        Assert.Equal(3, resp.Pairs.Count);

        // Order preserved and each pair echoes its originating request.
        Assert.Equal("alice", resp.Pairs[0].Request.Subject.Object.ObjectId);
        Assert.Equal("bob", resp.Pairs[1].Request.Subject.Object.ObjectId);
        Assert.Equal("other", resp.Pairs[2].Request.Resource.ObjectId);

        Assert.Equal(V1::CheckPermissionResponse.Types.Permissionship.HasPermission, resp.Pairs[0].Item.Permissionship);
        Assert.Equal(V1::CheckPermissionResponse.Types.Permissionship.NoPermission, resp.Pairs[1].Item.Permissionship);
        Assert.Equal(V1::CheckPermissionResponse.Types.Permissionship.NoPermission, resp.Pairs[2].Item.Permissionship);

        // One pinned revision -> all pairs share the single batch token.
        Assert.All(resp.Pairs, p => Assert.Equal(V1::CheckBulkPermissionsPair.ResponseOneofCase.Item, p.ResponseCase));
    }

    [Fact]
    public async Task CheckBulkPermissions_caveated_item_carries_partial_caveat_info()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedCaveatedViewerAsync(cluster.Datastore, "readme", "alice");

        var req = new V1::CheckBulkPermissionsRequest();
        req.Items.Add(BulkItem("readme", "view", "alice"));

        var resp = await Service(cluster).CheckBulkPermissions(req, FakeServerCallContext.Default);

        var pair = Assert.Single(resp.Pairs);
        Assert.Equal(
            V1::CheckPermissionResponse.Types.Permissionship.ConditionalPermission,
            pair.Item.Permissionship);
        Assert.NotNull(pair.Item.PartialCaveatInfo);
        Assert.NotEmpty(pair.Item.PartialCaveatInfo.MissingRequiredContext);
    }

    [Fact]
    public async Task CheckBulkPermissions_invalid_consistency_token_is_invalid_argument()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var req = new V1::CheckBulkPermissionsRequest
        {
            Consistency = new V1::Consistency
            {
                AtExactSnapshot = new V1::ZedToken { Token = "not-a-real-token" },
            },
        };
        req.Items.Add(BulkItem("readme", "view", "alice"));

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            Service(cluster).CheckBulkPermissions(req, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
    }

    [Fact]
    public async Task ImportBulkRelationships_loads_across_batches_and_returns_count()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var reader = new FakeAsyncStreamReader<V1::ImportBulkRelationshipsRequest>(
            ImportBatch(("doc1", "alice"), ("doc1", "bob")),
            new V1::ImportBulkRelationshipsRequest(), // empty batch -> skipped, no empty tx
            ImportBatch(("doc2", "carol")));

        var resp = await Service(cluster).ImportBulkRelationships(reader, FakeServerCallContext.Default);

        Assert.Equal(3ul, resp.NumLoaded);

        // The imported relationships are visible via a check.
        var check = await Service(cluster).CheckPermission(new V1::CheckPermissionRequest
        {
            Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "doc2" },
            Permission = "view",
            Subject = UserSubject("carol"),
        }, FakeServerCallContext.Default);
        Assert.Equal(V1::CheckPermissionResponse.Types.Permissionship.HasPermission, check.Permissionship);
    }

    [Fact]
    public async Task ExportBulkRelationships_pages_all_over_one_snapshot()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore,
            ("doc1", "viewer", "alice"),
            ("doc2", "viewer", "bob"),
            ("doc3", "editor", "carol"),
            ("doc4", "viewer", "dave"),
            ("doc5", "viewer", "erin"));

        var writer = new CollectingStreamWriter<V1::ExportBulkRelationshipsResponse>();
        await Service(cluster).ExportBulkRelationships(new V1::ExportBulkRelationshipsRequest
        {
            OptionalLimit = 2, // force multiple pages
        }, writer, FakeServerCallContext.Default);

        var all = writer.Collected.SelectMany(p => p.Relationships).ToList();
        Assert.Equal(5, all.Count);

        // Every page carries a continuation cursor token.
        Assert.All(writer.Collected, p => Assert.NotNull(p.AfterResultCursor));

        // Multiple pages were produced under the limit.
        Assert.True(writer.Collected.Count >= 2);

        var resourceIds = all.Select(r => r.Resource.ObjectId).OrderBy(x => x, StringComparer.Ordinal).ToList();
        Assert.Equal(new[] { "doc1", "doc2", "doc3", "doc4", "doc5" }, resourceIds);
    }

    [Fact]
    public async Task ExportBulkRelationships_filter_narrows_results()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore,
            ("doc1", "viewer", "alice"),
            ("doc1", "editor", "bob"),
            ("doc2", "viewer", "carol"));

        var writer = new CollectingStreamWriter<V1::ExportBulkRelationshipsResponse>();
        await Service(cluster).ExportBulkRelationships(new V1::ExportBulkRelationshipsRequest
        {
            OptionalRelationshipFilter = new V1::RelationshipFilter
            {
                ResourceType = "document",
                OptionalResourceId = "doc1",
                OptionalRelation = "viewer",
            },
        }, writer, FakeServerCallContext.Default);

        var all = writer.Collected.SelectMany(p => p.Relationships).ToList();
        var only = Assert.Single(all);
        Assert.Equal("doc1", only.Resource.ObjectId);
        Assert.Equal("viewer", only.Relation);
        Assert.Equal("alice", only.Subject.Object.ObjectId);
    }

    private static V1::CheckBulkPermissionsRequestItem BulkItem(string doc, string perm, string user) => new()
    {
        Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = doc },
        Permission = perm,
        Subject = UserSubject(user),
    };

    private static V1::ImportBulkRelationshipsRequest ImportBatch(params (string doc, string user)[] tuples)
    {
        var batch = new V1::ImportBulkRelationshipsRequest();
        foreach (var (doc, user) in tuples)
        {
            batch.Relationships.Add(new V1::Relationship
            {
                Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = doc },
                Relation = "viewer",
                Subject = UserSubject(user),
            });
        }
        return batch;
    }

    private static V1::WriteRelationshipsRequest WriteReq(
        V1::RelationshipUpdate.Types.Operation op, string doc, string user)
    {
        var req = new V1::WriteRelationshipsRequest();
        req.Updates.Add(new V1::RelationshipUpdate
        {
            Operation = op,
            Relationship = new V1::Relationship
            {
                Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = doc },
                Relation = "viewer",
                Subject = UserSubject(user),
            },
        });
        return req;
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

    /// <summary>A client-stream reader that replays a fixed sequence of inbound messages.</summary>
    private sealed class FakeAsyncStreamReader<T>(params T[] messages) : IAsyncStreamReader<T>
    {
        private int _index = -1;

        public T Current => messages[_index];

        public Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _index++;
            return Task.FromResult(_index < messages.Length);
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
