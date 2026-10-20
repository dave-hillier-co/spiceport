using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Spiceport.Api;
using Spiceport.Grains.Abstractions;
using V1 = Authzed.Api.V1;

namespace Spiceport.Grains.Tests;

// The deprecated aliases and Exp* RPCs are [Obsolete] in the generated code (deprecated in the vendored proto).
#pragma warning disable CS0612

/// <summary>
/// Drives the <c>authzed.api.v1</c> <see cref="AuthzedExperimentalV1Service"/> IN-PROCESS over the in-process
/// cluster's grain mesh (in-memory datastore). Verifies the on-demand relationship-counter RPCs: register +
/// count returns the matching count with a non-empty read-at token; the count tracks subsequent matching /
/// non-matching writes; unregister then count is FAILED_PRECONDITION; re-registering an existing name is
/// FAILED_PRECONDITION. Also verifies the deprecated aliases and the Exp* schema-tooling RPCs, which upstream
/// (<c>internal/services/v1/experimental.go</c>) serves from the same code paths as their v1 equivalents.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class AuthzedExperimentalV1ServiceTests
{
    private const string Schema = """
        definition user {}
        caveat ip_match(allowed string, user_ip string) {
            user_ip == allowed
        }
        definition document {
            relation viewer: user
            relation editor: user
            permission view = viewer + editor
            permission manage = editor
        }
        definition folder {
            relation viewer: user
        }
        """;

    private static AuthzedExperimentalV1Service Service(MeshTestCluster cluster) =>
        new(cluster.GrainFactory,
            new AuthzedPermissionsV1Service(
                cluster.Services.GetRequiredService<IPermissionChecker>(), cluster.GrainFactory,
                cluster.ReverseOps, cluster.RelationshipReads, cluster.SchemaProvider),
            new AuthzedSchemaV1Service(cluster.GrainFactory, cluster.SchemaProvider));

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
                    new RelationshipWire("folder", "docs", "viewer", "user", "carol", "...", null, null, null)),
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

    // ---- BulkCheckPermission (deprecated alias of PermissionsService.CheckBulkPermissions) ----

    private static V1::BulkCheckPermissionRequestItem BulkItem(string document, string permission, string user) => new()
    {
        Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = document },
        Permission = permission,
        Subject = new V1::SubjectReference { Object = new V1::ObjectReference { ObjectType = "user", ObjectId = user } },
    };

    [Fact]
    public async Task BulkCheckPermission_returns_pairs_in_request_order_with_checked_at()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await WriteViewer(cluster, "readme", "alice");

        var req = new V1::BulkCheckPermissionRequest();
        req.Items.Add(BulkItem("readme", "view", "alice"));
        req.Items.Add(BulkItem("readme", "view", "bob"));
        req.Items.Add(BulkItem("readme", "nonexistent_perm", "alice"));

        var resp = await Service(cluster).BulkCheckPermission(req, FakeServerCallContext.Default);

        Assert.False(string.IsNullOrEmpty(resp.CheckedAt?.Token));
        Assert.Equal(3, resp.Pairs.Count);
        for (var i = 0; i < req.Items.Count; i++)
            Assert.Equal(req.Items[i], resp.Pairs[i].Request);

        Assert.Equal(V1::BulkCheckPermissionPair.ResponseOneofCase.Item, resp.Pairs[0].ResponseCase);
        Assert.Equal(V1::CheckPermissionResponse.Types.Permissionship.HasPermission, resp.Pairs[0].Item.Permissionship);
        Assert.Equal(V1::BulkCheckPermissionPair.ResponseOneofCase.Item, resp.Pairs[1].ResponseCase);
        Assert.Equal(V1::CheckPermissionResponse.Types.Permissionship.NoPermission, resp.Pairs[1].Item.Permissionship);

        // A per-pair schema error is reported on that pair, not as a whole-request failure.
        Assert.Equal(V1::BulkCheckPermissionPair.ResponseOneofCase.Error, resp.Pairs[2].ResponseCase);
        Assert.Contains("nonexistent_perm", resp.Pairs[2].Error.Message);
    }

    [Fact]
    public async Task BulkCheckPermission_honours_request_consistency()
    {
        // The consistency on the deprecated request must reach the shared check path: a malformed
        // at_exact_snapshot token is the same InvalidArgument CheckBulkPermissions returns.
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var req = new V1::BulkCheckPermissionRequest
        {
            Consistency = new V1::Consistency { AtExactSnapshot = new V1::ZedToken { Token = "not-a-token" } },
        };
        req.Items.Add(BulkItem("readme", "view", "alice"));

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            Service(cluster).BulkCheckPermission(req, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
    }

    [Fact]
    public async Task BulkCheckPermission_more_than_10000_items_is_invalid_argument()
    {
        // SpiceDB maxBulkCheckCount, enforced on the bulk-check path both RPCs share.
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var req = new V1::BulkCheckPermissionRequest();
        for (var i = 0; i < 10_001; i++)
            req.Items.Add(BulkItem("readme", "view", "alice"));

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            Service(cluster).BulkCheckPermission(req, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal("check count of 10001 is greater than maximum allowed of 10000", ex.Status.Detail);
    }

    // ---- BulkImportRelationships / BulkExportRelationships (deprecated aliases) ----

    private static V1::BulkImportRelationshipsRequest ImportBatch(params (string doc, string user)[] tuples)
    {
        var batch = new V1::BulkImportRelationshipsRequest();
        foreach (var (doc, user) in tuples)
        {
            batch.Relationships.Add(new V1::Relationship
            {
                Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = doc },
                Relation = "viewer",
                Subject = new V1::SubjectReference { Object = new V1::ObjectReference { ObjectType = "user", ObjectId = user } },
            });
        }
        return batch;
    }

    [Fact]
    public async Task BulkImportRelationships_loads_across_batches_and_returns_count()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        var reader = new FakeAsyncStreamReader<V1::BulkImportRelationshipsRequest>(
            ImportBatch(("doc1", "alice"), ("doc1", "bob")),
            ImportBatch(("doc2", "carol")));

        var resp = await service.BulkImportRelationships(reader, FakeServerCallContext.Default);

        Assert.Equal(3ul, resp.NumLoaded);

        var check = new V1::BulkCheckPermissionRequest();
        check.Items.Add(BulkItem("doc2", "view", "carol"));
        var verdict = await service.BulkCheckPermission(check, FakeServerCallContext.Default);
        Assert.Equal(V1::CheckPermissionResponse.Types.Permissionship.HasPermission, verdict.Pairs[0].Item.Permissionship);
    }

    [Fact]
    public async Task BulkImportRelationships_duplicate_is_already_exists()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var reader = new FakeAsyncStreamReader<V1::BulkImportRelationshipsRequest>(
            ImportBatch(("doc1", "alice")),
            ImportBatch(("doc1", "alice")));

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            Service(cluster).BulkImportRelationships(reader, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.AlreadyExists, ex.StatusCode);
    }

    [Fact]
    public async Task BulkExportRelationships_pages_by_limit_with_continuation_cursor()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await WriteViewer(cluster, "a", "alice");
        await WriteViewer(cluster, "b", "bob");
        await WriteViewer(cluster, "c", "carol");

        var writer = new CollectingStreamWriter<V1::BulkExportRelationshipsResponse>();
        await Service(cluster).BulkExportRelationships(
            new V1::BulkExportRelationshipsRequest { OptionalLimit = 2 }, writer, FakeServerCallContext.Default);

        Assert.Equal([2, 1], writer.Collected.Select(r => r.Relationships.Count).ToArray());
        Assert.All(writer.Collected, r => Assert.False(string.IsNullOrEmpty(r.AfterResultCursor?.Token)));
        Assert.Equal(
            ["a", "b", "c"],
            writer.Collected.SelectMany(r => r.Relationships).Select(r => r.Resource.ObjectId).Order().ToArray());

        // Resuming from the first page's cursor yields exactly the remainder.
        var resumed = new CollectingStreamWriter<V1::BulkExportRelationshipsResponse>();
        await Service(cluster).BulkExportRelationships(
            new V1::BulkExportRelationshipsRequest { OptionalLimit = 2, OptionalCursor = writer.Collected[0].AfterResultCursor },
            resumed, FakeServerCallContext.Default);
        Assert.Single(resumed.Collected.SelectMany(r => r.Relationships));
    }

    [Fact]
    public async Task BulkExportRelationships_allows_limits_up_to_the_experimental_maximum_of_100000()
    {
        // Upstream's experimental export ceiling is ExperimentalServerOptions.MaxExportBatchSize (default
        // 100_000), not PermissionsService's 10_000.
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await WriteViewer(cluster, "a", "alice");

        var writer = new CollectingStreamWriter<V1::BulkExportRelationshipsResponse>();
        await Service(cluster).BulkExportRelationships(
            new V1::BulkExportRelationshipsRequest { OptionalLimit = 100_000 }, writer, FakeServerCallContext.Default);
        Assert.Single(writer.Collected.SelectMany(r => r.Relationships));

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).BulkExportRelationships(
            new V1::BulkExportRelationshipsRequest { OptionalLimit = 100_001 },
            new CollectingStreamWriter<V1::BulkExportRelationshipsResponse>(), FakeServerCallContext.Default));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal("provided limit 100001 is greater than maximum allowed of 100000", ex.Status.Detail);
    }

    // ---- Exp* schema tooling (same behaviour as SchemaService, Exp-typed messages) ----

    [Fact]
    public async Task ExperimentalReflectSchema_returns_definitions_relations_permissions_and_caveats()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var resp = await Service(cluster).ExperimentalReflectSchema(
            new V1::ExperimentalReflectSchemaRequest(), FakeServerCallContext.Default);

        Assert.Equal(["document", "folder", "user"], resp.Definitions.Select(d => d.Name).Order().ToArray());
        var doc = Assert.Single(resp.Definitions, d => d.Name == "document");
        var viewer = Assert.Single(doc.Relations, r => r.Name == "viewer");
        Assert.Equal("document", viewer.ParentDefinitionName);
        var subject = Assert.Single(viewer.SubjectTypes);
        Assert.Equal("user", subject.SubjectDefinitionName);
        Assert.Equal(V1::ExpTypeReference.TyperefOneofCase.IsTerminalSubject, subject.TyperefCase);
        Assert.Equal(["manage", "view"], doc.Permissions.Select(p => p.Name).Order().ToArray());

        var caveat = Assert.Single(resp.Caveats);
        Assert.Equal("ip_match", caveat.Name);
        Assert.Contains(caveat.Parameters, p => p.Name == "allowed" && p.Type == "string" && p.ParentCaveatName == "ip_match");
        Assert.Contains("user_ip == allowed", caveat.Expression);
    }

    [Fact]
    public async Task ExperimentalReflectSchema_applies_filters()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var req = new V1::ExperimentalReflectSchemaRequest();
        req.OptionalFilters.Add(new V1::ExpSchemaFilter { OptionalDefinitionNameFilter = "doc" });
        var resp = await Service(cluster).ExperimentalReflectSchema(req, FakeServerCallContext.Default);

        var def = Assert.Single(resp.Definitions);
        Assert.Equal("document", def.Name);
        Assert.Empty(resp.Caveats);
    }

    [Fact]
    public async Task ExperimentalReflectSchema_mutually_exclusive_filter_is_invalid_argument()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var req = new V1::ExperimentalReflectSchemaRequest();
        req.OptionalFilters.Add(new V1::ExpSchemaFilter
        {
            OptionalDefinitionNameFilter = "document",
            OptionalCaveatNameFilter = "ip",
        });

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            Service(cluster).ExperimentalReflectSchema(req, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
    }

    [Fact]
    public async Task ExperimentalDiffSchema_reports_expected_oneof_cases()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var comparison = """
            definition user {}
            definition group {}
            caveat ip_match(allowed string, user_ip string) {
                user_ip == allowed
            }
            definition document {
                relation viewer: user | group
                relation editor: user
                relation owner: user
                permission view = viewer
            }
            definition folder {
                relation viewer: user
            }
            """;

        var resp = await Service(cluster).ExperimentalDiffSchema(
            new V1::ExperimentalDiffSchemaRequest { ComparisonSchema = comparison }, FakeServerCallContext.Default);

        Assert.Contains(resp.Diffs, d => d.DiffCase == V1::ExpSchemaDiff.DiffOneofCase.DefinitionAdded
            && d.DefinitionAdded.Name == "group");
        Assert.Contains(resp.Diffs, d => d.DiffCase == V1::ExpSchemaDiff.DiffOneofCase.RelationAdded
            && d.RelationAdded.Name == "owner" && d.RelationAdded.ParentDefinitionName == "document");
        Assert.Contains(resp.Diffs, d => d.DiffCase == V1::ExpSchemaDiff.DiffOneofCase.PermissionRemoved
            && d.PermissionRemoved.Name == "manage");
        Assert.Contains(resp.Diffs, d => d.DiffCase == V1::ExpSchemaDiff.DiffOneofCase.RelationSubjectTypeAdded
            && d.RelationSubjectTypeAdded.Relation.Name == "viewer"
            && d.RelationSubjectTypeAdded.ChangedSubjectType.SubjectDefinitionName == "group");
        Assert.Contains(resp.Diffs, d => d.DiffCase == V1::ExpSchemaDiff.DiffOneofCase.PermissionExprChanged
            && d.PermissionExprChanged.Name == "view");
    }

    [Fact]
    public async Task ExperimentalDiffSchema_invalid_comparison_is_invalid_argument()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).ExperimentalDiffSchema(
            new V1::ExperimentalDiffSchemaRequest { ComparisonSchema = "definition {{{ bad" },
            FakeServerCallContext.Default));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
    }

    [Fact]
    public async Task ExperimentalComputablePermissions_returns_permissions_reachable_from_a_relation()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var resp = await Service(cluster).ExperimentalComputablePermissions(
            new V1::ExperimentalComputablePermissionsRequest { DefinitionName = "document", RelationName = "editor" },
            FakeServerCallContext.Default);

        Assert.Equal(["manage", "view"], resp.Permissions.Select(p => p.RelationName).ToArray());
        Assert.All(resp.Permissions, p =>
        {
            Assert.Equal("document", p.DefinitionName);
            Assert.True(p.IsPermission);
        });
    }

    [Fact]
    public async Task ExperimentalComputablePermissions_unknown_relation_is_failed_precondition()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).ExperimentalComputablePermissions(
            new V1::ExperimentalComputablePermissionsRequest { DefinitionName = "document", RelationName = "nope" },
            FakeServerCallContext.Default));
        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
    }

    [Fact]
    public async Task ExperimentalDependentRelations_returns_relations_a_permission_depends_on()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var resp = await Service(cluster).ExperimentalDependentRelations(
            new V1::ExperimentalDependentRelationsRequest { DefinitionName = "document", PermissionName = "view" },
            FakeServerCallContext.Default);

        Assert.Equal(["editor", "viewer"], resp.Relations.Select(r => r.RelationName).OrderBy(x => x).ToArray());
        Assert.All(resp.Relations, r => Assert.False(r.IsPermission));
    }

    [Fact]
    public async Task ExperimentalDependentRelations_on_base_relation_is_invalid_argument()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).ExperimentalDependentRelations(
            new V1::ExperimentalDependentRelationsRequest { DefinitionName = "document", PermissionName = "viewer" },
            FakeServerCallContext.Default));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
    }

    [Fact]
    public async Task ExperimentalDependentRelations_unknown_definition_is_not_found()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).ExperimentalDependentRelations(
            new V1::ExperimentalDependentRelationsRequest { DefinitionName = "missing", PermissionName = "view" },
            FakeServerCallContext.Default));
        Assert.Equal(StatusCode.NotFound, ex.StatusCode);
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
