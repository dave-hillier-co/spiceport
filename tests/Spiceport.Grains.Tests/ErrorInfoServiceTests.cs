using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Spiceport.Api;
using Spiceport.Core;
using Spiceport.Grains;
using Spiceport.Grains.Abstractions;
using V1 = Authzed.Api.V1;
using static Spiceport.Grains.Tests.ErrorInfoTests;

namespace Spiceport.Grains.Tests;

/// <summary>
/// The <c>google.rpc.ErrorInfo</c> SpiceDB attaches to the reasoned errors that originate past the request
/// checks — in the schema, the datastore, the dispatcher — driven through the <c>authzed.api.v1</c> services
/// in-process over the test cluster. Each test names the SpiceDB v1.49.2 error type it mirrors.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class ErrorInfoServiceTests
{
    private const string Schema = """
        definition user {}

        definition group {
            relation member: user
        }

        caveat only_on_tuesday(day string) {
            day == "tuesday"
        }

        definition document {
            relation viewer: user | group#member
            relation caveated_viewer: user with only_on_tuesday
            permission view = viewer
        }
        """;

    private static AuthzedPermissionsV1Service Permissions(MeshTestCluster cluster, IPermissionChecker? checker = null) =>
        new(checker ?? cluster.Services.GetRequiredService<IPermissionChecker>(), cluster.GrainFactory,
            cluster.ReverseOps, cluster.RelationshipReads, cluster.Services.GetRequiredService<ISchemaProvider>());

    private static AuthzedExperimentalV1Service Experimental(MeshTestCluster cluster) =>
        new(cluster.GrainFactory, cluster.SchemaProvider);

    private static V1::Relationship Rel(
        string relation, string subjectType = "user", string subjectId = "alice", string subjectRelation = "",
        string resourceType = "document") => new()
        {
            Resource = new V1::ObjectReference { ObjectType = resourceType, ObjectId = "readme" },
            Relation = relation,
            Subject = new V1::SubjectReference
            {
                Object = new V1::ObjectReference { ObjectType = subjectType, ObjectId = subjectId },
                OptionalRelation = subjectRelation,
            },
        };

    private static V1::WriteRelationshipsRequest Write(
        V1::Relationship rel, V1::RelationshipUpdate.Types.Operation operation = V1::RelationshipUpdate.Types.Operation.Touch) =>
        new() { Updates = { new V1::RelationshipUpdate { Operation = operation, Relationship = rel } } };

    private static async Task<RpcException> WriteFails(MeshTestCluster cluster, V1::WriteRelationshipsRequest request) =>
        await Assert.ThrowsAsync<RpcException>(() => Permissions(cluster).WriteRelationships(request, FakeServerCallContext.Default));

    // namespace.NewNamespaceNotFoundErr through rewriteError
    [Fact]
    public async Task Writing_to_an_unknown_definition_is_unknown_definition()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        AssertErrorInfo(
            await WriteFails(cluster, Write(Rel("viewer", resourceType: "folder"))),
            StatusCode.FailedPrecondition, "ERROR_REASON_UNKNOWN_DEFINITION", ("definition_name", "folder"));
    }

    // namespace.NewRelationNotFoundErr through rewriteError, for the subject's relation
    [Fact]
    public async Task Writing_an_unknown_subject_relation_is_unknown_relation_or_permission()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        AssertErrorInfo(
            await WriteFails(cluster, Write(Rel("viewer", "group", "eng", "admin"))),
            StatusCode.FailedPrecondition, "ERROR_REASON_UNKNOWN_RELATION_OR_PERMISSION",
            ("definition_name", "group"), ("relation_or_permission_name", "admin"));
    }

    // CannotWriteToPermissionError
    [Fact]
    public async Task Writing_to_a_permission_is_cannot_update_permission()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        AssertErrorInfo(
            await WriteFails(cluster, Write(Rel("view"))),
            StatusCode.InvalidArgument, "ERROR_REASON_CANNOT_UPDATE_PERMISSION",
            ("definition_name", "document"), ("permission_name", "view"));
    }

    // InvalidSubjectTypeError
    [Fact]
    public async Task Writing_a_disallowed_subject_type_is_invalid_subject_type()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        AssertErrorInfo(
            await WriteFails(cluster, Write(Rel("viewer", "group", "eng"))),
            StatusCode.InvalidArgument, "ERROR_REASON_INVALID_SUBJECT_TYPE",
            ("definition_name", "document"), ("relation_name", "viewer"), ("subject_type", "group"));
    }

    // InvalidSubjectTypeError with the caveats the subject needed
    [Fact]
    public async Task Writing_without_a_required_caveat_names_the_allowed_caveats()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        AssertErrorInfo(
            await WriteFails(cluster, Write(Rel("caveated_viewer"))),
            StatusCode.InvalidArgument, "ERROR_REASON_INVALID_SUBJECT_TYPE",
            ("definition_name", "document"), ("relation_name", "caveated_viewer"), ("subject_type", "user"),
            ("allowed_caveats", "only_on_tuesday"));
    }

    // PreconditionFailedError
    [Fact]
    public async Task An_unmet_write_precondition_is_a_precondition_failure()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var request = Write(Rel("viewer"));
        request.OptionalPreconditions.Add(new V1::Precondition
        {
            Operation = V1::Precondition.Types.Operation.MustMatch,
            Filter = new V1::RelationshipFilter
            {
                ResourceType = "document",
                OptionalResourceIdPrefix = "read",
                OptionalSubjectFilter = new V1::SubjectFilter
                {
                    SubjectType = "group",
                    OptionalRelation = new V1::SubjectFilter.Types.RelationFilter { Relation = "" },
                },
            },
        });

        AssertErrorInfo(
            await WriteFails(cluster, request),
            StatusCode.FailedPrecondition, "ERROR_REASON_WRITE_OR_DELETE_PRECONDITION_FAILURE",
            ("precondition_operation", "OPERATION_MUST_MATCH"),
            ("precondition_resource_type", "document"),
            ("precondition_resource_id_prefix", "read"),
            ("precondition_subject_type", "group"),
            ("precondition_subject_relation", ""));
    }

    // PreconditionFailedError on a delete
    [Fact]
    public async Task An_unmet_delete_precondition_is_a_precondition_failure()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var request = new V1::DeleteRelationshipsRequest
        {
            RelationshipFilter = new V1::RelationshipFilter { ResourceType = "document" },
            OptionalPreconditions =
            {
                new V1::Precondition
                {
                    Operation = V1::Precondition.Types.Operation.MustMatch,
                    Filter = new V1::RelationshipFilter
                    {
                        ResourceType = "document",
                        OptionalResourceId = "readme",
                        OptionalRelation = "viewer",
                        OptionalSubjectFilter = new V1::SubjectFilter { SubjectType = "user", OptionalSubjectId = "alice" },
                    },
                },
            },
        };

        AssertErrorInfo(
            await Assert.ThrowsAsync<RpcException>(() => Permissions(cluster).DeleteRelationships(request, FakeServerCallContext.Default)),
            StatusCode.FailedPrecondition, "ERROR_REASON_WRITE_OR_DELETE_PRECONDITION_FAILURE",
            ("precondition_operation", "OPERATION_MUST_MATCH"),
            ("precondition_resource_type", "document"),
            ("precondition_resource_id", "readme"),
            ("precondition_relation", "viewer"),
            ("precondition_subject_type", "user"),
            ("precondition_subject_id", "alice"));
    }

    private static readonly (string, string)[] RecreatedReadme =
    [
        ("relationship", "document:readme#viewer@group:eng#member"),
        ("resource_type", "document"),
        ("resource_object_id", "readme"),
        ("resource_relation", "viewer"),
        ("subject_type", "group"),
        ("subject_object_id", "eng"),
        ("subject_relation", "member"),
    ];

    // CreateRelationshipExistsError
    [Fact]
    public async Task Creating_an_existing_relationship_is_an_attempt_to_recreate_it()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var rel = Rel("viewer", "group", "eng", "member");
        await Permissions(cluster).WriteRelationships(Write(rel), FakeServerCallContext.Default);

        AssertErrorInfo(
            await WriteFails(cluster, Write(rel, V1::RelationshipUpdate.Types.Operation.Create)),
            StatusCode.AlreadyExists, "ERROR_REASON_ATTEMPT_TO_RECREATE_RELATIONSHIP", RecreatedReadme);
    }

    // CreateRelationshipExistsError from a bulk import
    [Fact]
    public async Task Importing_an_existing_relationship_is_an_attempt_to_recreate_it()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var rel = Rel("viewer", "group", "eng", "member");
        await Permissions(cluster).WriteRelationships(Write(rel), FakeServerCallContext.Default);

        var reader = new FakeStreamReader<V1::ImportBulkRelationshipsRequest>(
            new V1::ImportBulkRelationshipsRequest { Relationships = { rel } });
        AssertErrorInfo(
            await Assert.ThrowsAsync<RpcException>(() => Permissions(cluster).ImportBulkRelationships(reader, FakeServerCallContext.Default)),
            StatusCode.AlreadyExists, "ERROR_REASON_ATTEMPT_TO_RECREATE_RELATIONSHIP", RecreatedReadme);
    }

    // CouldNotTransactionallyDeleteError
    [Fact]
    public async Task A_transactional_delete_over_its_limit_names_the_filter()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var write = new V1::WriteRelationshipsRequest();
        foreach (var user in new[] { "alice", "bob" })
            write.Updates.Add(new V1::RelationshipUpdate { Operation = V1::RelationshipUpdate.Types.Operation.Touch, Relationship = Rel("viewer", subjectId: user) });
        await Permissions(cluster).WriteRelationships(write, FakeServerCallContext.Default);

        var delete = new V1::DeleteRelationshipsRequest
        {
            RelationshipFilter = new V1::RelationshipFilter
            {
                ResourceType = "document",
                OptionalRelation = "viewer",
                OptionalSubjectFilter = new V1::SubjectFilter
                {
                    SubjectType = "user",
                    OptionalRelation = new V1::SubjectFilter.Types.RelationFilter { Relation = "" },
                },
            },
            OptionalLimit = 1,
        };
        AssertErrorInfo(
            await Assert.ThrowsAsync<RpcException>(() => Permissions(cluster).DeleteRelationships(delete, FakeServerCallContext.Default)),
            StatusCode.InvalidArgument, "ERROR_REASON_TOO_MANY_RELATIONSHIPS_FOR_TRANSACTIONAL_DELETE",
            ("limit", "1"),
            ("filter_resource_type", "document"),
            ("filter_relation", "viewer"),
            ("filter_subject_type", "user"),
            ("filter_subject_relation", ""));
    }

    // dispatch.MaxDepthExceededError
    [Fact]
    public async Task A_check_past_the_maximum_depth_is_maximum_depth_exceeded()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Permissions(cluster, new ThrowingChecker(new MaxDepthExceededException()));

        AssertErrorInfo(
            await Assert.ThrowsAsync<RpcException>(() => service.CheckPermission(
                new V1::CheckPermissionRequest
                {
                    Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
                    Permission = "view",
                    Subject = Rel("viewer").Subject,
                },
                FakeServerCallContext.Default)),
            StatusCode.FailedPrecondition, "ERROR_REASON_MAXIMUM_DEPTH_EXCEEDED");
    }

    // A bulk-check pair error carries its ErrorInfo in the pair's google.rpc.Status.
    [Fact]
    public async Task A_bulk_check_pair_error_carries_its_error_info()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var response = await Permissions(cluster).CheckBulkPermissions(
            new V1::CheckBulkPermissionsRequest
            {
                Items =
                {
                    new V1::CheckBulkPermissionsRequestItem
                    {
                        Resource = new V1::ObjectReference { ObjectType = "folder", ObjectId = "readme" },
                        Permission = "view",
                        Subject = Rel("viewer").Subject,
                    },
                },
            },
            FakeServerCallContext.Default);

        var error = Assert.Single(response.Pairs).Error;
        Assert.Equal((int)StatusCode.FailedPrecondition, error.Code);
        var info = Assert.Single(error.Details).Unpack<Google.Rpc.ErrorInfo>();
        Assert.Equal("ERROR_REASON_UNKNOWN_DEFINITION", info.Reason);
        Assert.Equal("folder", info.Metadata["definition_name"]);
    }

    private static V1::RelationshipFilter CounterFilter() => new()
    {
        ResourceType = "document",
        OptionalRelation = "viewer",
        OptionalSubjectFilter = new V1::SubjectFilter { SubjectType = "user" },
    };

    // CounterAlreadyRegisteredError through rewriteError: the metadata describes the NEW filter.
    [Fact]
    public async Task Registering_a_counter_twice_is_counter_already_registered()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var register = new V1::ExperimentalRegisterRelationshipCounterRequest { Name = "viewers", RelationshipFilter = CounterFilter() };
        await Experimental(cluster).ExperimentalRegisterRelationshipCounter(register, FakeServerCallContext.Default);

        AssertErrorInfo(
            await Assert.ThrowsAsync<RpcException>(() =>
                Experimental(cluster).ExperimentalRegisterRelationshipCounter(register, FakeServerCallContext.Default)),
            StatusCode.FailedPrecondition, "ERROR_REASON_COUNTER_ALREADY_REGISTERED",
            ("counter_name", "viewers"),
            ("new_filter_resource_type", "document"),
            ("new_filter_resource_id", ""),
            ("new_filter_resource_id_prefix", ""),
            ("new_filter_relation", "viewer"),
            ("new_filter_subject_type", "user"),
            ("new_filter_subject_id", ""),
            ("new_filter_subject_relation", ""));
    }

    // CounterNotRegisteredError through rewriteError
    [Fact]
    public async Task Counting_an_unregistered_counter_is_counter_not_registered()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        AssertErrorInfo(
            await Assert.ThrowsAsync<RpcException>(() => Experimental(cluster).ExperimentalCountRelationships(
                new V1::ExperimentalCountRelationshipsRequest { Name = "missing" }, FakeServerCallContext.Default)),
            StatusCode.FailedPrecondition, "ERROR_REASON_COUNTER_NOT_REGISTERED", ("counter_name", "missing"));
        AssertErrorInfo(
            await Assert.ThrowsAsync<RpcException>(() => Experimental(cluster).ExperimentalUnregisterRelationshipCounter(
                new V1::ExperimentalUnregisterRelationshipCounterRequest { Name = "missing" }, FakeServerCallContext.Default)),
            StatusCode.FailedPrecondition, "ERROR_REASON_COUNTER_NOT_REGISTERED", ("counter_name", "missing"));
    }

    // spiceerrors.WithCodeAndReason(errors.New("name must be provided"), InvalidArgument, UNSPECIFIED): the
    // counter-name pattern admits the empty string, so the handlers reject it themselves.
    [Fact]
    public async Task A_counter_without_a_name_is_rejected_before_the_datastore()
    {
        var service = new AuthzedExperimentalV1Service(null!, null!);
        AssertErrorInfo(
            await Assert.ThrowsAsync<RpcException>(() => service.ExperimentalRegisterRelationshipCounter(
                new V1::ExperimentalRegisterRelationshipCounterRequest { RelationshipFilter = CounterFilter() },
                FakeServerCallContext.Default)),
            StatusCode.InvalidArgument, "ERROR_REASON_UNSPECIFIED");
        AssertErrorInfo(
            await Assert.ThrowsAsync<RpcException>(() => service.ExperimentalUnregisterRelationshipCounter(
                new V1::ExperimentalUnregisterRelationshipCounterRequest(), FakeServerCallContext.Default)),
            StatusCode.InvalidArgument, "ERROR_REASON_UNSPECIFIED");
        var count = await Assert.ThrowsAsync<RpcException>(() => service.ExperimentalCountRelationships(
            new V1::ExperimentalCountRelationshipsRequest(), FakeServerCallContext.Default));
        AssertErrorInfo(count, StatusCode.InvalidArgument, "ERROR_REASON_UNSPECIFIED");
        Assert.Equal("name must be provided", count.Status.Detail);
    }

    private static AuthzedSchemaV1Service SchemaService(MeshTestCluster cluster) =>
        new(cluster.GrainFactory, cluster.SchemaProvider);

    // schema.NewRelationNotFoundErr through rewriteError
    [Fact]
    public async Task Introspecting_an_unknown_relation_is_unknown_relation_or_permission()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        AssertErrorInfo(
            await Assert.ThrowsAsync<RpcException>(() => SchemaService(cluster).DependentRelations(
                new V1::DependentRelationsRequest { DefinitionName = "document", PermissionName = "edit" },
                FakeServerCallContext.Default)),
            StatusCode.FailedPrecondition, "ERROR_REASON_UNKNOWN_RELATION_OR_PERMISSION",
            ("definition_name", "document"), ("relation_or_permission_name", "edit"));
    }

    // NotAPermissionError
    [Fact]
    public async Task Asking_for_the_dependents_of_a_relation_is_not_a_permission()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        AssertErrorInfo(
            await Assert.ThrowsAsync<RpcException>(() => SchemaService(cluster).DependentRelations(
                new V1::DependentRelationsRequest { DefinitionName = "document", PermissionName = "viewer" },
                FakeServerCallContext.Default)),
            StatusCode.InvalidArgument, "ERROR_REASON_UNKNOWN_RELATION_OR_PERMISSION", ("relationName", "viewer"));
    }

    /// <summary>An <see cref="IPermissionChecker"/> that always throws the supplied exception.</summary>
    private sealed class ThrowingChecker(Exception ex) : IPermissionChecker
    {
        public Task<PermissionCheckResult> Check(
            string resourceType, string resourceId, string permission, ObjectAndRelation subject,
            IReadOnlyDictionary<string, object?>? caveatContext, ConsistencyRequirement? consistency = null,
            CancellationToken ct = default) => throw ex;

        public Task<BatchCheckResult> BatchCheck(
            IReadOnlyList<BatchCheckItem> items, ConsistencyRequirement? consistency = null,
            CancellationToken ct = default) => throw ex;
    }

    private sealed class FakeStreamReader<T>(params T[] messages) : IAsyncStreamReader<T>
    {
        private int _index = -1;

        public T Current => messages[_index];

        public Task<bool> MoveNext(CancellationToken cancellationToken) => Task.FromResult(++_index < messages.Length);
    }

    private sealed class FakeServerCallContext : ServerCallContext
    {
        public static FakeServerCallContext Default { get; } = new();

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

        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => Task.CompletedTask;
    }
}
