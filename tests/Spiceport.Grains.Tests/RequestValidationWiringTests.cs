using Grpc.Core;
using Spiceport.Api;
using V1 = Authzed.Api.V1;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Every <c>authzed.api.v1</c> handler validates its request (<see cref="RequestValidation"/>) before doing
/// anything else, as SpiceDB's interceptors do before its handlers run. The services are built with no
/// collaborators at all: a rejected request must never reach one, so no cluster is needed. Each request
/// carries one malformed field, and the detail is SpiceDB's text for it.
/// </summary>
public class RequestValidationWiringTests
{
    private const string BadType = "Document";

    private const string TypeViolation =
        "invalid ObjectReference.ObjectType: value does not match regex pattern " +
        "\"^([a-z][a-z0-9_]{1,61}[a-z0-9]/)*[a-z][a-z0-9_]{1,62}[a-z0-9]$\"";

    private const string OptionalTypeViolation =
        "invalid RelationshipFilter.ResourceType: value does not match regex pattern " +
        "\"^(([a-z][a-z0-9_]{1,61}[a-z0-9]/)*[a-z][a-z0-9_]{1,62}[a-z0-9])?$\"";

    private const string NameViolation =
        "value does not match regex pattern \"^([a-z][a-z0-9_]{1,62}[a-z0-9])?$\"";

    private const string Embedded = "embedded message failed validation | caused by: ";

    private static readonly AuthzedPermissionsV1Service Permissions = new(null!, null!, null!, null!, null!);
    private static readonly AuthzedSchemaV1Service Schema = new(null!, null!);
    private static readonly AuthzedExperimentalV1Service Experimental = new(null!, null!);

    private static V1::ObjectReference BadResource() => new() { ObjectType = BadType, ObjectId = "doc1" };

    private static V1::SubjectReference Alice() => new()
    {
        Object = new V1::ObjectReference { ObjectType = "user", ObjectId = "alice" },
    };

    private static V1::RelationshipFilter BadFilter() => new() { ResourceType = BadType };

    private static async Task AssertRejected(Func<Task> call, string detail, StatusCode code = StatusCode.InvalidArgument)
    {
        var ex = await Assert.ThrowsAsync<RpcException>(call);
        Assert.Equal(code, ex.StatusCode);
        Assert.Equal(detail, ex.Status.Detail);
    }

    [Fact]
    public Task CheckPermission() => AssertRejected(
        () => Permissions.CheckPermission(
            new V1::CheckPermissionRequest { Resource = BadResource(), Permission = "view", Subject = Alice() },
            FakeServerCallContext.Default),
        $"invalid CheckPermissionRequest.Resource: {Embedded}{TypeViolation}");

    [Fact]
    public Task CheckBulkPermissions() => AssertRejected(
        () => Permissions.CheckBulkPermissions(
            new V1::CheckBulkPermissionsRequest
            {
                Items = { new V1::CheckBulkPermissionsRequestItem { Resource = BadResource(), Permission = "view", Subject = Alice() } },
            },
            FakeServerCallContext.Default),
        $"invalid CheckBulkPermissionsRequest.Items[0]: {Embedded}invalid CheckBulkPermissionsRequestItem.Resource: {Embedded}{TypeViolation}");

    [Fact]
    public Task ExpandPermissionTree() => AssertRejected(
        () => Permissions.ExpandPermissionTree(
            new V1::ExpandPermissionTreeRequest { Resource = BadResource(), Permission = "view" },
            FakeServerCallContext.Default),
        $"invalid ExpandPermissionTreeRequest.Resource: {Embedded}{TypeViolation}");

    [Fact]
    public Task WriteRelationships() => AssertRejected(
        () => Permissions.WriteRelationships(
            new V1::WriteRelationshipsRequest
            {
                Updates =
                {
                    new V1::RelationshipUpdate
                    {
                        Operation = V1::RelationshipUpdate.Types.Operation.Touch,
                        Relationship = new V1::Relationship { Resource = BadResource(), Relation = "viewer", Subject = Alice() },
                    },
                },
            },
            FakeServerCallContext.Default),
        $"invalid WriteRelationshipsRequest.Updates[0]: {Embedded}invalid RelationshipUpdate.Relationship: {Embedded}" +
        $"invalid Relationship.Resource: {Embedded}{TypeViolation}");

    [Fact]
    public Task DeleteRelationships() => AssertRejected(
        () => Permissions.DeleteRelationships(
            new V1::DeleteRelationshipsRequest { RelationshipFilter = BadFilter() },
            FakeServerCallContext.Default),
        $"invalid DeleteRelationshipsRequest.RelationshipFilter: {Embedded}{OptionalTypeViolation}");

    [Fact]
    public Task ReadRelationships() => AssertRejected(
        () => Permissions.ReadRelationships(
            new V1::ReadRelationshipsRequest { RelationshipFilter = BadFilter() },
            new DiscardingStreamWriter<V1::ReadRelationshipsResponse>(),
            FakeServerCallContext.Default),
        $"invalid ReadRelationshipsRequest.RelationshipFilter: {Embedded}{OptionalTypeViolation}");

    [Fact]
    public Task LookupResources() => AssertRejected(
        () => Permissions.LookupResources(
            new V1::LookupResourcesRequest { ResourceObjectType = BadType, Permission = "view", Subject = Alice() },
            new DiscardingStreamWriter<V1::LookupResourcesResponse>(),
            FakeServerCallContext.Default),
        "invalid LookupResourcesRequest.ResourceObjectType: value does not match regex pattern " +
        "\"^([a-z][a-z0-9_]{1,61}[a-z0-9]/)*[a-z][a-z0-9_]{1,62}[a-z0-9]$\"");

    [Fact]
    public Task LookupSubjects() => AssertRejected(
        () => Permissions.LookupSubjects(
            new V1::LookupSubjectsRequest { Resource = BadResource(), Permission = "view", SubjectObjectType = "user" },
            new DiscardingStreamWriter<V1::LookupSubjectsResponse>(),
            FakeServerCallContext.Default),
        $"invalid LookupSubjectsRequest.Resource: {Embedded}{TypeViolation}");

    [Fact]
    public Task ImportBulkRelationships_validates_each_received_message() => AssertRejected(
        () => Permissions.ImportBulkRelationships(
            new FakeStreamReader<V1::ImportBulkRelationshipsRequest>(
                new V1::ImportBulkRelationshipsRequest(),
                new V1::ImportBulkRelationshipsRequest
                {
                    Relationships = { new V1::Relationship { Resource = BadResource(), Relation = "viewer", Subject = Alice() } },
                }),
            FakeServerCallContext.Default),
        $"invalid ImportBulkRelationshipsRequest.Relationships[0]: {Embedded}invalid Relationship.Resource: {Embedded}{TypeViolation}");

    [Fact]
    public Task ExportBulkRelationships() => AssertRejected(
        () => Permissions.ExportBulkRelationships(
            new V1::ExportBulkRelationshipsRequest { OptionalRelationshipFilter = BadFilter() },
            new DiscardingStreamWriter<V1::ExportBulkRelationshipsResponse>(),
            FakeServerCallContext.Default),
        $"invalid ExportBulkRelationshipsRequest.OptionalRelationshipFilter: {Embedded}{OptionalTypeViolation}");

    [Fact]
    public Task WriteSchema() => AssertRejected(
        () => Schema.WriteSchema(
            new V1::WriteSchemaRequest { Schema = new string('x', 4_194_305) },
            FakeServerCallContext.Default),
        "invalid WriteSchemaRequest.Schema: value length must be at most 4194304 bytes");

    [Fact]
    public Task ReflectSchema() => AssertRejected(
        () => Schema.ReflectSchema(
            new V1::ReflectSchemaRequest { Consistency = new V1::Consistency() },
            FakeServerCallContext.Default),
        "missing handling of consistency case in ", StatusCode.Internal);

    [Fact]
    public Task DiffSchema() => AssertRejected(
        () => Schema.DiffSchema(
            new V1::DiffSchemaRequest { Consistency = new V1::Consistency() },
            FakeServerCallContext.Default),
        "missing handling of consistency case in ", StatusCode.Internal);

    [Fact]
    public Task ComputablePermissions() => AssertRejected(
        () => Schema.ComputablePermissions(
            new V1::ComputablePermissionsRequest { Consistency = new V1::Consistency() },
            FakeServerCallContext.Default),
        "missing handling of consistency case in ", StatusCode.Internal);

    [Fact]
    public Task DependentRelations() => AssertRejected(
        () => Schema.DependentRelations(
            new V1::DependentRelationsRequest { Consistency = new V1::Consistency() },
            FakeServerCallContext.Default),
        "missing handling of consistency case in ", StatusCode.Internal);

    [Fact]
    public Task RegisterRelationshipCounter() => AssertRejected(
        () => Experimental.ExperimentalRegisterRelationshipCounter(
            new V1::ExperimentalRegisterRelationshipCounterRequest { Name = "Bad" },
            FakeServerCallContext.Default),
        $"invalid ExperimentalRegisterRelationshipCounterRequest.Name: {NameViolation}; " +
        "invalid ExperimentalRegisterRelationshipCounterRequest.RelationshipFilter: value is required");

    [Fact]
    public Task UnregisterRelationshipCounter() => AssertRejected(
        () => Experimental.ExperimentalUnregisterRelationshipCounter(
            new V1::ExperimentalUnregisterRelationshipCounterRequest { Name = "Bad" },
            FakeServerCallContext.Default),
        $"invalid ExperimentalUnregisterRelationshipCounterRequest.Name: {NameViolation}");

    [Fact]
    public Task CountRelationships() => AssertRejected(
        () => Experimental.ExperimentalCountRelationships(
            new V1::ExperimentalCountRelationshipsRequest { Name = "Bad" },
            FakeServerCallContext.Default),
        $"invalid ExperimentalCountRelationshipsRequest.Name: {NameViolation}");

    private sealed class DiscardingStreamWriter<T> : IServerStreamWriter<T>
    {
        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(T message) => Task.CompletedTask;
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
