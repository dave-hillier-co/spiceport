using Grpc.Core;
using Spiceport.Api;
using V1 = Authzed.Api.V1;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Pure request-shape tests for <see cref="RequestLimits"/> (no cluster). An unset proto3 message field
/// is <c>null</c> in C#; SpiceDB rejects a missing required submessage with INVALID_ARGUMENT through its
/// protoc-gen-validate interceptor (go-grpc-middleware's validator over authzed-go's <c>ValidateAll</c>)
/// before any handler runs, so the boundary must too, rather than letting a
/// <see cref="NullReferenceException"/> surface as an Internal/Unknown status. The expected details are
/// the exact text that interceptor produces for SpiceDB v1.49.2.
/// </summary>
/// <remarks>
/// PGV wraps a nested failure as <c>invalid Outer.Field: embedded message failed validation | caused by:
/// inner</c> and joins the violations <c>ValidateAll</c> collects with <c>"; "</c>.
/// </remarks>
public class RequestLimitsTests
{
    private const string Embedded = "embedded message failed validation | caused by: ";

    private static V1::Relationship ValidRelationship() => new()
    {
        Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "doc1" },
        Relation = "viewer",
        Subject = new V1::SubjectReference
        {
            Object = new V1::ObjectReference { ObjectType = "user", ObjectId = "alice" },
        },
    };

    private static V1::WriteRelationshipsRequest WriteOf(V1::Relationship? relationship)
    {
        var update = new V1::RelationshipUpdate { Operation = V1::RelationshipUpdate.Types.Operation.Touch };
        if (relationship is not null)
            update.Relationship = relationship;
        return new V1::WriteRelationshipsRequest { Updates = { update } };
    }

    private static void AssertInvalidArgument(V1::WriteRelationshipsRequest request, string detail)
    {
        var ex = Assert.Throws<RpcException>(() => RequestLimits.ValidateWriteRelationships(request));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal(detail, ex.Status.Detail);
    }

    private static void AssertInvalidArgument(V1::DeleteRelationshipsRequest request, string detail)
    {
        var ex = Assert.Throws<RpcException>(() => RequestLimits.ValidateDeleteRelationships(request));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal(detail, ex.Status.Detail);
    }

    private const string Update0 = "invalid WriteRelationshipsRequest.Updates[0]: " + Embedded;
    private const string InRelationship = "invalid RelationshipUpdate.Relationship: " + Embedded;
    private const string MissingRelationship = "invalid RelationshipUpdate.Relationship: value is required";
    private const string MissingFilter = "invalid Precondition.Filter: value is required";

    [Fact]
    public void ValidateWriteRelationships_update_without_relationship_is_invalid_argument() =>
        AssertInvalidArgument(WriteOf(null), Update0 + MissingRelationship);

    [Fact]
    public void ValidateWriteRelationships_relationship_without_resource_is_invalid_argument()
    {
        var rel = ValidRelationship();
        rel.Resource = null;
        AssertInvalidArgument(WriteOf(rel),
            Update0 + InRelationship + "invalid Relationship.Resource: value is required");
    }

    [Fact]
    public void ValidateWriteRelationships_relationship_without_subject_is_invalid_argument()
    {
        var rel = ValidRelationship();
        rel.Subject = null;
        AssertInvalidArgument(WriteOf(rel),
            Update0 + InRelationship + "invalid Relationship.Subject: value is required");
    }

    [Fact]
    public void ValidateWriteRelationships_subject_without_object_is_invalid_argument()
    {
        var rel = ValidRelationship();
        rel.Subject.Object = null;
        AssertInvalidArgument(WriteOf(rel),
            Update0 + InRelationship + "invalid Relationship.Subject: " + Embedded +
            "invalid SubjectReference.Object: value is required");
    }

    [Fact]
    public void ValidateWriteRelationships_relationship_without_resource_or_subject_joins_both_causes()
    {
        // One embedded wrapper whose cause is the Relationship's own ValidateAll multi-error.
        var rel = ValidRelationship();
        rel.Resource = null;
        rel.Subject = null;
        AssertInvalidArgument(WriteOf(rel),
            Update0 + InRelationship + "invalid Relationship.Resource: value is required; " +
            "invalid Relationship.Subject: value is required");
    }

    [Fact]
    public void ValidateWriteRelationships_names_the_index_of_the_malformed_update()
    {
        var request = WriteOf(ValidRelationship());
        request.Updates.Add(new V1::RelationshipUpdate { Operation = V1::RelationshipUpdate.Types.Operation.Touch });
        AssertInvalidArgument(request,
            "invalid WriteRelationshipsRequest.Updates[1]: " + Embedded + MissingRelationship);
    }

    [Fact]
    public void ValidateWriteRelationships_joins_every_violation_in_field_order()
    {
        var request = WriteOf(null);
        request.Updates.Add(new V1::RelationshipUpdate { Operation = V1::RelationshipUpdate.Types.Operation.Touch });
        request.OptionalPreconditions.Add(new V1::Precondition
        {
            Operation = V1::Precondition.Types.Operation.MustMatch,
        });
        AssertInvalidArgument(request,
            Update0 + MissingRelationship + "; " +
            "invalid WriteRelationshipsRequest.Updates[1]: " + Embedded + MissingRelationship + "; " +
            "invalid WriteRelationshipsRequest.OptionalPreconditions[0]: " + Embedded + MissingFilter);
    }

    [Fact]
    public void ValidateWriteRelationships_precondition_without_filter_is_invalid_argument()
    {
        var request = WriteOf(ValidRelationship());
        request.OptionalPreconditions.Add(new V1::Precondition
        {
            Operation = V1::Precondition.Types.Operation.MustMatch,
        });
        AssertInvalidArgument(request,
            "invalid WriteRelationshipsRequest.OptionalPreconditions[0]: " + Embedded + MissingFilter);
    }

    [Fact]
    public void ValidateDeleteRelationships_without_relationship_filter_is_invalid_argument() =>
        AssertInvalidArgument(new V1::DeleteRelationshipsRequest(),
            "invalid DeleteRelationshipsRequest.RelationshipFilter: value is required");

    [Fact]
    public void ValidateDeleteRelationships_joins_a_missing_filter_and_precondition_filter()
    {
        var request = new V1::DeleteRelationshipsRequest();
        request.OptionalPreconditions.Add(new V1::Precondition
        {
            Operation = V1::Precondition.Types.Operation.MustMatch,
        });
        AssertInvalidArgument(request,
            "invalid DeleteRelationshipsRequest.RelationshipFilter: value is required; " +
            "invalid DeleteRelationshipsRequest.OptionalPreconditions[0]: " + Embedded + MissingFilter);
    }

    [Fact]
    public void ValidateWriteRelationships_accepts_a_well_formed_request()
    {
        var request = WriteOf(ValidRelationship());
        request.OptionalPreconditions.Add(new V1::Precondition
        {
            Operation = V1::Precondition.Types.Operation.MustMatch,
            Filter = new V1::RelationshipFilter { ResourceType = "document" },
        });
        RequestLimits.ValidateWriteRelationships(request);
    }
}
