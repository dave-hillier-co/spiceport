using Grpc.Core;
using Spiceport.Api;
using V1 = Authzed.Api.V1;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Pure request-shape tests for <see cref="RequestLimits"/> (no cluster). An unset proto3 message field
/// is <c>null</c> in C#; SpiceDB rejects a missing required submessage with INVALID_ARGUMENT through its
/// protovalidate interceptor before any handler runs, so the boundary must too, rather than letting a
/// <see cref="NullReferenceException"/> surface as an Internal/Unknown status.
/// </summary>
public class RequestLimitsTests
{
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

    private static RpcException AssertInvalidArgument(V1::WriteRelationshipsRequest request, string fieldPath)
    {
        var ex = Assert.Throws<RpcException>(() => RequestLimits.ValidateWriteRelationships(request));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Contains($"{fieldPath}: value is required", ex.Status.Detail);
        return ex;
    }

    [Fact]
    public void ValidateWriteRelationships_update_without_relationship_is_invalid_argument() =>
        AssertInvalidArgument(WriteOf(null), "updates[0].relationship");

    [Fact]
    public void ValidateWriteRelationships_relationship_without_resource_is_invalid_argument()
    {
        var rel = ValidRelationship();
        rel.Resource = null;
        AssertInvalidArgument(WriteOf(rel), "updates[0].relationship.resource");
    }

    [Fact]
    public void ValidateWriteRelationships_relationship_without_subject_is_invalid_argument()
    {
        var rel = ValidRelationship();
        rel.Subject = null;
        AssertInvalidArgument(WriteOf(rel), "updates[0].relationship.subject");
    }

    [Fact]
    public void ValidateWriteRelationships_subject_without_object_is_invalid_argument()
    {
        var rel = ValidRelationship();
        rel.Subject.Object = null;
        AssertInvalidArgument(WriteOf(rel), "updates[0].relationship.subject.object");
    }

    [Fact]
    public void ValidateWriteRelationships_names_the_index_of_the_malformed_update()
    {
        var request = WriteOf(ValidRelationship());
        request.Updates.Add(new V1::RelationshipUpdate { Operation = V1::RelationshipUpdate.Types.Operation.Touch });
        AssertInvalidArgument(request, "updates[1].relationship");
    }

    [Fact]
    public void ValidateWriteRelationships_precondition_without_filter_is_invalid_argument()
    {
        var request = WriteOf(ValidRelationship());
        request.OptionalPreconditions.Add(new V1::Precondition
        {
            Operation = V1::Precondition.Types.Operation.MustMatch,
        });
        AssertInvalidArgument(request, "optional_preconditions[0].filter");
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
