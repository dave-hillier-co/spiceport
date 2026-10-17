using Google.Protobuf;
using Grpc.Core;
using V1 = Authzed.Api.V1;

namespace Spiceport.Api;

/// <summary>
/// The request-message validation SpiceDB's service interceptors run before any handler: go-grpc-middleware's
/// <c>validator</c> (protoc-gen-validate's <c>ValidateAll</c>, see <see cref="ProtoValidation"/>) and then
/// SpiceDB's <c>handwrittenvalidation</c> interceptor, which calls the <c>HandwrittenValidate</c> methods
/// authzed-go adds to a few messages (<c>proto/authzed/api/v1/00_handwritten_validation.go</c>). Either
/// failure is <see cref="StatusCode.InvalidArgument"/> with the error text as the detail.
/// </summary>
/// <remarks>
/// Every <c>authzed.api.v1</c> handler calls <see cref="Validate"/> first, as SpiceDB's interceptor chain
/// does; a client-streaming handler calls it on each message it receives.
/// </remarks>
internal static class RequestValidation
{
    /// <summary>
    /// Rejects <paramref name="request"/> with <see cref="StatusCode.InvalidArgument"/> when it breaks a
    /// <c>(validate.rules)</c> annotation or one of authzed-go's handwritten rules.
    /// </summary>
    internal static void Validate(IMessage request)
    {
        var violation = ProtoValidation.ValidateAll(request) ?? Handwritten(request);
        if (violation is not null)
            throw new RpcException(new Status(StatusCode.InvalidArgument, violation));
    }

    private const string AlphanumericRequired = "alphanumeric value is required";

    private const string WildcardRelation = "optionalrelation must be empty on subject if object ID is a wildcard";

    /// <summary>
    /// authzed-go's <c>HandwrittenValidate</c> for the top-level request types that have one; <c>null</c> when
    /// the request passes or its type has none. Each reports the first failure only, as Go's does.
    /// </summary>
    private static string? Handwritten(IMessage request) => request switch
    {
        V1::CheckPermissionRequest r => WildcardResource(r.Resource) ?? Subject(r.Subject),
        V1::ExpandPermissionTreeRequest r => WildcardResource(r.Resource),
        V1::DeleteRelationshipsRequest r => Preconditions(r.OptionalPreconditions) ?? Filter(r.RelationshipFilter),
        V1::WriteRelationshipsRequest r => Preconditions(r.OptionalPreconditions) ?? Updates(r.Updates),
        _ => null,
    };

    private static string? Preconditions(IEnumerable<V1::Precondition> preconditions) =>
        preconditions.Select(p => Filter(p.Filter)).FirstOrDefault(e => e is not null);

    private static string? Updates(IEnumerable<V1::RelationshipUpdate> updates) =>
        updates.Where(u => u.Relationship is not null)
            .Select(u => WildcardResource(u.Relationship.Resource) ?? Subject(u.Relationship.Subject))
            .FirstOrDefault(e => e is not null);

    private static string? WildcardResource(V1::ObjectReference? resource) =>
        resource?.ObjectId == "*" ? $"invalid ObjectReference.ObjectId: {AlphanumericRequired}" : null;

    private static string? Subject(V1::SubjectReference? subject) =>
        subject?.Object?.ObjectId == "*" && !string.IsNullOrEmpty(subject.OptionalRelation)
            ? $"invalid SubjectReference.OptionalRelation: {WildcardRelation}"
            : null;

    private static string? Filter(V1::RelationshipFilter? filter)
    {
        if (filter is null)
            return null;
        if (filter.OptionalResourceId == "*")
            return $"invalid RelationshipFilter.OptionalResourceId: {AlphanumericRequired}";

        var subjectFilter = filter.OptionalSubjectFilter;
        return subjectFilter?.OptionalSubjectId == "*" && !string.IsNullOrEmpty(subjectFilter.OptionalRelation?.Relation)
            ? $"invalid SubjectFilter.OptionalRelation: {WildcardRelation}"
            : null;
    }
}
