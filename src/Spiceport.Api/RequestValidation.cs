using Google.Protobuf;
using Grpc.Core;
using V1 = Authzed.Api.V1;

namespace Spiceport.Api;

/// <summary>
/// The request-message validation SpiceDB's interceptors run before any handler, in their order: the
/// consistency middleware's checks (<c>pkg/middleware/consistency</c>, in the default chain ahead of the
/// service-specific interceptors), then go-grpc-middleware's
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
        CheckConsistency(request);

        var violation = ProtoValidation.ValidateAll(request) ?? Handwritten(request);
        if (violation is not null)
            throw new RpcException(new Status(StatusCode.InvalidArgument, violation));
    }

    /// <summary>
    /// The request checks of SpiceDB's consistency middleware (<c>addRevisionToContextFromConsistency</c>),
    /// which picks the revision a request with a <c>consistency</c> field reads at before the
    /// protoc-gen-validate interceptor runs. A present <c>optional_cursor</c> supplies the revision and
    /// wins; otherwise an absent consistency or <c>minimize_latency</c> or <c>fully_consistent</c> needs
    /// nothing; a ZedToken must decode; and anything else falls to the middleware's default case,
    /// INTERNAL <c>missing handling of consistency case in %v</c>.
    /// </summary>
    /// <remarks>
    /// Only what can be decided without the datastore is reproduced here: the default case, an empty
    /// ZedToken (which never decodes: INVALID_ARGUMENT <c>invalid revision requested</c>), and an empty
    /// cursor token (which decodes to a cursor with no version: INTERNAL <c>unexpected consistency
    /// middleware error: cursor pointer was nil</c>). A non-empty token or cursor is still resolved in the
    /// handler, after field validation, so an undecodable one on an otherwise malformed request reports the
    /// field violation where SpiceDB would report the token. Spiceport's cursors are its own format, so
    /// SpiceDB's cursor decoding cannot be reproduced at all.
    /// </remarks>
    private static void CheckConsistency(IMessage request)
    {
        var descriptor = request.Descriptor;
        var consistencyField = descriptor.FindFieldByName("consistency");
        if (consistencyField?.MessageType != V1::Consistency.Descriptor)
            return;

        var cursorField = descriptor.FindFieldByName("optional_cursor");
        if (cursorField?.MessageType == V1::Cursor.Descriptor
            && cursorField.Accessor.GetValue(request) is V1::Cursor cursor)
        {
            if (cursor.Token.Length == 0)
                throw new RpcException(new Status(
                    StatusCode.Internal, "unexpected consistency middleware error: cursor pointer was nil"));
            return;
        }

        if (consistencyField.Accessor.GetValue(request) is not V1::Consistency consistency)
            return;

        switch (consistency.RequirementCase)
        {
            case V1::Consistency.RequirementOneofCase.MinimizeLatency when consistency.MinimizeLatency:
            case V1::Consistency.RequirementOneofCase.FullyConsistent when consistency.FullyConsistent:
                return;
            case V1::Consistency.RequirementOneofCase.AtLeastAsFresh:
            case V1::Consistency.RequirementOneofCase.AtExactSnapshot:
                var token = consistency.AtLeastAsFresh ?? consistency.AtExactSnapshot;
                if (token.Token.Length == 0)
                    throw new RpcException(new Status(StatusCode.InvalidArgument, "invalid revision requested"));
                return;
            default:
                throw new RpcException(new Status(
                    StatusCode.Internal, $"missing handling of consistency case in {GoText(consistency)}"));
        }
    }

    /// <summary>
    /// Go's <c>%v</c> of a <c>*v1.Consistency</c> that reaches the middleware's default case: the compact
    /// text form, which is empty for no requirement and the one boolean field otherwise.
    /// </summary>
    private static string GoText(V1::Consistency consistency) => consistency.RequirementCase switch
    {
        V1::Consistency.RequirementOneofCase.MinimizeLatency => $"minimize_latency:{GoBool(consistency.MinimizeLatency)}",
        V1::Consistency.RequirementOneofCase.FullyConsistent => $"fully_consistent:{GoBool(consistency.FullyConsistent)}",
        _ => string.Empty,
    };

    private static string GoBool(bool value) => value ? "true" : "false";

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
