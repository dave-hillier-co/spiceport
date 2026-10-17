using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using V1 = Authzed.Api.V1;

namespace Spiceport.Api;

/// <summary>
/// Request-shape limit validation at the gRPC service boundary, mirroring SpiceDB's
/// <c>permissionServer</c> request contract (see <c>internal/services/v1/relationships.go</c>
/// <c>WriteRelationships</c> and <c>internal/services/v1/permissions.go</c> <c>GetCaveatContext</c>).
/// </summary>
/// <remarks>
/// SpiceDB validates a request's shape BEFORE applying/evaluating it and returns
/// <see cref="StatusCode.InvalidArgument"/> for an over-limit or malformed request. Without these
/// guards an oversized or duplicate-bearing request is silently accepted, diverging from what
/// <c>zed</c> and other clients expect. The limit values are SpiceDB's defaults.
/// </remarks>
internal static class RequestLimits
{
    /// <summary>Maximum updates per <c>WriteRelationships</c> call (SpiceDB default).</summary>
    internal const int MaxUpdatesPerWrite = 1000;

    /// <summary>Maximum preconditions per write/delete call (SpiceDB default).</summary>
    internal const int MaxPreconditionsCount = 1000;

    /// <summary>Maximum serialized bytes of a single relationship's caveat context (SpiceDB default).</summary>
    internal const int MaxRelationshipContextSize = 25_000;

    /// <summary>Maximum serialized bytes of a request's caveat context (SpiceDB default).</summary>
    internal const int MaxCaveatContextSize = 4096;

    /// <summary>
    /// Validates the shape of a <c>WriteRelationships</c> request, throwing an
    /// <see cref="RpcException"/> with <see cref="StatusCode.InvalidArgument"/> for, in SpiceDB's order: a
    /// missing required submessage, transaction metadata over
    /// <see cref="RequestCeilings.MaxTransactionMetadataSize"/> bytes, more than
    /// <see cref="MaxUpdatesPerWrite"/> updates (ERROR_REASON_TOO_MANY_UPDATES_IN_REQUEST), more than
    /// <see cref="MaxPreconditionsCount"/> preconditions, a relationship appearing in more than one update
    /// (NewDuplicateRelationshipErr), or a per-relationship caveat context larger than
    /// <see cref="MaxRelationshipContextSize"/> bytes. Mirrors SpiceDB's <c>WriteRelationships</c> validation.
    /// </summary>
    internal static void ValidateWriteRelationships(V1::WriteRelationshipsRequest request)
    {
        // SpiceDB validates the request message (protoc-gen-validate) in an interceptor before its handler
        // runs, so a missing required submessage is reported ahead of everything the handler checks.
        ValidateRequiredWriteFields(request);

        // The handler's first check is the transaction metadata size, ahead of the update and precondition
        // limits.
        RequestCeilings.ValidateTransactionMetadata(request.OptionalTransactionMetadata);

        if (request.Updates.Count > MaxUpdatesPerWrite)
            throw new RpcException(new Status(
                StatusCode.InvalidArgument,
                $"too many updates ({request.Updates.Count}) for WriteRelationships call (maximum: {MaxUpdatesPerWrite}); " +
                "consider using ImportBulkRelationships API instead"));

        if (request.OptionalPreconditions.Count > MaxPreconditionsCount)
            throw new RpcException(new Status(
                StatusCode.InvalidArgument,
                $"precondition count of {request.OptionalPreconditions.Count} is greater than maximum allowed of {MaxPreconditionsCount}"));

        // A relationship can be specified in an update only once per overall request. The key ignores the
        // caveat and expiration (SpiceDB: V1StringRelationshipWithoutCaveatOrExpiration), so a CREATE and a
        // DELETE of the same tuple in one request is also a duplicate.
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var update in request.Updates)
        {
            var key = RelationshipKeyWithoutCaveatOrExpiration(update.Relationship);
            if (!seen.Add(key))
                throw new RpcException(new Status(
                    StatusCode.InvalidArgument,
                    $"found more than one update with relationship `{key}` in this request; " +
                    "a relationship can only be specified in an update once per overall WriteRelationships request"));

            // proto.Size of the caveat: a relationship with no caveat is size 0 and is always allowed.
            var contextSize = update.Relationship.OptionalCaveat?.CalculateSize() ?? 0;
            if (contextSize > MaxRelationshipContextSize)
                throw new RpcException(new Status(
                    StatusCode.InvalidArgument,
                    $"provided relationship `{key}` exceeded maximum allowed caveat size of {MaxRelationshipContextSize}"));
        }
    }

    /// <summary>
    /// Rejects a request whose caveat context exceeds <see cref="MaxCaveatContextSize"/> serialized bytes,
    /// mirroring SpiceDB's <c>GetCaveatContext</c>. A zero-or-less limit means "no limit"; here the limit is a
    /// positive constant so it always applies. Returns the (unchanged) context for fluent use.
    /// </summary>
    internal static Struct? ValidateCaveatContextSize(Struct? context)
    {
        if (context is not null)
        {
            var size = context.CalculateSize();
            if (size > MaxCaveatContextSize)
                throw new RpcException(new Status(
                    StatusCode.InvalidArgument,
                    $"request caveat context should have less than {MaxCaveatContextSize} bytes but had {size}"));
        }

        return context;
    }

    /// <summary>protoc-gen-validate's separator between a failed field and the nested failure it wraps.</summary>
    private const string Embedded = "embedded message failed validation | caused by: ";

    /// <summary>
    /// Rejects a <c>WriteRelationships</c> request missing a submessage the <c>authzed.api.v1</c> protos mark
    /// <c>(validate.rules).message.required</c>: an update's relationship, a relationship's resource or
    /// subject, a subject's object, or a precondition's filter. An unset message field is <c>null</c> here,
    /// so without this guard the request fails with a <see cref="NullReferenceException"/> (an
    /// Internal/Unknown status). SpiceDB's first interceptor, go-grpc-middleware's
    /// <c>validator.UnaryServerInterceptor</c>, calls the protoc-gen-validate <c>ValidateAll</c> generated in
    /// authzed-go and rejects the same request with <see cref="StatusCode.InvalidArgument"/> and
    /// <c>err.Error()</c> as the detail, before the handler runs; the text follows that format.
    /// </summary>
    private static void ValidateRequiredWriteFields(V1::WriteRelationshipsRequest request)
    {
        var violations = new List<string>();
        for (var i = 0; i < request.Updates.Count; i++)
        {
            var update = $"invalid WriteRelationshipsRequest.Updates[{i}]: {Embedded}";
            var rel = request.Updates[i].Relationship;
            if (rel is null)
            {
                violations.Add($"{update}invalid RelationshipUpdate.Relationship: value is required");
                continue;
            }

            // The Relationship's own ValidateAll multi-error is the cause of a single embedded wrapper.
            var causes = new List<string>();
            if (rel.Resource is null)
                causes.Add("invalid Relationship.Resource: value is required");
            if (rel.Subject is null)
                causes.Add("invalid Relationship.Subject: value is required");
            else if (rel.Subject.Object is null)
                causes.Add($"invalid Relationship.Subject: {Embedded}invalid SubjectReference.Object: value is required");

            if (causes.Count > 0)
                violations.Add($"{update}invalid RelationshipUpdate.Relationship: {Embedded}{string.Join("; ", causes)}");
        }

        AddMissingPreconditionFilters("WriteRelationshipsRequest", request.OptionalPreconditions, violations);
        ThrowIfViolations(violations);
    }

    /// <summary>
    /// Validates the shape of a <c>DeleteRelationships</c> request: its <c>relationship_filter</c> and every
    /// precondition's <c>filter</c> are <c>(validate.rules).message.required</c>. SpiceDB's protoc-gen-validate
    /// interceptor rejects a missing one with <see cref="StatusCode.InvalidArgument"/> before the handler runs;
    /// without this guard the wire mapping dereferences the unset (<c>null</c>) message instead.
    /// </summary>
    internal static void ValidateDeleteRelationships(V1::DeleteRelationshipsRequest request)
    {
        var violations = new List<string>();
        if (request.RelationshipFilter is null)
            violations.Add("invalid DeleteRelationshipsRequest.RelationshipFilter: value is required");
        AddMissingPreconditionFilters("DeleteRelationshipsRequest", request.OptionalPreconditions, violations);
        ThrowIfViolations(violations);
    }

    private static void AddMissingPreconditionFilters(
        string requestName, IReadOnlyList<V1::Precondition> preconditions, List<string> violations)
    {
        for (var i = 0; i < preconditions.Count; i++)
        {
            if (preconditions[i].Filter is null)
                violations.Add(
                    $"invalid {requestName}.OptionalPreconditions[{i}]: {Embedded}invalid Precondition.Filter: value is required");
        }
    }

    /// <summary>
    /// Throws <see cref="StatusCode.InvalidArgument"/> with protoc-gen-validate's <c>MultiError</c> text: the
    /// violations, in field order, joined with <c>"; "</c>.
    /// </summary>
    private static void ThrowIfViolations(List<string> violations)
    {
        if (violations.Count == 0)
            return;

        throw new RpcException(new Status(StatusCode.InvalidArgument, string.Join("; ", violations)));
    }

    private static string RelationshipKeyWithoutCaveatOrExpiration(V1::Relationship rel)
    {
        var subjectRelation = string.IsNullOrEmpty(rel.Subject.OptionalRelation)
            ? string.Empty
            : "#" + rel.Subject.OptionalRelation;
        return $"{rel.Resource.ObjectType}:{rel.Resource.ObjectId}#{rel.Relation}@" +
               $"{rel.Subject.Object.ObjectType}:{rel.Subject.Object.ObjectId}{subjectRelation}";
    }
}
