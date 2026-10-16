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
    /// <see cref="RpcException"/> with <see cref="StatusCode.InvalidArgument"/> for: more than
    /// <see cref="MaxUpdatesPerWrite"/> updates (ERROR_REASON_TOO_MANY_UPDATES_IN_REQUEST), more than
    /// <see cref="MaxPreconditionsCount"/> preconditions, a relationship appearing in more than one update
    /// (NewDuplicateRelationshipErr), or a per-relationship caveat context larger than
    /// <see cref="MaxRelationshipContextSize"/> bytes. Mirrors SpiceDB's <c>WriteRelationships</c> validation.
    /// </summary>
    internal static void ValidateWriteRelationships(V1::WriteRelationshipsRequest request)
    {
        ValidateRequiredWriteFields(request);

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

    /// <summary>
    /// Rejects a <c>WriteRelationships</c> request missing a submessage the <c>authzed.api.v1</c> protos mark
    /// <c>(buf.validate.field).required</c>: an update's relationship, a relationship's resource or subject, a
    /// subject's object, or a precondition's filter. An unset message field is <c>null</c> here, so without
    /// this guard the request fails with a <see cref="NullReferenceException"/> (an Internal/Unknown status).
    /// SpiceDB's protovalidate interceptor rejects the same request with <see cref="StatusCode.InvalidArgument"/>
    /// before the handler runs; the detail text follows protovalidate's <c>ValidationError</c> format.
    /// </summary>
    private static void ValidateRequiredWriteFields(V1::WriteRelationshipsRequest request)
    {
        var violations = new List<string>();
        for (var i = 0; i < request.Updates.Count; i++)
        {
            var rel = request.Updates[i].Relationship;
            var path = $"updates[{i}].relationship";
            if (rel is null)
            {
                violations.Add($"{path}: value is required");
                continue;
            }

            if (rel.Resource is null)
                violations.Add($"{path}.resource: value is required");
            if (rel.Subject is null)
                violations.Add($"{path}.subject: value is required");
            else if (rel.Subject.Object is null)
                violations.Add($"{path}.subject.object: value is required");
        }

        AddMissingPreconditionFilters(request.OptionalPreconditions, violations);
        ThrowIfViolations(violations);
    }

    /// <summary>
    /// Validates the shape of a <c>DeleteRelationships</c> request: its <c>relationship_filter</c> and every
    /// precondition's <c>filter</c> are <c>(buf.validate.field).required</c>. SpiceDB's protovalidate
    /// interceptor rejects a missing one with <see cref="StatusCode.InvalidArgument"/> before the handler runs;
    /// without this guard the wire mapping dereferences the unset (<c>null</c>) message instead.
    /// </summary>
    internal static void ValidateDeleteRelationships(V1::DeleteRelationshipsRequest request)
    {
        var violations = new List<string>();
        if (request.RelationshipFilter is null)
            violations.Add("relationship_filter: value is required");
        AddMissingPreconditionFilters(request.OptionalPreconditions, violations);
        ThrowIfViolations(violations);
    }

    private static void AddMissingPreconditionFilters(
        IReadOnlyList<V1::Precondition> preconditions, List<string> violations)
    {
        for (var i = 0; i < preconditions.Count; i++)
        {
            if (preconditions[i].Filter is null)
                violations.Add($"optional_preconditions[{i}].filter: value is required");
        }
    }

    /// <summary>Throws <see cref="StatusCode.InvalidArgument"/> in protovalidate's <c>ValidationError</c> format.</summary>
    private static void ThrowIfViolations(List<string> violations)
    {
        if (violations.Count == 0)
            return;

        var detail = violations.Count == 1
            ? "validation error: " + violations[0]
            : "validation errors:" + string.Concat(violations.Select(v => "\n - " + v));
        throw new RpcException(new Status(StatusCode.InvalidArgument, detail));
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
