using Grpc.Core;
using Spiceport.Core;
using Spiceport.Grains;

namespace Spiceport.Api;

/// <summary>
/// Up-front schema validation at the gRPC service boundary, mirroring SpiceDB's
/// <c>namespace.CheckNamespaceAndRelation(s)</c> (see <c>internal/namespace/util.go</c>).
/// </summary>
/// <remarks>
/// SpiceDB validates the requested object type and relation/permission (and, for checks, the subject
/// type/relation) against the schema BEFORE dispatching, and returns a <see cref="StatusCode.FailedPrecondition"/>
/// error for an unknown definition or relation/permission. Without this the engine would silently
/// return a <c>NO_PERMISSION</c> verdict for a client schema/typo bug, masking it as a legitimate
/// negative answer. Keeping the check here (not in the engine) preserves the engine's narrow
/// missing-relation tolerance for TTU/arrow targets.
/// </remarks>
internal static class SchemaValidation
{
    /// <summary>A (definition, relation/permission) pair to validate against the schema.</summary>
    /// <param name="DefinitionName">The object/definition type name.</param>
    /// <param name="RelationName">The relation or permission name.</param>
    /// <param name="AllowEllipsis">
    /// When true, a relation name equal to the ellipsis (<c>...</c>) is accepted without a schema lookup
    /// (used for subject references, whose relation is normalized to the ellipsis when absent).
    /// </param>
    internal readonly record struct TypeAndRelation(string DefinitionName, string RelationName, bool AllowEllipsis);

    /// <summary>
    /// Validates each (definition, relation) pair against the snapshot, throwing an
    /// <see cref="RpcException"/> with <see cref="StatusCode.FailedPrecondition"/> on the first
    /// unknown definition or unknown relation/permission (matching SpiceDB's error messages).
    /// </summary>
    internal static void CheckNamespaceAndRelations(SchemaSnapshot snapshot, params TypeAndRelation[] checks)
    {
        var error = TryCheckNamespaceAndRelations(snapshot, checks);
        if (error is not null)
            throw error;
    }

    /// <summary>
    /// Non-throwing variant of <see cref="CheckNamespaceAndRelations"/>: returns the
    /// <see cref="RpcException"/> for the first unknown definition or relation/permission, or
    /// <c>null</c> when every pair validates. Used by the bulk-check path, which surfaces a
    /// schema/typo bug PER PAIR (in that pair's <c>google.rpc.Status</c> error) rather than failing
    /// the whole RPC — mirroring SpiceDB's <c>CheckBulkPermissions</c> (it validates each grouped
    /// item's namespaces/relations and emits a <c>CheckBulkPermissionsPair_Error</c> on failure).
    /// </summary>
    internal static RpcException? TryCheckNamespaceAndRelations(SchemaSnapshot snapshot, params TypeAndRelation[] checks)
    {
        foreach (var check in checks)
        {
            var ns = snapshot.Namespaces.FirstOrDefault(n => n.Name == check.DefinitionName);
            if (ns is null)
                return NamespaceNotFound(check.DefinitionName);

            if (check.AllowEllipsis && check.RelationName == CoreConstants.Ellipsis)
                continue;

            if (!ns.Relations.Any(r => r.Name == check.RelationName))
                return RelationNotFound(check.DefinitionName, check.RelationName);
        }

        return null;
    }

    /// <summary>
    /// Rejects a check whose subject is the public wildcard (object id <c>*</c>), mirroring SpiceDB's
    /// <c>checkInternal</c> guard (<c>internal/graph/check.go</c>): a wildcard is only meaningful as a
    /// stored subject in a relationship, never as the subject being checked. Surfaces as
    /// <see cref="StatusCode.InvalidArgument"/> with SpiceDB's exact message, rather than letting the
    /// engine silently evaluate it (where a <c>*</c>-id subject could even match a stored wildcard tuple).
    /// </summary>
    internal static void RejectWildcardSubject(string subjectObjectId)
    {
        var error = WildcardSubjectError(subjectObjectId);
        if (error is not null)
            throw error;
    }

    /// <summary>
    /// Non-throwing variant of <see cref="RejectWildcardSubject"/>: returns the
    /// <see cref="RpcException"/> when the subject is the public wildcard, otherwise <c>null</c>.
    /// Used by the bulk-check path so a wildcard subject is reported in that one pair's error rather
    /// than failing the whole request.
    /// </summary>
    internal static RpcException? WildcardSubjectError(string subjectObjectId) =>
        subjectObjectId == CoreConstants.PublicWildcard
            ? new RpcException(new Status(
                StatusCode.InvalidArgument, "invalid argument: cannot perform check on wildcard subject"))
            : null;

    /// <summary>
    /// Converts an <see cref="RpcException"/> into a <c>google.rpc.Status</c> proto for embedding in a
    /// per-pair bulk-check result, mirroring SpiceDB's <c>status.FromError(...).Proto()</c>: the numeric
    /// <c>code</c> is the gRPC status code (whose values coincide with <c>google.rpc.Code</c>) and the
    /// <c>message</c> is the status detail.
    /// </summary>
    internal static Google.Rpc.Status ToRpcStatus(RpcException ex) =>
        new() { Code = (int)ex.StatusCode, Message = ex.Status.Detail };

    private static RpcException NamespaceNotFound(string definition) =>
        new(new Status(StatusCode.FailedPrecondition, $"object definition `{definition}` not found"));

    private static RpcException RelationNotFound(string definition, string relation) =>
        new(new Status(StatusCode.FailedPrecondition,
            $"relation/permission `{relation}` not found under definition `{definition}`"));
}
