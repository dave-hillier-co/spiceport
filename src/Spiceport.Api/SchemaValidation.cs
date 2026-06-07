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
        foreach (var check in checks)
        {
            var ns = snapshot.Namespaces.FirstOrDefault(n => n.Name == check.DefinitionName);
            if (ns is null)
                throw NamespaceNotFound(check.DefinitionName);

            if (check.AllowEllipsis && check.RelationName == CoreConstants.Ellipsis)
                continue;

            if (!ns.Relations.Any(r => r.Name == check.RelationName))
                throw RelationNotFound(check.DefinitionName, check.RelationName);
        }
    }

    private static RpcException NamespaceNotFound(string definition) =>
        new(new Status(StatusCode.FailedPrecondition, $"object definition `{definition}` not found"));

    private static RpcException RelationNotFound(string definition, string relation) =>
        new(new Status(StatusCode.FailedPrecondition,
            $"relation/permission `{relation}` not found under definition `{definition}`"));
}
