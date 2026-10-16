using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;

namespace Spiceport.Api;

/// <summary>
/// Per-RPC ceilings on the <c>authzed.api.v1</c> surface, mirroring SpiceDB's
/// <c>PermissionsServerConfig</c> defaults (<c>internal/services/v1/relationships.go</c>) and the checks
/// its handlers run before doing any work. Every violation is <see cref="StatusCode.InvalidArgument"/>
/// with SpiceDB's message.
/// </summary>
internal static class RequestCeilings
{
    /// <summary>Maximum <c>optional_limit</c> for ReadRelationships (SpiceDB <c>MaxReadRelationshipsLimit</c>).</summary>
    internal const uint MaxReadRelationshipsLimit = 1_000;

    /// <summary>Maximum <c>optional_limit</c> for DeleteRelationships (SpiceDB <c>MaxDeleteRelationshipsLimit</c>).</summary>
    internal const uint MaxDeleteRelationshipsLimit = 1_000;

    /// <summary>Maximum <c>optional_limit</c> for LookupResources (SpiceDB <c>MaxLookupResourcesLimit</c>).</summary>
    internal const uint MaxLookupResourcesLimit = 1_000;

    /// <summary>Maximum <c>optional_limit</c> for ExportBulkRelationships (SpiceDB <c>MaxBulkExportRelationshipsLimit</c>).</summary>
    internal const uint MaxBulkExportRelationshipsLimit = 10_000;

    /// <summary>
    /// Maximum JSON-encoded bytes of a write's or delete's <c>optional_transaction_metadata</c> (SpiceDB
    /// <c>MaximumTransactionMetadataSize</c>).
    /// </summary>
    internal const int MaxTransactionMetadataSize = 65_000;

    /// <summary>
    /// Rejects a non-zero <paramref name="providedLimit"/> above <paramref name="maxLimit"/> (SpiceDB
    /// <c>NewExceedsMaximumLimitErr</c>). Zero means "no limit" and always passes.
    /// </summary>
    internal static void ValidateLimit(uint providedLimit, uint maxLimit)
    {
        if (providedLimit > 0 && providedLimit > maxLimit)
        {
            throw new RpcException(new Status(
                StatusCode.InvalidArgument,
                $"provided limit {providedLimit} is greater than maximum allowed of {maxLimit}"));
        }
    }

    /// <summary>
    /// Rejects transaction metadata whose JSON encoding exceeds <see cref="MaxTransactionMetadataSize"/>
    /// bytes (SpiceDB <c>validateTransactionMetadata</c>, which measures <c>MarshalJSON</c> output, and
    /// <c>NewTransactionMetadataTooLargeErr</c>). Absent metadata always passes.
    /// </summary>
    internal static void ValidateTransactionMetadata(Struct? metadata)
    {
        if (metadata is null)
            return;

        var size = System.Text.Encoding.UTF8.GetByteCount(JsonFormatter.Default.Format(metadata));
        if (size > MaxTransactionMetadataSize)
        {
            throw new RpcException(new Status(
                StatusCode.InvalidArgument,
                $"metadata size of {size} is greater than maximum allowed of {MaxTransactionMetadataSize}"));
        }
    }
}
