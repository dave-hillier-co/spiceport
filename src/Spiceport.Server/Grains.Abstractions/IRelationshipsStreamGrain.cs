namespace Spiceport.Grains.Abstractions;

/// <summary>
/// Streams the data-plane read ops (ReadRelationships, BulkExportRelationships) as native Orleans
/// <see cref="IAsyncEnumerable{T}"/> grain calls over a single pinned snapshot, so the gRPC front door drives
/// one continuous stream with runtime backpressure instead of a per-page service→grain round-trip. Each item
/// carries its own opaque resume cursor, so a client-facing resume is byte-for-byte unchanged — the internal
/// page loop is deleted, the client cursor contract is not.
/// </summary>
/// <remarks>
/// Like <see cref="IReverseOpsStreamGrain"/> this grain is <see cref="IGrainWithGuidKey"/> under DEFAULT
/// placement, NOT a <c>[StatelessWorker]</c>: native <see cref="IAsyncEnumerable{T}"/> streaming holds the
/// live enumerator on one specific activation keyed by the caller-minted request id, so the calling service
/// mints a fresh <see cref="System.Guid"/> per RPC and every <c>MoveNext</c> lands on the same activation.
/// The write side and all other data-plane ops stay on the stateless-worker <see cref="IRelationshipsGrain"/>.
/// </remarks>
public interface IRelationshipsStreamGrain : IGrainWithGuidKey
{
    /// <summary>
    /// Streams relationships matching the filter, in ascending canonical-tuple order, over one revision
    /// resolved once at the start. Each item carries the canonical tuple as its resume cursor (resumption
    /// skips tuples at or before it) and the per-message read-at token. The caller applies any client limit
    /// by stopping enumeration.
    /// </summary>
    IAsyncEnumerable<RelationshipStreamItem> StreamReadRelationships(
        ReadRelationshipsArgs args, CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams a bulk export over a single pinned snapshot: with no cursor the grain resolves and pins a
    /// revision from the request consistency; with a cursor it reads the exact revision the cursor encodes.
    /// Each item's resume cursor carries that pinned revision plus the last tuple, so a reconnect reads the
    /// same snapshot. The caller applies any client limit/batching by how it consumes the stream.
    /// </summary>
    IAsyncEnumerable<RelationshipStreamItem> StreamBulkExportRelationships(
        BulkExportRelationshipsArgs args, CancellationToken cancellationToken = default);
}
