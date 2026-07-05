namespace Spiceport.Grains.Abstractions;

/// <summary>
/// Streams the reverse engine ops (LookupSubjects, LookupResources) as native Orleans
/// <see cref="IAsyncEnumerable{T}"/> grain calls, so the gRPC front door drives one continuous stream with
/// runtime backpressure instead of a per-page service→grain round-trip. Each item carries its own opaque
/// resume cursor, so a client-facing limited stream still resumes byte-for-byte from a fresh grain
/// activation — the internal page loop is deleted, the client cursor contract is not.
/// </summary>
/// <remarks>
/// Unlike <see cref="IReverseOpsGrain"/> (a stateless worker keyed by the constant <c>0</c>), this grain is
/// <see cref="IGrainWithGuidKey"/> and runs under Orleans' DEFAULT placement — it is deliberately NOT a
/// <c>[StatelessWorker]</c>. The grain-side extension that backs native <see cref="IAsyncEnumerable{T}"/>
/// streaming holds the live enumerator in memory on ONE specific activation keyed by a client-minted request
/// id; two calls through the same reference to a stateless worker can land on different activations, which
/// would break the enumeration. The calling service therefore mints a fresh <see cref="System.Guid"/> per
/// RPC so <c>StartEnumeration</c> and every <c>MoveNext</c> for that stream target the SAME brand-new
/// activation. Per-stream activations are reclaimed by ordinary idle collection.
/// <para>
/// Cancellation flows via the trailing <see cref="System.Threading.CancellationToken"/> (a real Orleans 10.1
/// grain-method token, checked cooperatively inside the iterator). <c>Expand</c> stays unary on
/// <see cref="IReverseOpsGrain"/> — it returns a whole tree, has no cursor, and needs no streaming.
/// </para>
/// </remarks>
public interface IReverseOpsStreamGrain : IGrainWithGuidKey
{
    /// <summary>
    /// Streams the subjects (of the requested type/subrelation) holding the resource's permission, one at a
    /// time, each with the opaque cursor positioned immediately after it. The caller applies any client limit
    /// by stopping enumeration; there is no page cap inside the grain.
    /// </summary>
    IAsyncEnumerable<FoundSubjectStreamItem> StreamLookupSubjects(
        LookupSubjectsArgs args, CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams the resources (of the requested type) on which the subject holds the permission, one at a
    /// time, each with the opaque cursor positioned immediately after it. When <see cref="LookupResourcesArgs.Limit"/>
    /// is set the grain uses the cursor-bearing live traversal (so every item carries a resume cursor); an
    /// unlimited, cursorless enumeration may take the Leopard fast path (a complete candidate set confirmed by
    /// Check), matching the prior unary grain exactly. The caller applies any client limit by stopping.
    /// </summary>
    IAsyncEnumerable<FoundResourceWire> StreamLookupResources(
        LookupResourcesArgs args, CancellationToken cancellationToken = default);
}
