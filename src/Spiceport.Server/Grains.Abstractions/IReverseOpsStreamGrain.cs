namespace Spiceport.Grains.Abstractions;

/// <summary>
/// Streams the reverse engine ops (LookupSubjects, LookupResources) as native Orleans
/// <see cref="IAsyncEnumerable{T}"/> grain calls, so the gRPC front door drives one continuous stream with
/// runtime backpressure instead of a per-page service→grain round-trip. Each item carries its own opaque
/// resume cursor, so a client-facing limited stream still resumes byte-for-byte from a fresh grain
/// activation — the internal page loop is deleted, the client cursor contract is not. Also carries the
/// unary whole-resource permission tree expansion (<c>ExpandPermissionTree</c>), so one grain interface
/// serves the whole reverse-ops surface.
/// </summary>
/// <remarks>
/// This grain is <see cref="IGrainWithGuidKey"/> and runs under Orleans' DEFAULT placement — it is
/// deliberately NOT a <c>[StatelessWorker]</c>. The grain-side extension that backs native
/// <see cref="IAsyncEnumerable{T}"/> streaming holds the live enumerator in memory on ONE specific
/// activation keyed by a client-minted request id; two calls through the same reference to a stateless
/// worker can land on different activations, which would break the enumeration. The calling service
/// therefore mints a fresh <see cref="System.Guid"/> per streaming RPC so <c>StartEnumeration</c> and every
/// <c>MoveNext</c> for that stream target the SAME brand-new activation. Per-stream activations are
/// reclaimed by ordinary idle collection.
/// <para>
/// Cancellation flows via the trailing <see cref="System.Threading.CancellationToken"/> (a real Orleans 10.1
/// grain-method token, checked cooperatively inside the iterator).
/// </para>
/// <para>
/// <c>ExpandPermissionTree</c> stays unary — it returns a whole tree, has no cursor, and needs no streaming
/// or per-call enumerator state. Unlike the streaming methods, callers should NOT mint a fresh
/// <see cref="System.Guid"/> per Expand call: a single call has no follow-up <c>MoveNext</c>, so there is no
/// enumerator-affinity requirement, and reusing a stable well-known key (<see cref="ExpandKey"/>, currently
/// <see cref="System.Guid.Empty"/>) lets every Expand call reuse the same warm activation instead of
/// fragmenting into one activation per call. This is safe because the grain implementation is stateless
/// across calls outside the lifetime of a single streaming enumeration — <c>ExpandPermissionTree</c> touches
/// no per-stream field, so concurrent/serial Expand calls sharing one activation cannot observe each other's
/// state.
/// </para>
/// </remarks>
public interface IReverseOpsStreamGrain : IGrainWithGuidKey
{
    /// <summary>The stable, well-known key every caller uses for unary <see cref="ExpandPermissionTree"/>
    /// calls, so they reuse one warm activation instead of minting a fresh <see cref="System.Guid"/> per
    /// call the way the streaming methods do.</summary>
    public static readonly Guid ExpandKey = Guid.Empty;

    /// <summary>Expands the resource's permission into a structural permission tree.</summary>
    Task<ExpandTreeReply> ExpandPermissionTree(ExpandTreeArgs args);

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
