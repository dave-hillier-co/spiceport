namespace Spiceport.Grains.Abstractions;

/// <summary>
/// The deliberate gRPC-status taxonomy a cross-silo dispatch failure collapses to, mirroring SpiceDB's
/// <c>rewriteError</c> (<c>internal/dispatch/graph/graph.go</c>) and the remote dispatch boundary
/// (<c>internal/dispatch/remote/cluster.go</c>). Each value names the gRPC <c>StatusCode</c> the API
/// front door must emit so a remote grain/transport failure surfaces as a stable, documented code
/// instead of an opaque Orleans exception.
/// </summary>
public enum DispatchErrorCode
{
    /// <summary>
    /// A transient transport / silo-availability failure mid-dispatch (silo unreachable, message
    /// rejected, Orleans timeout). Retriable: maps to gRPC <c>Unavailable</c>, NOT <c>Internal</c>, so a
    /// client (or <c>zed</c>) retries rather than treating a transient hop failure as fatal. Mirrors
    /// SpiceDB returning request-failure metadata for a retriable dispatch error.
    /// </summary>
    Unavailable = 0,

    /// <summary>The dispatch was cancelled (caller cancellation). Maps to gRPC <c>Cancelled</c>.</summary>
    Cancelled = 1,

    /// <summary>The dispatch exceeded its deadline. Maps to gRPC <c>DeadlineExceeded</c>.</summary>
    DeadlineExceeded = 2,

    /// <summary>
    /// An unexpected, non-transient failure with no better classification. Maps to gRPC <c>Internal</c>.
    /// </summary>
    Internal = 3,
}

/// <summary>
/// Carries a cross-silo dispatch failure back across the Orleans boundary with its deliberately-chosen
/// gRPC <see cref="DispatchErrorCode"/> already settled, so the failure round-trips as a stable code
/// rather than an opaque/lost-type Orleans transport exception.
/// </summary>
/// <remarks>
/// <para>
/// Raised by <c>OrleansDispatcher</c> at the dispatch hop when a remote grain call fails with a
/// transport/availability/cancellation/unknown error (the cases SpiceDB's <c>rewriteError</c> rewrites).
/// Known typed DOMAIN exceptions (max-depth, invalid-consistency, precondition, write-conflict,
/// schema-write-validation) are deliberately NOT collapsed into this type — they keep their own
/// semantics and pass through unchanged.
/// </para>
/// <para>
/// Annotated <c>[GenerateSerializer]</c> so it round-trips the grain boundary cleanly (a child
/// sub-problem may fail on a remote silo, several hops below the API call, and the failure must travel
/// back with its code intact). The API front door catches it and emits <see cref="Code"/>.
/// </para>
/// </remarks>
[GenerateSerializer]
public sealed class DispatchFailedException : Exception
{
    /// <summary>The gRPC status the API front door must emit for this dispatch failure.</summary>
    [Id(0)]
    public DispatchErrorCode Code { get; }

    /// <summary>Creates the exception carrying the settled dispatch error code and a human-readable reason.</summary>
    public DispatchFailedException(DispatchErrorCode code, string message) : base(message) => Code = code;

    /// <summary>Creates the exception carrying the settled code, a reason, and the underlying cause.</summary>
    public DispatchFailedException(DispatchErrorCode code, string message, Exception innerException)
        : base(message, innerException) => Code = code;
}
