using Orleans.Runtime;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// The deliberate, pure classification of an exception raised by (or surfaced from) a cross-silo grain
/// dispatch — the port's equivalent of SpiceDB's <c>rewriteError</c>
/// (<c>internal/dispatch/graph/graph.go</c>) at the remote dispatch boundary. Splitting it out as a
/// pure function makes the taxonomy unit-testable without provoking a real silo failure.
/// </summary>
public static class DispatchErrorMapper
{
    /// <summary>
    /// The outcome of classifying a dispatch-boundary exception.
    /// </summary>
    /// <param name="PassThrough">
    /// When <see langword="true"/>, the exception is a known typed DOMAIN exception that already carries
    /// its own gRPC meaning (max-depth, invalid-consistency, precondition, write-conflict,
    /// schema-write-validation, caveat-evaluation) and must be re-thrown UNCHANGED — never collapsed into
    /// a transport error. <see cref="Code"/> is unused in this case.
    /// </param>
    /// <param name="Code">
    /// When <paramref name="PassThrough"/> is <see langword="false"/>, the deliberately-chosen gRPC code
    /// the transport/cancellation/unknown failure collapses to.
    /// </param>
    public readonly record struct Classification(bool PassThrough, DispatchErrorCode Code)
    {
        /// <summary>A domain exception that must be re-thrown as-is.</summary>
        public static Classification Domain { get; } = new(PassThrough: true, Code: default);

        /// <summary>A transport/cancellation/unknown failure mapped to <paramref name="code"/>.</summary>
        public static Classification Mapped(DispatchErrorCode code) => new(PassThrough: false, Code: code);
    }

    /// <summary>
    /// Classifies <paramref name="exception"/> into the dispatch-error taxonomy. Pure: no I/O, no silo.
    /// </summary>
    /// <remarks>
    /// Order matters. Known domain exceptions are recognised FIRST so a domain failure that happens to be
    /// wrapped is never mislabeled as a transport error. Then cancellation/deadline, then transport /
    /// availability (retriable -> <see cref="DispatchErrorCode.Unavailable"/>), then everything else
    /// -> <see cref="DispatchErrorCode.Internal"/>.
    /// </remarks>
    public static Classification Classify(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        // 1. Known typed domain exceptions keep their own gRPC semantics — pass through unchanged.
        if (IsDomainException(exception))
            return Classification.Domain;

        // 2. Cancellation / deadline. A cancellation surfaced as a TimeoutException (a deadline elapsed)
        //    is DeadlineExceeded; a plain caller cancellation is Cancelled.
        if (exception is OperationCanceledException)
            return Classification.Mapped(DispatchErrorCode.Cancelled);

        // 3. Orleans transport / silo-availability failures are TRANSIENT -> retriable Unavailable
        //    (NOT Internal): a silo dropping mid-check should let the client retry, matching SpiceDB.
        if (IsTransportFailure(exception))
            return Classification.Mapped(DispatchErrorCode.Unavailable);

        // 4. Anything else is unexpected -> Internal.
        return Classification.Mapped(DispatchErrorCode.Internal);
    }

    /// <summary>
    /// Whether <paramref name="exception"/> is a known typed domain exception that already has a gRPC
    /// meaning the API layer maps directly, and so must NOT be collapsed into a transport error.
    /// </summary>
    public static bool IsDomainException(Exception exception) =>
        exception is MaxDepthExceededException
            or InvalidConsistencyTokenException
            or CaveatEvaluationException
            or PreconditionFailedException
            or WriteConflictException
            or SchemaWriteValidationException
            or RevisionNotFoundException // pinned revision fell below the GC floor: caller-facing InvalidArgument.
            or DispatchFailedException; // already-classified failure from a deeper hop: keep its code.

    /// <summary>
    /// Whether <paramref name="exception"/> is a transient Orleans transport / silo-availability /
    /// timeout failure that should be reported as retriable <see cref="DispatchErrorCode.Unavailable"/>.
    /// </summary>
    public static bool IsTransportFailure(Exception exception) =>
        exception switch
        {
            SiloUnavailableException => true,
            OrleansMessageRejectionException => true,
            // Orleans surfaces a response timeout / dropped request as a TimeoutException; treat it as a
            // transient hop failure (retriable), not a deadline the caller set.
            TimeoutException => true,
            // Any other Orleans-runtime failure (e.g. no compatible silo for placement) is an
            // availability problem at the mesh layer -> retriable.
            OrleansException => true,
            _ => false,
        };

    /// <summary>
    /// Translates an exception surfaced from (or at) a cross-silo <c>ICheckGrain.DispatchCheck</c> hop
    /// into the exception the caller should see: a known domain exception (and an already-classified
    /// <see cref="DispatchFailedException"/>) is re-thrown unchanged; everything else is collapsed to a
    /// <see cref="DispatchFailedException"/> carrying its deliberately-mapped <see cref="DispatchErrorCode"/>.
    /// Pure, given <see cref="Classify"/>.
    /// </summary>
    /// <remarks>
    /// Shared by <see cref="CheckDispatchOutgoingCallFilter"/> (the normal path: every exception the
    /// grain call itself produces) and <see cref="OrleansDispatcher"/>'s own cancellation-bridging catch
    /// (the one case — the caller's own <c>CancellationToken</c> firing first inside
    /// <c>Task.WaitAsync</c> — that never reaches the filter at all, because it is raised locally against
    /// an abandoned await rather than surfaced from the grain call's own fault).
    /// </remarks>
    public static Exception Translate(Exception exception)
    {
        var classification = Classify(exception);
        if (classification.PassThrough)
            return exception;

        var reason = classification.Code switch
        {
            DispatchErrorCode.Unavailable =>
                "the permission check could not reach the silo that owns this sub-problem; the failure " +
                "is transient and the request may be retried",
            DispatchErrorCode.Cancelled => "the permission check was cancelled",
            DispatchErrorCode.DeadlineExceeded => "the permission check exceeded its deadline",
            _ => "the permission check failed with an unexpected dispatch error",
        };

        return new DispatchFailedException(classification.Code, reason, exception);
    }
}
