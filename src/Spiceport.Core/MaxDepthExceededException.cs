namespace Spiceport.Core;

/// <summary>
/// Thrown when a permission check exhausts its recursion depth budget before reaching a definitive
/// answer. Mirrors SpiceDB's <c>MaxDepthExceededError</c> (dispatch.CheckDepth): a graph/schema deeper
/// than the configured max depth (or a true cycle the bloom cannot otherwise bound) is treated as a
/// misconfiguration error, NOT a confident "not a member" verdict. The API layer maps this to gRPC
/// <c>FailedPrecondition</c>, matching observable <c>zed</c>/SpiceDB behaviour.
/// </summary>
/// <remarks>
/// Annotated <c>[GenerateSerializer]</c> so it round-trips the Orleans grain boundary cleanly: a graph
/// deeper than maxDepth (or a genuine cycle) may exhaust its depth several hops below the API call, on a
/// remote silo, and the resulting error must travel back to the caller with its concrete type intact so
/// the API layer maps it to gRPC <c>FailedPrecondition</c>. (CLAUDE.md requires every exception that
/// crosses a grain boundary to be <c>[GenerateSerializer]</c>.) It carries no extra serialized state
/// beyond the base <see cref="Exception"/> message.
/// </remarks>
[GenerateSerializer]
public sealed class MaxDepthExceededException : Exception
{
    /// <summary>Creates the exception with a human-readable reason.</summary>
    public MaxDepthExceededException(string message) : base(message) { }

    /// <summary>Creates the exception with the default max-depth-exceeded message.</summary>
    public MaxDepthExceededException()
        : base("the check request has exceeded the maximum allowable depth; this usually indicates a " +
               "misconfigured schema or a cycle, and may be raised for legitimately deep data")
    {
    }
}
