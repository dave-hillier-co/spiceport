namespace Spiceport.Core;

/// <summary>
/// Thrown when a permission check exhausts its recursion depth budget before reaching a definitive
/// answer. Mirrors SpiceDB's <c>MaxDepthExceededError</c> (dispatch.CheckDepth): a graph/schema deeper
/// than the configured max depth (or a true cycle the bloom cannot otherwise bound) is treated as a
/// misconfiguration error, NOT a confident "not a member" verdict. The API layer maps this to gRPC
/// <c>FailedPrecondition</c>, matching observable <c>zed</c>/SpiceDB behaviour.
/// </summary>
/// <remarks>
/// Round-trips the Orleans grain boundary via Orleans' exception serializer (a depth-exhausted
/// sub-problem may be evaluated on a remote silo, where the exception is raised and must travel back to
/// the caller). Modelled on the peer <see cref="InvalidConsistencyTokenException"/>, which is likewise a
/// plain <see cref="Exception"/> in this assembly thrown below the grain and caught in the API layer.
/// </remarks>
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
