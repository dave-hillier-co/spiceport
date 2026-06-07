namespace Spiceport.Core;

/// <summary>
/// Thrown when a consistency requirement cannot be honoured: a malformed token, a token from a
/// different datastore where that is an error (at-exact-snapshot), or an exact snapshot revision
/// that is no longer available. Maps to gRPC <c>InvalidArgument</c>/<c>FailedPrecondition</c>.
/// </summary>
public sealed class InvalidConsistencyTokenException : Exception
{
    /// <summary>Creates the exception with a human-readable reason.</summary>
    public InvalidConsistencyTokenException(string message) : base(message) { }
}
