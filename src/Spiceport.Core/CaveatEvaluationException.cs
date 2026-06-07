namespace Spiceport.Core;

/// <summary>The kind of caveat-evaluation failure, used to map onto a gRPC status code.</summary>
public enum CaveatEvaluationErrorKind
{
    /// <summary>
    /// A supplied context value did not match the caveat parameter's declared type (e.g. a string
    /// supplied for an <c>int</c> parameter). Maps to gRPC <c>InvalidArgument</c>, mirroring
    /// SpiceDB's <c>ParameterTypeError</c>.
    /// </summary>
    ParameterTypeMismatch = 0,

    /// <summary>
    /// A relationship referenced a caveat name not present in the schema at the read revision
    /// (a stale-cache / schema-skew condition). Maps to gRPC <c>FailedPrecondition</c>, mirroring
    /// SpiceDB's <c>CaveatNameNotFoundErr</c>.
    /// </summary>
    UnknownCaveat = 1,
}

/// <summary>
/// Thrown when a caveat cannot be evaluated because the request context is type-incompatible with the
/// caveat's declared parameters, or because a relationship references a caveat that does not exist in
/// the schema. Mirrors SpiceDB's <c>ParameterTypeError</c> / <c>CaveatNameNotFoundErr</c>; the API
/// layer maps <see cref="Kind"/> onto a gRPC status code.
/// </summary>
/// <remarks>
/// Round-trips the Orleans grain boundary via Orleans' default exception serializer (a caveat may be
/// evaluated on a remote silo), like the peer <see cref="MaxDepthExceededException"/> and
/// <see cref="InvalidConsistencyTokenException"/>.
/// </remarks>
public sealed class CaveatEvaluationException : Exception
{
    /// <summary>The failure kind, used by the API layer to choose a gRPC status code.</summary>
    public CaveatEvaluationErrorKind Kind { get; }

    /// <summary>Creates the exception with a failure kind and human-readable reason.</summary>
    public CaveatEvaluationException(CaveatEvaluationErrorKind kind, string message) : base(message) =>
        Kind = kind;
}
