namespace Spiceport.Grains.Abstractions;

/// <summary>
/// Which way a precondition can fail: a MUST_MATCH whose filter matched nothing, or a MUST_NOT_MATCH
/// whose filter matched something.
/// </summary>
public enum PreconditionFailureKind
{
    /// <summary>The filter was required to match at least one relationship, but matched none.</summary>
    MustMatchFoundNone = 0,

    /// <summary>The filter was required to match nothing, but matched at least one relationship.</summary>
    MustNotMatchFoundOne = 1,
}

/// <summary>
/// Thrown inside a write transaction when a precondition is not satisfied against the snapshot the
/// writes would commit at. The transaction is abandoned (nothing commits). Maps to gRPC
/// <c>FailedPrecondition</c>. Mirrors SpiceDB's precondition-failed error.
/// </summary>
/// <remarks>
/// Annotated <c>[GenerateSerializer]</c> so it round-trips across the Orleans grain boundary (the grain
/// throws it; the gRPC front door catches it and maps to FailedPrecondition).
/// </remarks>
[GenerateSerializer]
public sealed class PreconditionFailedException : Exception
{
    /// <summary>How the precondition failed.</summary>
    [Id(0)]
    public PreconditionFailureKind Kind { get; }

    /// <summary>The zero-based index of the failing precondition within the request.</summary>
    [Id(1)]
    public int PreconditionIndex { get; }

    /// <summary>Creates the exception describing the failing precondition.</summary>
    public PreconditionFailedException(PreconditionFailureKind kind, int preconditionIndex, string message)
        : base(message)
    {
        Kind = kind;
        PreconditionIndex = preconditionIndex;
    }
}
