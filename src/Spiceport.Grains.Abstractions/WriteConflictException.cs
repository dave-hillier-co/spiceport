namespace Spiceport.Grains.Abstractions;

/// <summary>
/// Distinguishes the two write-conflict conditions SpiceDB maps to different gRPC codes.
/// </summary>
public enum WriteConflictKind
{
    /// <summary>
    /// A CREATE update targeted an already-existing relationship. A permanent duplicate-create
    /// (SpiceDB's <c>CreateRelationshipExistsError</c>); maps to gRPC <c>AlreadyExists</c>. The client
    /// must NOT blindly retry it as a transient conflict.
    /// </summary>
    CreateExisting = 0,

    /// <summary>
    /// A genuine write-write serialization failure at commit (SpiceDB's <c>SerializationError</c>);
    /// maps to gRPC <c>Aborted</c> so the client retries the whole transaction.
    /// </summary>
    Serialization = 1,
}

/// <summary>
/// Thrown by the relationships grain when a write fails to commit because of a conflict. The underlying
/// datastore exceptions (<c>CreateRelationshipExistsException</c> / <c>SerializationException</c>) are not
/// Orleans-serializable across the grain boundary, so the grain re-wraps them in this
/// <c>[GenerateSerializer]</c> exception, preserving the <see cref="Kind"/> the gRPC front door needs to
/// pick the correct status code (AlreadyExists vs. Aborted).
/// </summary>
[GenerateSerializer]
public sealed class WriteConflictException : Exception
{
    /// <summary>Which conflict condition occurred.</summary>
    [Id(0)]
    public WriteConflictKind Kind { get; }

    /// <summary>Creates the exception for the given conflict kind and message.</summary>
    public WriteConflictException(WriteConflictKind kind, string message) : base(message) => Kind = kind;
}
