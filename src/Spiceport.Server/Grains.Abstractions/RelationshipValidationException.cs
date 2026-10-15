namespace Spiceport.Grains.Abstractions;

/// <summary>Which class of schema violation a <see cref="RelationshipValidationException"/> reports.</summary>
public enum RelationshipValidationErrorKind
{
    /// <summary>
    /// A referenced object definition or relation does not exist in the schema (SpiceDB
    /// <c>NamespaceNotFound</c> / <c>RelationNotFound</c>). Maps to gRPC <c>FailedPrecondition</c>.
    /// </summary>
    NotFound = 0,

    /// <summary>
    /// The relationship names things the schema defines but combines them in a way it forbids: a write to
    /// a permission, or a subject type/subrelation/caveat/expiration outside the relation's allowed direct
    /// relations (SpiceDB <c>CannotWriteToPermissionError</c> / <c>InvalidSubjectTypeError</c>). Maps to
    /// gRPC <c>InvalidArgument</c>.
    /// </summary>
    Invalid = 1,
}

/// <summary>
/// Thrown when a written relationship is not permitted by the schema effective at the commit's revision.
/// Nothing in the request is applied. Mirrors SpiceDB's write-time
/// <c>relationships.ValidateRelationshipUpdates</c>.
/// </summary>
/// <remarks>
/// Annotated <c>[GenerateSerializer]</c> so it round-trips across the Orleans grain boundary.
/// </remarks>
[GenerateSerializer]
public sealed class RelationshipValidationException : Exception
{
    /// <summary>Creates the exception with its kind and the SpiceDB-worded message.</summary>
    public RelationshipValidationException(RelationshipValidationErrorKind kind, string message)
        : base(message)
    {
        Kind = kind;
    }

    /// <summary>Whether a referenced definition/relation was missing or the combination was forbidden.</summary>
    [Id(0)]
    public RelationshipValidationErrorKind Kind { get; }
}
