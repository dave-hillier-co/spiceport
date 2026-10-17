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
/// The specific rule a <see cref="RelationshipValidationException"/> reports, matching the SpiceDB error type
/// whose structured details (<c>ErrorInfo</c> reason and metadata) the gRPC front door reproduces.
/// </summary>
public enum RelationshipViolation
{
    /// <summary>A referenced object definition does not exist (SpiceDB <c>NamespaceNotFoundError</c>).</summary>
    DefinitionNotFound = 0,

    /// <summary>A referenced relation or permission does not exist (SpiceDB <c>RelationNotFoundError</c>).</summary>
    RelationNotFound = 1,

    /// <summary>The relation written is a permission (SpiceDB <c>CannotWriteToPermissionError</c>).</summary>
    WriteToPermission = 2,

    /// <summary>The subject is not one of the relation's allowed types (SpiceDB <c>InvalidSubjectTypeError</c>).</summary>
    InvalidSubjectType = 3,
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
    /// <summary>
    /// Creates the exception for <paramref name="violation"/> with the SpiceDB-worded message and the names its
    /// structured details report: the definition and relation the violation concerns, and for an invalid
    /// subject type the subject's allowed-relation source and (when the subject lacked a required caveat) the
    /// comma-joined caveats it needed.
    /// </summary>
    public RelationshipValidationException(
        RelationshipViolation violation,
        string message,
        string definitionName,
        string? relationName = null,
        string? subjectType = null,
        string? allowedCaveats = null)
        : base(message)
    {
        Violation = violation;
        Kind = violation is RelationshipViolation.DefinitionNotFound or RelationshipViolation.RelationNotFound
            ? RelationshipValidationErrorKind.NotFound
            : RelationshipValidationErrorKind.Invalid;
        DefinitionName = definitionName;
        RelationName = relationName;
        SubjectType = subjectType;
        AllowedCaveats = allowedCaveats;
    }

    /// <summary>Whether a referenced definition/relation was missing or the combination was forbidden.</summary>
    [Id(0)]
    public RelationshipValidationErrorKind Kind { get; }

    /// <summary>The specific rule the relationship broke.</summary>
    [Id(1)]
    public RelationshipViolation Violation { get; }

    /// <summary>The definition the violation concerns.</summary>
    [Id(2)]
    public string DefinitionName { get; }

    /// <summary>The relation or permission the violation concerns, when it concerns one.</summary>
    [Id(3)]
    public string? RelationName { get; }

    /// <summary>For an invalid subject type, the written subject's allowed-relation source (<c>user:*</c>, <c>group#member</c>).</summary>
    [Id(4)]
    public string? SubjectType { get; }

    /// <summary>For an uncaveated subject allowed only with caveats, those caveats, comma-joined.</summary>
    [Id(5)]
    public string? AllowedCaveats { get; }
}
