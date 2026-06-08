namespace Spiceport.Grains.Abstractions;

/// <summary>
/// Thrown when a schema write would leave existing relationships dangling: a removed definition,
/// relation, or allowed subject type that written relationships still reference. The schema swap and
/// the persisting transaction are abandoned (nothing changes). Maps to gRPC <c>FailedPrecondition</c>.
/// Mirrors SpiceDB's schema-write data-validation error.
/// </summary>
/// <remarks>
/// Annotated <c>[GenerateSerializer]</c> so it round-trips across the Orleans grain boundary.
/// </remarks>
[GenerateSerializer]
public sealed class SchemaWriteValidationException : Exception
{
    /// <summary>Creates the exception naming the offending definition/relation and an example relationship.</summary>
    public SchemaWriteValidationException(string message) : base(message) { }
}
