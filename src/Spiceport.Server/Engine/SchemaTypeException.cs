namespace Spiceport.Engine;

/// <summary>
/// Thrown by <see cref="SchemaTypeValidator"/> when a compiled schema fails type-system or caveat
/// validation at write time: an undefined relation/permission/subject-type reference, a permission used
/// on the left of an arrow, a wildcard imported into an arrow, a self-referential relation, a missing
/// allowed-types list, a duplicate allowed type, an undefined <c>with &lt;caveat&gt;</c>, a duplicate or
/// reused definition/caveat name, or a caveat whose CEL is unparseable, parameterless, or leaves a
/// declared parameter unreferenced. Mirrors SpiceDB's <c>schema.TypeError</c> /
/// <c>ValidateCaveatDefinition</c>, which surface as gRPC <c>FailedPrecondition</c>.
/// </summary>
/// <remarks>
/// This is a plain (non-Orleans-serializable) exception raised inside the engine; the grain boundary
/// re-wraps it in a <c>[GenerateSerializer]</c> carrier so it round-trips to the gRPC front door.
/// </remarks>
public sealed class SchemaTypeException(string message) : Exception(message);
