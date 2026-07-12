namespace Spiceport.Grains.Abstractions;

/// <summary>
/// A serializable mirror of the engine's <c>FoundSubject</c>, carried across the
/// <see cref="ISubjectFrontierGrain"/> boundary. Unlike <see cref="FoundSubjectWire"/> (the
/// post-collapse client-edge shape), this is the PRE-CONTEXT shape: the caveat travels as its stable
/// serialized form (never collapsed against a request context) and wildcard exclusions are carried in
/// full, because this reply is the whole memoized frontier a caller collapses per-request, not a single
/// already-collapsed result.
/// </summary>
/// <param name="SubjectId">The concrete subject id, or "*" for a wildcard match.</param>
/// <param name="Caveat">The verbatim gating caveat expression, or null if unconditional.</param>
/// <param name="IsWildcard">True when <see cref="SubjectId"/> is the public wildcard.</param>
/// <param name="ExcludedSubjects">
/// For a wildcard match, the concrete subjects excluded from it. Note: <see cref="FoundSubjectWire"/>
/// (the client-facing collapsed shape) still has no excluded-subjects field, so
/// <c>ReverseOps</c> drops these at the client edge exactly as it always has — carrying them
/// here only keeps the memoized frontier a byte-faithful mirror of the engine's own output.
/// </param>
[GenerateSerializer, Immutable]
public sealed record FrontierSubjectWire(
    [property: Id(0)] string SubjectId,
    [property: Id(1)] SerializedCaveat? Caveat,
    [property: Id(2)] bool IsWildcard,
    [property: Id(3)] IReadOnlyList<FrontierSubjectWire>? ExcludedSubjects);

/// <summary>The reply from <see cref="ISubjectFrontierGrain.GetFrontier"/>: the whole materialized frontier.</summary>
/// <param name="Subjects">Every subject the engine's full walk found, in the engine's own walk order.</param>
[GenerateSerializer, Immutable]
public sealed record SubjectFrontierReply(
    [property: Id(0)] IReadOnlyList<FrontierSubjectWire> Subjects);
