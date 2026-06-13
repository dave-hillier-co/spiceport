using Spiceport.Core;

namespace Spiceport.Engine;

/// <summary>
/// A subject found by <see cref="LookupSubjectsEngine.LookupSubjects"/> for a resource and relation.
/// </summary>
/// <remarks>
/// Port of SpiceDB's <c>v1.FoundSubject</c>. A non-null <see cref="Caveat"/> is the "Caveated marker":
/// the subject is conditionally included and the expression is carried verbatim so a caller can
/// <c>Collapse</c>-evaluate it against a request context. <see cref="IsWildcard"/> is true when the
/// subject is a public wildcard (<c>"*"</c>), meaning "every subject of the requested type".
/// </remarks>
/// <param name="SubjectId">The concrete subject id, or <c>"*"</c> for a wildcard match.</param>
/// <param name="Caveat">The accumulated caveat, or null if unconditional.</param>
/// <param name="IsWildcard">True when <see cref="SubjectId"/> is the public wildcard <c>"*"</c>.</param>
/// <param name="ExcludedSubjects">
/// For a wildcard match, the concrete subjects excluded from it (SpiceDB's
/// <c>excluded_subjects</c>): the wildcard means "every subject of the type <em>except</em> these".
/// Each excluded entry carries its own optional <see cref="Caveat"/> (a conditionally-excluded
/// subject). Null/empty for concrete subjects and for wildcards with no exclusions.
/// </param>
public sealed record FoundSubject(
    string SubjectId,
    CaveatExpression? Caveat = null,
    bool IsWildcard = false,
    IReadOnlyList<FoundSubject>? ExcludedSubjects = null);
