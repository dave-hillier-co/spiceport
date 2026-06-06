using Spiceport.Core;

namespace Spiceport.Grains.Abstractions;

/// <summary>
/// A permission-check request sent across the grain boundary.
/// </summary>
/// <remarks>
/// Uses a dedicated Orleans-serializable record rather than leaking the Engine's
/// <c>CheckResult</c> or proto types across the grain boundary.
/// </remarks>
/// <param name="ResourceType">The resource object type (e.g. "document").</param>
/// <param name="ResourceId">The resource object id (e.g. "readme").</param>
/// <param name="Permission">The permission/relation to check on the resource.</param>
/// <param name="SubjectType">The subject object type (e.g. "user").</param>
/// <param name="SubjectId">The subject object id (e.g. "alice").</param>
/// <param name="SubjectRelation">
/// The subject subrelation, or <see cref="CoreConstants.Ellipsis"/> ("...") for a direct subject.
/// </param>
/// <param name="Context">
/// Optional request-time caveat context. Values are scalars/lists/maps, matching the
/// caveat context contract; Orleans serializes them.
/// </param>
[GenerateSerializer]
public sealed record CheckRequest(
    [property: Id(0)] string ResourceType,
    [property: Id(1)] string ResourceId,
    [property: Id(2)] string Permission,
    [property: Id(3)] string SubjectType,
    [property: Id(4)] string SubjectId,
    [property: Id(5)] string SubjectRelation,
    [property: Id(6)] IReadOnlyDictionary<string, object?>? Context);

/// <summary>
/// The verdict of a permission check. Integer values intentionally mirror the
/// Engine's <c>Membership</c> enum (NotMember=0, Member=1, Caveated=2).
/// </summary>
public enum CheckVerdict
{
    NotMember = 0,
    Member = 1,
    Caveated = 2,
}

/// <summary>
/// The result of a permission check.
/// </summary>
/// <param name="Verdict">The membership verdict.</param>
/// <param name="MissingFields">
/// When <see cref="Verdict"/> is <see cref="CheckVerdict.Caveated"/>, the caveat parameter
/// names that were missing from the supplied context; otherwise empty.
/// </param>
[GenerateSerializer]
public sealed record CheckReply(
    [property: Id(0)] CheckVerdict Verdict,
    [property: Id(1)] IReadOnlyList<string> MissingFields);

/// <summary>
/// Grain that performs permission checks by wrapping the check engine.
/// Addressed as a single stateless worker with integer key 0.
/// </summary>
public interface ICheckGrain : IGrainWithIntegerKey
{
    Task<CheckReply> Check(CheckRequest request);
}
