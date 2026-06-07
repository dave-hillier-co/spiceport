using Spiceport.Core;

namespace Spiceport.Engine;

/// <summary>
/// A resource found by <see cref="LookupResourcesEngine.LookupResources"/> for a subject and permission.
/// </summary>
/// <remarks>
/// Port of SpiceDB's <c>v1.LookupResourcesResponse</c> / <c>possibleResource</c>. A
/// <see cref="Membership.Caveated"/> result means the resource is reachable only if the named
/// <see cref="MissingContextParams"/> satisfy the caveat — identical to <c>CAVEATED_MEMBER</c> +
/// <c>MissingExprFields</c>. The membership is never <see cref="Membership.NotMember"/>: non-members
/// are simply not yielded.
/// </remarks>
/// <param name="ResourceId">The reachable resource object id.</param>
/// <param name="ForSubjectIds">Which input subject ids reached this resource.</param>
/// <param name="Membership"><see cref="Membership.Member"/> or <see cref="Membership.Caveated"/>.</param>
/// <param name="MissingContextParams">When <see cref="Membership.Caveated"/>, the unresolved caveat params.</param>
/// <param name="AfterCursor">A resume token positioned after this result.</param>
public sealed record FoundResource(
    string ResourceId,
    IReadOnlyList<string> ForSubjectIds,
    Membership Membership,
    IReadOnlyList<string>? MissingContextParams = null,
    LookupResourcesCursor? AfterCursor = null)
{
    /// <summary>The unresolved caveat param names, or an empty list.</summary>
    public IReadOnlyList<string> MissingContextParams { get; init; } = MissingContextParams ?? [];
}
