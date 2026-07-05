using Spiceport.Core;

namespace Spiceport.Engine;

/// <summary>The membership verdict returned by a <see cref="CheckEngine"/> check.</summary>
public enum Membership
{
    /// <summary>The subject is definitively not a member.</summary>
    NotMember = 0,

    /// <summary>The subject is definitively a member.</summary>
    Member = 1,

    /// <summary>
    /// Membership is conditional on a caveat whose context could not be fully determined.
    /// The unresolved parameter names are reported in <see cref="CheckResult.MissingExprFields"/>.
    /// </summary>
    Caveated = 2,
}

/// <summary>
/// The result of a permission check: the verdict and (when caveated) the missing context fields.
/// </summary>
/// <param name="Verdict">The membership verdict.</param>
/// <param name="MissingExprFields">
/// When <paramref name="Verdict"/> is <see cref="Membership.Caveated"/>, the caveat parameter
/// names that were unavailable in the supplied context. Empty otherwise.
/// </param>
public sealed record CheckResult(
    Membership Verdict,
    IReadOnlyList<string>? MissingExprFields = null)
{
    /// <summary>True if the subject is a member.</summary>
    public bool IsMember => Verdict == Membership.Member;

    /// <summary>The caveat parameter names that were unavailable, or an empty list.</summary>
    public IReadOnlyList<string> MissingExprFields { get; init; } = MissingExprFields ?? [];
}
