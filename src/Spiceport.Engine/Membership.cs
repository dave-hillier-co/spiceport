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
    /// Membership is conditional on a caveat. Reserved for future caveat evaluation;
    /// the current engine never returns this value.
    /// </summary>
    Caveated = 2,
}

/// <summary>
/// The result of a permission check, including the verdict and (for diagnostics) the
/// number of recursive dispatch steps performed.
/// </summary>
/// <param name="Verdict">The membership verdict.</param>
/// <param name="DispatchCount">The number of recursive check steps performed.</param>
public sealed record CheckResult(Membership Verdict, int DispatchCount = 0)
{
    /// <summary>True if the subject is a member.</summary>
    public bool IsMember => Verdict == Membership.Member;
}
