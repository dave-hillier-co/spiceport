namespace Spiceport.Engine;

/// <summary>
/// The internal result of evaluating one node of the check graph for a single (resource, subject):
/// a tri-state membership together with an optional caveat condition that gates it.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><description><see cref="Member"/> with a null <see cref="Caveat"/>: a fully-determined member.</description></item>
/// <item><description><see cref="Member"/> with a non-null <see cref="Caveat"/>: a caveated member; the
/// expression must evaluate true for membership to hold.</description></item>
/// <item><description><see cref="NotMember"/>: not a member by any path.</description></item>
/// </list>
/// </remarks>
internal readonly record struct Branch(bool Member, CaveatExpression? Caveat)
{
    /// <summary>A fully-determined member (no caveat).</summary>
    public static readonly Branch DefiniteMember = new(true, null);

    /// <summary>Not a member.</summary>
    public static readonly Branch None = new(false, null);

    /// <summary>A member gated by the given caveat expression (null = unconditional member).</summary>
    public static Branch CaveatedMember(CaveatExpression? caveat) => new(true, caveat);

    /// <summary>True if this is a member with no caveat (enables short-circuiting unions/arrows).</summary>
    public bool IsDetermined => Member && Caveat is null;

    /// <summary>
    /// Union (OR): if either branch is a determined member, the result is a determined member.
    /// Otherwise membership is the OR of the two and caveats are OR-combined.
    /// </summary>
    public Branch Or(Branch other)
    {
        if (IsDetermined || other.IsDetermined)
            return DefiniteMember;
        if (!Member)
            return other;
        if (!other.Member)
            return this;
        return CaveatedMember(CaveatExpression.CombineOr(Caveat, other.Caveat));
    }

    /// <summary>
    /// Intersection (AND): a member only if both are members; caveats are AND-combined.
    /// </summary>
    public Branch And(Branch other)
    {
        if (!Member || !other.Member)
            return None;
        return CaveatedMember(CaveatExpression.CombineAnd(Caveat, other.Caveat));
    }

    /// <summary>
    /// Exclusion (base AND NOT excluded): if the excluded branch is a determined member, the result
    /// is removed (not a member). If the excluded branch is caveated, the base caveat is combined
    /// with the negation of the excluded caveat. If not excluded at all, the base is unchanged.
    /// </summary>
    public Branch Subtract(Branch excluded)
    {
        if (!Member)
            return None;
        if (excluded.IsDetermined)
            return None; // fully excluded.
        if (!excluded.Member)
            return this; // not excluded.
        return CaveatedMember(CaveatExpression.Subtract(Caveat, excluded.Caveat!));
    }
}
