using Spiceport.Core;

namespace Spiceport.Engine;

/// <summary>
/// A boolean combination of caveats threaded through the check graph. A node is either a leaf
/// (a single <see cref="ContextualizedCaveat"/>) or a composite (OR / AND / NOT over children).
/// </summary>
/// <remarks>
/// Mirrors SpiceDB's <c>CaveatExpression</c> / <c>CaveatOperation</c>. The engine builds these
/// while combining membership across union (OR), intersection (AND), exclusion (base AND NOT
/// excluded), and arrows (tupleset caveat AND computed result), then collapses the whole tree to a
/// single verdict during final evaluation.
/// </remarks>
public abstract record CaveatExpression
{
    /// <summary>A leaf caveat: a named caveat with its relationship-supplied context.</summary>
    public sealed record Leaf(ContextualizedCaveat Caveat) : CaveatExpression;

    /// <summary>A logical OR over child expressions.</summary>
    public sealed record Or(IReadOnlyList<CaveatExpression> Children) : CaveatExpression;

    /// <summary>A logical AND over child expressions.</summary>
    public sealed record And(IReadOnlyList<CaveatExpression> Children) : CaveatExpression;

    /// <summary>A logical NOT of a single child expression.</summary>
    public sealed record Not(CaveatExpression Child) : CaveatExpression;

    /// <summary>Wraps a contextualized caveat as a leaf expression.</summary>
    public static CaveatExpression FromCaveat(ContextualizedCaveat caveat) => new Leaf(caveat);

    /// <summary>
    /// Combines two optional expressions with OR. A null operand means "unconditionally true" for
    /// that branch, which makes the whole OR unconditionally true (returns null).
    /// </summary>
    public static CaveatExpression? CombineOr(CaveatExpression? a, CaveatExpression? b)
    {
        if (a is null || b is null)
            return null; // a determined (caveat-free) member dominates an OR.
        return Flatten<Or>(a, b, children => new Or(children));
    }

    /// <summary>
    /// Combines two optional expressions with AND. A null operand means "unconditionally true",
    /// so the result is simply the other operand.
    /// </summary>
    public static CaveatExpression? CombineAnd(CaveatExpression? a, CaveatExpression? b)
    {
        if (a is null)
            return b;
        if (b is null)
            return a;
        return Flatten<And>(a, b, children => new And(children));
    }

    /// <summary>
    /// Combines a base expression with the negation of an excluded expression:
    /// <c>base AND NOT(excluded)</c>. A null excluded operand means the resource is fully excluded;
    /// callers handle that as removal, so this expects a non-null excluded expression.
    /// </summary>
    public static CaveatExpression? Subtract(CaveatExpression? baseExpr, CaveatExpression excluded) =>
        CombineAnd(baseExpr, new Not(excluded));

    /// <summary>
    /// Combines two optional expressions with OR where a null operand is treated as "unconditional"
    /// and the OTHER operand is returned (mirrors SpiceDB's <c>caveats.Or</c>, used by the subject-set
    /// wildcard algebra). This differs from <see cref="CombineOr"/> (the short-circuited OR, where a
    /// null operand collapses the whole OR to null).
    /// </summary>
    public static CaveatExpression? OrKeepOther(CaveatExpression? a, CaveatExpression? b)
    {
        if (a is null)
            return b;
        if (b is null)
            return a;
        return Flatten<Or>(a, b, children => new Or(children));
    }

    /// <summary>Negates an optional expression; inverting a null (unconditional) yields null.</summary>
    public static CaveatExpression? Invert(CaveatExpression? expr) =>
        expr is null ? null : new Not(expr);

    private static CaveatExpression Flatten<TOp>(
        CaveatExpression a,
        CaveatExpression b,
        Func<IReadOnlyList<CaveatExpression>, CaveatExpression> make)
        where TOp : CaveatExpression
    {
        var children = new List<CaveatExpression>();
        AddFlattened<TOp>(children, a);
        AddFlattened<TOp>(children, b);
        return make(children);
    }

    private static void AddFlattened<TOp>(List<CaveatExpression> into, CaveatExpression expr)
        where TOp : CaveatExpression
    {
        if (expr is TOp)
        {
            into.AddRange(expr switch
            {
                Or o => o.Children,
                And n => n.Children,
                _ => [expr],
            });
        }
        else
        {
            into.Add(expr);
        }
    }
}
