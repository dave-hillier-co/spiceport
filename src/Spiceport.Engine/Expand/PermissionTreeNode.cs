using Spiceport.Core;

namespace Spiceport.Engine;

/// <summary>
/// The mode used when expanding a permission tree.
/// </summary>
/// <remarks>Mirrors SpiceDB's <c>DispatchExpandRequest_SHALLOW</c> / <c>RECURSIVE</c>.</remarks>
public enum ExpandMode
{
    /// <summary>
    /// Expand one level only: a base relation's directly-written subjects are returned verbatim,
    /// including non-terminal usersets (subrelations), without recursing into them.
    /// </summary>
    Shallow,

    /// <summary>
    /// Expand fully: non-terminal usersets reached from a base relation are themselves expanded,
    /// producing a nested tree.
    /// </summary>
    Recursive,
}

/// <summary>
/// A single direct subject of a base relation, carried verbatim from a written tuple, with its
/// optional per-tuple caveat. Public wildcards are represented as a subject whose
/// <see cref="ObjectAndRelation.IsPublicWildcard"/> is true (object id <c>"*"</c>).
/// </summary>
/// <param name="Subject">The subject ONR (may be a wildcard or a non-terminal userset).</param>
/// <param name="Caveat">The per-tuple caveat, or null if unconditional.</param>
/// <remarks>Port of SpiceDB's <c>core.DirectSubject</c>.</remarks>
public sealed record DirectSubject(ObjectAndRelation Subject, CaveatExpression? Caveat = null);

/// <summary>
/// A node in an expanded permission tree. The tree mirrors the userset-rewrite structure of the
/// expanded relation; every node records the resource ONR it <see cref="Expanded"/>.
/// </summary>
/// <remarks>
/// Port of SpiceDB's <c>core.RelationTupleTreeNode</c>. A <see cref="Leaf"/> carries the directly
/// written subjects of a base relation (including wildcards and non-terminal usersets); a
/// <see cref="SetOp"/> carries a union / intersection / exclusion over child nodes. The tree is the
/// structural expansion and is not collapsed against a request context: a caller may evaluate the
/// per-node and per-subject <see cref="Caveat"/> expressions with a <see cref="CaveatEvaluator"/>.
/// </remarks>
public abstract record PermissionTreeNode(ObjectAndRelation Expanded, CaveatExpression? Caveat = null)
{
    /// <summary>The directly-written subjects of a base relation (incl. wildcards and usersets).</summary>
    /// <param name="Expanded">The resource ONR this leaf expands.</param>
    /// <param name="Subjects">The direct subjects, each with its per-tuple caveat.</param>
    /// <param name="Caveat">An optional caveat applying to the whole leaf (e.g. from an arrow tupleset).</param>
    public sealed record Leaf(
        ObjectAndRelation Expanded,
        IReadOnlyList<DirectSubject> Subjects,
        CaveatExpression? Caveat = null) : PermissionTreeNode(Expanded, Caveat);

    /// <summary>An intermediate set operation (union / intersection / exclusion) over children.</summary>
    /// <param name="Expanded">The resource ONR this node expands.</param>
    /// <param name="Operation">The combination operator.</param>
    /// <param name="Children">The child nodes (operands).</param>
    /// <param name="Caveat">An optional caveat applying to the whole node.</param>
    public sealed record SetOp(
        ObjectAndRelation Expanded,
        SetOperationType Operation,
        IReadOnlyList<PermissionTreeNode> Children,
        CaveatExpression? Caveat = null) : PermissionTreeNode(Expanded, Caveat);
}
