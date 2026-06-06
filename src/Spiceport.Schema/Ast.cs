using System.Collections.Immutable;

namespace Spiceport.Schema;

/// <summary>Root of a parsed schema file: a list of top-level definitions.</summary>
internal sealed record SchemaFileNode(ImmutableList<DefinitionNode> Definitions, ImmutableList<CaveatNode> Caveats);

/// <summary>A <c>definition</c> block containing relations and permissions.</summary>
internal sealed record DefinitionNode(string Name, ImmutableList<RelationNode> Relations, ImmutableList<PermissionNode> Permissions);

/// <summary>A <c>relation</c> declaration with its allowed subject types.</summary>
internal sealed record RelationNode(string Name, ImmutableList<TypeRefNode> AllowedTypes);

/// <summary>One allowed subject type within a relation's type expression.</summary>
/// <param name="TypeName">Subject namespace, possibly path-qualified.</param>
/// <param name="IsWildcard">True for the <c>:*</c> public-wildcard form.</param>
/// <param name="Subrelation">Optional <c>#relation</c> subrelation (null when absent).</param>
/// <param name="CaveatName">Optional <c>with caveat</c> name (parsed; deferred semantically).</param>
/// <param name="RequiresExpiration">True if <c>with expiration</c> trait present.</param>
internal sealed record TypeRefNode(
    string TypeName,
    bool IsWildcard,
    string? Subrelation,
    string? CaveatName,
    bool RequiresExpiration);

/// <summary>A <c>permission</c> declaration with its compute expression.</summary>
internal sealed record PermissionNode(string Name, ExprNode Expression);

/// <summary>A <c>caveat</c> block (parsed; compiled to a placeholder definition).</summary>
internal sealed record CaveatNode(string Name);

/// <summary>Base type for permission compute expressions.</summary>
internal abstract record ExprNode;

/// <summary>A bare relation/permission reference (computed userset).</summary>
internal sealed record ReferenceExpr(string Name) : ExprNode;

/// <summary>The <c>nil</c> empty-set operand.</summary>
internal sealed record NilExpr : ExprNode;

/// <summary>An arrow expression <c>tupleset-&gt;computed</c>, optionally functioned.</summary>
internal sealed record ArrowExpr(string Tupleset, string Computed, string? Function) : ExprNode;

/// <summary>A binary set operation between two operands.</summary>
internal sealed record BinaryExpr(SetOp Op, ExprNode Left, ExprNode Right) : ExprNode;

/// <summary>Set operators usable in compute expressions.</summary>
internal enum SetOp
{
    Union,
    Intersection,
    Exclusion,
}
