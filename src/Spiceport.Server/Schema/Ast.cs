using System.Collections.Immutable;

namespace Spiceport.Schema;

/// <summary>Root of a parsed schema file: a list of top-level definitions.</summary>
internal sealed record SchemaFileNode(ImmutableList<DefinitionNode> Definitions, ImmutableList<CaveatNode> Caveats);

/// <summary>A <c>definition</c> block containing relations and permissions, with its leading comments.</summary>
internal sealed record DefinitionNode(
    string Name,
    ImmutableList<RelationNode> Relations,
    ImmutableList<PermissionNode> Permissions,
    ImmutableList<string> Comments);

/// <summary>A <c>relation</c> declaration with its allowed subject types and leading comments.</summary>
internal sealed record RelationNode(string Name, ImmutableList<TypeRefNode> AllowedTypes, ImmutableList<string> Comments);

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

/// <summary>A <c>permission</c> declaration with its compute expression and leading comments.</summary>
internal sealed record PermissionNode(string Name, ExprNode Expression, ImmutableList<string> Comments);

/// <summary>
/// A <c>caveat</c> block: a name, typed parameters (in source order), the raw CEL body text, and its
/// leading comments.
/// </summary>
/// <param name="Name">The caveat name.</param>
/// <param name="Parameters">The parameters, in source order.</param>
/// <param name="Expression">The body between the braces, trimmed: the text the schema stores.</param>
/// <param name="Comments">The comments leading the <c>caveat</c> keyword.</param>
/// <param name="Line">The 1-based line of the <c>caveat</c> keyword.</param>
/// <param name="Column">The 1-based column of the <c>caveat</c> keyword.</param>
/// <param name="CelSource">
/// The body exactly as SpiceDB's schema parser slices it for CEL: from its first token to the end of its last
/// token, where a newline that SpiceDB's lexer turns into a synthetic semicolon counts as a token. Caveat
/// compile errors are positioned and quoted against this text, as upstream's are.
/// </param>
/// <param name="CelStartLine">The 0-based line of <paramref name="CelSource"/>'s start in the schema.</param>
/// <param name="CelStartColumn">The 0-based column, in code points, of <paramref name="CelSource"/>'s start.</param>
internal sealed record CaveatNode(
    string Name,
    ImmutableList<CaveatParameterNode> Parameters,
    string Expression,
    ImmutableList<string> Comments,
    int Line,
    int Column,
    string CelSource,
    int CelStartLine,
    int CelStartColumn);

/// <summary>One <c>name type</c> parameter inside a caveat parameter list, with its name's 1-based position.</summary>
internal sealed record CaveatParameterNode(string Name, CaveatTypeRefNode Type, int Line, int Column);

/// <summary>A caveat parameter type reference, possibly with generic child types (e.g. <c>list&lt;int&gt;</c>).</summary>
internal sealed record CaveatTypeRefNode(string Name, ImmutableList<CaveatTypeRefNode> ChildTypes);

/// <summary>Base type for permission compute expressions.</summary>
internal abstract record ExprNode;

/// <summary>A bare relation/permission reference (computed userset).</summary>
internal sealed record ReferenceExpr(string Name) : ExprNode;

/// <summary>The <c>nil</c> empty-set operand.</summary>
internal sealed record NilExpr : ExprNode;

/// <summary>The <c>self</c> operand: the resource treated as its own subject.</summary>
internal sealed record SelfExpr : ExprNode;

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
