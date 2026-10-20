namespace Spiceport.Engine;

/// <summary>
/// A source location in a CEL expression: a 1-based line and a 0-based column counted in code points,
/// as cel-go's <c>common.Location</c>.
/// </summary>
internal readonly record struct CelLocation(int Line, int Column)
{
    /// <summary>cel-go's <c>common.NoLocation</c>.</summary>
    public static readonly CelLocation None = new(-1, -1);
}

/// <summary>
/// A node of the parsed (macro-expanded) CEL expression the static checker walks: the shape of cel-go's
/// <c>common/ast.Expr</c>, as its parser produces it. Nodes compare by reference; the checker keys its
/// type map on the node itself where cel-go keys it on the node id.
/// </summary>
internal abstract class CelExpr(CelLocation location)
{
    /// <summary>The start location cel-go's parser records for the node's id.</summary>
    public CelLocation Location { get; } = location;
}

/// <summary>The literal kinds of cel-go's parser.</summary>
internal enum CelLiteralKind
{
    Bool,
    Bytes,
    Double,
    Int,
    Null,
    String,
    Uint,
}

/// <summary>A literal. Only a string literal's value is ever consulted (an optional field selection).</summary>
internal sealed class CelLiteral(CelLocation location, CelLiteralKind kind, string? stringValue = null) : CelExpr(location)
{
    public CelLiteralKind Kind { get; } = kind;

    public string? StringValue { get; } = stringValue;
}

/// <summary>An identifier reference.</summary>
internal sealed class CelIdent(CelLocation location, string name) : CelExpr(location)
{
    public string Name { get; } = name;
}

/// <summary>A field selection, or (when <see cref="TestOnly"/>) a <c>has()</c> presence test.</summary>
internal sealed class CelSelect(CelLocation location, CelExpr operand, string field, bool testOnly) : CelExpr(location)
{
    public CelExpr Operand { get; } = operand;

    public string Field { get; } = field;

    public bool TestOnly { get; } = testOnly;
}

/// <summary>A global (no <see cref="Target"/>) or receiver-style function call; operators are calls too.</summary>
internal sealed class CelCall(CelLocation location, string function, CelExpr? target, IReadOnlyList<CelExpr> args)
    : CelExpr(location)
{
    public string Function { get; } = function;

    public CelExpr? Target { get; } = target;

    public IReadOnlyList<CelExpr> Args { get; } = args;
}

/// <summary>A list literal; <see cref="OptionalIndices"/> are the <c>[?e]</c> elements.</summary>
internal sealed class CelList(CelLocation location, IReadOnlyList<CelExpr> elements, IReadOnlySet<int> optionalIndices)
    : CelExpr(location)
{
    public IReadOnlyList<CelExpr> Elements { get; } = elements;

    public IReadOnlySet<int> OptionalIndices { get; } = optionalIndices;
}

/// <summary>One <c>key: value</c> entry of a map literal.</summary>
internal sealed record CelMapEntry(CelExpr Key, CelExpr Value, bool Optional);

/// <summary>A map literal.</summary>
internal sealed class CelMap(CelLocation location, IReadOnlyList<CelMapEntry> entries) : CelExpr(location)
{
    public IReadOnlyList<CelMapEntry> Entries { get; } = entries;
}

/// <summary>One <c>field: value</c> initializer of a message construction.</summary>
internal sealed record CelStructField(CelLocation Location, string Name, CelExpr Value, bool Optional);

/// <summary>A message construction, <c>pkg.Type{field: value}</c>.</summary>
internal sealed class CelStruct(CelLocation location, string typeName, IReadOnlyList<CelStructField> fields)
    : CelExpr(location)
{
    public string TypeName { get; } = typeName;

    public IReadOnlyList<CelStructField> Fields { get; } = fields;
}

/// <summary>A fold, as the parser's macros (<c>all</c>, <c>exists</c>, <c>map</c>, ...) expand to.</summary>
internal sealed class CelComprehension(
    CelLocation location,
    CelExpr iterRange,
    string iterVar,
    string accuVar,
    CelExpr accuInit,
    CelExpr loopCondition,
    CelExpr loopStep,
    CelExpr result) : CelExpr(location)
{
    public CelExpr IterRange { get; } = iterRange;

    public string IterVar { get; } = iterVar;

    public string AccuVar { get; } = accuVar;

    public CelExpr AccuInit { get; } = accuInit;

    public CelExpr LoopCondition { get; } = loopCondition;

    public CelExpr LoopStep { get; } = loopStep;

    public CelExpr Result { get; } = result;
}

/// <summary>The placeholder cel-go's parser leaves where it reported an error.</summary>
internal sealed class CelUnspecified(CelLocation location) : CelExpr(location);
