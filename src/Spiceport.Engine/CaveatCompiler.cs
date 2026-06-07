using Cel;

namespace Spiceport.Engine;

/// <summary>
/// Compiles (parses + type-binds) caveat CEL expressions at schema-write time, using the same CEL
/// environment the runtime <see cref="CaveatEvaluator"/> evaluates against (so the SpiceDB custom
/// functions <c>ipaddress</c>/<c>in_cidr</c>/<c>isSubtreeOf</c> resolve during the parse). Mirrors the
/// compile step of SpiceDB's <c>caveats.DeserializeCaveatWithTypeSet</c>: a syntactically invalid
/// expression is rejected at write time rather than deferred to a later Check.
/// </summary>
/// <remarks>
/// The underlying <c>Cel</c> library produces a non-serializable program delegate, so unlike SpiceDB
/// (which serializes the compiled CEL AST) the port continues to persist the verbatim expression text;
/// this method exists purely to fail an unparseable caveat at write time.
/// </remarks>
public static class CaveatCompiler
{
    private static readonly CelEnvironment Environment = CaveatCelEnvironment.Build();

    /// <summary>
    /// Parses the given CEL expression, throwing a <see cref="CelException"/> (or argument exception) if
    /// it is not a syntactically valid caveat expression.
    /// </summary>
    public static void Parse(string expression)
    {
        ArgumentNullException.ThrowIfNull(expression);
        Environment.Parse(expression);
    }
}
