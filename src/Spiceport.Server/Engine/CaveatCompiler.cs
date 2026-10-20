using System.Text;
using Spiceport.Core;

namespace Spiceport.Engine;

/// <summary>
/// Compiles a caveat's CEL expression at schema-write time, as SpiceDB's
/// <c>caveats.CompileCaveatWithSource</c> (<c>pkg/caveats/compile.go</c>) does: parse, statically
/// type-check against the declared parameters, and require a boolean result. An expression that would
/// fail upstream's compile is rejected here, with upstream's message, rather than deferred to a Check.
/// </summary>
/// <remarks>
/// <para>
/// The <c>Cel</c> package has no type checker, so the check is this port's own:
/// <see cref="CelAstBuilder"/> (cel-go's parser visitor and macros over the package's ANTLR grammar) and
/// <see cref="CelTypeChecker"/> (cel-go's checker), with SpiceDB's environment in
/// <see cref="CelDeclarations"/> and <see cref="CaveatParameterTypes"/>. As upstream, the result must be
/// exactly <c>bool</c>: an expression typed <c>dyn</c> (an <c>any</c> parameter, a <c>map&lt;any&gt;</c>
/// field, <c>dyn(x)</c>) is rejected even though it may evaluate to a boolean.
/// </para>
/// <para>
/// The compiled form is not persisted: the port stores the verbatim expression text (SpiceDB stores the
/// checked CEL AST), and the runtime <see cref="CaveatEvaluator"/> evaluates that text with the
/// <c>Cel</c> package.
/// </para>
/// </remarks>
public static class CaveatCompiler
{
    private const int MaxCaveatExpressionSize = 100_000;

    /// <summary>
    /// Compiles <paramref name="expression"/>, the caveat body exactly as SpiceDB's schema parser slices
    /// it, against <paramref name="parameterTypes"/>. Throws a <see cref="CaveatCompileException"/> carrying
    /// SpiceDB's message when it does not compile.
    /// </summary>
    /// <param name="caveatName">The caveat name; cel-go's source description in error messages.</param>
    /// <param name="expression">The CEL expression text.</param>
    /// <param name="parameterTypes">The declared parameters.</param>
    /// <param name="startLine">The 0-based line of the expression's start within the schema.</param>
    /// <param name="startColumn">The 0-based column (in code points) of the expression's start.</param>
    public static void Compile(
        string caveatName,
        string expression,
        IReadOnlyDictionary<string, CaveatTypeReference> parameterTypes,
        int startLine = 0,
        int startColumn = 0)
    {
        ArgumentNullException.ThrowIfNull(caveatName);
        ArgumentNullException.ThrowIfNull(expression);
        ArgumentNullException.ThrowIfNull(parameterTypes);

        var variables = parameterTypes.ToDictionary(p => p.Key, p => CaveatParameterTypes.ToCelType(p.Value));

        if (Encoding.UTF8.GetByteCount(expression.Trim()) > MaxCaveatExpressionSize)
        {
            throw new CaveatCompileException(
                $"caveat expression provided exceeds maximum allowed size of {MaxCaveatExpressionSize} characters");
        }

        var issues = new CelIssues();
        var parsed = CelAstBuilder.Parse(expression, issues);
        if (issues.HasErrors)
            throw Failure(issues, expression, caveatName, startLine, startColumn);

        // cel-go builds the checker environment lazily, after a successful parse; a parameter that
        // redeclares a standard type identifier fails it, reported as one issue without a location.
        var overlapping = variables.Keys
            .Where(CelDeclarations.TypeIdentifiers.ContainsKey)
            .Order(StringComparer.Ordinal)
            .Select(name => $"overlapping identifier for name '{name}'")
            .ToList();
        if (overlapping.Count > 0)
        {
            issues.Report(CelLocation.None, string.Join("\n", overlapping));
            throw Failure(issues, expression, caveatName, startLine, startColumn);
        }

        var outputType = CelTypeChecker.Check(parsed, variables, issues);
        if (issues.HasErrors)
            throw Failure(issues, expression, caveatName, startLine, startColumn);

        if (!ReferenceEquals(outputType, CheckedType.Bool))
            throw new CaveatCompileException($"caveat expression must result in a boolean value: found `{outputType}`");
    }

    /// <summary>
    /// Renders the issues as SpiceDB does after relocating them into the schema: a location on the
    /// expression's first line shifts by the start column, every location shifts by the start line, and
    /// the snippet stays the expression's own line.
    /// </summary>
    private static CaveatCompileException Failure(
        CelIssues issues, string expression, string caveatName, int startLine, int startColumn)
    {
        var relocated = issues.Errors
            .Select(e => e with
            {
                Location = e.Location.Line == 1
                    ? new CelLocation(startLine + e.Location.Line, startColumn + e.Location.Column)
                    : new CelLocation(startLine + e.Location.Line, e.Location.Column),
            })
            .ToList();
        return new CaveatCompileException(CelIssues.ToDisplayString(relocated, issues.Count, expression, caveatName));
    }
}
