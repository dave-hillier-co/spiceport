using Cel;
using Spiceport.Core;

namespace Spiceport.Engine;

/// <summary>The kind of outcome produced by evaluating a single caveat expression.</summary>
public enum CaveatOutcome
{
    /// <summary>The caveat evaluated to a definite <c>true</c>.</summary>
    DefinitelyTrue = 0,

    /// <summary>The caveat evaluated to a definite <c>false</c>.</summary>
    DefinitelyFalse = 1,

    /// <summary>
    /// The caveat could not be fully determined because one or more declared parameters
    /// referenced by the expression were absent from the supplied context.
    /// </summary>
    Caveated = 2,
}

/// <summary>
/// The result of evaluating a caveat expression: its outcome and, when
/// <see cref="CaveatOutcome.Caveated"/>, the names of the parameters that were missing.
/// </summary>
/// <param name="Outcome">The evaluation outcome.</param>
/// <param name="MissingFields">The declared parameter names that were unavailable, if any.</param>
public sealed record CaveatResult(CaveatOutcome Outcome, IReadOnlyList<string> MissingFields)
{
    /// <summary>A definite-true result with no missing fields.</summary>
    public static readonly CaveatResult True = new(CaveatOutcome.DefinitelyTrue, []);

    /// <summary>A definite-false result with no missing fields.</summary>
    public static readonly CaveatResult False = new(CaveatOutcome.DefinitelyFalse, []);

    /// <summary>Creates a caveated result carrying the given missing field names.</summary>
    public static CaveatResult Missing(IEnumerable<string> fields) =>
        new(CaveatOutcome.Caveated, fields.Distinct().ToList());
}

/// <summary>
/// Evaluates SpiceDB-style CEL caveats using the <c>Cel</c> NuGet package.
/// </summary>
/// <remarks>
/// <para>
/// Registers the SpiceDB custom functions/types used by caveat expressions:
/// <c>ipaddress(string)</c> with <c>.in_cidr(string)</c>, and a map
/// <c>.isSubtreeOf(map)</c> structural-subtree check. <c>timestamp</c> and
/// <c>duration</c> are provided by the underlying CEL implementation.
/// </para>
/// <para>
/// Partial evaluation: the underlying CEL engine short-circuits <c>||</c>/<c>&amp;&amp;</c> at the
/// operator level, so a missing variable on the non-determining branch still yields a definite
/// verdict (e.g. <c>allowed || tier &gt; 5</c> with <c>{allowed:true}</c> is definitely-true even
/// though <c>tier</c> is absent). The expression is executed directly against the supplied context;
/// only when the engine genuinely needs an absent variable (raising
/// <see cref="CelUndeclaredReferenceException"/>, or an overload/macro failure caused by the
/// resulting null operand) is the result <see cref="CaveatOutcome.Caveated"/>, carrying the declared
/// parameters that are referenced by the expression yet absent from the merged context. This mirrors
/// SpiceDB's <c>cel.OptPartialEval</c> + <c>PartialVars</c> behaviour for boolean short-circuits.
/// </para>
/// <para>
/// Context values are validated and coerced against the caveat's declared parameter types before
/// evaluation (mirroring SpiceDB's <c>ConvertContextToParameters</c>): a value whose type cannot be
/// converted to the declared type raises <see cref="CaveatEvaluationException"/> with
/// <see cref="CaveatEvaluationErrorKind.ParameterTypeMismatch"/> (gRPC <c>InvalidArgument</c>), and a
/// <c>uint</c> parameter is preserved as an unsigned <see cref="ulong"/> rather than narrowed to a
/// signed <see cref="long"/>. An unknown caveat name raises
/// <see cref="CaveatEvaluationErrorKind.UnknownCaveat"/>.
/// </para>
/// </remarks>
public sealed class CaveatEvaluator
{
    private readonly IReadOnlyDictionary<string, CaveatDefinition> _caveats;
    private readonly CelEnvironment _env;

    /// <summary>Creates an evaluator over the given caveat definitions, keyed by name.</summary>
    public CaveatEvaluator(IEnumerable<CaveatDefinition> caveats)
    {
        ArgumentNullException.ThrowIfNull(caveats);
        _caveats = caveats.ToDictionary(c => c.Name);
        _env = CaveatCelEnvironment.Build();
    }

    /// <summary>
    /// Evaluates the named caveat against the relationship context merged with the request context
    /// (request context overrides). Returns <see cref="CaveatOutcome.Caveated"/> when a referenced
    /// declared parameter is absent and the CEL engine cannot short-circuit around it.
    /// </summary>
    /// <exception cref="CaveatEvaluationException">
    /// The caveat name is unknown (<see cref="CaveatEvaluationErrorKind.UnknownCaveat"/>), or a
    /// supplied context value is type-incompatible with the declared parameter
    /// (<see cref="CaveatEvaluationErrorKind.ParameterTypeMismatch"/>).
    /// </exception>
    public CaveatResult Evaluate(
        string caveatName,
        IReadOnlyDictionary<string, object?>? relationshipContext,
        IReadOnlyDictionary<string, object?>? requestContext)
    {
        ArgumentNullException.ThrowIfNull(caveatName);

        // An unknown caveat name is a schema-skew/stale-cache condition. SpiceDB's CaveatRunner.get
        // returns CaveatNameNotFoundErr; surface it loudly rather than silently denying.
        if (!_caveats.TryGetValue(caveatName, out var def))
            throw new CaveatEvaluationException(
                CaveatEvaluationErrorKind.UnknownCaveat,
                $"caveat with name `{caveatName}` not found");

        var expression = System.Text.Encoding.UTF8.GetString(def.SerializedExpression);

        // Merge request then relationship context (relationship/stored context overrides request),
        // validating + coercing each value against the declared parameter type. A mismatch becomes
        // a ParameterTypeError (gRPC InvalidArgument), as in SpiceDB's ConvertContextToParameters.
        var vars = new Dictionary<string, object?>();
        AddContext(vars, requestContext, def.ParameterTypes);
        AddContext(vars, relationshipContext, def.ParameterTypes);

        // Candidate missing set: declared parameters referenced by the expression yet absent from the
        // merged context. Only reported when the CEL engine actually needs one of them (i.e. could
        // not short-circuit the surrounding ||/&&); the engine handles boolean short-circuiting.
        var missing = def.ParameterTypes.Keys
            .Where(p => !vars.ContainsKey(p) && ReferencesIdentifier(expression, p))
            .ToList();

        object? result;
        try
        {
            result = _env.Program(expression, vars);
        }
        catch (Exception ex) when (IsMissingReferenceError(ex))
        {
            // A genuinely-needed parameter was absent (short-circuit did not save us). The missing
            // reference can surface directly (CelUndeclaredReferenceException) or wrapped by an
            // operator/macro overload failure (e.g. `somelist.all(...)` on a null var).
            if (missing.Count > 0)
                return CaveatResult.Missing(missing);

            // No declared parameter is missing yet evaluation still failed: surface it rather
            // than silently masking a genuine expression/overload bug.
            throw;
        }

        if (result is bool b)
            return b ? CaveatResult.True : CaveatResult.False;

        // Non-boolean result: treat as undetermined rather than asserting membership.
        return missing.Count > 0 ? CaveatResult.Missing(missing) : CaveatResult.False;
    }

    /// <summary>
    /// Collapses a <see cref="CaveatExpression"/> tree to a single result against the request
    /// context, following short-circuiting AND/OR/NOT rules: AND is false if any operand is
    /// definitely false; OR is true if any operand is definitely true; otherwise missing fields
    /// accumulate across operands and the result is <see cref="CaveatOutcome.Caveated"/>.
    /// </summary>
    public CaveatResult EvaluateExpression(
        CaveatExpression expression,
        IReadOnlyDictionary<string, object?>? requestContext)
    {
        ArgumentNullException.ThrowIfNull(expression);
        return expression switch
        {
            CaveatExpression.Leaf leaf =>
                Evaluate(leaf.Caveat.CaveatName, leaf.Caveat.Context, requestContext),

            CaveatExpression.Or or => EvaluateOr(or.Children, requestContext),
            CaveatExpression.And and => EvaluateAnd(and.Children, requestContext),
            CaveatExpression.Not not => Invert(EvaluateExpression(not.Child, requestContext)),
            _ => CaveatResult.False,
        };
    }

    private CaveatResult EvaluateOr(
        IReadOnlyList<CaveatExpression> children,
        IReadOnlyDictionary<string, object?>? requestContext)
    {
        var missing = new List<string>();
        var anyCaveated = false;
        foreach (var child in children)
        {
            var r = EvaluateExpression(child, requestContext);
            if (r.Outcome == CaveatOutcome.DefinitelyTrue)
                return CaveatResult.True; // short-circuit
            if (r.Outcome == CaveatOutcome.Caveated)
            {
                anyCaveated = true;
                missing.AddRange(r.MissingFields);
            }
        }
        return anyCaveated ? CaveatResult.Missing(missing) : CaveatResult.False;
    }

    private CaveatResult EvaluateAnd(
        IReadOnlyList<CaveatExpression> children,
        IReadOnlyDictionary<string, object?>? requestContext)
    {
        var missing = new List<string>();
        var anyCaveated = false;
        foreach (var child in children)
        {
            var r = EvaluateExpression(child, requestContext);
            if (r.Outcome == CaveatOutcome.DefinitelyFalse)
                return CaveatResult.False; // short-circuit
            if (r.Outcome == CaveatOutcome.Caveated)
            {
                anyCaveated = true;
                missing.AddRange(r.MissingFields);
            }
        }
        return anyCaveated ? CaveatResult.Missing(missing) : CaveatResult.True;
    }

    private static CaveatResult Invert(CaveatResult r) => r.Outcome switch
    {
        CaveatOutcome.DefinitelyTrue => CaveatResult.False,
        CaveatOutcome.DefinitelyFalse => CaveatResult.True,
        _ => r, // caveated negation stays caveated with the same missing fields.
    };

    /// <summary>True if <paramref name="expression"/> references the identifier as a whole word.</summary>
    private static bool ReferencesIdentifier(string expression, string identifier) =>
        System.Text.RegularExpressions.Regex.IsMatch(
            expression, $@"(?<![\w.]){System.Text.RegularExpressions.Regex.Escape(identifier)}\b");

    /// <summary>
    /// True if the exception (or its inner chain) indicates a reference to a variable that was
    /// not supplied — directly as <see cref="CelUndeclaredReferenceException"/>, or wrapped by an
    /// overload/macro failure caused by the resulting null/unknown operand.
    /// </summary>
    private static bool IsMissingReferenceError(Exception ex)
    {
        for (var e = ex; e is not null; e = e.InnerException)
        {
            if (e is CelUndeclaredReferenceException)
                return true;
            if (e is CelNoSuchOverloadException && ReferenceEquals(e, ex) && ex.InnerException is null)
                return true; // bare overload failure on a missing-operand macro (no inner detail)
        }

        return false;
    }

    /// <summary>
    /// Merges <paramref name="ctx"/> into <paramref name="vars"/>, validating and coercing each value
    /// against its declared parameter type. A value whose type cannot be converted to the declared
    /// type raises <see cref="CaveatEvaluationException"/>. Unknown parameters (not declared by the
    /// caveat) are skipped, mirroring SpiceDB's <c>SkipUnknownParameters</c>; a null value clears the
    /// parameter (treated as absent).
    /// </summary>
    private static void AddContext(
        Dictionary<string, object?> vars,
        IReadOnlyDictionary<string, object?>? ctx,
        IReadOnlyDictionary<string, CaveatTypeReference> parameterTypes)
    {
        if (ctx is null)
            return;
        foreach (var (k, v) in ctx)
        {
            if (v is null)
            {
                vars.Remove(k);
                continue;
            }

            // SkipUnknownParameters: a context value with no declared parameter is ignored, not an
            // error (matches SpiceDB; the value cannot be referenced by the typed expression anyway).
            if (!parameterTypes.TryGetValue(k, out var declared))
                continue;

            vars[k] = ConvertValue(k, declared, Normalize(v));
        }
    }

    /// <summary>
    /// Normalizes a context value into a CEL-friendly representation: a lazily-deserialized
    /// <see cref="System.Text.Json.JsonElement"/> becomes plain CLR values; nested maps/lists recurse.
    /// Integral/floating CLR values are passed through with their natural width preserved so that
    /// <see cref="ConvertValue"/> can validate against the declared type (uint64 is not narrowed here).
    /// </summary>
    private static object Normalize(object value) => value switch
    {
        System.Text.Json.JsonElement je => NormalizeJson(je),
        bool or string or long or double or int or short or byte or sbyte or ushort or uint or ulong or float or decimal => value,
        IReadOnlyDictionary<string, object?> map =>
            map.Where(kv => kv.Value is not null)
               .ToDictionary(kv => kv.Key, kv => Normalize(kv.Value!)),
        System.Collections.IEnumerable e and not string =>
            e.Cast<object?>().Where(x => x is not null).Select(x => Normalize(x!)).ToList(),
        _ => value,
    };

    /// <summary>
    /// Validates and coerces a normalized value against the declared caveat parameter type, mirroring
    /// SpiceDB's <c>VariableType.ConvertValue</c>. Numeric types accept the target type, a JSON number,
    /// or a numeric string; <c>int</c> requires an integral value, <c>uint</c> requires a non-negative
    /// integral value preserved as <see cref="ulong"/>, <c>double</c> accepts any number. <c>string</c>
    /// and <c>bool</c> require an exact match. <c>list</c>/<c>map</c> recurse into their child types.
    /// </summary>
    private static object ConvertValue(string param, CaveatTypeReference type, object value)
    {
        switch (type.TypeName)
        {
            case "bool":
                return value is bool ? value : throw TypeError(param, "bool", value);

            case "string":
                return value is string ? value : throw TypeError(param, "string", value);

            case "bytes":
            case "duration":
            case "timestamp":
            case "ipaddress":
                // These flow through as strings (parsed by the registered functions / engine).
                return value is string ? value : throw TypeError(param, type.TypeName, value);

            case "int":
                return ToInt64(param, value);

            case "uint":
                return ToUInt64(param, value);

            case "double":
                return ToDouble(param, value);

            case "any":
                return value;

            case "list":
            {
                if (value is not List<object> items)
                    throw TypeError(param, "list", value);
                var child = type.ChildTypes?.FirstOrDefault();
                return child is null
                    ? items
                    : items.Select(item => ConvertValue(param, child, item)).ToList();
            }

            case "map":
            {
                if (value is not Dictionary<string, object> entries)
                    throw TypeError(param, "map", value);
                var valueType = type.ChildTypes is { Count: >= 1 } ct ? ct[^1] : null;
                return valueType is null
                    ? entries
                    : entries.ToDictionary(kv => kv.Key, kv => ConvertValue(param, valueType, kv.Value));
            }

            default:
                // Unknown declared type keyword: pass the value through unchanged.
                return value;
        }
    }

    private static long ToInt64(string param, object value)
    {
        switch (value)
        {
            case long l: return l;
            case int or short or sbyte or byte or ushort or uint: return Convert.ToInt64(value);
            case ulong ul when ul <= long.MaxValue: return (long)ul;
            case double d when d == Math.Floor(d) && !double.IsInfinity(d): return (long)d;
            case float f when f == Math.Floor(f) && !float.IsInfinity(f): return (long)f;
            case decimal m when m == Math.Floor(m): return (long)m;
            case string s when long.TryParse(s, System.Globalization.NumberStyles.Integer,
                System.Globalization.CultureInfo.InvariantCulture, out var parsed): return parsed;
            default: throw TypeError(param, "int", value);
        }
    }

    private static ulong ToUInt64(string param, object value)
    {
        switch (value)
        {
            case ulong ul: return ul;
            case long l when l >= 0: return (ulong)l;
            case int or short or sbyte when Convert.ToInt64(value) >= 0: return (ulong)Convert.ToInt64(value);
            case byte or ushort or uint: return Convert.ToUInt64(value);
            case double d when d >= 0 && d == Math.Floor(d) && !double.IsInfinity(d): return (ulong)d;
            case float f when f >= 0 && f == Math.Floor(f) && !float.IsInfinity(f): return (ulong)f;
            case decimal m when m >= 0 && m == Math.Floor(m): return (ulong)m;
            case string s when ulong.TryParse(s, System.Globalization.NumberStyles.Integer,
                System.Globalization.CultureInfo.InvariantCulture, out var parsed): return parsed;
            default: throw TypeError(param, "uint", value);
        }
    }

    private static double ToDouble(string param, object value) => value switch
    {
        double d => d,
        float f => f,
        decimal m => (double)m,
        long or int or short or sbyte or byte or ushort or uint or ulong => Convert.ToDouble(value),
        string s when double.TryParse(s, System.Globalization.NumberStyles.Float,
            System.Globalization.CultureInfo.InvariantCulture, out var parsed) => parsed,
        _ => throw TypeError(param, "double", value),
    };

    private static CaveatEvaluationException TypeError(string param, string expected, object value) =>
        new(CaveatEvaluationErrorKind.ParameterTypeMismatch,
            $"could not convert context parameter `{param}`: a {expected} value is required, but found " +
            $"{value.GetType().Name} `{value}`");

    /// <summary>
    /// Converts a lazily-deserialized <see cref="System.Text.Json.JsonElement"/> (how relationship
    /// caveat context arrives) into plain CLR values the CEL engine can compare and select on:
    /// numbers to <see cref="long"/>/<see cref="double"/>, objects to
    /// <see cref="Dictionary{TKey,TValue}"/>, arrays to <see cref="List{T}"/>.
    /// </summary>
    private static object NormalizeJson(System.Text.Json.JsonElement e)
    {
        switch (e.ValueKind)
        {
            case System.Text.Json.JsonValueKind.True:
                return true;
            case System.Text.Json.JsonValueKind.False:
                return false;
            case System.Text.Json.JsonValueKind.String:
                return e.GetString()!;
            case System.Text.Json.JsonValueKind.Number:
                return e.TryGetInt64(out var l) ? l : e.GetDouble();
            case System.Text.Json.JsonValueKind.Object:
                var map = new Dictionary<string, object>();
                foreach (var prop in e.EnumerateObject())
                {
                    if (prop.Value.ValueKind is not (System.Text.Json.JsonValueKind.Null or System.Text.Json.JsonValueKind.Undefined))
                        map[prop.Name] = NormalizeJson(prop.Value);
                }

                return map;
            case System.Text.Json.JsonValueKind.Array:
                var list = new List<object>();
                foreach (var item in e.EnumerateArray())
                {
                    if (item.ValueKind is not (System.Text.Json.JsonValueKind.Null or System.Text.Json.JsonValueKind.Undefined))
                        list.Add(NormalizeJson(item));
                }

                return list;
            default:
                return e.ToString();
        }
    }
}
