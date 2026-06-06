using System.Net;
using System.Net.Sockets;
using System.Numerics;
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
/// Because the library has no unknowns/residual-AST support, partial evaluation uses a
/// shim: the set of declared parameters referenced by the expression but absent from the
/// supplied context is the candidate "missing" set. The expression is still executed against
/// the supplied context — short-circuiting (e.g. <c>false &amp;&amp; missing</c>) can yield a
/// definite result even when some candidates are absent. If execution throws
/// <see cref="CelUndeclaredReferenceException"/>, a missing parameter was genuinely needed and
/// the result is <see cref="CaveatOutcome.Caveated"/> with the candidate missing names.
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
        _env = BuildEnvironment();
    }

    /// <summary>
    /// Evaluates the named caveat against the relationship context merged with the request context
    /// (request context overrides). Returns <see cref="CaveatOutcome.Caveated"/> when a referenced
    /// declared parameter is absent. An unknown caveat name evaluates to definitely-false.
    /// </summary>
    public CaveatResult Evaluate(
        string caveatName,
        IReadOnlyDictionary<string, object?>? relationshipContext,
        IReadOnlyDictionary<string, object?>? requestContext)
    {
        ArgumentNullException.ThrowIfNull(caveatName);

        if (!_caveats.TryGetValue(caveatName, out var def))
            return CaveatResult.False;

        var expression = System.Text.Encoding.UTF8.GetString(def.SerializedExpression);

        var vars = new Dictionary<string, object?>();
        AddContext(vars, requestContext);
        AddContext(vars, relationshipContext); // written/stored context overrides request context

        // Partial-eval shim: a declared parameter that the expression actually references but
        // that is absent from the merged context makes the caveat undeterminable -> Caveated.
        // We decide this up front (rather than relying on the CEL engine to throw) because a
        // missing variable can silently arrive as null at a custom function and yield a bogus
        // definite result. Short-circuiting over a missing variable (residual AST) is deferred.
        var missing = def.ParameterTypes.Keys
            .Where(p => !vars.ContainsKey(p) && ReferencesIdentifier(expression, p))
            .ToList();
        if (missing.Count > 0)
            return CaveatResult.Missing(missing);

        object? result;
        try
        {
            result = _env.Program(expression, vars);
        }
        catch (Exception ex) when (IsMissingReferenceError(ex))
        {
            // A genuinely-needed parameter was absent (short-circuit did not save us). The
            // missing reference can surface directly (CelUndeclaredReferenceException) or wrapped
            // by an operator/macro overload failure (e.g. `somelist.all(...)` on a null var).
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

    private static void AddContext(Dictionary<string, object?> vars, IReadOnlyDictionary<string, object?>? ctx)
    {
        if (ctx is null)
            return;
        foreach (var (k, v) in ctx)
        {
            if (v is not null)
                vars[k] = Normalize(v);
            else
                vars.Remove(k);
        }
    }

    /// <summary>
    /// Normalizes a context value into a CEL-friendly representation. JSON numbers and integral
    /// values are widened to <see cref="long"/>/<see cref="double"/>; nested maps/lists recurse.
    /// </summary>
    private static object Normalize(object value) => value switch
    {
        System.Text.Json.JsonElement je => NormalizeJson(je),
        bool or string or long or double => value,
        int i => (long)i,
        short s => (long)s,
        byte by => (long)by,
        uint ui => (long)ui,
        ulong ul => (long)ul,
        float f => (double)f,
        decimal dec => (double)dec,
        IReadOnlyDictionary<string, object?> map =>
            map.Where(kv => kv.Value is not null)
               .ToDictionary(kv => kv.Key, kv => Normalize(kv.Value!)),
        System.Collections.IEnumerable e and not string =>
            e.Cast<object?>().Where(x => x is not null).Select(x => Normalize(x!)).ToList(),
        _ => value,
    };

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

    private static CelEnvironment BuildEnvironment()
    {
        var env = new CelEnvironment(null, null);

        // ipaddress(string) -> the string itself (represented as a string in this engine).
        // Pairing with in_cidr below gives the SpiceDB ip.in_cidr(cidr) behavior.
        env.RegisterFunction("ipaddress", new[] { typeof(string) }, args => args[0]);

        // <ipaddress|string>.in_cidr(cidr_string) -> bool. The receiver is declared `ipaddress`
        // but flows through context as a boxed string, so accept `object` and coerce.
        env.RegisterFunction("in_cidr", new[] { typeof(object), typeof(string) }, args =>
            InCidr(AsString(args[0]), AsString(args[1])));

        // <map>.isSubtreeOf(other_map) -> bool
        env.RegisterFunction(
            "isSubtreeOf",
            new[] { typeof(object), typeof(object) },
            args => IsSubtreeOf(args[0]!, args[1]!));

        return env;
    }

    private static string AsString(object? o) => o as string ?? o?.ToString() ?? string.Empty;

    /// <summary>Returns true if <paramref name="ip"/> is contained within the CIDR network.</summary>
    private static bool InCidr(string ip, string cidr)
    {
        if (!IPAddress.TryParse(ip, out var addr))
            return false;

        var slash = cidr.IndexOf('/');
        if (slash < 0)
            return IPAddress.TryParse(cidr, out var single) && single.Equals(addr);

        if (!IPAddress.TryParse(cidr[..slash], out var network)
            || !int.TryParse(cidr[(slash + 1)..], out var prefixLen))
            return false;

        if (addr.AddressFamily != network.AddressFamily)
            return false;

        var addrBytes = addr.GetAddressBytes();
        var netBytes = network.GetAddressBytes();
        if (addrBytes.Length != netBytes.Length)
            return false;

        var totalBits = addrBytes.Length * 8;
        if (prefixLen < 0 || prefixLen > totalBits)
            return false;

        for (var bit = 0; bit < prefixLen; bit++)
        {
            var byteIndex = bit / 8;
            var mask = (byte)(1 << (7 - (bit % 8)));
            if ((addrBytes[byteIndex] & mask) != (netBytes[byteIndex] & mask))
                return false;
        }
        return true;
    }

    /// <summary>
    /// Returns true if <paramref name="left"/> is a structural subtree of <paramref name="right"/>:
    /// every key in left exists in right with an equal value (recursively for nested maps).
    /// </summary>
    private static bool IsSubtreeOf(object left, object right)
    {
        if (left is not IDictionary<string, object> l || right is not IDictionary<string, object> r)
            return false;

        foreach (var (key, lv) in l)
        {
            if (!r.TryGetValue(key, out var rv))
                return false;

            if (lv is IDictionary<string, object> && rv is IDictionary<string, object>)
            {
                if (!IsSubtreeOf(lv, rv))
                    return false;
            }
            else if (!ValuesEqual(lv, rv))
            {
                return false;
            }
        }
        return true;
    }

    private static bool ValuesEqual(object? a, object? b)
    {
        if (a is null || b is null)
            return Equals(a, b);
        if (a is IConvertible && b is IConvertible && IsNumeric(a) && IsNumeric(b))
            return Convert.ToDouble(a).Equals(Convert.ToDouble(b));
        return a.Equals(b);
    }

    private static bool IsNumeric(object o) =>
        o is sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal or BigInteger;
}
