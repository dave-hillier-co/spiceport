using System.Collections.Immutable;
using System.Text;
using Spiceport.Core;

namespace Spiceport.Engine.Tests;

/// <summary>
/// Direct unit tests for <see cref="CaveatEvaluator"/> covering CEL partial-evaluation short-circuits
/// over missing context, parameter type validation/coercion (including uint64 preservation), and the
/// unknown-caveat error path. These mirror SpiceDB's <c>cel.OptPartialEval</c> +
/// <c>ConvertContextToParameters</c> semantics.
/// </summary>
public class CaveatEvaluatorTests
{
    private static CaveatDefinition Caveat(
        string name, string expr, params (string Name, string Type)[] parameters)
    {
        var types = parameters.ToImmutableDictionary(
            p => p.Name, p => new CaveatTypeReference(p.Type));
        return new CaveatDefinition(name, Encoding.UTF8.GetBytes(expr), types);
    }

    private static CaveatEvaluator Evaluator(params CaveatDefinition[] defs) => new(defs);

    private static Dictionary<string, object?> Ctx(params (string Key, object? Value)[] pairs) =>
        pairs.ToDictionary(p => p.Key, p => p.Value);

    [Fact]
    public void Or_WithTrueBranch_ShortCircuitsOverMissingVariable_ToDefiniteTrue()
    {
        // `allowed || tier > 5` with {allowed:true} and tier absent: SpiceDB short-circuits the ||
        // and returns a definite true rather than Caveated.
        var eval = Evaluator(Caveat("c", "allowed || tier > 5", ("allowed", "bool"), ("tier", "int")));

        var result = eval.Evaluate("c", null, Ctx(("allowed", true)));

        Assert.Equal(CaveatOutcome.DefinitelyTrue, result.Outcome);
    }

    [Fact]
    public void And_WithFalseBranch_ShortCircuitsOverMissingVariable_ToDefiniteFalse()
    {
        // `denied && tier > 5` with {denied:false} and tier absent: short-circuit to definite false.
        var eval = Evaluator(Caveat("c", "denied && tier > 5", ("denied", "bool"), ("tier", "int")));

        var result = eval.Evaluate("c", null, Ctx(("denied", false)));

        Assert.Equal(CaveatOutcome.DefinitelyFalse, result.Outcome);
    }

    [Fact]
    public void Or_WithFalseBranch_AndMissingVariable_IsCaveated()
    {
        // `allowed || tier > 5` with {allowed:false}: the || cannot short-circuit; tier is needed.
        var eval = Evaluator(Caveat("c", "allowed || tier > 5", ("allowed", "bool"), ("tier", "int")));

        var result = eval.Evaluate("c", null, Ctx(("allowed", false)));

        Assert.Equal(CaveatOutcome.Caveated, result.Outcome);
        Assert.Contains("tier", result.MissingFields);
    }

    [Fact]
    public void MissingReceiverForCustomFunction_IsCaveated_NotFalse()
    {
        // user_ip absent: the receiver of in_cidr is unbound, so the result is undetermined.
        var eval = Evaluator(Caveat("c", "user_ip.in_cidr(cidr)", ("user_ip", "ipaddress"), ("cidr", "string")));

        var result = eval.Evaluate("c", null, Ctx(("cidr", "10.0.0.0/8")));

        Assert.Equal(CaveatOutcome.Caveated, result.Outcome);
        Assert.Contains("user_ip", result.MissingFields);
    }

    [Fact]
    public void UnknownCaveatName_Throws_UnknownCaveat()
    {
        var eval = Evaluator(Caveat("c", "true"));

        var ex = Assert.Throws<CaveatEvaluationException>(
            () => eval.Evaluate("does_not_exist", null, null));

        Assert.Equal(CaveatEvaluationErrorKind.UnknownCaveat, ex.Kind);
    }

    [Fact]
    public void WrongTypeForIntParameter_Throws_ParameterTypeMismatch()
    {
        var eval = Evaluator(Caveat("c", "age > 18", ("age", "int")));

        var ex = Assert.Throws<CaveatEvaluationException>(
            () => eval.Evaluate("c", null, Ctx(("age", "not-a-number"))));

        Assert.Equal(CaveatEvaluationErrorKind.ParameterTypeMismatch, ex.Kind);
    }

    [Fact]
    public void Uint64NearMaxValue_IsPreservedNotNarrowedToNegativeLong()
    {
        // 2^63 + 1 fits in a uint64 but overflows a signed long; if narrowed it would become a large
        // negative value and the comparison would flip. Preserving uint semantics keeps it definite-true.
        const ulong big = (1UL << 63) + 1;
        var eval = Evaluator(Caveat("c", "n > 9223372036854775807u", ("n", "uint")));

        var result = eval.Evaluate("c", null, Ctx(("n", big)));

        Assert.Equal(CaveatOutcome.DefinitelyTrue, result.Outcome);
    }

    [Fact]
    public void NegativeValueForUintParameter_Throws_ParameterTypeMismatch()
    {
        var eval = Evaluator(Caveat("c", "n > 0u", ("n", "uint")));

        var ex = Assert.Throws<CaveatEvaluationException>(
            () => eval.Evaluate("c", null, Ctx(("n", -5L))));

        Assert.Equal(CaveatEvaluationErrorKind.ParameterTypeMismatch, ex.Kind);
    }

    [Fact]
    public void IntParameter_AcceptsNumericStringAndJsonNumber()
    {
        // JSON numbers arrive as double/long; a numeric string is also coerced, matching SpiceDB.
        var eval = Evaluator(Caveat("c", "age >= 18", ("age", "int")));

        Assert.Equal(CaveatOutcome.DefinitelyTrue, eval.Evaluate("c", null, Ctx(("age", 21.0))).Outcome);
        Assert.Equal(CaveatOutcome.DefinitelyTrue, eval.Evaluate("c", null, Ctx(("age", "21"))).Outcome);
    }
}
