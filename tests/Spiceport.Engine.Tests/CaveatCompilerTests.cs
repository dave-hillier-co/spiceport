using Spiceport.Core;
using Spiceport.Schema;

namespace Spiceport.Engine.Tests;

/// <summary>
/// Unit tests for <see cref="CaveatCompiler"/>: the static CEL check SpiceDB applies to a caveat body at
/// schema-write time (<c>pkg/caveats/compile.go</c> <c>CompileCaveatWithSource</c> over cel-go's parser and
/// checker). Every expected message is the one SpiceDB v1.49.2 produces for the same parameters and body
/// through <c>caveats.CompileCaveatWithName</c>.
/// </summary>
public class CaveatCompilerTests
{
    /// <summary>Compiles <paramref name="expression"/> as caveat <c>c</c> with the given parameter list.</summary>
    private static void Compile(string parameters, string expression) =>
        CaveatCompiler.Compile("c", expression, ParameterTypes(parameters));

    private static string Rejection(string parameters, string expression) =>
        Assert.Throws<CaveatCompileException>(() => Compile(parameters, expression)).Message;

    /// <summary>Parameter types as the schema compiler models them, e.g. <c>"x int, l list&lt;int&gt;"</c>.</summary>
    private static IReadOnlyDictionary<string, CaveatTypeReference> ParameterTypes(string parameters) =>
        SchemaCompiler.CompileStoredSchema($"caveat c({parameters}) {{ true }}").Caveats[0].ParameterTypes;

    // ---- accepted ----

    [Theory]
    [InlineData("x int", "x == 1")]
    [InlineData("b bool", "!!b")]
    [InlineData("i int", "--i == 1")]
    [InlineData("s string", "s.startsWith('a') || s.endsWith('b') && s.contains('c')")]
    [InlineData("s string", "s.matches('^a') && matches(s, 'a') && size(s) > s.size()")]
    [InlineData("u uint", "u == 1 && u > 1")]
    [InlineData("i int, u uint", "i == u")]
    [InlineData("d double", "d > 1.0")]
    [InlineData("by bytes", "by == b'abc' && size(by) == 3")]
    [InlineData("du duration, ts timestamp", "ts - ts > du && ts + du > ts && du > duration('1h')")]
    [InlineData("ts timestamp", "ts.getHours() == 1 && ts.getHours('UTC') == 1 && ts.getFullYear() > 2000")]
    [InlineData("l list<int>", "1 in l && l[0] == 1 && [1] + [2] == l")]
    [InlineData("m map<int>", "'a' in m && m['a'] == 1 && m.a == 1 && has(m.a)")]
    [InlineData("a any", "a == 1 && a.b == 1 && a[0] == 1 && a.size() > 0")]
    [InlineData("a any, b bool", "a && b")]
    [InlineData("ip ipaddress, cidr string", "ip.in_cidr(cidr) && ip.in_cidr('10.0.0.0/8') && ip == ip")]
    [InlineData("x map<any>", "x.isSubtreeOf(x) && {'a': 1}.isSubtreeOf(x)")]
    [InlineData("x map<map<string>>, y map<any>", "x.isSubtreeOf(y)")]
    [InlineData("l list<int>", "l.all(e, e > 0) && l.exists(e, e > 0) && l.exists_one(e, e > 0)")]
    [InlineData("l list<int>", "l.map(e, e * 2) == [2] && l.map(e, e > 0, e * 2)[0] == 2")]
    [InlineData("l list<int>", "l.filter(e, e > 0).size() == 1")]
    [InlineData("m map<int>", "m.all(k, k == 'a')")]
    [InlineData("a any", "a.all(e, e)")]
    [InlineData("ll list<list<int>>", "ll.all(x, x.all(y, y > 0))")]
    [InlineData("b bool", "(b ? 1 : 2) == 1")]
    [InlineData("l list<int>", "[1, 'a'][0] == 1 && [] == l && {'a': 1, 'b': 'c'}.a == 1")]
    [InlineData("i int", "type(i) == int && dyn(i) == 1 && int('1') == i && string(i) == '1'")]
    [InlineData("a any, ts timestamp, ip ipaddress", "null == a && ts == null && ip == null")]
    [InlineData("m map<int>", "m.?a.hasValue() && m[?'a'].orValue(1) == 1 && optional.of(1).value() == 1")]
    [InlineData("i int", "optional.of(i).optMap(x, x + 1).value() == 2")]
    [InlineData("i int", "google.protobuf.Int64Value{value: 1} == i")]
    [InlineData("b bool", "b // a comment\n && true")]
    public void AcceptsWellTypedBooleanExpressions(string parameters, string expression) =>
        Compile(parameters, expression);

    [Theory]
    // The conformance corpus's caveats (TestData/*.yaml), each compiled against its declared parameters.
    [InlineData("somemap map<any>", "somemap.foo == 42 && somemap.bar < 56")]
    [InlineData("somelist list<int>", "somelist.all(i, i >= 42)")]
    [InlineData("user_ip ipaddress, cidr string", "user_ip.in_cidr(cidr)")]
    [InlineData("user_ip ipaddress, cidr string", "!user_ip.in_cidr(cidr)")]
    [InlineData("current_time timestamp, expiration timestamp", "current_time < expiration")]
    [InlineData("limit uint, count uint", "count < limit")]
    [InlineData("admin_teams list<string>, self string", "self in admin_teams")]
    [InlineData("actual string, required string", "actual == required")]
    [InlineData("day_of_week string", "day_of_week == 'tuesday'")]
    [InlineData("somesecret int, somecondition int", "somecondition == somesecret")]
    [InlineData("memberparam int", "memberparam >= 42")]
    [InlineData("thirdcondition bool", "thirdcondition")]
    public void AcceptsTheConformanceCorpusCaveats(string parameters, string expression) =>
        Compile(parameters, expression);

    [Fact]
    public void ComparesMixedNumericTypesOnceAComprehensionHasBeenChecked()
    {
        // cel-go disables the int/double ordering overloads, but its comprehension scopes are built
        // without that filter, and the environment restored afterwards is too: so the mixed comparison
        // type-checks after a macro, and only after one.
        Compile("i int, l list<int>", "l.all(e, true) && i < 1.0");

        Assert.Equal(
            "ERROR: c:1:3: found no matching overload for '_<_' applied to '(int, double)'\n"
            + " | i < 1.0 && l.all(e, true)\n | ..^",
            Rejection("i int, l list<int>", "i < 1.0 && l.all(e, true)"));
    }

    // ---- rejected: type errors ----

    [Fact]
    public void RejectsANonBooleanOperandOfALogicalOperator()
    {
        // The issue's example: accepted before because the boolean-output check was dynamic.
        Assert.Equal(
            "ERROR: c:1:11: expected type 'bool' but found 'int'\n | x == 1 || y\n | ..........^",
            Rejection("x int, y int", "x == 1 || y"));
    }

    [Fact]
    public void ReportsEveryOperandTypeErrorInSourceOrder()
    {
        Assert.Equal(
            "ERROR: c:1:6: expected type 'bool' but found 'int'\n | b || i || s || d\n | .....^\n"
            + "ERROR: c:1:11: expected type 'bool' but found 'string'\n | b || i || s || d\n | ..........^\n"
            + "ERROR: c:1:16: expected type 'bool' but found 'double'\n | b || i || s || d\n | ...............^",
            Rejection("b bool, i int, s string, d double", "b || i || s || d"));
    }

    [Theory]
    [InlineData("x int", "x == \"a\"",
        "ERROR: c:1:3: found no matching overload for '_==_' applied to '(int, string)'\n | x == \"a\"\n | ..^")]
    [InlineData("i int, d double", "i < d",
        "ERROR: c:1:3: found no matching overload for '_<_' applied to '(int, double)'\n | i < d\n | ..^")]
    [InlineData("x uint", "x == 1u",
        "ERROR: c:1:3: found no matching overload for '_==_' applied to '(int, uint)'\n | x == 1u\n | ..^")]
    [InlineData("m map<int>", "1 in m",
        "ERROR: c:1:3: found no matching overload for '@in' applied to '(int, map(string, int))'\n | 1 in m\n | ..^")]
    [InlineData("user_ip ipaddress, allowed_ips list<string>", "user_ip in allowed_ips",
        "ERROR: c:1:9: found no matching overload for '@in' applied to '(IPAddress, list(string))'\n | user_ip in allowed_ips\n | ........^")]
    [InlineData("ip ipaddress", "ip.in_cidr(1)",
        "ERROR: c:1:11: found no matching overload for 'in_cidr' applied to 'IPAddress.(int)'\n | ip.in_cidr(1)\n | ..........^")]
    [InlineData("l list<int>", "l.filter(e, e)",
        "ERROR: c:1:9: found no matching overload for '_?_:_' applied to '(int, list(int), list(int))'\n | l.filter(e, e)\n | ........^")]
    public void RejectsMismatchedOperandTypes(string parameters, string expression, string expected) =>
        Assert.Equal(expected, Rejection(parameters, expression));

    [Theory]
    [InlineData("x int", "x == 1 && y",
        "ERROR: c:1:11: undeclared reference to 'y' (in container '')\n | x == 1 && y\n | ..........^")]
    [InlineData("x int", "x.foo()",
        "ERROR: c:1:6: undeclared reference to 'foo' (in container '')\n | x.foo()\n | .....^")]
    [InlineData("ip ipaddress", "ipaddress(\"1.2.3.4\") == ip",
        "ERROR: c:1:10: undeclared reference to 'ipaddress' (in container '')\n | ipaddress(\"1.2.3.4\") == ip\n | .........^")]
    public void RejectsUndeclaredIdentifiersAndFunctions(string parameters, string expression, string expected) =>
        Assert.Equal(expected, Rejection(parameters, expression));

    [Theory]
    [InlineData("i int", "has(i.a)",
        "ERROR: c:1:4: type 'int' does not support field selection\n | has(i.a)\n | ...^")]
    [InlineData("ip ipaddress", "ip.foo == 1",
        "ERROR: c:1:3: unexpected failed resolution of 'IPAddress'\n | ip.foo == 1\n | ..^")]
    [InlineData("l list<int>", "l.all(e, e)",
        "ERROR: c:1:10: expected type 'bool' but found 'int'\n | l.all(e, e)\n | .........^")]
    public void RejectsIllTypedSelectionsAndMacroBodies(string parameters, string expression, string expected) =>
        Assert.Equal(expected, Rejection(parameters, expression));

    // ---- rejected: non-boolean output ----

    [Theory]
    [InlineData("x int", "x", "int")]
    [InlineData("x uint", "x", "int")]
    [InlineData("x string", "x", "string")]
    [InlineData("x double", "x", "double")]
    [InlineData("x bytes", "x", "bytes")]
    [InlineData("x duration", "x", "google.protobuf.Duration")]
    [InlineData("x timestamp", "x", "google.protobuf.Timestamp")]
    [InlineData("x list<int>", "x", "list(int)")]
    [InlineData("x map<int>", "x", "map(string, int)")]
    [InlineData("x ipaddress", "x", "IPAddress")]
    [InlineData("x int", "type(x)", "type(int)")]
    [InlineData("x int", "optional.of(x)", "optional_type(int)")]
    [InlineData("x int", "x == 1 ? \"a\" : \"b\"", "string")]
    [InlineData("l list<int>", "l.map(e, e * 2)", "list(int)")]
    public void RejectsANonBooleanResult(string parameters, string expression, string found) =>
        Assert.Equal(
            $"caveat expression must result in a boolean value: found `{found}`",
            Rejection(parameters, expression));

    [Theory]
    // SpiceDB requires the output type to be exactly bool: dyn is rejected, even where the value may
    // turn out to be a boolean at evaluation time.
    [InlineData("x any", "x")]
    [InlineData("x map<any>", "x.foo")]
    [InlineData("x int", "dyn(x)")]
    [InlineData("b bool, a any", "b ? a : true")]
    public void RejectsADynamicResult(string parameters, string expression) =>
        Assert.Equal(
            "caveat expression must result in a boolean value: found `dyn`",
            Rejection(parameters, expression));

    // ---- rejected: parse errors ----

    [Theory]
    [InlineData("x int", "x > > 0",
        "ERROR: c:1:5: Syntax error: extraneous input '>' expecting {'[', '{', '(', '.', '-', '!', 'true', 'false', 'null', NUM_FLOAT, NUM_INT, NUM_UINT, STRING, BYTES, IDENTIFIER}\n | x > > 0\n | ....^")]
    [InlineData("b bool", "b && while",
        "ERROR: c:1:6: reserved identifier: while\n | b && while\n | .....^")]
    [InlineData("s string", "has(s)",
        "ERROR: c:1:5: invalid argument to has() macro\n | has(s)\n | ....^")]
    [InlineData("s string", "s == \"a\\q\"",
        "ERROR: c:1:6: Syntax error: token recognition error at: '\"a\\q'\n | s == \"a\\q\"\n | .....^\n"
        + "ERROR: c:1:10: Syntax error: token recognition error at: '\"'\n | s == \"a\\q\"\n | .........^\n"
        + "ERROR: c:1:11: Syntax error: mismatched input '<EOF>' expecting {'[', '{', '(', '.', '-', '!', 'true', 'false', 'null', NUM_FLOAT, NUM_INT, NUM_UINT, STRING, BYTES, IDENTIFIER}\n | s == \"a\\q\"\n | ..........^")]
    public void RejectsWhatCelGoDoesNotParse(string parameters, string expression, string expected) =>
        Assert.Equal(expected, Rejection(parameters, expression));

    // ---- the environment ----

    [Fact]
    public void RejectsAParameterRedeclaringAStandardTypeIdentifier() =>
        Assert.Equal(
            "ERROR: c:-1:0: overlapping identifier for name 'int'",
            Rejection("int int", "int == 1"));

    [Theory]
    [InlineData("foo", "unknown type `foo`")]
    [InlineData("list", "type `list` requires 1 generic types; found 0")]
    [InlineData("map<string, int>", "type `map` requires 1 generic types; found 2")]
    [InlineData("list<foo>", "unknown type `foo`")]
    public void RejectsAnUnknownParameterType(string type, string expected) =>
        Assert.Equal(
            expected,
            Assert.Throws<CaveatCompileException>(() => CaveatParameterTypes.Validate(ParameterTypes($"x {type}")["x"])).Message);

    [Fact]
    public void RelocatesErrorsToTheExpressionsPositionInTheSchema()
    {
        // As SpiceDB: a location on the expression's first line shifts by the start column, every line
        // by the start line; the snippet is still the expression's own line.
        var ex = Assert.Throws<CaveatCompileException>(() =>
            CaveatCompiler.Compile("c", "x == 1 || y", ParameterTypes("x int, y int"), startLine: 0, startColumn: 25));
        Assert.Equal("ERROR: c:1:36: expected type 'bool' but found 'int'\n | x == 1 || y\n | ...........^", ex.Message);

        ex = Assert.Throws<CaveatCompileException>(() =>
            CaveatCompiler.Compile("c", "b &&\n  i", ParameterTypes("b bool, i int"), startLine: 3, startColumn: 4));
        Assert.Equal("ERROR: c:5:3: expected type 'bool' but found 'int'", ex.Message);
    }
}
