using System.Globalization;
using System.Numerics;
using System.Text.RegularExpressions;
using Antlr4.Runtime;
using Cel.Internal;

namespace Spiceport.Engine;

/// <summary>
/// Parses a CEL expression into the macro-expanded <see cref="CelExpr"/> tree the static checker walks,
/// reporting the errors cel-go's parser reports. The grammar, lexer and parser are the <c>Cel</c>
/// package's ANTLR ones (the same <c>CEL.g4</c> cel-go generates its parser from); this class is a
/// transliteration of cel-go's <c>parser/parser.go</c> visitor and <c>parser/macro.go</c> expansions
/// (<c>authzed/cel-go</c> v0.20.2), with the options SpiceDB's caveat environment sets: the standard
/// macros, optional syntax, and the optional library's <c>optMap</c> macro.
/// </summary>
/// <remarks>
/// The parser's own error listeners are replaced, as cel-go replaces them: the <c>Cel</c> package's
/// <c>CelEnvironment.Parse</c> silently skips a lexer error (an unterminated or badly escaped string),
/// where cel-go reports it as a syntax error.
/// Not reproduced: cel-go's recursion-depth (250) and error-recovery (256) limits, which only a
/// pathological expression reaches.
/// </remarks>
internal sealed class CelAstBuilder
{
    private const string AccumulatorName = "__result__";

    private static readonly HashSet<string> ReservedIds =
    [
        "as", "break", "const", "continue", "else", "false", "for", "function", "if", "import", "in", "let",
        "loop", "package", "namespace", "null", "return", "true", "var", "void", "while",
    ];

    private static readonly Regex ReservedIdentifier = new("no viable alternative at input '.(true|false|null)'");

    private readonly CelIssues _errors;

    private CelAstBuilder(CelIssues errors)
    {
        _errors = errors;
    }

    /// <summary>
    /// Parses <paramref name="expression"/>, reporting syntax and macro errors to <paramref name="errors"/>.
    /// The returned tree is only meaningful when no error was reported.
    /// </summary>
    public static CelExpr Parse(string expression, CelIssues errors)
    {
        var builder = new CelAstBuilder(errors);
        var listener = new SyntaxErrorListener(errors);

        var lexer = new GoDisplayLexer(CharStreams.fromString(expression));
        lexer.RemoveErrorListeners();
        lexer.AddErrorListener(listener);

        var parser = new CelParser(new CommonTokenStream(lexer));
        parser.RemoveErrorListeners();
        parser.AddErrorListener(listener);

        return builder.Visit(parser.start());
    }

    private CelExpr Visit(ParserRuleContext? tree)
    {
        switch (tree)
        {
            case CelParser.StartContext start:
                return Visit(start.e);

            case CelParser.ExprContext expr:
                if (expr.op is null)
                    return Visit(expr.e);
                {
                    var result = Visit(expr.e);
                    var location = Location(expr.op);
                    var ifTrue = Visit(expr.e1);
                    var ifFalse = Visit(expr.e2);
                    return GlobalCallOrMacro(location, "_?_:_", result, ifTrue, ifFalse);
                }

            case CelParser.ConditionalOrContext or:
                return VisitLogical(or, or.e, or._ops, or._e1.Cast<ParserRuleContext>().ToList(), "_||_", "'||'");

            case CelParser.ConditionalAndContext and:
                return VisitLogical(and, and.e, and._ops, and._e1.Cast<ParserRuleContext>().ToList(), "_&&_", "'&&'");

            case CelParser.RelationContext relation:
                if (relation.op is null)
                    return Visit(relation.calc());
                return VisitBinary(relation, relation.op, relation.relation(0), relation.relation(1));

            case CelParser.CalcContext calc:
                if (calc.op is null)
                    return Visit(calc.unary());
                return VisitBinary(calc, calc.op, calc.calc(0), calc.calc(1));

            case CelParser.MemberExprContext memberExpr:
                return Visit(memberExpr.member());

            case CelParser.LogicalNotContext not:
                if (not._ops.Count % 2 == 0)
                    return Visit(not.member());
                return GlobalCallOrMacro(Location(not._ops[0]), "!_", Visit(not.member()));

            case CelParser.NegateContext negate:
                if (negate._ops.Count % 2 == 0)
                    return Visit(negate.member());
                return GlobalCallOrMacro(Location(negate._ops[0]), "-_", Visit(negate.member()));

            case CelParser.UnaryContext unary:
                return new CelLiteral(Location(unary), CelLiteralKind.String, "<<error>>");

            case CelParser.PrimaryExprContext primaryExpr:
                return Visit(primaryExpr.primary());

            case CelParser.SelectContext select:
                return VisitSelect(select);

            case CelParser.MemberCallContext memberCall:
                return VisitMemberCall(memberCall);

            case CelParser.IndexContext index:
                return VisitIndex(index);

            case CelParser.IdentOrGlobalCallContext identOrCall:
                return VisitIdentOrGlobalCall(identOrCall);

            case CelParser.NestedContext nested:
                return Visit(nested.e);

            case CelParser.CreateListContext createList:
                return VisitCreateList(createList);

            case CelParser.CreateStructContext createStruct:
                return new CelMap(Location(createStruct.op), VisitMapInitializerList(createStruct.entries));

            case CelParser.CreateMessageContext createMessage:
                return VisitCreateMessage(createMessage);

            case CelParser.ConstantLiteralContext constant:
                return Visit(constant.literal());

            case CelParser.IntContext intLiteral:
                return VisitInt(intLiteral);

            case CelParser.UintContext uintLiteral:
                return VisitUint(uintLiteral);

            case CelParser.DoubleContext doubleLiteral:
                return VisitDouble(doubleLiteral);

            case CelParser.StringContext stringLiteral:
                Unquote(stringLiteral, stringLiteral.GetText(), isBytes: false);
                return new CelLiteral(Location(stringLiteral), CelLiteralKind.String, stringLiteral.GetText());

            case CelParser.BytesContext bytesLiteral:
                Unquote(bytesLiteral, bytesLiteral.tok.Text[1..], isBytes: true);
                return new CelLiteral(Location(bytesLiteral), CelLiteralKind.Bytes);

            case CelParser.BoolTrueContext boolTrue:
                return new CelLiteral(Location(boolTrue), CelLiteralKind.Bool);

            case CelParser.BoolFalseContext boolFalse:
                return new CelLiteral(Location(boolFalse), CelLiteralKind.Bool);

            case CelParser.NullContext nullLiteral:
                return new CelLiteral(Location(nullLiteral), CelLiteralKind.Null);
        }

        // Typically only reached once the parser has already reported a syntax error.
        if (_errors.Errors.Count == 0)
        {
            var text = tree is null ? "<<nil>>" : $"<<{tree.GetType().Name}>>";
            return ReportError(CelLocation.None, $"unknown parse element encountered: {text}");
        }

        return new CelUnspecified(CelLocation.None);
    }

    private CelExpr VisitLogical(
        ParserRuleContext ctx, ParserRuleContext? first, IList<IToken> ops, IReadOnlyList<ParserRuleContext> rest,
        string function, string wanted)
    {
        var result = Visit(first);
        if (ops.Count == 0)
            return result;

        var terms = new List<CelExpr> { result };
        var opLocations = new List<CelLocation>();
        for (var i = 0; i < ops.Count; i++)
        {
            if (i >= rest.Count)
                return ReportError(Location(ctx), $"unexpected character, wanted {wanted}");
            terms.Add(Visit(rest[i]));
            opLocations.Add(Location(ops[i]));
        }

        return BalancedTree(function, terms, opLocations, 0, opLocations.Count - 1);
    }

    /// <summary>cel-go's balancing logic manager: <c>a || b || c || d</c> becomes a balanced binary tree.</summary>
    private static CelExpr BalancedTree(
        string function, List<CelExpr> terms, List<CelLocation> ops, int lo, int hi)
    {
        var mid = (lo + hi + 1) / 2;
        var left = mid == lo ? terms[mid] : BalancedTree(function, terms, ops, lo, mid - 1);
        var right = mid == hi ? terms[mid + 1] : BalancedTree(function, terms, ops, mid + 1, hi);
        return new CelCall(ops[mid], function, null, [left, right]);
    }

    private CelExpr VisitBinary(ParserRuleContext ctx, IToken op, ParserRuleContext? lhsCtx, ParserRuleContext? rhsCtx)
    {
        var function = op.Text switch
        {
            "<" => "_<_",
            "<=" => "_<=_",
            ">" => "_>_",
            ">=" => "_>=_",
            "==" => "_==_",
            "!=" => "_!=_",
            "in" => "@in",
            "+" => "_+_",
            "-" => "_-_",
            "*" => "_*_",
            "/" => "_/_",
            "%" => "_%_",
            _ => null,
        };
        if (function is null)
            return ReportError(Location(ctx), "operator not found");

        var lhs = Visit(lhsCtx);
        var location = Location(op);
        var rhs = Visit(rhsCtx);
        return GlobalCallOrMacro(location, function, lhs, rhs);
    }

    private CelExpr VisitSelect(CelParser.SelectContext ctx)
    {
        var operand = Visit(ctx.member());
        if (ctx.id is null || ctx.op is null)
            return new CelUnspecified(Location(ctx));

        var id = ctx.id.Text;
        if (ctx.opt is not null)
        {
            return new CelCall(
                Location(ctx.op), "_?._", null,
                [operand, new CelLiteral(Location(ctx.id), CelLiteralKind.String, id)]);
        }

        return new CelSelect(Location(ctx.op), operand, id, testOnly: false);
    }

    private CelExpr VisitMemberCall(CelParser.MemberCallContext ctx)
    {
        var operand = Visit(ctx.member());
        if (ctx.id is null)
            return new CelUnspecified(Location(ctx));

        var location = Location(ctx.open);
        return ReceiverCallOrMacro(location, ctx.id.Text, operand, VisitExprList(ctx.args));
    }

    private CelExpr VisitIndex(CelParser.IndexContext ctx)
    {
        var target = Visit(ctx.member());
        if (ctx.op is null)
            return new CelUnspecified(Location(ctx));

        var location = Location(ctx.op);
        var index = Visit(ctx.index);
        var function = ctx.opt is not null ? "_[?_]" : "_[_]";
        return GlobalCallOrMacro(location, function, target, index);
    }

    private CelExpr VisitIdentOrGlobalCall(CelParser.IdentOrGlobalCallContext ctx)
    {
        var identName = ctx.leadingDot is not null ? "." : string.Empty;
        if (ctx.id is null)
            return new CelUnspecified(Location(ctx));

        var id = ctx.id.Text;
        if (ReservedIds.Contains(id))
            return ReportError(Location(ctx), $"reserved identifier: {id}");

        identName += id;
        if (ctx.op is not null)
        {
            var location = Location(ctx.op);
            return GlobalCallOrMacro(location, identName, VisitExprList(ctx.args));
        }

        return new CelIdent(Location(ctx.id), identName);
    }

    private CelExpr VisitCreateList(CelParser.CreateListContext ctx)
    {
        var location = Location(ctx.op);
        var elements = new List<CelExpr>();
        var optionals = new HashSet<int>();
        if (ctx.elems is not null)
        {
            var elems = ctx.elems._elems;
            for (var i = 0; i < elems.Count; i++)
            {
                elements.Add(Visit(elems[i].e));
                if (elems[i].opt is not null)
                    optionals.Add(i);
            }
        }

        return new CelList(location, elements, optionals);
    }

    private List<CelMapEntry> VisitMapInitializerList(CelParser.MapInitializerListContext? ctx)
    {
        var result = new List<CelMapEntry>();
        if (ctx is null || ctx._keys is null)
            return result;

        for (var i = 0; i < ctx._cols.Count; i++)
        {
            if (i >= ctx._keys.Count || i >= ctx._values.Count)
                return [];
            var optKey = ctx._keys[i];
            var key = Visit(optKey.e);
            var value = Visit(ctx._values[i]);
            result.Add(new CelMapEntry(key, value, optKey.opt is not null));
        }

        return result;
    }

    private CelExpr VisitCreateMessage(CelParser.CreateMessageContext ctx)
    {
        var messageName = string.Join(".", ctx._ids.Select(id => id.Text));
        if (ctx.leadingDot is not null)
            messageName = "." + messageName;

        var location = Location(ctx.op);
        var fields = new List<CelStructField>();
        var entries = ctx.entries;
        if (entries is not null && entries._fields is not null)
        {
            for (var i = 0; i < entries._fields.Count; i++)
            {
                if (i >= entries._cols.Count || i >= entries._values.Count)
                {
                    fields = [];
                    break;
                }

                var optField = entries._fields[i];
                if (optField.IDENTIFIER() is not { } id)
                {
                    fields = [];
                    break;
                }

                var initLocation = Location(entries._cols[i]);
                var value = Visit(entries._values[i]);
                fields.Add(new CelStructField(initLocation, id.GetText(), value, optField.opt is not null));
            }
        }

        return new CelStruct(location, messageName, fields);
    }

    private CelExpr VisitInt(CelParser.IntContext ctx)
    {
        var text = ctx.tok.Text;
        var hex = text.StartsWith("0x", StringComparison.Ordinal);
        var digits = hex ? text[2..] : text;
        var negative = ctx.sign is not null;
        if (!TryParseMagnitude(digits, hex, out var magnitude)
            || (negative ? -magnitude < long.MinValue : magnitude > long.MaxValue))
        {
            return ReportError(Location(ctx), "invalid int literal");
        }

        return new CelLiteral(Location(ctx), CelLiteralKind.Int);
    }

    private CelExpr VisitUint(CelParser.UintContext ctx)
    {
        var text = ctx.tok.Text[..^1];
        var hex = text.StartsWith("0x", StringComparison.Ordinal);
        var digits = hex ? text[2..] : text;
        if (!TryParseMagnitude(digits, hex, out var magnitude) || magnitude > ulong.MaxValue)
            return ReportError(Location(ctx), "invalid uint literal");

        return new CelLiteral(Location(ctx), CelLiteralKind.Uint);
    }

    private CelExpr VisitDouble(CelParser.DoubleContext ctx)
    {
        var text = (ctx.sign?.Text ?? string.Empty) + ctx.tok.Text;
        if (!double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var value)
            || double.IsInfinity(value))
        {
            return ReportError(Location(ctx), "invalid double literal");
        }

        return new CelLiteral(Location(ctx), CelLiteralKind.Double);
    }

    private static bool TryParseMagnitude(string digits, bool hex, out BigInteger magnitude)
    {
        magnitude = BigInteger.Zero;
        if (digits.Length == 0)
            return false;
        foreach (var c in digits)
        {
            var digit = hex ? HexValue(c) : c is >= '0' and <= '9' ? c - '0' : -1;
            if (digit < 0)
                return false;
            magnitude = magnitude * (hex ? 16 : 10) + digit;
        }

        return true;
    }

    private List<CelExpr> VisitExprList(CelParser.ExprListContext? ctx) =>
        ctx is null ? [] : ctx._e.Select(e => Visit(e)).ToList();

    private void Unquote(ParserRuleContext ctx, string value, bool isBytes)
    {
        if (CelUnescape.Unescape(value, isBytes) is { } error)
            ReportError(Location(ctx), error);
    }

    // ---- macros (parser/macro.go, cel/library.go optMap) ----

    private CelExpr GlobalCallOrMacro(CelLocation location, string function, params CelExpr[] args) =>
        GlobalCallOrMacro(location, function, (IReadOnlyList<CelExpr>)args);

    private CelExpr GlobalCallOrMacro(CelLocation location, string function, IReadOnlyList<CelExpr> args)
    {
        if (function == "has" && args.Count == 1)
        {
            if (args[0] is CelSelect select)
                return new CelSelect(location, select.Operand, select.Field, testOnly: true);
            return ReportError(args[0].Location, "invalid argument to has() macro");
        }

        return new CelCall(location, function, null, args);
    }

    private CelExpr ReceiverCallOrMacro(CelLocation location, string function, CelExpr target, IReadOnlyList<CelExpr> args)
    {
        switch (function, args.Count)
        {
            case ("all", 2):
                return Quantifier(location, Quantifier_.All, target, args);
            case ("exists", 2):
                return Quantifier(location, Quantifier_.Exists, target, args);
            case ("exists_one", 2):
                return Quantifier(location, Quantifier_.ExistsOne, target, args);
            case ("map", 2):
            case ("map", 3):
                return MakeMap(location, target, args);
            case ("filter", 2):
                return MakeFilter(location, target, args);
            case ("optMap", 2):
                return OptMap(location, target, args);
            default:
                return new CelCall(location, function, target, args);
        }
    }

    private enum Quantifier_
    {
        All,
        Exists,
        ExistsOne,
    }

    private CelExpr Quantifier(CelLocation at, Quantifier_ kind, CelExpr target, IReadOnlyList<CelExpr> args)
    {
        if (args[0] is not CelIdent v)
            return ReportError(args[0].Location, "argument must be a simple name");

        CelExpr Accu() => new CelIdent(at, AccumulatorName);
        CelExpr init, condition, step, result;
        switch (kind)
        {
            case Quantifier_.All:
                init = new CelLiteral(at, CelLiteralKind.Bool);
                condition = new CelCall(at, "@not_strictly_false", null, [Accu()]);
                step = new CelCall(at, "_&&_", null, [Accu(), args[1]]);
                result = Accu();
                break;
            case Quantifier_.Exists:
                init = new CelLiteral(at, CelLiteralKind.Bool);
                condition = new CelCall(at, "@not_strictly_false", null, [new CelCall(at, "!_", null, [Accu()])]);
                step = new CelCall(at, "_||_", null, [Accu(), args[1]]);
                result = Accu();
                break;
            default:
                init = new CelLiteral(at, CelLiteralKind.Int);
                condition = new CelLiteral(at, CelLiteralKind.Bool);
                step = new CelCall(at, "_?_:_", null,
                [
                    args[1],
                    new CelCall(at, "_+_", null, [Accu(), new CelLiteral(at, CelLiteralKind.Int)]),
                    Accu(),
                ]);
                result = new CelCall(at, "_==_", null, [Accu(), new CelLiteral(at, CelLiteralKind.Int)]);
                break;
        }

        return new CelComprehension(at, target, v.Name, AccumulatorName, init, condition, step, result);
    }

    private CelExpr MakeMap(CelLocation at, CelExpr target, IReadOnlyList<CelExpr> args)
    {
        if (args[0] is not CelIdent v)
            return ReportError(args[0].Location, "argument is not an identifier");

        var filter = args.Count == 3 ? args[1] : null;
        var fn = args.Count == 3 ? args[2] : args[1];

        CelExpr step = new CelCall(at, "_+_", null,
            [new CelIdent(at, AccumulatorName), new CelList(at, [fn], new HashSet<int>())]);
        if (filter is not null)
            step = new CelCall(at, "_?_:_", null, [filter, step, new CelIdent(at, AccumulatorName)]);

        return new CelComprehension(
            at, target, v.Name, AccumulatorName,
            new CelList(at, [], new HashSet<int>()),
            new CelLiteral(at, CelLiteralKind.Bool),
            step,
            new CelIdent(at, AccumulatorName));
    }

    private CelExpr MakeFilter(CelLocation at, CelExpr target, IReadOnlyList<CelExpr> args)
    {
        if (args[0] is not CelIdent v)
            return ReportError(args[0].Location, "argument is not an identifier");

        CelExpr step = new CelCall(at, "_+_", null,
            [new CelIdent(at, AccumulatorName), new CelList(at, [args[0]], new HashSet<int>())]);
        step = new CelCall(at, "_?_:_", null, [args[1], step, new CelIdent(at, AccumulatorName)]);

        return new CelComprehension(
            at, target, v.Name, AccumulatorName,
            new CelList(at, [], new HashSet<int>()),
            new CelLiteral(at, CelLiteralKind.Bool),
            step,
            new CelIdent(at, AccumulatorName));
    }

    private CelExpr OptMap(CelLocation at, CelExpr target, IReadOnlyList<CelExpr> args)
    {
        if (args[0] is not CelIdent v)
            return ReportError(args[0].Location, "optMap() variable name must be a simple identifier");

        return new CelCall(at, "_?_:_", null,
        [
            new CelCall(at, "hasValue", target, []),
            new CelCall(at, "optional.of", null,
            [
                new CelComprehension(
                    at,
                    new CelList(at, [], new HashSet<int>()),
                    "#unused",
                    v.Name,
                    new CelCall(at, "value", target, []),
                    new CelLiteral(at, CelLiteralKind.Bool),
                    new CelIdent(at, v.Name),
                    args[1]),
            ]),
            new CelCall(at, "optional.none", null, []),
        ]);
    }

    // ---- locations and errors ----

    private CelUnspecified ReportError(CelLocation location, string message)
    {
        _errors.Report(location, message);
        return new CelUnspecified(location);
    }

    private static CelLocation Location(IToken token) => new(token.Line, token.Column);

    private static CelLocation Location(ParserRuleContext ctx) =>
        ctx.Start is { } start ? Location(start) : CelLocation.None;

    private static int HexValue(char c) =>
        c switch
        {
            >= '0' and <= '9' => c - '0',
            >= 'a' and <= 'f' => c - 'a' + 10,
            >= 'A' and <= 'F' => c - 'A' + 10,
            _ => -1,
        };

    /// <summary>
    /// The <c>Cel</c> package's lexer, reporting an unrecognized token's text verbatim as the Go ANTLR
    /// runtime does (the C# runtime escapes newlines and tabs in it).
    /// </summary>
    private sealed class GoDisplayLexer(ICharStream input) : CelLexer(input)
    {
        public override string GetErrorDisplay(string s) => s;
    }

    /// <summary>cel-go's parser-as-error-listener: every lexer and parser error is a syntax error.</summary>
    private sealed class SyntaxErrorListener(CelIssues errors) : IAntlrErrorListener<int>, IAntlrErrorListener<IToken>
    {
        public void SyntaxError(
            TextWriter output, IRecognizer recognizer, int offendingSymbol, int line, int charPositionInLine,
            string msg, RecognitionException e) => Report(line, charPositionInLine, msg);

        public void SyntaxError(
            TextWriter output, IRecognizer recognizer, IToken offendingSymbol, int line, int charPositionInLine,
            string msg, RecognitionException e) => Report(line, charPositionInLine, msg);

        private void Report(int line, int column, string msg)
        {
            if (msg.Contains("no viable alternative", StringComparison.Ordinal))
                msg = ReservedIdentifier.Replace(msg, "mismatched input '$1' expecting IDENTIFIER");
            errors.Report(new CelLocation(line, column), $"Syntax error: {msg}");
        }
    }
}
