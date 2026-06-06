using System.Collections.Immutable;
using Spiceport.Core;

namespace Spiceport.Schema;

/// <summary>
/// Recursive-descent parser turning a token stream into a <see cref="SchemaFileNode"/> AST.
/// Supports definitions, relations (with type refs incl. <c>#subrelation</c> and <c>:*</c>),
/// and permissions with <c>+ &amp; -</c> and <c>-&gt;</c> arrow operators plus <c>nil</c>.
/// Caveat blocks are consumed but not deeply modelled.
/// </summary>
internal sealed class Parser
{
    private readonly List<Token> _tokens;
    private readonly string _source;
    private int _pos;

    private Parser(List<Token> tokens, string source)
    {
        _tokens = tokens;
        _source = source;
    }

    public static SchemaFileNode Parse(string input)
    {
        var tokens = new Lexer(input).Tokenize();
        return new Parser(tokens, input).ParseFile();
    }

    private Token Current => _tokens[_pos];

    private Token Advance() => _tokens[_pos++];

    private bool Is(TokenType type) => Current.Type == type;

    private bool IsKeyword(string keyword) => Current.Type == TokenType.Keyword && Current.Text == keyword;

    private Token Expect(TokenType type, string what)
    {
        if (!Is(type))
        {
            throw Error($"expected {what}, found '{TokenText()}'");
        }

        return Advance();
    }

    private string TokenText() => Current.Type == TokenType.Eof ? "<eof>" : Current.Text;

    private SchemaCompileException Error(string message) => new(message, Current.Line, Current.Column);

    private SchemaFileNode ParseFile()
    {
        var definitions = ImmutableList.CreateBuilder<DefinitionNode>();
        var caveats = ImmutableList.CreateBuilder<CaveatNode>();

        while (!Is(TokenType.Eof))
        {
            if (Is(TokenType.Semicolon))
            {
                Advance();
                continue;
            }

            if (IsKeyword("use"))
            {
                // Feature-flag import, e.g. `use expiration`. Parsed and ignored: the features
                // it gates (expiration, etc.) are always available in this engine.
                Advance();
                ParseTypePath();
            }
            else if (IsKeyword("definition"))
            {
                definitions.Add(ParseDefinition());
            }
            else if (IsKeyword("caveat"))
            {
                caveats.Add(ParseCaveat());
            }
            else
            {
                throw Error($"expected 'definition' or 'caveat', found '{TokenText()}'");
            }
        }

        return new SchemaFileNode(definitions.ToImmutable(), caveats.ToImmutable());
    }

    private DefinitionNode ParseDefinition()
    {
        Advance(); // 'definition'
        string name = ParseTypePath();
        Expect(TokenType.LeftBrace, "'{'");

        var relations = ImmutableList.CreateBuilder<RelationNode>();
        var permissions = ImmutableList.CreateBuilder<PermissionNode>();

        while (!Is(TokenType.RightBrace) && !Is(TokenType.Eof))
        {
            if (Is(TokenType.Semicolon))
            {
                Advance();
                continue;
            }

            if (IsKeyword("relation"))
            {
                relations.Add(ParseRelation());
            }
            else if (IsKeyword("permission"))
            {
                permissions.Add(ParsePermission());
            }
            else
            {
                throw Error($"expected 'relation' or 'permission', found '{TokenText()}'");
            }
        }

        Expect(TokenType.RightBrace, "'}'");
        return new DefinitionNode(name, relations.ToImmutable(), permissions.ToImmutable());
    }

    private RelationNode ParseRelation()
    {
        Advance(); // 'relation'
        string name = Expect(TokenType.Identifier, "relation name").Text;
        Expect(TokenType.Colon, "':'");

        var types = ImmutableList.CreateBuilder<TypeRefNode>();
        types.Add(ParseTypeRef());
        while (Is(TokenType.Pipe))
        {
            Advance();
            types.Add(ParseTypeRef());
        }

        ConsumeOptionalTerminator();
        return new RelationNode(name, types.ToImmutable());
    }

    private TypeRefNode ParseTypeRef()
    {
        string typeName = ParseTypePath();
        bool isWildcard = false;
        string? subrelation = null;

        if (Is(TokenType.Colon))
        {
            Advance();
            Expect(TokenType.Star, "'*'");
            isWildcard = true;
        }
        else if (Is(TokenType.Hash))
        {
            Advance();
            if (Is(TokenType.Ellipsis))
            {
                Advance();
                subrelation = CoreConstants.Ellipsis;
            }
            else
            {
                subrelation = Expect(TokenType.Identifier, "subrelation name").Text;
            }
        }

        string? caveatName = null;
        bool requiresExpiration = false;

        // 'with' is not a reserved keyword; it is parsed as an identifier.
        while (Current.Type == TokenType.Identifier && Current.Text is "with" or "and")
        {
            Advance(); // 'with' / 'and'
            if (Current.Type == TokenType.Identifier && Current.Text == "expiration")
            {
                Advance();
                requiresExpiration = true;
            }
            else
            {
                caveatName = ParseTypePath();
            }
        }

        return new TypeRefNode(typeName, isWildcard, subrelation, caveatName, requiresExpiration);
    }

    private PermissionNode ParsePermission()
    {
        Advance(); // 'permission'
        string name = Expect(TokenType.Identifier, "permission name").Text;

        // Optional type annotation: permission p: user | org = ...  (parsed and ignored).
        if (Is(TokenType.Colon))
        {
            Advance();
            ParseTypePath();
            while (Is(TokenType.Pipe))
            {
                Advance();
                ParseTypePath();
            }
        }

        Expect(TokenType.Equals, "'='");
        ExprNode expr = ParseExpression();
        ConsumeOptionalTerminator();
        return new PermissionNode(name, expr);
    }

    // Expression precedence (lowest binds last): exclusion '-' < intersection '&' < union '+'.
    private ExprNode ParseExpression() => ParseExclusion();

    private ExprNode ParseExclusion()
    {
        ExprNode left = ParseIntersection();
        while (Is(TokenType.Minus))
        {
            Advance();
            ExprNode right = ParseIntersection();
            left = new BinaryExpr(SetOp.Exclusion, left, right);
        }

        return left;
    }

    private ExprNode ParseIntersection()
    {
        ExprNode left = ParseUnion();
        while (Is(TokenType.And))
        {
            Advance();
            ExprNode right = ParseUnion();
            left = new BinaryExpr(SetOp.Intersection, left, right);
        }

        return left;
    }

    private ExprNode ParseUnion()
    {
        ExprNode left = ParsePrimary();
        while (Is(TokenType.Plus))
        {
            Advance();
            ExprNode right = ParsePrimary();
            left = new BinaryExpr(SetOp.Union, left, right);
        }

        return left;
    }

    private ExprNode ParsePrimary()
    {
        if (Is(TokenType.LeftParen))
        {
            Advance();
            ExprNode inner = ParseExpression();
            Expect(TokenType.RightParen, "')'");
            return inner;
        }

        if (IsKeyword("nil"))
        {
            Advance();
            return new NilExpr();
        }

        if (Is(TokenType.Identifier))
        {
            string name = Advance().Text;

            if (Is(TokenType.RightArrow))
            {
                Advance();
                string computed = Expect(TokenType.Identifier, "arrow target relation").Text;
                return new ArrowExpr(name, computed, Function: null);
            }

            if (Is(TokenType.Period))
            {
                // tupleset.any(target) / tupleset.all(target)
                Advance();
                string fn = Expect(TokenType.Identifier, "arrow function name").Text;
                Expect(TokenType.LeftParen, "'('");
                string computed = Expect(TokenType.Identifier, "arrow target relation").Text;
                Expect(TokenType.RightParen, "')'");
                return new ArrowExpr(name, computed, fn);
            }

            return new ReferenceExpr(name);
        }

        throw Error($"expected an expression operand, found '{TokenText()}'");
    }

    private CaveatNode ParseCaveat()
    {
        Advance(); // 'caveat'
        string name = ParseTypePath();

        // Parameter list: ( name type, name type, ... )
        Expect(TokenType.LeftParen, "'('");
        var parameters = ImmutableList.CreateBuilder<CaveatParameterNode>();
        if (!Is(TokenType.RightParen))
        {
            parameters.Add(ParseCaveatParameter());
            while (Is(TokenType.Comma))
            {
                Advance();
                parameters.Add(ParseCaveatParameter());
            }
        }

        Expect(TokenType.RightParen, "')'");

        // Body: { CEL ... } — captured verbatim from source, not parsed here.
        Token open = Expect(TokenType.LeftBrace, "'{'");
        SkipBalancedBraces(out Token close);

        // Slice raw CEL text between the braces, trimming surrounding whitespace.
        int bodyStart = open.Offset + 1;
        int bodyEnd = close.Offset;
        string expression = _source[bodyStart..bodyEnd].Trim();

        return new CaveatNode(name, parameters.ToImmutable(), expression);
    }

    private CaveatParameterNode ParseCaveatParameter()
    {
        string paramName = Expect(TokenType.Identifier, "caveat parameter name").Text;
        CaveatTypeRefNode type = ParseCaveatTypeRef();
        return new CaveatParameterNode(paramName, type);
    }

    private CaveatTypeRefNode ParseCaveatTypeRef()
    {
        string typeName = Expect(TokenType.Identifier, "caveat parameter type").Text;
        var children = ImmutableList.CreateBuilder<CaveatTypeRefNode>();

        if (Is(TokenType.LessThan))
        {
            Advance();
            children.Add(ParseCaveatTypeRef());
            while (Is(TokenType.Comma))
            {
                Advance();
                children.Add(ParseCaveatTypeRef());
            }

            Expect(TokenType.GreaterThan, "'>'");
        }

        return new CaveatTypeRefNode(typeName, children.ToImmutable());
    }

    private void SkipBalancedBraces(out Token closing)
    {
        int depth = 1;
        while (depth > 0 && !Is(TokenType.Eof))
        {
            if (Is(TokenType.LeftBrace))
            {
                depth++;
            }
            else if (Is(TokenType.RightBrace))
            {
                depth--;
                if (depth == 0)
                {
                    closing = Advance();
                    return;
                }
            }

            Advance();
        }

        throw Error("unterminated caveat body");
    }

    /// <summary>Parses a slash-separated namespace path such as <c>org/user</c>.</summary>
    private string ParseTypePath()
    {
        var segments = new List<string> { Expect(TokenType.Identifier, "type name").Text };
        while (Is(TokenType.Slash))
        {
            Advance();
            segments.Add(Expect(TokenType.Identifier, "type path segment").Text);
        }

        return string.Join("/", segments);
    }

    private void ConsumeOptionalTerminator()
    {
        if (Is(TokenType.Semicolon))
        {
            Advance();
        }
    }
}
