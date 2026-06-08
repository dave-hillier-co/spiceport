namespace Spiceport.Schema;

/// <summary>The kinds of tokens produced by the schema DSL <see cref="Lexer"/>.</summary>
internal enum TokenType
{
    Eof,
    Keyword,
    Identifier,
    Number,
    String,
    LeftBrace,
    RightBrace,
    LeftParen,
    RightParen,
    Pipe,
    Plus,
    Minus,
    And,
    Slash,
    Equals,
    Colon,
    Semicolon,
    RightArrow,
    Hash,
    Ellipsis,
    Star,
    Period,
    Comma,
    LessThan,
    GreaterThan,

    /// <summary>
    /// Any character not otherwise recognized. Emitted rather than thrown so that caveat
    /// bodies (captured verbatim) may contain arbitrary CEL operators such as <c>!</c>,
    /// <c>?</c> or <c>%</c>; outside a caveat body the parser rejects it as a syntax error.
    /// </summary>
    Unknown,
}

/// <summary>A single lexeme: its kind, text, 1-based source position, and absolute character offset.</summary>
internal readonly record struct Token(TokenType Type, string Text, int Line, int Column, int Offset);

/// <summary>
/// Hand-written character-by-character lexer for the SpiceDB schema DSL.
/// Whitespace and comments are skipped; newlines do not produce tokens (the
/// parser is whitespace/terminator insensitive for the supported subset).
/// </summary>
internal sealed class Lexer
{
    private static readonly HashSet<string> Keywords =
        ["definition", "relation", "permission", "caveat", "nil", "use", "with"];

    /// <summary>
    /// Identifiers that are only promoted to keywords when the matching <c>use &lt;flag&gt;</c>
    /// declaration is present. Mirrors SpiceDB's flaggable lexer (pkg/schemadsl/lexer/flags.go):
    /// <c>use expiration</c> promotes <c>expiration</c> and <c>and</c>; <c>use self</c> promotes
    /// <c>self</c>. The flag must be declared before any definition or caveat.
    /// </summary>
    private static readonly IReadOnlyDictionary<string, string[]> FlagPromotedKeywords =
        new Dictionary<string, string[]>
        {
            ["expiration"] = ["expiration", "and"],
            ["self"] = ["self"],
        };

    private readonly string _input;
    private int _pos;
    private int _line = 1;
    private int _column = 1;

    public Lexer(string input) => _input = input;

    private char Current => _pos < _input.Length ? _input[_pos] : '\0';
    private char Peek(int ahead = 1) => _pos + ahead < _input.Length ? _input[_pos + ahead] : '\0';
    private bool AtEnd => _pos >= _input.Length;

    private void Advance()
    {
        if (_input[_pos] == '\n')
        {
            _line++;
            _column = 1;
        }
        else
        {
            _column++;
        }

        _pos++;
    }

    /// <summary>Tokenizes the entire input, ending with an <see cref="TokenType.Eof"/> token.</summary>
    public List<Token> Tokenize()
    {
        var tokens = new List<Token>();
        while (true)
        {
            var token = Next();
            tokens.Add(token);
            if (token.Type == TokenType.Eof)
            {
                break;
            }
        }

        return ApplyFlags(tokens);
    }

    /// <summary>
    /// Applies <c>use &lt;flag&gt;</c> declarations, promoting flag-gated identifiers
    /// (<c>expiration</c>, <c>and</c>, <c>self</c>) to keywords. Mirrors SpiceDB's
    /// flaggable lexer: a <c>use</c> flag must appear before any <c>definition</c> or
    /// <c>caveat</c> (otherwise it is rejected), and only the declared flags take effect.
    /// </summary>
    private static List<Token> ApplyFlags(List<Token> tokens)
    {
        var promoted = new HashSet<string>();
        bool seenDefinition = false;

        for (int i = 0; i < tokens.Count; i++)
        {
            Token token = tokens[i];

            if (token.Type == TokenType.Keyword && token.Text == "use")
            {
                if (seenDefinition)
                {
                    throw new SchemaCompileException(
                        "use flags must appear before any definition or caveat", token.Line, token.Column);
                }

                if (i + 1 >= tokens.Count || tokens[i + 1].Type is not (TokenType.Identifier or TokenType.Keyword))
                {
                    throw new SchemaCompileException(
                        "expected a feature name after 'use'", token.Line, token.Column);
                }

                string flag = tokens[i + 1].Text;
                if (!FlagPromotedKeywords.ContainsKey(flag))
                {
                    throw new SchemaCompileException(
                        $"unknown use flag '{flag}'", tokens[i + 1].Line, tokens[i + 1].Column);
                }

                promoted.UnionWith(FlagPromotedKeywords[flag]);
                continue;
            }

            if (token.Type == TokenType.Keyword && token.Text is "definition" or "caveat")
            {
                seenDefinition = true;
            }
        }

        if (promoted.Count == 0)
        {
            return tokens;
        }

        for (int i = 0; i < tokens.Count; i++)
        {
            Token token = tokens[i];
            if (token.Type == TokenType.Identifier && promoted.Contains(token.Text))
            {
                tokens[i] = token with { Type = TokenType.Keyword };
            }
        }

        return tokens;
    }

    private Token Next()
    {
        SkipTrivia();

        if (AtEnd)
        {
            return new Token(TokenType.Eof, string.Empty, _line, _column, _pos);
        }

        int line = _line;
        int col = _column;
        int off = _pos;
        char c = Current;

        switch (c)
        {
            case '{': Advance(); return new Token(TokenType.LeftBrace, "{", line, col, off);
            case '}': Advance(); return new Token(TokenType.RightBrace, "}", line, col, off);
            case '(': Advance(); return new Token(TokenType.LeftParen, "(", line, col, off);
            case ')': Advance(); return new Token(TokenType.RightParen, ")", line, col, off);
            case '|': Advance(); return new Token(TokenType.Pipe, "|", line, col, off);
            case '+': Advance(); return new Token(TokenType.Plus, "+", line, col, off);
            case '&': Advance(); return new Token(TokenType.And, "&", line, col, off);
            case '/': Advance(); return new Token(TokenType.Slash, "/", line, col, off);
            case '=': Advance(); return new Token(TokenType.Equals, "=", line, col, off);
            case ':': Advance(); return new Token(TokenType.Colon, ":", line, col, off);
            case ';': Advance(); return new Token(TokenType.Semicolon, ";", line, col, off);
            case '#': Advance(); return new Token(TokenType.Hash, "#", line, col, off);
            case '*': Advance(); return new Token(TokenType.Star, "*", line, col, off);
            case ',': Advance(); return new Token(TokenType.Comma, ",", line, col, off);
            case '<': Advance(); return new Token(TokenType.LessThan, "<", line, col, off);
            case '>': Advance(); return new Token(TokenType.GreaterThan, ">", line, col, off);
            case '-':
                if (Peek() == '>')
                {
                    Advance();
                    Advance();
                    return new Token(TokenType.RightArrow, "->", line, col, off);
                }

                Advance();
                return new Token(TokenType.Minus, "-", line, col, off);
            case '.':
                if (Peek() == '.' && Peek(2) == '.')
                {
                    Advance();
                    Advance();
                    Advance();
                    return new Token(TokenType.Ellipsis, "...", line, col, off);
                }

                Advance();
                return new Token(TokenType.Period, ".", line, col, off);
            case '"':
            case '\'':
                return LexString(line, col, off);
        }

        if (IsIdentStart(c))
        {
            return LexIdentifierOrKeyword(line, col, off);
        }

        if (char.IsDigit(c))
        {
            return LexNumber(line, col, off);
        }

        // Unrecognized character: emit an Unknown token rather than throwing, so verbatim
        // caveat-body capture can balance braces over arbitrary CEL operators.
        Advance();
        return new Token(TokenType.Unknown, c.ToString(), line, col, off);
    }

    private void SkipTrivia()
    {
        while (!AtEnd)
        {
            char c = Current;
            if (c is ' ' or '\t' or '\r' or '\n')
            {
                Advance();
                continue;
            }

            if (c == '/' && Peek() == '/')
            {
                while (!AtEnd && Current != '\n')
                {
                    Advance();
                }

                continue;
            }

            if (c == '/' && Peek() == '*')
            {
                Advance();
                Advance();
                while (!AtEnd && !(Current == '*' && Peek() == '/'))
                {
                    Advance();
                }

                if (!AtEnd)
                {
                    Advance();
                    Advance();
                }

                continue;
            }

            return;
        }
    }

    private Token LexIdentifierOrKeyword(int line, int col, int off)
    {
        int start = _pos;
        while (!AtEnd && IsIdentPart(Current))
        {
            Advance();
        }

        string text = _input[start.._pos];
        var type = Keywords.Contains(text) ? TokenType.Keyword : TokenType.Identifier;
        return new Token(type, text, line, col, off);
    }

    private Token LexNumber(int line, int col, int off)
    {
        int start = _pos;
        while (!AtEnd && char.IsDigit(Current))
        {
            Advance();
        }

        return new Token(TokenType.Number, _input[start.._pos], line, col, off);
    }

    private Token LexString(int line, int col, int off)
    {
        char quote = Current;
        Advance();
        int start = _pos;
        while (!AtEnd && Current != quote)
        {
            if (Current == '\\')
            {
                Advance();
            }

            Advance();
        }

        if (AtEnd)
        {
            throw new SchemaCompileException("unterminated string literal", line, col);
        }

        string text = _input[start.._pos];
        Advance();
        return new Token(TokenType.String, text, line, col, off);
    }

    private static bool IsIdentStart(char c) => char.IsLetter(c) || c == '_';

    private static bool IsIdentPart(char c) => char.IsLetterOrDigit(c) || c == '_';
}
