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
}

/// <summary>A single lexeme: its kind, text, and 1-based source position.</summary>
internal readonly record struct Token(TokenType Type, string Text, int Line, int Column);

/// <summary>
/// Hand-written character-by-character lexer for the SpiceDB schema DSL.
/// Whitespace and comments are skipped; newlines do not produce tokens (the
/// parser is whitespace/terminator insensitive for the supported subset).
/// </summary>
internal sealed class Lexer
{
    private static readonly HashSet<string> Keywords =
        ["definition", "relation", "permission", "caveat", "nil"];

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
                return tokens;
            }
        }
    }

    private Token Next()
    {
        SkipTrivia();

        if (AtEnd)
        {
            return new Token(TokenType.Eof, string.Empty, _line, _column);
        }

        int line = _line;
        int col = _column;
        char c = Current;

        switch (c)
        {
            case '{': Advance(); return new Token(TokenType.LeftBrace, "{", line, col);
            case '}': Advance(); return new Token(TokenType.RightBrace, "}", line, col);
            case '(': Advance(); return new Token(TokenType.LeftParen, "(", line, col);
            case ')': Advance(); return new Token(TokenType.RightParen, ")", line, col);
            case '|': Advance(); return new Token(TokenType.Pipe, "|", line, col);
            case '+': Advance(); return new Token(TokenType.Plus, "+", line, col);
            case '&': Advance(); return new Token(TokenType.And, "&", line, col);
            case '/': Advance(); return new Token(TokenType.Slash, "/", line, col);
            case '=': Advance(); return new Token(TokenType.Equals, "=", line, col);
            case ':': Advance(); return new Token(TokenType.Colon, ":", line, col);
            case ';': Advance(); return new Token(TokenType.Semicolon, ";", line, col);
            case '#': Advance(); return new Token(TokenType.Hash, "#", line, col);
            case '*': Advance(); return new Token(TokenType.Star, "*", line, col);
            case ',': Advance(); return new Token(TokenType.Comma, ",", line, col);
            case '-':
                if (Peek() == '>')
                {
                    Advance();
                    Advance();
                    return new Token(TokenType.RightArrow, "->", line, col);
                }

                Advance();
                return new Token(TokenType.Minus, "-", line, col);
            case '.':
                if (Peek() == '.' && Peek(2) == '.')
                {
                    Advance();
                    Advance();
                    Advance();
                    return new Token(TokenType.Ellipsis, "...", line, col);
                }

                Advance();
                return new Token(TokenType.Period, ".", line, col);
            case '"':
            case '\'':
                return LexString(line, col);
        }

        if (IsIdentStart(c))
        {
            return LexIdentifierOrKeyword(line, col);
        }

        if (char.IsDigit(c))
        {
            return LexNumber(line, col);
        }

        throw new SchemaCompileException($"unexpected character '{c}'", line, col);
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

    private Token LexIdentifierOrKeyword(int line, int col)
    {
        int start = _pos;
        while (!AtEnd && IsIdentPart(Current))
        {
            Advance();
        }

        string text = _input[start.._pos];
        var type = Keywords.Contains(text) ? TokenType.Keyword : TokenType.Identifier;
        return new Token(type, text, line, col);
    }

    private Token LexNumber(int line, int col)
    {
        int start = _pos;
        while (!AtEnd && char.IsDigit(Current))
        {
            Advance();
        }

        return new Token(TokenType.Number, _input[start.._pos], line, col);
    }

    private Token LexString(int line, int col)
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
        return new Token(TokenType.String, text, line, col);
    }

    private static bool IsIdentStart(char c) => char.IsLetter(c) || c == '_';

    private static bool IsIdentPart(char c) => char.IsLetterOrDigit(c) || c == '_';
}
