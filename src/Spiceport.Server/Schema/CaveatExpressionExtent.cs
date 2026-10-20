namespace Spiceport.Schema;

/// <summary>
/// Locates a caveat body's CEL expression as SpiceDB's schema parser does
/// (<c>pkg/schemadsl/parser/parser.go</c> <c>consumeCaveatExpression</c>): the source from the start of
/// the body's first token to the end of its last token, whitespace and comments being ignored tokens.
/// SpiceDB's lexer (<c>pkg/schemadsl/lexer/lex_def.go</c>) turns a newline that follows an identifier,
/// keyword (any alphanumeric run, numbers included), <c>}</c>, <c>)</c> or <c>*</c> into a synthetic
/// semicolon token, so an expression ending such a line keeps that newline.
/// </summary>
/// <remarks>
/// The text found here is what cel-go compiles upstream, so caveat compile errors carry upstream's line,
/// column and source snippet. It is not what the schema stores (<see cref="CaveatNode.Expression"/>).
/// </remarks>
internal static class CaveatExpressionExtent
{
    private enum Kind
    {
        None,
        Identifier,
        RightBrace,
        RightParen,
        Star,
        String,
        SyntheticSemicolon,
        Newline,
        Other,
    }

    /// <summary>
    /// Returns the [start, end) offsets of the expression within <c>[bodyStart, bodyEnd)</c>; an empty
    /// range at <paramref name="bodyStart"/> when the body has no tokens.
    /// </summary>
    public static (int Start, int End) Find(string source, int bodyStart, int bodyEnd)
    {
        int? start = null;
        var end = bodyStart;
        var last = Kind.None;

        void Significant(int tokenStart, int tokenEnd, Kind kind)
        {
            start ??= tokenStart;
            end = tokenEnd;
            last = kind;
        }

        var i = bodyStart;
        while (i < bodyEnd)
        {
            var c = source[i];
            if (c is ' ' or '\t')
            {
                i++;
            }
            else if (c is '\r' or '\n')
            {
                if (last is Kind.Identifier or Kind.RightBrace or Kind.RightParen or Kind.Star)
                    Significant(i, i + 1, Kind.SyntheticSemicolon);
                else
                    last = Kind.Newline;
                i++;
            }
            else if (c == '/' && i + 1 < bodyEnd && source[i + 1] == '/')
            {
                while (i < bodyEnd && source[i] is not ('\r' or '\n'))
                    i++;
            }
            else if (c == '/' && i + 1 < bodyEnd && source[i + 1] == '*')
            {
                var close = source.IndexOf("*/", i + 2, bodyEnd - (i + 2), StringComparison.Ordinal);
                i = close < 0 ? bodyEnd : close + 2;
            }
            else if (char.IsLetterOrDigit(c) || c == '_')
            {
                var tokenStart = i;
                while (i < bodyEnd && (char.IsLetterOrDigit(source[i]) || source[i] == '_'))
                    i++;
                Significant(tokenStart, i, Kind.Identifier);
            }
            else if (c is '"' or '\'')
            {
                var tokenStart = i;
                var terminator = c.ToString();
                var allowNewlines = false;
                if (Matches(source, i, bodyEnd, "\"\"\"") || Matches(source, i, bodyEnd, "'''"))
                {
                    // Upstream terminates a ''' string with """, and so does this.
                    terminator = "\"\"\"";
                    allowNewlines = true;
                    i += 3;
                }
                else
                {
                    i++;
                }

                while (i < bodyEnd && !Matches(source, i, bodyEnd, terminator))
                {
                    if (!allowNewlines && source[i] is '\r' or '\n')
                        break;
                    i++;
                }

                if (Matches(source, i, bodyEnd, terminator))
                    i += terminator.Length;
                Significant(tokenStart, i, Kind.String);
            }
            else
            {
                var kind = c switch
                {
                    '}' => Kind.RightBrace,
                    ')' => Kind.RightParen,
                    '*' => Kind.Star,
                    _ => Kind.Other,
                };
                Significant(i, i + 1, kind);
                i++;
            }
        }

        return start is { } s ? (s, end) : (bodyStart, bodyStart);
    }

    /// <summary>The 0-based line and 0-based column, in code points, of <paramref name="offset"/>.</summary>
    public static (int Line, int Column) LineAndColumn(string source, int offset)
    {
        var line = 0;
        var lineStart = 0;
        for (var i = 0; i < offset; i++)
        {
            if (source[i] == '\n')
            {
                line++;
                lineStart = i + 1;
            }
        }

        var column = 0;
        for (var i = lineStart; i < offset; i++)
        {
            if (!char.IsLowSurrogate(source[i]))
                column++;
        }

        return (line, column);
    }

    private static bool Matches(string source, int index, int end, string value) =>
        index + value.Length <= end && string.CompareOrdinal(source, index, value, 0, value.Length) == 0;
}
