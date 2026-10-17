namespace Spiceport.Engine;

/// <summary>
/// The validation half of cel-go's <c>parser/unescape.go</c>: decides whether a quoted string or bytes
/// literal unescapes, and with which message it fails. The decoded value is never needed by the checker.
/// </summary>
internal static class CelUnescape
{
    private const string Generic = "unable to unescape string";

    /// <summary>Returns cel-go's error message for an invalid literal, or null when it unescapes.</summary>
    public static string? Unescape(string value, bool isBytes)
    {
        // All strings normalize newlines to the \n representation.
        value = value.Replace("\r\n", "\n").Replace("\r", "\n");
        var n = value.Length;
        if (n < 2)
            return Generic;

        var isRawLiteral = false;
        if (value[0] is 'r' or 'R')
        {
            value = value[1..];
            n = value.Length;
            isRawLiteral = true;
        }

        if (n == 0 || value[0] != value[n - 1] || (value[0] != '"' && value[0] != '\''))
            return Generic;

        if (n >= 6)
        {
            if (value.StartsWith("'''", StringComparison.Ordinal))
            {
                if (!value.EndsWith("'''", StringComparison.Ordinal))
                    return Generic;
                value = "\"" + value[3..(n - 3)] + "\"";
            }
            else if (value.StartsWith("\"\"\"", StringComparison.Ordinal))
            {
                if (!value.EndsWith("\"\"\"", StringComparison.Ordinal))
                    return Generic;
                value = "\"" + value[3..(n - 3)] + "\"";
            }

            n = value.Length;
        }

        value = value[1..(n - 1)];
        if (isRawLiteral || !value.Contains('\\'))
            return null;

        var s = value.AsSpan();
        while (s.Length > 0)
        {
            if (UnescapeChar(ref s, isBytes) is { } error)
                return error;
        }

        return null;
    }

    private static string? UnescapeChar(ref ReadOnlySpan<char> s, bool isBytes)
    {
        if (s[0] != '\\')
        {
            s = s[1..];
            return null;
        }

        if (s.Length <= 1)
            return "unable to unescape string, found '\\' as last character";

        var c = s[1];
        s = s[2..];
        switch (c)
        {
            case 'a' or 'b' or 'f' or 'n' or 'r' or 't' or 'v' or '\\' or '\'' or '"' or '`' or '?':
                return null;

            case 'x' or 'X' or 'u' or 'U':
            {
                var count = c switch { 'x' or 'X' => 2, 'u' => 4, _ => 8 };
                if (isBytes && c is 'u' or 'U')
                    return Generic;
                if (s.Length < count)
                    return Generic;
                long v = 0;
                for (var j = 0; j < count; j++)
                {
                    var x = HexValue(s[j]);
                    if (x < 0)
                        return Generic;
                    v = (v << 4) | (uint)x;
                }

                s = s[count..];
                if (!isBytes && v > 0x10FFFF)
                    return Generic;
                return null;
            }

            case '0' or '1' or '2' or '3':
            {
                if (s.Length < 2)
                    return "unable to unescape octal sequence in string";
                for (var j = 0; j < 2; j++)
                {
                    if (s[j] is < '0' or > '7')
                        return "unable to unescape octal sequence in string";
                }

                s = s[2..];
                return null;
            }

            default:
                return Generic;
        }
    }

    private static int HexValue(char c) =>
        c switch
        {
            >= '0' and <= '9' => c - '0',
            >= 'a' and <= 'f' => c - 'a' + 10,
            >= 'A' and <= 'F' => c - 'A' + 10,
            _ => -1,
        };
}
