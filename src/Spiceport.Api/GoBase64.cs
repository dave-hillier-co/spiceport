using System.Text;

namespace Spiceport.Api;

/// <summary>
/// Where Go's <c>base64.StdEncoding.DecodeString</c> rejects its input: the offset its
/// <c>CorruptInputError</c> reports (<c>illegal base64 data at input byte N</c>), which SpiceDB quotes when
/// a ZedToken is not base64. A port of <c>encoding/base64</c>'s <c>decodeQuantum</c>, over the input's UTF-8
/// bytes: newlines and carriage returns are skipped, padding is required, and anything else outside the
/// standard alphabet is an error where .NET's decoder would skip whitespace.
/// </summary>
internal static class GoBase64
{
    private const byte Pad = (byte)'=';

    /// <summary>The offset Go's decoder reports for <paramref name="input"/>, or <c>null</c> when Go accepts it.</summary>
    internal static int? CorruptInputOffset(string input)
    {
        var src = Encoding.UTF8.GetBytes(input);
        var si = 0;
        while (si < src.Length)
        {
            var error = DecodeQuantum(src, ref si);
            if (error is not null)
                return error;
        }

        return null;
    }

    private static int? DecodeQuantum(byte[] src, ref int si)
    {
        for (var j = 0; j < 4; j++)
        {
            if (src.Length == si)
                return j == 0 ? null : si - j;

            var input = src[si];
            si++;

            if (IsAlphabet(input))
                continue;

            if (input is (byte)'\n' or (byte)'\r')
            {
                j--;
                continue;
            }

            if (input != Pad)
                return si - 1;

            switch (j)
            {
                case 0:
                case 1:
                    return si - 1;
                case 2:
                    SkipNewlines(src, ref si);
                    if (si == src.Length)
                        return src.Length;
                    if (src[si] != Pad)
                        return si - 1;
                    si++;
                    break;
            }

            SkipNewlines(src, ref si);
            return si < src.Length ? si : null;
        }

        return null;
    }

    private static void SkipNewlines(byte[] src, ref int si)
    {
        while (si < src.Length && src[si] is (byte)'\n' or (byte)'\r')
            si++;
    }

    private static bool IsAlphabet(byte b) =>
        b is >= (byte)'A' and <= (byte)'Z' or >= (byte)'a' and <= (byte)'z' or >= (byte)'0' and <= (byte)'9'
            or (byte)'+' or (byte)'/';
}
