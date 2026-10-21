using System.Globalization;
using System.Text;
using Google.Protobuf.WellKnownTypes;
using V1 = Authzed.Api.V1;

namespace Spiceport.Api;

/// <summary>
/// SpiceDB's string forms of an <c>authzed.api.v1</c> relationship (<c>pkg/tuple/v1.go</c>), which its error
/// messages and <c>ErrorInfo</c> metadata quote: <c>resource:id#relation@subject:id[#relation]</c>, then for
/// <see cref="Full"/> the caveat as <c>[name:{context}]</c> and the expiration as
/// <c>[expiration:RFC3339Nano]</c>.
/// </summary>
/// <remarks>
/// The caveat context is Go protojson's compact rendering of a <c>Struct</c>: keys sorted, no insignificant
/// whitespace. (protojson deliberately perturbs its whitespace per build, so a given SpiceDB binary may add
/// a space after a separator; the compact form is the canonical one.)
/// </remarks>
internal static class GoRelationshipStrings
{
    /// <summary><c>V1StringRelationshipWithoutCaveatOrExpiration</c>.</summary>
    internal static string WithoutCaveatOrExpiration(V1::Relationship rel)
    {
        var subject = $"{rel.Subject.Object.ObjectType}:{rel.Subject.Object.ObjectId}";
        if (!string.IsNullOrEmpty(rel.Subject.OptionalRelation))
            subject += "#" + rel.Subject.OptionalRelation;
        return $"{rel.Resource.ObjectType}:{rel.Resource.ObjectId}#{rel.Relation}@{subject}";
    }

    /// <summary><c>MustV1StringRelationship</c>: the relationship with its caveat and expiration.</summary>
    internal static string Full(V1::Relationship rel)
    {
        var text = new StringBuilder(WithoutCaveatOrExpiration(rel));
        if (rel.OptionalCaveat is { CaveatName.Length: > 0 } caveat)
        {
            text.Append('[').Append(caveat.CaveatName);
            if (caveat.Context is { Fields.Count: > 0 } context)
                AppendStruct(text.Append(':'), context);
            text.Append(']');
        }

        if (rel.OptionalExpiresAt is { } expiresAt)
            text.Append("[expiration:").Append(Rfc3339Nano(expiresAt)).Append(']');

        return text.ToString();
    }

    /// <summary>Go's <c>time.RFC3339Nano</c> of a UTC instant: trailing fractional zeros dropped, <c>Z</c> zone.</summary>
    private static string Rfc3339Nano(Timestamp timestamp)
    {
        var seconds = DateTimeOffset.FromUnixTimeSeconds(timestamp.Seconds).UtcDateTime;
        var text = seconds.ToString("yyyy-MM-dd'T'HH:mm:ss", CultureInfo.InvariantCulture);
        if (timestamp.Nanos != 0)
            text += "." + timestamp.Nanos.ToString("D9", CultureInfo.InvariantCulture).TrimEnd('0');
        return text + "Z";
    }

    private static void AppendStruct(StringBuilder text, Struct value)
    {
        text.Append('{');
        var first = true;
        foreach (var (key, field) in value.Fields.OrderBy(f => f.Key, StringComparer.Ordinal))
        {
            if (!first)
                text.Append(',');
            first = false;
            AppendString(text, key);
            text.Append(':');
            AppendValue(text, field);
        }
        text.Append('}');
    }

    private static void AppendValue(StringBuilder text, Value value)
    {
        switch (value.KindCase)
        {
            case Value.KindOneofCase.StringValue:
                AppendString(text, value.StringValue);
                break;
            case Value.KindOneofCase.NumberValue:
                text.Append(GoFloat(value.NumberValue));
                break;
            case Value.KindOneofCase.BoolValue:
                text.Append(value.BoolValue ? "true" : "false");
                break;
            case Value.KindOneofCase.StructValue:
                AppendStruct(text, value.StructValue);
                break;
            case Value.KindOneofCase.ListValue:
                text.Append('[');
                for (var i = 0; i < value.ListValue.Values.Count; i++)
                {
                    if (i > 0)
                        text.Append(',');
                    AppendValue(text, value.ListValue.Values[i]);
                }
                text.Append(']');
                break;
            default:
                text.Append("null");
                break;
        }
    }

    /// <summary>protojson's string escaping: <c>"</c>, <c>\</c> and control characters only.</summary>
    private static void AppendString(StringBuilder text, string value)
    {
        text.Append('"');
        foreach (var c in value)
        {
            switch (c)
            {
                case '"': text.Append("\\\""); break;
                case '\\': text.Append("\\\\"); break;
                case '\b': text.Append("\\b"); break;
                case '\f': text.Append("\\f"); break;
                case '\n': text.Append("\\n"); break;
                case '\r': text.Append("\\r"); break;
                case '\t': text.Append("\\t"); break;
                default:
                    if (c < ' ')
                        text.Append(CultureInfo.InvariantCulture, $"\\u{(int)c:x4}");
                    else
                        text.Append(c);
                    break;
            }
        }
        text.Append('"');
    }

    /// <summary>
    /// protojson's rendering of a double: the shortest round-trip digits, in plain notation for magnitudes in
    /// [1e-6, 1e21) and Go's exponent notation (<c>1e+21</c>, <c>1e-07</c> shortened to <c>1e-7</c>) otherwise.
    /// </summary>
    internal static string GoFloat(double value)
    {
        if (double.IsNaN(value))
            return "\"NaN\"";
        if (double.IsInfinity(value))
            return value > 0 ? "\"Infinity\"" : "\"-Infinity\"";
        if (value == 0)
            return "0";

        // Shortest round-trip digits and decimal exponent: value = 0.d1d2...dn * 10^point.
        var shortest = value.ToString("R", CultureInfo.InvariantCulture);
        var negative = shortest.StartsWith('-');
        var (digits, point) = Decompose(negative ? shortest[1..] : shortest);

        var abs = Math.Abs(value);
        var sign = negative ? "-" : string.Empty;
        if (abs < 1e-6 || abs >= 1e21)
        {
            var mantissa = digits.Length == 1 ? digits : digits[0] + "." + digits[1..];
            var exponent = point - 1;
            var exponentText = exponent < 0
                ? "-" + (-exponent).ToString(CultureInfo.InvariantCulture).PadLeft(2, '0')
                : "+" + exponent.ToString(CultureInfo.InvariantCulture).PadLeft(2, '0');
            if (exponentText.Length == 3 && exponentText[0] == '-' && exponentText[1] == '0')
                exponentText = "-" + exponentText[2];
            return $"{sign}{mantissa}e{exponentText}";
        }

        string plain;
        if (point <= 0)
            plain = "0." + new string('0', -point) + digits;
        else if (point >= digits.Length)
            plain = digits + new string('0', point - digits.Length);
        else
            plain = digits[..point] + "." + digits[point..];
        return sign + plain;
    }

    /// <summary>Splits .NET's round-trip text into significant digits and the decimal point's position.</summary>
    private static (string Digits, int Point) Decompose(string text)
    {
        var exponent = 0;
        var e = text.IndexOfAny(['E', 'e']);
        if (e >= 0)
        {
            exponent = int.Parse(text[(e + 1)..], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture);
            text = text[..e];
        }

        var dot = text.IndexOf('.');
        var integerPart = dot >= 0 ? text[..dot] : text;
        var fraction = dot >= 0 ? text[(dot + 1)..] : string.Empty;
        var digits = integerPart + fraction;
        var point = integerPart.Length + exponent;

        var leading = digits.Length - digits.TrimStart('0').Length;
        digits = digits[leading..];
        point -= leading;
        digits = digits.TrimEnd('0');
        return (digits.Length == 0 ? "0" : digits, point);
    }
}
