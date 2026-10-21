using System.Text;
using Google.Protobuf;
using Google.Protobuf.Reflection;

namespace Spiceport.Api;

/// <summary>
/// Go's <c>String()</c> of a generated protobuf message (<c>prototext</c>, single-line): each populated field
/// as <c>name:value</c> in declaration order, separated by one space, a string quoted and a message in braces.
/// SpiceDB quotes it in <c>ErrorInfo</c> metadata, e.g. <c>InvalidFilterError</c>'s <c>filter</c>.
/// </summary>
/// <remarks>
/// Only the field shapes the quoted messages use are supported: singular strings (whose validated values are
/// printable ASCII, so escaping a quote and a backslash suffices) and singular messages. Go's prototext
/// deliberately perturbs its whitespace per build; a given SpiceDB binary may add a space after a separator,
/// and the single-space form here is the canonical one.
/// </remarks>
internal static class GoPrototext
{
    internal static string Format(IMessage message)
    {
        var text = new StringBuilder();
        Append(text, message);
        return text.ToString();
    }

    private static void Append(StringBuilder text, IMessage message)
    {
        var first = true;
        foreach (var field in message.Descriptor.Fields.InDeclarationOrder())
        {
            if (field.IsRepeated || field.IsMap)
                throw new NotSupportedException($"prototext of repeated field {field.FullName}");

            var value = field.Accessor.GetValue(message);
            switch (field.FieldType)
            {
                case FieldType.String:
                    var s = (string)value;
                    if (s.Length == 0 && !field.HasPresence)
                        continue;
                    Separate(text, ref first).Append(field.Name).Append(':').Append(Quote(s));
                    break;
                case FieldType.Message:
                    if (value is not IMessage nested)
                        continue;
                    Separate(text, ref first).Append(field.Name).Append(":{");
                    Append(text, nested);
                    text.Append('}');
                    break;
                default:
                    throw new NotSupportedException($"prototext of {field.FieldType} field {field.FullName}");
            }
        }
    }

    private static StringBuilder Separate(StringBuilder text, ref bool first)
    {
        if (!first)
            text.Append(' ');
        first = false;
        return text;
    }

    private static string Quote(string s) => "\"" + s.Replace("\\", "\\\\").Replace("\"", "\\\"") + "\"";
}
