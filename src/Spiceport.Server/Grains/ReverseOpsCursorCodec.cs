using System.Collections.Immutable;
using System.Text;
using Spiceport.Engine;

namespace Spiceport.Grains;

/// <summary>
/// Encodes and decodes the opaque continuation cursors carried on the reverse-op grain replies and the
/// gRPC responses, so callers treat them as a black-box token.
/// </summary>
/// <remarks>
/// LookupResources resumes from the engine's <see cref="LookupResourcesCursor"/> (one ordered section
/// per nesting level). LookupSubjects has no engine cursor — its results are deterministically ordered
/// by subject id, so a cursor is simply the last id already returned and resumption skips ids at or
/// before it. Both encode to a URL-safe base64 string; an empty/whitespace token means "from the start".
/// </remarks>
internal static class ReverseOpsCursorCodec
{
    private const char SectionSeparator = ';';
    private const char FieldSeparator = ':';

    /// <summary>Encodes a LookupResources engine cursor to an opaque token, or null when there is none.</summary>
    public static string? Encode(LookupResourcesCursor? cursor)
    {
        if (cursor is null || cursor.Sections.Count == 0)
            return null;

        var sb = new StringBuilder();
        for (var i = 0; i < cursor.Sections.Count; i++)
        {
            if (i > 0)
                sb.Append(SectionSeparator);
            var s = cursor.Sections[i];
            sb.Append(s.EntrypointIndex)
              .Append(FieldSeparator)
              .Append(Uri.EscapeDataString(s.LastResourceId));
        }
        return ToToken(sb.ToString());
    }

    /// <summary>Decodes an opaque token back to a LookupResources engine cursor, or null when empty.</summary>
    public static LookupResourcesCursor? DecodeResources(string? token)
    {
        var raw = FromToken(token);
        if (raw is null)
            return null;

        var sections = ImmutableList.CreateBuilder<LookupResourcesCursorSection>();
        foreach (var part in raw.Split(SectionSeparator, StringSplitOptions.RemoveEmptyEntries))
        {
            var fields = part.Split(FieldSeparator, 2);
            if (fields.Length != 2 || !int.TryParse(fields[0], out var idx))
                throw new FormatException($"Malformed lookup-resources cursor token: '{part}'.");
            sections.Add(new LookupResourcesCursorSection(idx, Uri.UnescapeDataString(fields[1])));
        }
        return sections.Count == 0 ? null : new LookupResourcesCursor(sections.ToImmutable());
    }

    /// <summary>Encodes the last subject id returned as a LookupSubjects continuation token.</summary>
    public static string EncodeSubjectId(string lastSubjectId) => ToToken(lastSubjectId);

    /// <summary>Decodes a LookupSubjects token back to the last subject id, or null when empty.</summary>
    public static string? DecodeSubjectId(string? token) => FromToken(token);

    private static string ToToken(string raw) =>
        Convert.ToBase64String(Encoding.UTF8.GetBytes(raw));

    private static string? FromToken(string? token)
    {
        if (string.IsNullOrWhiteSpace(token))
            return null;
        return Encoding.UTF8.GetString(Convert.FromBase64String(token));
    }
}
