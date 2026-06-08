using System.Collections.Immutable;
using System.Text;
using Spiceport.Core;
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

    // Per-section kind tags. Exactly one resume mechanism applies per section.
    private const string TagLeaf = "L";        // Portion-1 self-match: LastResourceId follows.
    private const string TagQuery = "Q";       // Query entrypoint: a six-field keyset follows.
    private const string TagStructural = "S";  // Structural rewrite / query first-chunk: no payload.

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
            EncodeSection(sb, cursor.Sections[i]);
        }
        return ToToken(sb.ToString());
    }

    private static void EncodeSection(StringBuilder sb, LookupResourcesCursorSection s)
    {
        sb.Append(s.EntrypointIndex).Append(FieldSeparator);
        if (s.LastResourceId is { } lastId)
        {
            sb.Append(TagLeaf).Append(FieldSeparator).Append(Uri.EscapeDataString(lastId));
        }
        else if (s.AfterKeyset is { } k)
        {
            sb.Append(TagQuery);
            foreach (var part in KeysetParts(k))
                sb.Append(FieldSeparator).Append(Uri.EscapeDataString(part));
        }
        else
        {
            sb.Append(TagStructural);
        }
    }

    private static string[] KeysetParts(RelationshipReference k) =>
    [
        k.Subject.ObjectType, k.Subject.ObjectId, k.Subject.Relation,
        k.Resource.ObjectType, k.Resource.ObjectId, k.Resource.Relation,
    ];

    /// <summary>Decodes an opaque token back to a LookupResources engine cursor, or null when empty.</summary>
    public static LookupResourcesCursor? DecodeResources(string? token)
    {
        var raw = FromToken(token);
        if (raw is null)
            return null;

        var sections = ImmutableList.CreateBuilder<LookupResourcesCursorSection>();
        foreach (var part in raw.Split(SectionSeparator, StringSplitOptions.RemoveEmptyEntries))
            sections.Add(DecodeSection(part));
        return sections.Count == 0 ? null : new LookupResourcesCursor(sections.ToImmutable());
    }

    private static LookupResourcesCursorSection DecodeSection(string part)
    {
        var fields = part.Split(FieldSeparator);
        if (fields.Length < 2 || !int.TryParse(fields[0], out var idx))
            throw new FormatException($"Malformed lookup-resources cursor section: '{part}'.");

        switch (fields[1])
        {
            case TagLeaf when fields.Length == 3:
                return new LookupResourcesCursorSection(idx, LastResourceId: Uri.UnescapeDataString(fields[2]));
            case TagStructural when fields.Length == 2:
                return new LookupResourcesCursorSection(idx);
            case TagQuery when fields.Length == 8:
                var p = new string[6];
                for (var i = 0; i < 6; i++)
                    p[i] = Uri.UnescapeDataString(fields[i + 2]);
                var keyset = new RelationshipReference(
                    Resource: new ObjectAndRelation(p[3], p[4], p[5]),
                    Subject: new ObjectAndRelation(p[0], p[1], p[2]));
                return new LookupResourcesCursorSection(idx, AfterKeyset: keyset);
            default:
                throw new FormatException($"Malformed lookup-resources cursor section: '{part}'.");
        }
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
