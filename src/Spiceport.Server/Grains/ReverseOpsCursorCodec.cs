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
/// per nesting level) when the live traversal produced the stream, or from a candidate-resume cursor
/// (tag <c>C</c> plus the last resource id) when the Leopard accelerator did: that stream is the
/// covered candidates in ordinal id order, so resumption skips ids at or before the last one. LookupSubjects has no engine cursor — its results are deterministically ordered
/// by subject id, so a cursor is simply the last id already returned and resumption skips ids at or
/// before it. Both encode to a standard-alphabet base64 string (<c>+</c>, <c>/</c> and <c>=</c> padding,
/// as SpiceDB's own cursors use <c>base64.StdEncoding</c>), so a token placed in a URL must be
/// percent-encoded; an empty/whitespace token means "from the start".
/// </remarks>
internal static class ReverseOpsCursorCodec
{
    private const char SectionSeparator = ';';
    private const char FieldSeparator = ':';

    // Per-section kind tags. Exactly one resume mechanism applies per section.
    private const string TagLeaf = "L";        // Portion-1 self-match: LastResourceId follows.
    private const string TagQuery = "Q";       // Query entrypoint: a six-field keyset follows.
    private const string TagStructural = "S";  // Structural rewrite / query first-chunk: no payload.
    private const string TagCandidate = "C";   // Whole-token candidate resume: the last resource id follows.

    /// <summary>
    /// Encodes a LookupResources candidate-resume cursor: the stream it resumes is ordered ordinally by
    /// resource id, so the last id emitted positions it completely.
    /// </summary>
    public static string EncodeCandidateResume(string lastResourceId) =>
        ToToken(TagCandidate + FieldSeparator + Uri.EscapeDataString(lastResourceId));

    /// <summary>
    /// Decodes a LookupResources token as a candidate-resume cursor; false (with no exception) when the
    /// token is empty or is an engine cursor, which <see cref="DecodeResources"/> then handles.
    /// </summary>
    public static bool TryDecodeCandidateResume(string? token, out string lastResourceId)
    {
        lastResourceId = string.Empty;
        var raw = FromToken(token);
        const string prefix = TagCandidate + ":";
        if (raw is null || !raw.StartsWith(prefix, StringComparison.Ordinal))
            return false;

        lastResourceId = Uri.UnescapeDataString(raw[prefix.Length..]);
        return true;
    }

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
