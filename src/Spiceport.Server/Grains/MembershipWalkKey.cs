namespace Spiceport.Grains;

/// <summary>
/// Encodes and decodes the <c>IMembershipWalkGrain</c> string key, which IS the canonical identity of "the
/// membership-walk closure rooted at subject key <c>subjType:subjId#subjRelation</c> at
/// <c>(revision, schemaHash)</c>": <c>subjType/subjId/subjRelation/revision/schemaHash</c>.
/// </summary>
/// <remarks>
/// Mirrors <see cref="SubjectFrontierKey"/>'s escaping/parsing conventions exactly: components are
/// URL-style escaped so a literal separator in any field cannot corrupt the key, and — because a walk runs
/// over a reader pinned to the key's exact revision, not a quantized window — this key carries the revision
/// string verbatim rather than an optimized/exact mode segment, the same reasoning <see cref="GrainKey"/>'s
/// remarks give for omitting one.
/// </remarks>
internal static class MembershipWalkKey
{
    private const char Separator = '/';

    public static string Build(string subjectType, string subjectId, string subjectRelation, string revision, string schemaHash) =>
        string.Join(Separator, [
            Escape(subjectType),
            Escape(subjectId),
            Escape(subjectRelation),
            Escape(revision),
            Escape(schemaHash),
        ]);

    public static MembershipWalkKeyParts Parse(string key)
    {
        var parts = key.Split(Separator);
        if (parts.Length != 5)
            throw new FormatException($"Malformed membership-walk-grain key (expected 5 segments): '{key}'.");

        return new MembershipWalkKeyParts(
            Unescape(parts[0]),
            Unescape(parts[1]),
            Unescape(parts[2]),
            Unescape(parts[3]),
            Unescape(parts[4]));
    }

    private static string Escape(string s) => Uri.EscapeDataString(s);

    private static string Unescape(string s) => Uri.UnescapeDataString(s);
}

/// <summary>The decoded components of an <c>IMembershipWalkGrain</c> string key.</summary>
internal sealed record MembershipWalkKeyParts(
    string SubjectType,
    string SubjectId,
    string SubjectRelation,
    string Revision,
    string SchemaHash);
