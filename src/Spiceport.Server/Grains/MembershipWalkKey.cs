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
    public static string Build(string subjectType, string subjectId, string subjectRelation, string revision, string schemaHash) =>
        GrainKeyCodec.Join(subjectType, subjectId, subjectRelation, revision, schemaHash);

    public static MembershipWalkKeyParts Parse(string key)
    {
        var parts = GrainKeyCodec.Split(key, 5);

        return new MembershipWalkKeyParts(parts[0], parts[1], parts[2], parts[3], parts[4]);
    }
}

/// <summary>The decoded components of an <c>IMembershipWalkGrain</c> string key.</summary>
internal sealed record MembershipWalkKeyParts(
    string SubjectType,
    string SubjectId,
    string SubjectRelation,
    string Revision,
    string SchemaHash);
