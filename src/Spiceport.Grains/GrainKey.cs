using Spiceport.Core;

namespace Spiceport.Grains;

/// <summary>
/// Encodes and decodes the <c>ICheckGrain</c> string key, which IS the canonical sub-problem
/// identity: <c>resType/resId/relation/subjType/subjId/subjRelation/quantizedRevision/schemaHash</c>.
/// </summary>
/// <remarks>
/// The revision component is the request's revision string form. Callers are expected to pin an
/// already-quantized (optimized) revision at the top of a check, so structurally-identical
/// sub-problems made within the same window collide on the same grain identity (and hence cache
/// entry), while the component remains a real, snapshot-able revision the grain can resolve a reader
/// for. Components are URL-style escaped so a literal separator in any field cannot corrupt the key.
/// </remarks>
internal static class GrainKey
{
    private const char Separator = '/';

    public static string Build(
        ObjectAndRelation resource,
        ObjectAndRelation subject,
        string revision,
        string schemaHash,
        RevisionMode mode) =>
        string.Join(Separator, [
            Escape(resource.ObjectType),
            Escape(resource.ObjectId),
            Escape(resource.Relation),
            Escape(subject.ObjectType),
            Escape(subject.ObjectId),
            Escape(subject.Relation),
            Escape(revision),
            Escape(schemaHash),
            // The cache mode is part of the identity: an exact-revision sub-problem must never share a
            // grain (and hence a branch cache entry) with an optimized one even at the same revision.
            ((int)mode).ToString(System.Globalization.CultureInfo.InvariantCulture),
        ]);

    public static GrainKeyParts Parse(string key)
    {
        var parts = key.Split(Separator);
        if (parts.Length != 9)
            throw new FormatException($"Malformed check-grain key (expected 9 segments): '{key}'.");

        return new GrainKeyParts(
            new ObjectAndRelation(Unescape(parts[0]), Unescape(parts[1]), Unescape(parts[2])),
            new ObjectAndRelation(Unescape(parts[3]), Unescape(parts[4]), Unescape(parts[5])),
            Unescape(parts[6]),
            Unescape(parts[7]),
            (RevisionMode)int.Parse(parts[8], System.Globalization.CultureInfo.InvariantCulture));
    }

    private static string Escape(string s) => Uri.EscapeDataString(s);

    private static string Unescape(string s) => Uri.UnescapeDataString(s);
}

/// <summary>The decoded components of an <c>ICheckGrain</c> string key.</summary>
internal sealed record GrainKeyParts(
    ObjectAndRelation Resource,
    ObjectAndRelation Subject,
    string Revision,
    string SchemaHash,
    RevisionMode Mode);
