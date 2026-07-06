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
/// <para>
/// The key deliberately carries NO optimized-vs-exact mode segment. That distinction matters only at
/// <c>RevisionResolver</c> time, when deciding WHICH revision string to pin for a
/// <c>ConsistencyRequirement</c>. Once a revision string is chosen, every hop reads a snapshot pinned
/// at exactly that string (<see cref="Spiceport.Datastore.IDatastore.SnapshotReader"/> is a pure
/// function of the revision value, not of why it was chosen) — there is no caller-side branch cache
/// left to protect from folding an exact read into a quantized bucket (that dispatcher was removed).
/// So two sub-problems with the identical revision string always compute the identical answer
/// regardless of the mode that produced the string, and sharing one grain activation (and its
/// activation memo) between them is exact, not approximate, for both.
/// </para>
/// </remarks>
internal static class GrainKey
{
    private const char Separator = '/';

    public static string Build(
        ObjectAndRelation resource,
        ObjectAndRelation subject,
        string revision,
        string schemaHash) =>
        string.Join(Separator, [
            Escape(resource.ObjectType),
            Escape(resource.ObjectId),
            Escape(resource.Relation),
            Escape(subject.ObjectType),
            Escape(subject.ObjectId),
            Escape(subject.Relation),
            Escape(revision),
            Escape(schemaHash),
        ]);

    public static GrainKeyParts Parse(string key)
    {
        var parts = key.Split(Separator);
        if (parts.Length != 8)
            throw new FormatException($"Malformed check-grain key (expected 8 segments): '{key}'.");

        return new GrainKeyParts(
            new ObjectAndRelation(Unescape(parts[0]), Unescape(parts[1]), Unescape(parts[2])),
            new ObjectAndRelation(Unescape(parts[3]), Unescape(parts[4]), Unescape(parts[5])),
            Unescape(parts[6]),
            Unescape(parts[7]));
    }

    private static string Escape(string s) => Uri.EscapeDataString(s);

    private static string Unescape(string s) => Uri.UnescapeDataString(s);
}

/// <summary>The decoded components of an <c>ICheckGrain</c> string key.</summary>
internal sealed record GrainKeyParts(
    ObjectAndRelation Resource,
    ObjectAndRelation Subject,
    string Revision,
    string SchemaHash);
