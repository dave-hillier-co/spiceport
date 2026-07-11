using Spiceport.Core;

namespace Spiceport.Grains;

/// <summary>
/// Encodes and decodes the <c>ISubjectFrontierGrain</c> string key, which IS the canonical identity of
/// "the pre-context subject frontier of resource#relation for subjectType(#subjectRelation) at
/// (quantizedRevision, schemaHash)": <c>resType/resId/relation/subjType/subjRelation/quantizedRevision/schemaHash</c>.
/// </summary>
/// <remarks>
/// Mirrors <see cref="GrainKey"/>'s escaping/parsing conventions exactly (see its remarks for why the key
/// carries no optimized-vs-exact mode segment): components are URL-style escaped so a literal separator in
/// any field cannot corrupt the key, and two requests naming the identical revision string always compute
/// the identical frontier regardless of the consistency mode that produced that string.
/// </remarks>
internal static class SubjectFrontierKey
{
    private const char Separator = '/';

    public static string Build(
        ObjectAndRelation resource,
        string subjectType,
        string subjectRelation,
        string revision,
        string schemaHash) =>
        string.Join(Separator, [
            Escape(resource.ObjectType),
            Escape(resource.ObjectId),
            Escape(resource.Relation),
            Escape(subjectType),
            Escape(subjectRelation),
            Escape(revision),
            Escape(schemaHash),
        ]);

    public static SubjectFrontierKeyParts Parse(string key)
    {
        var parts = key.Split(Separator);
        if (parts.Length != 7)
            throw new FormatException($"Malformed subject-frontier-grain key (expected 7 segments): '{key}'.");

        return new SubjectFrontierKeyParts(
            new ObjectAndRelation(Unescape(parts[0]), Unescape(parts[1]), Unescape(parts[2])),
            Unescape(parts[3]),
            Unescape(parts[4]),
            Unescape(parts[5]),
            Unescape(parts[6]));
    }

    private static string Escape(string s) => Uri.EscapeDataString(s);

    private static string Unescape(string s) => Uri.UnescapeDataString(s);
}

/// <summary>The decoded components of an <c>ISubjectFrontierGrain</c> string key.</summary>
internal sealed record SubjectFrontierKeyParts(
    ObjectAndRelation Resource,
    string SubjectType,
    string SubjectRelation,
    string Revision,
    string SchemaHash);
