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
    public static string Build(
        ObjectAndRelation resource,
        string subjectType,
        string subjectRelation,
        string revision,
        string schemaHash) =>
        GrainKeyCodec.Join(
            resource.ObjectType,
            resource.ObjectId,
            resource.Relation,
            subjectType,
            subjectRelation,
            revision,
            schemaHash);

    public static SubjectFrontierKeyParts Parse(string key)
    {
        var parts = GrainKeyCodec.Split(key, 7);

        return new SubjectFrontierKeyParts(
            new ObjectAndRelation(parts[0], parts[1], parts[2]),
            parts[3],
            parts[4],
            parts[5],
            parts[6]);
    }
}

/// <summary>The decoded components of an <c>ISubjectFrontierGrain</c> string key.</summary>
internal sealed record SubjectFrontierKeyParts(
    ObjectAndRelation Resource,
    string SubjectType,
    string SubjectRelation,
    string Revision,
    string SchemaHash);
