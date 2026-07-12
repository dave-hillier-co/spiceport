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
    // schemaHash scopes the routing keyspace (a schema change yields a fresh set of grain identities for
    // structurally-identical sub-problems) AND names the schema the grain must evaluate under: CheckGrain
    // resolves the compiled schema for this hash at its pinned revision (see CheckGrain), so the schema is a
    // pure function of the key's revision rather than the grain's local ISchemaProvider.Current.
    public static string Build(
        ObjectAndRelation resource,
        ObjectAndRelation subject,
        string revision,
        string schemaHash) =>
        GrainKeyCodec.Join(
            resource.ObjectType,
            resource.ObjectId,
            resource.Relation,
            subject.ObjectType,
            subject.ObjectId,
            subject.Relation,
            revision,
            schemaHash);

    public static GrainKeyParts Parse(string key)
    {
        var parts = GrainKeyCodec.Split(key, 8);

        // parts[7] (schemaHash) is carried back: the grain resolves the compiled schema for it at the
        // pinned revision (see CheckGrain), so evaluation is a pure function of the key's revision.
        return new GrainKeyParts(
            new ObjectAndRelation(parts[0], parts[1], parts[2]),
            new ObjectAndRelation(parts[3], parts[4], parts[5]),
            parts[6],
            parts[7]);
    }
}

/// <summary>The decoded components of an <c>ICheckGrain</c> string key.</summary>
internal sealed record GrainKeyParts(
    ObjectAndRelation Resource,
    ObjectAndRelation Subject,
    string Revision,
    string SchemaHash);
