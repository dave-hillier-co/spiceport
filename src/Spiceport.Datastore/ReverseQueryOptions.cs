using Spiceport.Core;

namespace Spiceport.Datastore;

/// <summary>The sort order a reverse query yields its relationships in.</summary>
public enum ReverseQuerySort
{
    /// <summary>No ordering guarantee (the default; cheapest).</summary>
    Unsorted = 0,

    /// <summary>
    /// Subject-first total order: <c>(subjectType, subjectId, subjectRelation, resourceType, resourceId,
    /// resourceRelation)</c> by ordinal string comparison. Port of SpiceDB's <c>BySubject</c> sort, the
    /// stable order LookupResources uses for cursored, resumable reverse traversal.
    /// </summary>
    BySubject = 1,
}

/// <summary>
/// Options for <see cref="IDatastoreReader.ReverseQueryRelationships"/> controlling ordering and keyset
/// resumption. The default (no options) preserves the original unordered, unbounded behaviour.
/// </summary>
/// <remarks>
/// When <paramref name="Sort"/> is <see cref="ReverseQuerySort.BySubject"/> the relationships are
/// returned in a deterministic total order. <paramref name="After"/> resumes that order strictly after a
/// previously-returned relationship (exclusive keyset), so a caller can page without re-seeing rows. The
/// full six-tuple is the relationship's primary key, so the order has no ties and resumption is exact.
/// </remarks>
/// <param name="Sort">The ordering to apply.</param>
/// <param name="After">An exclusive keyset position to resume after; requires a non-Unsorted sort.</param>
public sealed record ReverseQueryOptions(
    ReverseQuerySort Sort = ReverseQuerySort.Unsorted,
    RelationshipReference? After = null)
{
    /// <summary>The <see cref="ReverseQuerySort.BySubject"/> comparison over two relationships.</summary>
    public static int CompareBySubject(Relationship x, Relationship y) =>
        CompareBySubject(x.Reference, y.Reference);

    /// <summary>The <see cref="ReverseQuerySort.BySubject"/> comparison over two relationship references.</summary>
    public static int CompareBySubject(RelationshipReference x, RelationshipReference y)
    {
        int c;
        if ((c = string.CompareOrdinal(x.Subject.ObjectType, y.Subject.ObjectType)) != 0) return c;
        if ((c = string.CompareOrdinal(x.Subject.ObjectId, y.Subject.ObjectId)) != 0) return c;
        if ((c = string.CompareOrdinal(x.Subject.Relation, y.Subject.Relation)) != 0) return c;
        if ((c = string.CompareOrdinal(x.Resource.ObjectType, y.Resource.ObjectType)) != 0) return c;
        if ((c = string.CompareOrdinal(x.Resource.ObjectId, y.Resource.ObjectId)) != 0) return c;
        return string.CompareOrdinal(x.Resource.Relation, y.Resource.Relation);
    }
}
