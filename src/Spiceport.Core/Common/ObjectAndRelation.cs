namespace Spiceport.Core;

/// <summary>
/// An object qualified by a relation name: <c>namespace:object_id#relation</c>.
/// Also known as an ONR. Used for both resources and subjects in a relationship.
/// </summary>
/// <param name="ObjectType">The namespace / definition name (e.g. "document", "user", "org/user").</param>
/// <param name="ObjectId">The object identifier. May be <see cref="CoreConstants.PublicWildcard"/> for subjects.</param>
/// <param name="Relation">The relation name, or <see cref="CoreConstants.Ellipsis"/> ("...") for subjects with no subrelation.</param>
public sealed record ObjectAndRelation(string ObjectType, string ObjectId, string Relation)
{
    /// <summary>Returns a copy with a different relation.</summary>
    public ObjectAndRelation WithRelation(string newRelation) => this with { Relation = newRelation };

    /// <summary>Drops the object id, returning the namespace/relation pair.</summary>
    public RelationReference AsRelationReference() => new(ObjectType, Relation);

    /// <summary>True if this ONR refers to a public wildcard subject.</summary>
    public bool IsPublicWildcard => ObjectId == CoreConstants.PublicWildcard;

    /// <summary>
    /// Formats as the canonical SpiceDB ONR string. An ellipsis relation is omitted
    /// (i.e. <c>namespace:object_id</c>), otherwise <c>namespace:object_id#relation</c>.
    /// </summary>
    public override string ToString() => TupleStrings.FormatObjectAndRelation(this);
}
