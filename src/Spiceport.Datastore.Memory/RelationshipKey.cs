using Spiceport.Core;

namespace Spiceport.Datastore.Memory;

/// <summary>
/// The full identity of a relationship for storage purposes:
/// resource type/id/relation and subject type/id/relation. Caveat, expiration and
/// integrity are payload, not identity.
/// </summary>
internal readonly record struct RelationshipKey(
    string ResourceType,
    string ResourceId,
    string ResourceRelation,
    string SubjectType,
    string SubjectId,
    string SubjectRelation)
{
    public static RelationshipKey From(Relationship rel) => new(
        rel.Resource.ObjectType,
        rel.Resource.ObjectId,
        rel.Resource.Relation,
        rel.Subject.ObjectType,
        rel.Subject.ObjectId,
        rel.Subject.Relation);
}
