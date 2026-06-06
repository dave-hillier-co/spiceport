namespace Spiceport.Core;

/// <summary>
/// A complete relationship (tuple): a resource and subject, plus optional caveat,
/// expiration, and integrity metadata.
/// </summary>
/// <param name="Reference">The identifying resource + subject pair.</param>
/// <param name="OptionalCaveat">Optional contextualized caveat.</param>
/// <param name="OptionalExpiration">Optional expiration time after which the relationship is no longer valid.</param>
/// <param name="OptionalIntegrity">Optional cryptographic integrity metadata.</param>
public sealed record Relationship(
    RelationshipReference Reference,
    ContextualizedCaveat? OptionalCaveat = null,
    DateTimeOffset? OptionalExpiration = null,
    RelationshipIntegrity? OptionalIntegrity = null)
{
    /// <summary>The resource ONR.</summary>
    public ObjectAndRelation Resource => Reference.Resource;

    /// <summary>The subject ONR.</summary>
    public ObjectAndRelation Subject => Reference.Subject;

    /// <summary>Creates a relationship from a resource and subject ONR.</summary>
    public static Relationship Create(
        ObjectAndRelation resource,
        ObjectAndRelation subject,
        ContextualizedCaveat? caveat = null,
        DateTimeOffset? expiration = null,
        RelationshipIntegrity? integrity = null) =>
        new(new RelationshipReference(resource, subject), caveat, expiration, integrity);

    /// <summary>Returns a copy without integrity metadata.</summary>
    public Relationship WithoutIntegrity() => this with { OptionalIntegrity = null };

    /// <summary>Returns a copy with a different (or no) caveat.</summary>
    public Relationship WithCaveat(ContextualizedCaveat? caveat) => this with { OptionalCaveat = caveat };

    /// <summary>
    /// Validates the relationship. Throws <see cref="ArgumentException"/> if any field is empty
    /// or if the resource object id is a wildcard (resources may never be wildcards).
    /// </summary>
    public void Validate()
    {
        if (string.IsNullOrEmpty(Resource.ObjectType) || string.IsNullOrEmpty(Resource.ObjectId) || string.IsNullOrEmpty(Resource.Relation))
            throw new ArgumentException("relationship resource must be fully specified");
        if (string.IsNullOrEmpty(Subject.ObjectType) || string.IsNullOrEmpty(Subject.ObjectId) || string.IsNullOrEmpty(Subject.Relation))
            throw new ArgumentException("relationship subject must be fully specified");
        if (Resource.IsPublicWildcard)
            throw new ArgumentException("relationship resource object id may not be a wildcard '*'");
    }

    /// <summary>Formats the relationship as a canonical SpiceDB tuple string.</summary>
    public override string ToString() => TupleStrings.FormatRelationship(this);
}
