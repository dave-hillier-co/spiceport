using System.Collections.Immutable;

namespace Spiceport.Core;

/// <summary>The kind of subject permitted by an <see cref="AllowedRelation"/>.</summary>
public enum AllowedRelationKind
{
    /// <summary>A specific subject type (optionally with a subrelation).</summary>
    Relation = 0,

    /// <summary>A public wildcard: all subjects of the given type (e.g. <c>user:*</c>).</summary>
    PublicWildcard = 1,
}

/// <summary>
/// References a caveat required on relationships that match an <see cref="AllowedRelation"/>.
/// </summary>
/// <param name="CaveatName">The name of the required caveat.</param>
public sealed record AllowedCaveat(string CaveatName);

/// <summary>
/// Placeholder trait indicating an allowed relation requires an expiration.
/// Mirrors SpiceDB's currently-empty ExpirationTrait proto message.
/// </summary>
public sealed record ExpirationTrait;

/// <summary>
/// Defines one permitted subject type for a base relation.
/// </summary>
/// <param name="ObjectType">The subject namespace (e.g. "user", "org/user").</param>
/// <param name="Kind">Whether this is a specific relation or a public wildcard.</param>
/// <param name="RelationName">
/// The subject's subrelation when <paramref name="Kind"/> is <see cref="AllowedRelationKind.Relation"/>.
/// Use <see cref="CoreConstants.Ellipsis"/> for direct subjects.
/// </param>
/// <param name="RequiredCaveat">Optional caveat that must be present on matching relationships.</param>
/// <param name="RequiresExpiration">Whether matching relationships must carry an expiration.</param>
public sealed record AllowedRelation(
    string ObjectType,
    AllowedRelationKind Kind,
    string? RelationName = null,
    AllowedCaveat? RequiredCaveat = null,
    bool RequiresExpiration = false)
{
    /// <summary>True if this allowed relation is a public wildcard.</summary>
    public bool IsPublicWildcard => Kind == AllowedRelationKind.PublicWildcard;

    /// <summary>Creates an allowed direct subject type with the given subrelation (default ellipsis).</summary>
    public static AllowedRelation Direct(
        string objectType,
        string relationName = CoreConstants.Ellipsis,
        AllowedCaveat? requiredCaveat = null,
        bool requiresExpiration = false) =>
        new(objectType, AllowedRelationKind.Relation, relationName, requiredCaveat, requiresExpiration);

    /// <summary>Creates a public wildcard allowed relation for the given type.</summary>
    public static AllowedRelation Wildcard(
        string objectType,
        AllowedCaveat? requiredCaveat = null,
        bool requiresExpiration = false) =>
        new(objectType, AllowedRelationKind.PublicWildcard, null, requiredCaveat, requiresExpiration);
}

/// <summary>
/// The set of subject types allowed for a base relation.
/// </summary>
/// <param name="AllowedDirectRelations">The allowed subject types.</param>
public sealed record TypeInformation(ImmutableList<AllowedRelation> AllowedDirectRelations);
