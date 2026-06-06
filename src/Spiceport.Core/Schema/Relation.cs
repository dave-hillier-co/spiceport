using System.Collections.Immutable;

namespace Spiceport.Core;

/// <summary>
/// A relation or permission within a <see cref="NamespaceDefinition"/>.
/// </summary>
/// <param name="Name">The relation/permission name.</param>
/// <param name="UsersetRewrite">
/// Non-null for a permission (a computed relation); null for a base relation (written as tuples).
/// </param>
/// <param name="TypeInformation">
/// The allowed subject types for a base relation. Typically null for permissions.
/// </param>
/// <param name="AliasingRelation">Optional relation this one is a canonical alias of.</param>
/// <param name="CanonicalCacheKey">Optional precomputed canonical cache key.</param>
public sealed record Relation(
    string Name,
    UsersetRewrite? UsersetRewrite = null,
    TypeInformation? TypeInformation = null,
    string? AliasingRelation = null,
    string? CanonicalCacheKey = null)
{
    /// <summary>True if this is a permission (has a userset rewrite).</summary>
    public bool IsPermission => UsersetRewrite is not null;

    /// <summary>Creates a base relation with the given allowed subject types.</summary>
    public static Relation BaseRelation(string name, params AllowedRelation[] allowedTypes) =>
        new(name, TypeInformation: new TypeInformation([.. allowedTypes]));

    /// <summary>Creates a permission with the given userset rewrite.</summary>
    public static Relation Permission(string name, UsersetRewrite rewrite) =>
        new(name, UsersetRewrite: rewrite);
}

/// <summary>
/// A schema definition for an object type (a SpiceDB "definition").
/// </summary>
/// <param name="Name">
/// The namespace name. May contain "/" path segments
/// (pattern <c>([a-z][a-z0-9_]{1,62}[a-z0-9]/)*[a-z][a-z0-9_]{1,62}[a-z0-9]</c>).
/// </param>
/// <param name="Relations">The relations and permissions, with names unique within the namespace.</param>
public sealed record NamespaceDefinition(string Name, ImmutableList<Relation> Relations)
{
    /// <summary>Creates a namespace definition from a name and relations.</summary>
    public static NamespaceDefinition Create(string name, params Relation[] relations) =>
        new(name, [.. relations]);
}
