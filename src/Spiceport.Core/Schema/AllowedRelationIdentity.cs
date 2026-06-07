namespace Spiceport.Core;

/// <summary>
/// Computes the canonical source-string identity of an <see cref="AllowedRelation"/>, mirroring
/// SpiceDB's <c>schema.SourceForAllowedRelation</c> (<c>pkg/schema/definition.go</c>).
/// </summary>
/// <remarks>
/// The identity folds the object type, the subrelation (ellipsis-normalized), the public-wildcard marker,
/// the required caveat name, and the expiration trait. Two allowed types are "the same" for diff and
/// duplicate-detection purposes iff their identities are equal, so a change of caveat name or of the
/// <c>with expiration</c> trait is a genuine difference (as it is in SpiceDB), not a no-op.
/// </remarks>
public static class AllowedRelationIdentity
{
    /// <summary>Returns the canonical source string for the given allowed relation.</summary>
    public static string Source(AllowedRelation allowed)
    {
        ArgumentNullException.ThrowIfNull(allowed);

        var hasCaveat = allowed.RequiredCaveat is not null;
        var hasExpiration = allowed.RequiresExpiration;

        var traits = string.Empty;
        if (hasCaveat || hasExpiration)
        {
            traits = " with ";
            if (hasCaveat)
                traits += allowed.RequiredCaveat!.CaveatName;
            if (hasCaveat && hasExpiration)
                traits += " and ";
            if (hasExpiration)
                traits += "expiration";
        }

        if (allowed.IsPublicWildcard)
            return $"{allowed.ObjectType}:*{traits}";

        var rel = allowed.RelationName ?? CoreConstants.Ellipsis;
        if (rel != CoreConstants.Ellipsis)
            return $"{allowed.ObjectType}#{rel}{traits}";

        return $"{allowed.ObjectType}{traits}";
    }
}
