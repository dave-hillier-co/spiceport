using Spiceport.Core;
using Spiceport.Grains.Abstractions;
using Spiceport.Schema;

namespace Spiceport.Grains;

/// <summary>
/// Write-time schema validation of relationship updates, mirroring SpiceDB's
/// <c>relationships.ValidateRelationshipUpdates</c> / <c>ValidateOneRelationship</c>
/// (<c>internal/relationships/validation.go</c>): every update's resource definition and relation, and its
/// subject definition and (non-ellipsis) subrelation, must exist; the relation must not be a permission;
/// and the subject (type, subrelation or wildcard, caveat, expiration) must be one of the relation's
/// allowed direct relations. A DELETE uses SpiceDB's deletion rule, which ignores the caveat and
/// expiration traits when no caveat is given.
/// </summary>
/// <remarks>
/// Not ported: SpiceDB's check of a relationship's caveat-context values against the caveat's declared
/// parameter types (<c>caveats.ConvertContextToParameters</c> with <c>ErrorForUnknownParameters</c>).
/// </remarks>
internal static class RelationshipSchemaValidator
{
    /// <summary>Which SpiceDB validation rule an update is checked under.</summary>
    public enum Rule
    {
        /// <summary><c>ValidateRelationshipForCreateOrTouch</c>: the subject must match an allowed type exactly.</summary>
        CreateOrTouch,

        /// <summary><c>ValidateRelationshipForDeletion</c>: an unspecified caveat is ignored.</summary>
        Deletion,
    }

    /// <summary>
    /// Validates every update in request order under its operation's rule, throwing a
    /// <see cref="RelationshipValidationException"/> for the first violation.
    /// </summary>
    public static void ValidateUpdates(CompiledSchema schema, IEnumerable<RelationshipUpdateWire> updates)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(updates);

        var byName = ByName(schema);
        foreach (var update in updates)
        {
            var rule = update.Operation == RelationshipUpdateOpWire.Delete ? Rule.Deletion : Rule.CreateOrTouch;
            ValidateOne(byName, WireConvert.ToRelationship(update.Relationship), rule);
        }
    }

    /// <summary>Validates relationships that will be created or touched (a bulk import's rows).</summary>
    public static void ValidateForCreateOrTouch(CompiledSchema schema, IEnumerable<RelationshipWire> relationships)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ArgumentNullException.ThrowIfNull(relationships);

        var byName = ByName(schema);
        foreach (var rel in relationships)
            ValidateOne(byName, WireConvert.ToRelationship(rel), Rule.CreateOrTouch);
    }

    private static Dictionary<string, NamespaceDefinition> ByName(CompiledSchema schema) =>
        schema.Namespaces.ToDictionary(n => n.Name, StringComparer.Ordinal);

    private static void ValidateOne(
        IReadOnlyDictionary<string, NamespaceDefinition> byName, Relationship rel, Rule rule)
    {
        // The resource definition and relation must exist (permissions count as existing here; the
        // permission-write rejection comes after the subject checks, in SpiceDB's order).
        if (!byName.TryGetValue(rel.Resource.ObjectType, out var resourceDef))
            throw NamespaceNotFound(rel.Resource.ObjectType);

        var relation = FindRelation(resourceDef, rel.Resource.Relation)
            ?? throw RelationNotFound(rel.Resource.ObjectType, rel.Resource.Relation);

        // The subject definition, and its subrelation unless it is the ellipsis, must exist.
        if (!byName.TryGetValue(rel.Subject.ObjectType, out var subjectDef))
            throw NamespaceNotFound(rel.Subject.ObjectType);

        if (rel.Subject.Relation != CoreConstants.Ellipsis && FindRelation(subjectDef, rel.Subject.Relation) is null)
            throw RelationNotFound(rel.Subject.ObjectType, rel.Subject.Relation);

        if (relation.IsPermission)
        {
            throw new RelationshipValidationException(
                RelationshipValidationErrorKind.Invalid,
                $"cannot write a relationship to permission `{rel.Resource.Relation}` under definition `{rel.Resource.ObjectType}`");
        }

        var caveatName = rel.OptionalCaveat?.CaveatName;
        var isWildcard = rel.Subject.ObjectId == CoreConstants.PublicWildcard;
        var written = isWildcard
            ? AllowedRelation.Wildcard(rel.Subject.ObjectType, Caveat(caveatName), rel.OptionalExpiration is not null)
            : AllowedRelation.Direct(rel.Subject.ObjectType, rel.Subject.Relation, Caveat(caveatName), rel.OptionalExpiration is not null);
        var writtenSource = AllowedRelationIdentity.Source(written);
        var allowed = relation.TypeInformation?.AllowedDirectRelations ?? [];

        bool isAllowed;
        if (rule == Rule.CreateOrTouch || caveatName is not null)
        {
            // Writing, or a caveat was named: the subject must match an allowed type exactly, traits and all.
            isAllowed = allowed.Any(a => AllowedRelationIdentity.Source(a) == writtenSource);
        }
        else if (isWildcard)
        {
            // Deletion with no caveat: any wildcard of the subject type, whatever its traits.
            isAllowed = allowed.Any(a => a.IsPublicWildcard && a.ObjectType == rel.Subject.ObjectType);
        }
        else
        {
            // Deletion with no caveat: any direct type with the subject's type and subrelation.
            isAllowed = allowed.Any(a =>
                !a.IsPublicWildcard
                && a.ObjectType == rel.Subject.ObjectType
                && (a.RelationName ?? CoreConstants.Ellipsis) == rel.Subject.Relation);
        }

        if (!isAllowed)
            throw InvalidSubjectType(rel, writtenSource, allowed);
    }

    private static Relation? FindRelation(NamespaceDefinition def, string name) =>
        def.Relations.FirstOrDefault(r => r.Name == name);

    private static AllowedCaveat? Caveat(string? name) => name is null ? null : new AllowedCaveat(name);

    private static RelationshipValidationException NamespaceNotFound(string name) =>
        new(RelationshipValidationErrorKind.NotFound, $"object definition `{name}` not found");

    private static RelationshipValidationException RelationNotFound(string ns, string relation) =>
        new(RelationshipValidationErrorKind.NotFound,
            $"relation/permission `{relation}` not found under definition `{ns}`");

    /// <summary>
    /// SpiceDB's <c>NewInvalidSubjectTypeError</c> wording: an uncaveated subject whose type is allowed only
    /// with caveats names those caveats; otherwise the closest allowed type (by the fuzzy subsequence
    /// match and Levenshtein rank SpiceDB uses) is offered as a suggestion when there is one.
    /// </summary>
    private static RelationshipValidationException InvalidSubjectType(
        Relationship rel, string writtenSource, IReadOnlyList<AllowedRelation> allowed)
    {
        var target = $"{rel.Resource.ObjectType}#{rel.Resource.Relation}";

        if (rel.OptionalCaveat is null)
        {
            var requiredCaveats = allowed
                .Where(a => a.RequiredCaveat is { CaveatName.Length: > 0 }
                    && a.ObjectType == rel.Subject.ObjectType
                    // SpiceDB compares allowedType.GetRelation(), which is "" for a wildcard
                    // allowed type, so a wildcard type never matches a subject's relation.
                    && !a.IsPublicWildcard
                    && (a.RelationName ?? CoreConstants.Ellipsis) == rel.Subject.Relation
                    && rel.Subject.ObjectId != CoreConstants.PublicWildcard
                    && a.RequiresExpiration == (rel.OptionalExpiration is not null))
                .Select(a => a.RequiredCaveat!.CaveatName)
                .Distinct(StringComparer.Ordinal)
                .ToList();
            if (requiredCaveats.Count > 0)
            {
                return new RelationshipValidationException(
                    RelationshipValidationErrorKind.Invalid,
                    $"subjects of type `{writtenSource}` are not allowed on relation `{target}` without one of the following caveats: {string.Join(",", requiredCaveats)}");
            }
        }

        var suggestion = allowed
            .Select(AllowedRelationIdentity.Source)
            .Where(candidate => IsSubsequence(writtenSource, candidate))
            .Select((candidate, index) => (candidate, index, distance: Levenshtein(writtenSource, candidate)))
            .OrderBy(m => m.distance)
            .ThenBy(m => m.index)
            .Select(m => m.candidate)
            .FirstOrDefault();

        var message = suggestion is null
            ? $"subjects of type `{writtenSource}` are not allowed on relation `{target}`"
            : $"subjects of type `{writtenSource}` are not allowed on relation `{target}`; did you mean `{suggestion}`?";
        return new RelationshipValidationException(RelationshipValidationErrorKind.Invalid, message);
    }

    /// <summary>The fuzzy-match test SpiceDB's suggestion uses: every rune of source appears in target, in order.</summary>
    private static bool IsSubsequence(string source, string target)
    {
        var i = 0;
        foreach (var c in target)
        {
            if (i < source.Length && source[i] == c)
                i++;
        }
        return i == source.Length;
    }

    private static int Levenshtein(string a, string b)
    {
        var previous = new int[b.Length + 1];
        var current = new int[b.Length + 1];
        for (var j = 0; j <= b.Length; j++)
            previous[j] = j;

        for (var i = 1; i <= a.Length; i++)
        {
            current[0] = i;
            for (var j = 1; j <= b.Length; j++)
            {
                var cost = a[i - 1] == b[j - 1] ? 0 : 1;
                current[j] = Math.Min(Math.Min(current[j - 1] + 1, previous[j] + 1), previous[j - 1] + cost);
            }
            (previous, current) = (current, previous);
        }
        return previous[b.Length];
    }
}
