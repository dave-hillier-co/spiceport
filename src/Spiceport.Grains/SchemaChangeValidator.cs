using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;
using Spiceport.Schema;

namespace Spiceport.Grains;

/// <summary>
/// Validates that swapping the current compiled schema for a new one will not leave existing
/// relationships dangling. It diffs the two schemas for REMOVALS — a removed definition, a removed
/// relation, or a removed allowed subject type — and, for each removal, queries the datastore (via the
/// write transaction's reader, against the snapshot the swap commits at) for any relationship that still
/// references it. If one is found the change is rejected with <see cref="SchemaWriteValidationException"/>.
/// </summary>
/// <remarks>
/// Mirrors SpiceDB's <c>sanityCheckNamespaceChanges</c> / <c>ensureNoRelationshipsExistWithResourceType</c>.
/// What is intentionally NOT rejected here (always safe, or deferred):
/// <list type="bullet">
/// <item>Adding definitions / relations / allowed subject types.</item>
/// <item>Permission-only changes (a permission is computed, never written as a relationship).</item>
/// <item>Removing a permission (permissions hold no stored relationships).</item>
/// <item>Caveat parameter type changes (deferred; checked by SpiceDB's caveat diff).</item>
/// </list>
/// Removing an allowed subject type is checked from the subject side via the reverse query.
/// </remarks>
public static class SchemaChangeValidator
{
    /// <summary>
    /// Throws <see cref="SchemaWriteValidationException"/> if applying <paramref name="next"/> in place of
    /// <paramref name="current"/> would orphan any relationship in <paramref name="reader"/>.
    /// </summary>
    public static async Task ValidateAsync(
        CompiledSchema current,
        CompiledSchema next,
        IDatastoreReader reader,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(current);
        ArgumentNullException.ThrowIfNull(next);
        ArgumentNullException.ThrowIfNull(reader);

        var nextByName = next.Namespaces.ToDictionary(n => n.Name);

        foreach (var existingDef in current.Namespaces)
        {
            if (!nextByName.TryGetValue(existingDef.Name, out var nextDef))
            {
                // Whole definition removed: reject if ANY relationship has it as the resource type.
                await EnsureNoResourceTypeAsync(reader, existingDef.Name, cancellationToken)
                    .ConfigureAwait(false);
                continue;
            }

            var nextRelations = nextDef.Relations.ToDictionary(r => r.Name);

            foreach (var existingRel in existingDef.Relations)
            {
                // Permissions are computed, never written as relationships: removing one is always safe.
                if (existingRel.IsPermission)
                    continue;

                if (!nextRelations.TryGetValue(existingRel.Name, out var nextRel) || nextRel.IsPermission)
                {
                    // Base relation removed (or turned into a permission): reject if any relationship is
                    // written under resource-type#relation, or references it as subject-type#relation.
                    await EnsureNoRelationAsync(reader, existingDef.Name, existingRel.Name, cancellationToken)
                        .ConfigureAwait(false);
                    continue;
                }

                // Relation kept: reject removal of an allowed subject type that is still referenced.
                await EnsureNoRemovedAllowedTypesAsync(reader, existingDef.Name, existingRel, nextRel, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
    }

    private static async Task EnsureNoResourceTypeAsync(
        IDatastoreReader reader, string resourceType, CancellationToken ct)
    {
        var filter = new RelationshipsFilter { OptionalResourceType = resourceType };
        await foreach (var rel in reader.QueryRelationships(filter, ct).ConfigureAwait(false))
        {
            throw new SchemaWriteValidationException(
                $"cannot remove definition `{resourceType}`: at least one relationship still references it as a resource type (e.g. {rel})");
        }
    }

    private static async Task EnsureNoRelationAsync(
        IDatastoreReader reader, string definition, string relation, CancellationToken ct)
    {
        // Left side: written under resource-type#relation.
        var resourceFilter = new RelationshipsFilter
        {
            OptionalResourceType = definition,
            OptionalResourceRelation = relation,
        };
        await foreach (var rel in reader.QueryRelationships(resourceFilter, ct).ConfigureAwait(false))
        {
            throw new SchemaWriteValidationException(
                $"cannot remove relation `{relation}` in definition `{definition}`: at least one relationship still references it (e.g. {rel})");
        }

        // Right side: referenced as a subject of this type with this subrelation.
        var subjectFilter = new SubjectsFilter(
            SubjectType: definition,
            RelationFilter: new SubjectRelationFilter(NonEllipsisRelation: relation));
        await foreach (var rel in reader.ReverseQueryRelationships(subjectFilter, ct).ConfigureAwait(false))
        {
            throw new SchemaWriteValidationException(
                $"cannot remove relation `{relation}` in definition `{definition}`: at least one relationship references it as part of a subject (e.g. {rel})");
        }
    }

    private static async Task EnsureNoRemovedAllowedTypesAsync(
        IDatastoreReader reader, string definition, Relation existingRel, Relation nextRel, CancellationToken ct)
    {
        var existingAllowed = existingRel.TypeInformation?.AllowedDirectRelations;
        if (existingAllowed is null || existingAllowed.Count == 0)
            return;

        var nextAllowed = nextRel.TypeInformation?.AllowedDirectRelations;

        foreach (var allowed in existingAllowed)
        {
            if (nextAllowed is not null && nextAllowed.Any(a => SameAllowedType(a, allowed)))
                continue; // still allowed.

            // This allowed subject type was removed: reject if any relationship under
            // definition#relation has a subject matching it. A direct subject (ellipsis subrelation)
            // and a subrelation subject (e.g. group#member) need different relation filters.
            var subjectRelation = allowed.RelationName ?? CoreConstants.Ellipsis;
            var relationFilter = subjectRelation == CoreConstants.Ellipsis
                ? new SubjectRelationFilter(IncludeEllipsisRelation: true)
                : new SubjectRelationFilter(NonEllipsisRelation: subjectRelation);
            var subjectIds = allowed.IsPublicWildcard
                ? (IReadOnlyList<string>)[CoreConstants.PublicWildcard]
                : null;

            var filter = new RelationshipsFilter
            {
                OptionalResourceType = definition,
                OptionalResourceRelation = existingRel.Name,
                OptionalSubjectsSelectors =
                [
                    new SubjectsSelector(
                        OptionalSubjectType: allowed.ObjectType,
                        OptionalSubjectIds: subjectIds,
                        RelationFilter: relationFilter),
                ],
            };

            await foreach (var rel in reader.QueryRelationships(filter, ct).ConfigureAwait(false))
            {
                var desc = allowed.IsPublicWildcard ? $"{allowed.ObjectType}:*" : allowed.ObjectType;
                throw new SchemaWriteValidationException(
                    $"cannot remove allowed subject type `{desc}` from `{definition}#{existingRel.Name}`: at least one relationship still references it (e.g. {rel})");
            }
        }
    }

    private static bool SameAllowedType(AllowedRelation a, AllowedRelation b) =>
        a.ObjectType == b.ObjectType
        && a.Kind == b.Kind
        && (a.RelationName ?? CoreConstants.Ellipsis) == (b.RelationName ?? CoreConstants.Ellipsis);
}
