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
/// </list>
/// Caveat parameter removal and parameter type changes ARE rejected unconditionally (no datastore
/// query), mirroring SpiceDB's <c>sanityCheckCaveatChanges</c>, since existing relationships may carry
/// context typed by the old parameter.
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

        // Reuse the shared diff core, then run datastore orphan checks only on the removal deltas. Permission
        // removals / additions / allowed-type additions are always safe and carry no datastore check.
        foreach (var delta in SchemaDiff.Compute(current, next))
        {
            switch (delta)
            {
                case SchemaDelta.DefinitionRemoved(var def):
                    // Whole definition removed: reject if ANY relationship has it as the resource type.
                    await EnsureNoResourceTypeAsync(reader, def.Name, cancellationToken)
                        .ConfigureAwait(false);
                    break;

                case SchemaDelta.RelationRemoved(var defName, var rel):
                    // Base relation removed (or turned into a permission): reject if any relationship is
                    // written under resource-type#relation, or references it as subject-type#relation.
                    await EnsureNoRelationAsync(reader, defName, rel.Name, cancellationToken)
                        .ConfigureAwait(false);
                    break;

                case SchemaDelta.RelationSubjectTypeRemoved(var defName, var rel, var allowed):
                    // Allowed subject type removed: reject if any relationship still references it.
                    await EnsureNoRemovedAllowedTypeAsync(reader, defName, rel.Name, allowed, cancellationToken)
                        .ConfigureAwait(false);
                    break;

                case SchemaDelta.CaveatParameterRemoved(var caveatName, var paramName, _):
                    // SpiceDB's sanityCheckCaveatChanges rejects parameter removal unconditionally (existing
                    // relationships may carry context typed by the old parameter).
                    throw new SchemaWriteValidationException(
                        $"cannot remove parameter `{paramName}` on caveat `{caveatName}`");

                case SchemaDelta.CaveatParameterTypeChanged(var caveatName, var paramName, _, _):
                    // Likewise, a parameter type change is rejected unconditionally.
                    throw new SchemaWriteValidationException(
                        $"cannot change the type of parameter `{paramName}` on caveat `{caveatName}`");
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
        await foreach (var rel in reader.ReverseQueryRelationships(subjectFilter, cancellationToken: ct).ConfigureAwait(false))
        {
            throw new SchemaWriteValidationException(
                $"cannot remove relation `{relation}` in definition `{definition}`: at least one relationship references it as part of a subject (e.g. {rel})");
        }
    }

    private static async Task EnsureNoRemovedAllowedTypeAsync(
        IDatastoreReader reader, string definition, string relationName, AllowedRelation allowed, CancellationToken ct)
    {
        // This allowed subject type was removed: reject if any relationship under definition#relation has a
        // subject matching it. A direct subject (ellipsis subrelation) and a subrelation subject (e.g.
        // group#member) need different relation filters.
        var subjectRelation = allowed.RelationName ?? CoreConstants.Ellipsis;
        var relationFilter = subjectRelation == CoreConstants.Ellipsis
            ? new SubjectRelationFilter(IncludeEllipsisRelation: true)
            : new SubjectRelationFilter(NonEllipsisRelation: subjectRelation);
        var subjectIds = allowed.IsPublicWildcard
            ? (IReadOnlyList<string>)[CoreConstants.PublicWildcard]
            : null;

        // Mirror SpiceDB's RelationAllowedTypeRemoved orphan check: the removed allowed type's identity
        // includes its required caveat and expiration trait, so the orphan query must filter on them too.
        // Removing `user with cav1` only orphans relationships that actually carry cav1 (not cav2/no-caveat);
        // removing `user with expiration` only orphans relationships that carry an expiration.
        var caveatFilter = allowed.RequiredCaveat is { CaveatName.Length: > 0 } caveat
            ? new CaveatNameFilter(CaveatFilterOption.HasMatchingCaveat, caveat.CaveatName)
            : new CaveatNameFilter(CaveatFilterOption.NoCaveat);
        var expirationOption = allowed.RequiresExpiration
            ? ExpirationFilterOption.HasExpiration
            : ExpirationFilterOption.NoExpiration;

        var filter = new RelationshipsFilter
        {
            OptionalResourceType = definition,
            OptionalResourceRelation = relationName,
            OptionalSubjectsSelectors =
            [
                new SubjectsSelector(
                    OptionalSubjectType: allowed.ObjectType,
                    OptionalSubjectIds: subjectIds,
                    RelationFilter: relationFilter),
            ],
            OptionalCaveatNameFilter = caveatFilter,
            OptionalExpirationOption = expirationOption,
        };

        await foreach (var rel in reader.QueryRelationships(filter, ct).ConfigureAwait(false))
        {
            throw new SchemaWriteValidationException(
                $"cannot remove allowed subject type `{AllowedRelationIdentity.Source(allowed)}` from `{definition}#{relationName}`: at least one relationship still references it (e.g. {rel})");
        }
    }
}
