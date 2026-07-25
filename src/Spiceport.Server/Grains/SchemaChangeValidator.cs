using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;
using Spiceport.Schema;

namespace Spiceport.Grains;

/// <summary>
/// One diff-derived guard a schema change must pass before it may commit. A closed union so the checks
/// can be BOTH evaluated client-side (against a pinned reader, producing the descriptive
/// <see cref="SchemaWriteValidationException"/> messages) AND — for the data-dependent shape — attached
/// to the schema-write <see cref="CommitRequest"/> as MUST_NOT_MATCH preconditions, so the sequencer
/// grain re-proves data-nonexistence atomically at the commit snapshot (closing the window between the
/// client-side validation read and the commit).
/// </summary>
public abstract record SchemaChangeCheck
{
    private SchemaChangeCheck() { }

    /// <summary>
    /// An unconditional rejection (no datastore query), e.g. a caveat parameter removal/type change.
    /// Evaluation throws <see cref="SchemaWriteValidationException"/> with <paramref name="Message"/>
    /// the moment it is reached, preserving the diff-order precedence of the historical inline checks.
    /// </summary>
    public sealed record Unconditional(string Message) : SchemaChangeCheck;

    /// <summary>
    /// A data-existence guard: the change is valid only if <paramref name="Filter"/> matches NO live
    /// relationship. <paramref name="Describe"/> renders the rejection message from the first offending
    /// relationship (the historical "(e.g. {rel})" text, byte-identical).
    /// </summary>
    public sealed record NoOrphans(RelationshipsFilter Filter, Func<Relationship, string> Describe) : SchemaChangeCheck;
}

/// <summary>
/// Validates that swapping the current compiled schema for a new one will not leave existing
/// relationships dangling. It diffs the two schemas for REMOVALS — a removed definition, a removed
/// relation, or a removed allowed subject type — and, for each removal, queries the datastore for any
/// relationship that still references it. If one is found the change is rejected with
/// <see cref="SchemaWriteValidationException"/>.
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
/// The checks are computed as data (<see cref="SchemaChangeCheck"/>) via <see cref="ComputeChecks"/> so
/// the schema-write path can also ship the <see cref="SchemaChangeCheck.NoOrphans"/> filters to the
/// sequencer grain as commit preconditions; <see cref="ValidateAsync"/> remains the single-call
/// compute-and-evaluate form.
/// </remarks>
public static class SchemaChangeValidator
{
    /// <summary>
    /// Throws <see cref="SchemaWriteValidationException"/> if applying <paramref name="next"/> in place of
    /// <paramref name="current"/> would orphan any relationship in <paramref name="reader"/>.
    /// </summary>
    public static Task ValidateAsync(
        CompiledSchema current,
        CompiledSchema next,
        IDatastoreReader reader,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(current);
        ArgumentNullException.ThrowIfNull(next);
        ArgumentNullException.ThrowIfNull(reader);

        return EvaluateAsync(ComputeChecks(current, next), reader, cancellationToken);
    }

    /// <summary>
    /// Evaluates the computed checks in order against <paramref name="reader"/>: an
    /// <see cref="SchemaChangeCheck.Unconditional"/> throws immediately; a
    /// <see cref="SchemaChangeCheck.NoOrphans"/> throws (with the first offending relationship rendered
    /// into the message) if its filter matches anything. Iteration order preserves the diff-order
    /// precedence the historical inline checks had.
    /// </summary>
    public static Task EvaluateAsync(
        IReadOnlyList<SchemaChangeCheck> checks,
        IDatastoreReader reader,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(checks);
        ArgumentNullException.ThrowIfNull(reader);

        return EvaluateCore(checks, reader.QueryRelationships, cancellationToken);
    }

    /// <summary>
    /// Evaluates the computed checks against the storage-direct <see cref="ISnapshotScanner"/> seam at
    /// the pinned <paramref name="revision"/> — the schema-change data guards are broad existence scans,
    /// exactly the workload the scan seam serves (see <see cref="ISnapshotScanner"/>). Semantics are
    /// identical to the reader-based overload (which the reference-model path and tests keep).
    /// </summary>
    public static Task EvaluateAsync(
        IReadOnlyList<SchemaChangeCheck> checks,
        ISnapshotScanner scanner,
        IRevision revision,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(checks);
        ArgumentNullException.ThrowIfNull(scanner);
        ArgumentNullException.ThrowIfNull(revision);

        return EvaluateCore(checks, (filter, ct) => scanner.Scan(filter, revision, ct), cancellationToken);
    }

    private static async Task EvaluateCore(
        IReadOnlyList<SchemaChangeCheck> checks,
        Func<RelationshipsFilter, CancellationToken, IAsyncEnumerable<Relationship>> query,
        CancellationToken cancellationToken)
    {
        foreach (var check in checks)
        {
            switch (check)
            {
                case SchemaChangeCheck.Unconditional(var message):
                    throw new SchemaWriteValidationException(message);

                case SchemaChangeCheck.NoOrphans(var filter, var describe):
                    await foreach (var rel in query(filter, cancellationToken).ConfigureAwait(false))
                        throw new SchemaWriteValidationException(describe(rel));
                    break;
            }
        }
    }

    /// <summary>
    /// Computes the ordered guard list for replacing <paramref name="current"/> with
    /// <paramref name="next"/>: one <see cref="SchemaChangeCheck"/> per removal-delta datastore probe
    /// (in <see cref="SchemaDiff.Compute"/> order, two for a removed relation — resource side then
    /// subject side), plus <see cref="SchemaChangeCheck.Unconditional"/> entries for the caveat-parameter
    /// rejections. Pure (no datastore access): the caller decides where the filters are evaluated.
    /// </summary>
    public static IReadOnlyList<SchemaChangeCheck> ComputeChecks(CompiledSchema current, CompiledSchema next)
    {
        ArgumentNullException.ThrowIfNull(current);
        ArgumentNullException.ThrowIfNull(next);

        // Reuse the shared diff core, then derive orphan checks only from the removal deltas. Permission
        // removals / additions / allowed-type additions are always safe and carry no datastore check.
        var checks = new List<SchemaChangeCheck>();
        foreach (var delta in SchemaDiff.Compute(current, next))
        {
            switch (delta)
            {
                case SchemaDelta.DefinitionRemoved(var def):
                    // Whole definition removed: reject if ANY relationship has it as the resource type.
                    checks.Add(new SchemaChangeCheck.NoOrphans(
                        new RelationshipsFilter { OptionalResourceType = def.Name },
                        rel => $"cannot remove definition `{def.Name}`: at least one relationship still references it as a resource type (e.g. {rel})"));
                    break;

                case SchemaDelta.RelationRemoved(var defName, var rel):
                    // Base relation removed (or turned into a permission): reject if any relationship is
                    // written under resource-type#relation, or references it as subject-type#relation.
                    AddRelationRemovedChecks(checks, defName, rel.Name);
                    break;

                case SchemaDelta.RelationSubjectTypeRemoved(var defName, var rel, var allowed):
                    // Allowed subject type removed: reject if any relationship still references it.
                    checks.Add(RemovedAllowedTypeCheck(defName, rel.Name, allowed));
                    break;

                case SchemaDelta.CaveatParameterRemoved(var caveatName, var paramName, _):
                    // SpiceDB's sanityCheckCaveatChanges rejects parameter removal unconditionally (existing
                    // relationships may carry context typed by the old parameter).
                    checks.Add(new SchemaChangeCheck.Unconditional(
                        $"cannot remove parameter `{paramName}` on caveat `{caveatName}`"));
                    break;

                case SchemaDelta.CaveatParameterTypeChanged(var caveatName, var paramName, _, _):
                    // Likewise, a parameter type change is rejected unconditionally.
                    checks.Add(new SchemaChangeCheck.Unconditional(
                        $"cannot change the type of parameter `{paramName}` on caveat `{caveatName}`"));
                    break;
            }
        }

        return checks;
    }

    private static void AddRelationRemovedChecks(List<SchemaChangeCheck> checks, string definition, string relation)
    {
        // Left side: written under resource-type#relation.
        checks.Add(new SchemaChangeCheck.NoOrphans(
            new RelationshipsFilter
            {
                OptionalResourceType = definition,
                OptionalResourceRelation = relation,
            },
            rel => $"cannot remove relation `{relation}` in definition `{definition}`: at least one relationship still references it (e.g. {rel})"));

        // Right side: referenced as a subject of this type with this subrelation. Historically this was a
        // reverse scan (SubjectsFilter(SubjectType: definition, RelationFilter: NonEllipsisRelation)); it
        // is expressed here as a subject-only forward filter so it can also ride the schema-write commit
        // as a MUST_NOT_MATCH precondition. EQUIVALENCE: both MvccSnapshotReader.QueryRelationships and
        // MvccSnapshotReader.ReverseQueryRelationships (options null) enumerate the SAME set — every
        // non-expired row of state.LiveAt(revision) — and keep a row purely by their filter's Matches.
        // SubjectsFilter(SubjectType: D, RelationFilter: F).Matches(rel) with no subject ids and no
        // resource constraints reduces to (rel.Subject.ObjectType == D && F.Matches(rel.Subject.Relation));
        // RelationshipsFilter { OptionalSubjectsSelectors = [SubjectsSelector(D, null, F)] }.Matches(rel)
        // has no resource/caveat/expiration constraints set, so it reduces to SubjectsSelector.Matches =
        // (rel.Subject.ObjectType == D && F.Matches(rel.Subject.Relation)) — the identical predicate with
        // the identical SubjectRelationFilter F, hence the identical existence answer (and, both scans
        // running in the same storage order, the identical first offending example row).
        checks.Add(new SchemaChangeCheck.NoOrphans(
            new RelationshipsFilter
            {
                OptionalSubjectsSelectors =
                [
                    new SubjectsSelector(
                        OptionalSubjectType: definition,
                        RelationFilter: new SubjectRelationFilter(NonEllipsisRelation: relation)),
                ],
            },
            rel => $"cannot remove relation `{relation}` in definition `{definition}`: at least one relationship references it as part of a subject (e.g. {rel})"));
    }

    private static SchemaChangeCheck RemovedAllowedTypeCheck(
        string definition, string relationName, AllowedRelation allowed)
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

        return new SchemaChangeCheck.NoOrphans(
            filter,
            rel => $"cannot remove allowed subject type `{AllowedRelationIdentity.Source(allowed)}` from `{definition}#{relationName}`: at least one relationship still references it (e.g. {rel})");
    }
}
