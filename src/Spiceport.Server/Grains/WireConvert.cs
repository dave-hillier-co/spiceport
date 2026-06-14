using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// Conversions between the Orleans-serializable wire types and the core/datastore types. These are the
/// single copy of the relationship payload and full-filter conversions shared by the relationships grain
/// and the grain-backed datastore (do not duplicate the logic).
/// </summary>
internal static class WireConvert
{
    /// <summary>Converts a wire relationship to a core relationship (normalizing the subject relation).</summary>
    public static Relationship ToRelationship(RelationshipWire wire)
    {
        var resource = new ObjectAndRelation(wire.ResourceType, wire.ResourceId, wire.ResourceRelation);
        var subjectRelation = string.IsNullOrEmpty(wire.SubjectRelation) ? CoreConstants.Ellipsis : wire.SubjectRelation;
        var subject = new ObjectAndRelation(wire.SubjectType, wire.SubjectId, subjectRelation);
        var caveat = wire.CaveatName is { Length: > 0 }
            ? new ContextualizedCaveat(wire.CaveatName, wire.CaveatContext)
            : null;
        return Relationship.Create(resource, subject, caveat, wire.Expiration);
    }

    /// <summary>
    /// Converts a core relationship to its wire form. NOTE: <c>OptionalIntegrity</c> is intentionally
    /// dropped — the wire type carries no integrity field. This is the same loss the data-plane write
    /// path already incurs; integrity is unused by the engine and the conformance corpus.
    /// </summary>
    public static RelationshipWire ToWire(Relationship rel) => new(
        rel.Resource.ObjectType, rel.Resource.ObjectId, rel.Resource.Relation,
        rel.Subject.ObjectType, rel.Subject.ObjectId, rel.Subject.Relation,
        rel.OptionalCaveat?.CaveatName, rel.OptionalCaveat?.Context, rel.OptionalExpiration);

    // --- Full filter (lossless, used for counters) ---

    /// <summary>Converts the lossless full-filter wire form to a core <see cref="RelationshipsFilter"/>.</summary>
    public static RelationshipsFilter ToCoreFilter(FullRelationshipsFilterWire w)
    {
        IReadOnlyList<SubjectsSelector>? selectors = w.OptionalSubjectsSelectors?.Select(ToCoreSelector).ToList();
        CaveatNameFilter? caveatFilter = w.OptionalCaveatNameFilter is { } cf
            ? new CaveatNameFilter(ToCoreCaveatOption(cf.Option), cf.CaveatName)
            : null;

        return new RelationshipsFilter
        {
            OptionalResourceType = w.OptionalResourceType,
            OptionalResourceIds = w.OptionalResourceIds,
            OptionalResourceIdPrefix = w.OptionalResourceIdPrefix,
            OptionalResourceRelation = w.OptionalResourceRelation,
            OptionalSubjectsSelectors = selectors,
            OptionalCaveatNameFilter = caveatFilter,
            OptionalExpirationOption = ToCoreExpirationOption(w.OptionalExpirationOption),
        };
    }

    /// <summary>Converts a core <see cref="RelationshipsFilter"/> to the lossless full-filter wire form.</summary>
    public static FullRelationshipsFilterWire ToFullFilter(RelationshipsFilter f)
    {
        IReadOnlyList<SubjectsSelectorWire>? selectors = f.OptionalSubjectsSelectors?.Select(ToWireSelector).ToList();
        CaveatNameFilterWire? caveatFilter = f.OptionalCaveatNameFilter is { } cf
            ? new CaveatNameFilterWire((int)cf.Option, cf.CaveatName)
            : null;

        return new FullRelationshipsFilterWire(
            f.OptionalResourceType,
            f.OptionalResourceIds,
            f.OptionalResourceIdPrefix,
            f.OptionalResourceRelation,
            selectors,
            caveatFilter,
            (int)f.OptionalExpirationOption);
    }

    private static SubjectsSelector ToCoreSelector(SubjectsSelectorWire s) =>
        new(
            s.OptionalSubjectType,
            s.OptionalSubjectIds,
            s.RelationFilter is { } rf
                ? new SubjectRelationFilter(rf.NonEllipsisRelation, rf.IncludeEllipsisRelation, rf.OnlyNonEllipsisRelations)
                : null);

    private static SubjectsSelectorWire ToWireSelector(SubjectsSelector s) =>
        new(
            s.OptionalSubjectType,
            s.OptionalSubjectIds,
            s.RelationFilter is { } rf
                ? new SubjectRelationFilterWire(rf.NonEllipsisRelation, rf.IncludeEllipsisRelation, rf.OnlyNonEllipsisRelations)
                : null);

    private static CaveatFilterOption ToCoreCaveatOption(int option) => option switch
    {
        (int)CaveatFilterOption.HasMatchingCaveat => CaveatFilterOption.HasMatchingCaveat,
        (int)CaveatFilterOption.NoCaveat => CaveatFilterOption.NoCaveat,
        _ => CaveatFilterOption.None,
    };

    private static ExpirationFilterOption ToCoreExpirationOption(int option) => option switch
    {
        (int)ExpirationFilterOption.HasExpiration => ExpirationFilterOption.HasExpiration,
        (int)ExpirationFilterOption.NoExpiration => ExpirationFilterOption.NoExpiration,
        _ => ExpirationFilterOption.None,
    };
}
