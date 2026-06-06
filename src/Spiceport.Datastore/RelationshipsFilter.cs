using Spiceport.Core;

namespace Spiceport.Datastore;

/// <summary>Whether to match relationships based on the presence of a caveat.</summary>
public enum CaveatFilterOption
{
    /// <summary>Do not filter on caveats.</summary>
    None = 0,

    /// <summary>Match only relationships carrying a caveat with the named name.</summary>
    HasMatchingCaveat = 1,

    /// <summary>Match only relationships that have no caveat.</summary>
    NoCaveat = 2,
}

/// <summary>Whether to match relationships based on the presence of an expiration.</summary>
public enum ExpirationFilterOption
{
    /// <summary>Do not filter on expiration.</summary>
    None = 0,

    /// <summary>Match only relationships that have an expiration set.</summary>
    HasExpiration = 1,

    /// <summary>Match only relationships that have no expiration set.</summary>
    NoExpiration = 2,
}

/// <summary>A filter on a relationship's caveat.</summary>
/// <param name="Option">The caveat filtering mode.</param>
/// <param name="CaveatName">Required when <paramref name="Option"/> is <see cref="CaveatFilterOption.HasMatchingCaveat"/>.</param>
public sealed record CaveatNameFilter(CaveatFilterOption Option, string? CaveatName = null);

/// <summary>A filter on the relation of a subject.</summary>
/// <param name="NonEllipsisRelation">When set, matches subjects with exactly this (non-ellipsis) relation.</param>
/// <param name="IncludeEllipsisRelation">When true, ellipsis-relation subjects are included.</param>
/// <param name="OnlyNonEllipsisRelations">When true, only non-ellipsis subjects match (any non-ellipsis relation).</param>
public sealed record SubjectRelationFilter(
    string? NonEllipsisRelation = null,
    bool IncludeEllipsisRelation = false,
    bool OnlyNonEllipsisRelations = false)
{
    /// <summary>A filter matching any subject relation.</summary>
    public static SubjectRelationFilter Any { get; } =
        new(NonEllipsisRelation: null, IncludeEllipsisRelation: true, OnlyNonEllipsisRelations: false);

    /// <summary>True if this filter places no constraint on the subject relation.</summary>
    public bool MatchesAny =>
        NonEllipsisRelation is null && IncludeEllipsisRelation && !OnlyNonEllipsisRelations;

    /// <summary>Returns true if the given subject relation satisfies this filter.</summary>
    public bool Matches(string subjectRelation)
    {
        if (MatchesAny)
            return true;

        var isEllipsis = subjectRelation == CoreConstants.Ellipsis;

        if (NonEllipsisRelation is not null)
        {
            if (!isEllipsis && subjectRelation == NonEllipsisRelation)
                return true;
            if (isEllipsis && IncludeEllipsisRelation)
                return true;
            return false;
        }

        if (OnlyNonEllipsisRelations)
            return !isEllipsis;

        if (isEllipsis)
            return IncludeEllipsisRelation;

        // No non-ellipsis constraint specified: a concrete relation matches unless restricted.
        return true;
    }
}

/// <summary>Selects subjects by type, ids and relation. Used as one alternative within a filter.</summary>
/// <param name="OptionalSubjectType">When set, matches only subjects of this type.</param>
/// <param name="OptionalSubjectIds">When set, matches only subjects whose id is in this list.</param>
/// <param name="RelationFilter">Constraint on the subject relation.</param>
public sealed record SubjectsSelector(
    string? OptionalSubjectType = null,
    IReadOnlyList<string>? OptionalSubjectIds = null,
    SubjectRelationFilter? RelationFilter = null)
{
    /// <summary>Returns true if the given subject ONR satisfies this selector.</summary>
    public bool Matches(ObjectAndRelation subject)
    {
        if (OptionalSubjectType is not null && subject.ObjectType != OptionalSubjectType)
            return false;
        if (OptionalSubjectIds is { Count: > 0 } && !OptionalSubjectIds.Contains(subject.ObjectId))
            return false;
        var relFilter = RelationFilter ?? SubjectRelationFilter.Any;
        return relFilter.Matches(subject.Relation);
    }
}

/// <summary>
/// A filter over relationships, applied from the resource side. All set fields are ANDed;
/// when a field is null/empty it places no constraint.
/// </summary>
public sealed record RelationshipsFilter
{
    /// <summary>When set, only relationships whose resource has this type match.</summary>
    public string? OptionalResourceType { get; init; }

    /// <summary>When set, only relationships whose resource id is in this list match.</summary>
    public IReadOnlyList<string>? OptionalResourceIds { get; init; }

    /// <summary>When set, only relationships whose resource id begins with this prefix match. Mutually exclusive with <see cref="OptionalResourceIds"/>.</summary>
    public string? OptionalResourceIdPrefix { get; init; }

    /// <summary>When set, only relationships with this resource relation match.</summary>
    public string? OptionalResourceRelation { get; init; }

    /// <summary>When set (non-empty), a relationship matches if its subject satisfies any selector.</summary>
    public IReadOnlyList<SubjectsSelector>? OptionalSubjectsSelectors { get; init; }

    /// <summary>When set, filters by caveat presence/name.</summary>
    public CaveatNameFilter? OptionalCaveatNameFilter { get; init; }

    /// <summary>Filters by expiration presence.</summary>
    public ExpirationFilterOption OptionalExpirationOption { get; init; } = ExpirationFilterOption.None;

    /// <summary>Returns true if the given relationship satisfies this filter.</summary>
    public bool Matches(Relationship rel)
    {
        var resource = rel.Resource;

        if (OptionalResourceType is not null && resource.ObjectType != OptionalResourceType)
            return false;

        if (OptionalResourceIds is { Count: > 0 } && !OptionalResourceIds.Contains(resource.ObjectId))
            return false;

        if (!string.IsNullOrEmpty(OptionalResourceIdPrefix) && !resource.ObjectId.StartsWith(OptionalResourceIdPrefix, StringComparison.Ordinal))
            return false;

        if (OptionalResourceRelation is not null && resource.Relation != OptionalResourceRelation)
            return false;

        if (OptionalSubjectsSelectors is { Count: > 0 } selectors)
        {
            var anyMatch = false;
            foreach (var selector in selectors)
            {
                if (selector.Matches(rel.Subject))
                {
                    anyMatch = true;
                    break;
                }
            }
            if (!anyMatch)
                return false;
        }

        if (OptionalCaveatNameFilter is { } caveatFilter)
        {
            switch (caveatFilter.Option)
            {
                case CaveatFilterOption.NoCaveat:
                    if (rel.OptionalCaveat is not null)
                        return false;
                    break;
                case CaveatFilterOption.HasMatchingCaveat:
                    if (rel.OptionalCaveat is null || rel.OptionalCaveat.CaveatName != caveatFilter.CaveatName)
                        return false;
                    break;
            }
        }

        switch (OptionalExpirationOption)
        {
            case ExpirationFilterOption.HasExpiration:
                if (rel.OptionalExpiration is null)
                    return false;
                break;
            case ExpirationFilterOption.NoExpiration:
                if (rel.OptionalExpiration is not null)
                    return false;
                break;
        }

        return true;
    }
}

/// <summary>A filter over relationships, applied from the subject side (reverse query).</summary>
/// <param name="SubjectType">The subject type to query.</param>
/// <param name="OptionalSubjectIds">When set, restricts to subjects with these ids.</param>
/// <param name="RelationFilter">Constraint on the subject relation.</param>
/// <param name="OptionalResourceType">When set, restricts to resources of this type.</param>
/// <param name="OptionalResourceRelation">When set, restricts to resources with this relation.</param>
public sealed record SubjectsFilter(
    string SubjectType,
    IReadOnlyList<string>? OptionalSubjectIds = null,
    SubjectRelationFilter? RelationFilter = null,
    string? OptionalResourceType = null,
    string? OptionalResourceRelation = null)
{
    /// <summary>Returns true if the given relationship's subject satisfies this filter.</summary>
    public bool Matches(Relationship rel)
    {
        var subject = rel.Subject;
        if (subject.ObjectType != SubjectType)
            return false;
        if (OptionalSubjectIds is { Count: > 0 } && !OptionalSubjectIds.Contains(subject.ObjectId))
            return false;
        var relFilter = RelationFilter ?? SubjectRelationFilter.Any;
        if (!relFilter.Matches(subject.Relation))
            return false;
        if (OptionalResourceType is not null && rel.Resource.ObjectType != OptionalResourceType)
            return false;
        if (OptionalResourceRelation is not null && rel.Resource.Relation != OptionalResourceRelation)
            return false;
        return true;
    }
}
