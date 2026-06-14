namespace Spiceport.Grains.Abstractions;

/// <summary>
/// A relationship row on the wire, carrying its MVCC visibility window. Mirrors the in-memory
/// <c>StoredRelationship</c> (payload + created/deleted revision stamps) so the whole datastore state
/// round-trips across the Orleans boundary.
/// </summary>
[GenerateSerializer]
public sealed record StoredRelationshipWire(
    [property: Id(0)] RelationshipWire Relationship,
    [property: Id(1)] long CreatedRevision,
    [property: Id(2)] long? DeletedRevision);

/// <summary>A schema-bytes version stamped with the revision at which it was written.</summary>
[GenerateSerializer]
public sealed record SchemaVersionWire(
    [property: Id(0)] long Revision,
    [property: Id(1)] byte[] Bytes,
    [property: Id(2)] string Hash);

/// <summary>
/// An MVCC version of a registered counter, stamped with the revision at which it was written. A null
/// <see cref="Filter"/> marks a tombstone (the counter was unregistered at this revision).
/// </summary>
[GenerateSerializer]
public sealed record CounterVersionWire(
    [property: Id(0)] long Revision,
    [property: Id(1)] string Name,
    [property: Id(2)] FullRelationshipsFilterWire? Filter);

/// <summary>
/// The full relationships filter on the wire (lossless mirror of the core <c>RelationshipsFilter</c>),
/// used for counters: the counter's registered filter must round-trip exactly through the grain.
/// </summary>
[GenerateSerializer]
public sealed record FullRelationshipsFilterWire(
    [property: Id(0)] string? OptionalResourceType,
    [property: Id(1)] IReadOnlyList<string>? OptionalResourceIds,
    [property: Id(2)] string? OptionalResourceIdPrefix,
    [property: Id(3)] string? OptionalResourceRelation,
    [property: Id(4)] IReadOnlyList<SubjectsSelectorWire>? OptionalSubjectsSelectors,
    [property: Id(5)] CaveatNameFilterWire? OptionalCaveatNameFilter,
    [property: Id(6)] int OptionalExpirationOption);

/// <summary>One subject selector within a <see cref="FullRelationshipsFilterWire"/>.</summary>
[GenerateSerializer]
public sealed record SubjectsSelectorWire(
    [property: Id(0)] string? OptionalSubjectType,
    [property: Id(1)] IReadOnlyList<string>? OptionalSubjectIds,
    [property: Id(2)] SubjectRelationFilterWire? RelationFilter);

/// <summary>A subject-relation constraint within a <see cref="SubjectsSelectorWire"/>.</summary>
[GenerateSerializer]
public sealed record SubjectRelationFilterWire(
    [property: Id(0)] string? NonEllipsisRelation,
    [property: Id(1)] bool IncludeEllipsisRelation,
    [property: Id(2)] bool OnlyNonEllipsisRelations);

/// <summary>A caveat-presence/name constraint within a <see cref="FullRelationshipsFilterWire"/>.</summary>
[GenerateSerializer]
public sealed record CaveatNameFilterWire(
    [property: Id(0)] int Option,
    [property: Id(1)] string? CaveatName);

/// <summary>A lightweight head probe: head revision plus the schema hash effective at that head.</summary>
[GenerateSerializer]
public sealed record DatastoreHeadWire(
    [property: Id(0)] long Head,
    [property: Id(1)] string? SchemaHash);
