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

/// <summary>
/// A lightweight head probe: head revision, the schema hash effective at that head, and the current GC
/// floor (the revision below which MVCC history has been collected — reads/cursors below it are invalid).
/// </summary>
[GenerateSerializer]
public sealed record DatastoreHeadWire(
    [property: Id(0)] long Head,
    [property: Id(1)] string? SchemaHash,
    [property: Id(2)] long GcFloor = 0);

/// <summary>
/// A proposed commit submitted to the event-sourced datastore grain WITHOUT a final revision: the
/// resolved relationship changes (Touch / Delete on full payloads), an optional new schema (bytes), and
/// the counter deltas (a null <see cref="CounterDeltaWire.Filter"/> being a tombstone). The grain — the
/// single serialization point — mints the revision and stamps it onto a canonical <c>LogEvent</c>.
/// Preconditions and create-conflicts are already resolved caller-side; this carries only the net diff.
/// </summary>
[GenerateSerializer]
public sealed record ProposedWrite(
    [property: Id(0)] IReadOnlyList<RelationshipUpdateWire> RelationshipChanges,
    [property: Id(1)] byte[]? SchemaBytes,
    [property: Id(2)] IReadOnlyList<CounterDeltaWire> CounterChanges);

/// <summary>
/// The durable "head" pointer entry for the event-sourced datastore grain (a single grain-storage row).
/// It records the two distinct counters: the contiguous append-only log <see cref="LogVersion"/> (the
/// CustomStorage optimistic-concurrency version, = the number of confirmed events), and the latest
/// timestamp <see cref="HeadRevision"/> (the MVCC / zedtoken revision carried in the events). It also
/// records the <see cref="SnapshotVersion"/> at which the latest periodic snapshot was taken, so a cold
/// read replays only the log tail above the snapshot.
/// </summary>
[GenerateSerializer]
public sealed record LogHeadEntry(
    [property: Id(0)] int LogVersion,
    [property: Id(1)] long HeadRevision,
    [property: Id(2)] int SnapshotVersion);

/// <summary>
/// A mutable holder for the immutable <see cref="DatastoreGrainState"/>, required because
/// <c>JournaledGrain&lt;TState,TEvent&gt;</c> mutates its state object in place via
/// <c>TransitionState</c>, whereas <see cref="DatastoreGrainState"/> is an immutable record. The fold
/// replaces <see cref="Value"/> with a new immutable state per applied event.
/// </summary>
[GenerateSerializer]
public sealed class DatastoreStateHolder
{
    /// <summary>The current confirmed datastore state.</summary>
    [Id(0)]
    public DatastoreGrainState Value { get; set; } = new();
}
