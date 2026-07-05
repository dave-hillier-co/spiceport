namespace Spiceport.Grains.Abstractions;

/// <summary>
/// A counter change at a revision. A null <see cref="Filter"/> is a tombstone (the counter was
/// unregistered at this revision); otherwise it is the (re)registered filter.
/// </summary>
[GenerateSerializer]
public sealed record CounterDeltaWire(
    [property: Id(0)] string Name,
    [property: Id(1)] FullRelationshipsFilterWire? Filter);

/// <summary>
/// One entry in the datastore's append-only event log: everything that changed at a single committed
/// revision. The revision IS the log offset (the global order); folding the ordered sequence of
/// <see cref="LogEvent"/>s reproduces the datastore state, and the same feed powers Watch and the
/// per-silo read projections.
/// </summary>
/// <remarks>
/// The event is SELF-CONTAINED / FOLDABLE: every payload needed to reproduce the committed state is
/// carried inline. <see cref="RelationshipChanges"/> carry full relationship payloads, <see cref="SchemaChange"/>
/// carries the new schema version (revision + bytes + hash; null = no schema change), and
/// <see cref="CounterChanges"/> carry name + filter (null filter = tombstone). A consumer can fold the
/// ordered event sequence from empty without any side state.
/// </remarks>
[GenerateSerializer, Immutable]
public sealed record LogEvent(
    [property: Id(0)] long Revision,
    [property: Id(1)] IReadOnlyList<RelationshipUpdateWire> RelationshipChanges,
    [property: Id(2)] SchemaVersionWire? SchemaChange,
    [property: Id(3)] IReadOnlyList<CounterDeltaWire> CounterChanges);

/// <summary>A bounded page of the event log, plus the head revision observed at read time.</summary>
[GenerateSerializer]
public sealed record LogSegment(
    [property: Id(0)] IReadOnlyList<LogEvent> Events,
    [property: Id(1)] long HeadRevision);

/// <summary>
/// The read side of the datastore's event log: an ordered feed of committed changes by revision (= the
/// global offset), consumed by per-silo projections and the Watch API.
/// </summary>
public interface IDatastoreLog
{
    /// <summary>
    /// Returns up to <paramref name="maxCount"/> change-bearing events whose revision is strictly greater
    /// than <paramref name="afterRevision"/>, in ascending revision order, plus the current head revision.
    /// Throws <c>RevisionNotFoundException</c> if <paramref name="afterRevision"/> is older than the
    /// retained GC window.
    /// </summary>
    Task<LogSegment> ReadFrom(long afterRevision, int maxCount);
}
