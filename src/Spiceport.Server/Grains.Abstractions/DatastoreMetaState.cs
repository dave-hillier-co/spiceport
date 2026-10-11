using System.Collections.Immutable;
using Orleans.Concurrency;

namespace Spiceport.Grains.Abstractions;

/// <summary>
/// The journaled state of the sequencer datastore grain: everything the sequencer materializes EXCEPT
/// the relationship rows. Rows are owned by the per-key <see cref="IGraphShardGrain"/> projections
/// (each persists its own <see cref="GraphShardState"/>), so this record carries only the head
/// revision, the schema/counter version histories, the GC floor, the object types ever seen per
/// direction (the index's enumeration roots), and the PENDING TAIL — the committed events not yet
/// projected into the shard grains, plus the revision the projection has reached.
/// </summary>
/// <remarks>
/// The pending tail is what makes the projection crash-safe under any snapshot policy: a committed
/// event leaves the journaled state only when a <see cref="DatastoreJournalEntry.Projected"/> entry
/// records that every shard it touches has persisted it. It is bounded by the sequencer's flush
/// interval, so the state stays small however large the graph grows.
/// </remarks>
[GenerateSerializer]
public sealed record DatastoreMetaState
{
    /// <summary>The head (freshest committed) revision.</summary>
    [Id(0)]
    public long HeadRevision { get; init; }

    /// <summary>All schema versions, in write order (compacted below the GC floor).</summary>
    [Id(1)]
    public ImmutableList<SchemaVersionWire> Schemas { get; init; } = ImmutableList<SchemaVersionWire>.Empty;

    /// <summary>All counter versions, in write order (tombstones included; compacted below the floor).</summary>
    [Id(2)]
    public ImmutableList<CounterVersionWire> Counters { get; init; } = ImmutableList<CounterVersionWire>.Empty;

    /// <summary>The MVCC GC floor: history strictly below it has been collected.</summary>
    [Id(3)]
    public long GcFloor { get; init; }

    /// <summary>
    /// The committed events not yet projected into the shard grains, ascending by revision. Every event
    /// above <see cref="ProjectedThrough"/> is here; none at or below it is.
    /// </summary>
    [Id(4)]
    public ImmutableList<LogEvent> Pending { get; init; } = ImmutableList<LogEvent>.Empty;

    /// <summary>
    /// The revision the shard projection has reached: every event at or below it is persisted by every
    /// shard grain it touches, and its touched keys are in the shard index.
    /// </summary>
    [Id(5)]
    public long ProjectedThrough { get; init; }

    /// <summary>The resource types any committed relationship has named — the forward index's roots.</summary>
    [Id(6)]
    public ImmutableSortedSet<string> ForwardTypes { get; init; } = ImmutableSortedSet<string>.Empty.WithComparer(StringComparer.Ordinal);

    /// <summary>The subject types any committed relationship has named — the reverse index's roots.</summary>
    [Id(7)]
    public ImmutableSortedSet<string> ReverseTypes { get; init; } = ImmutableSortedSet<string>.Empty.WithComparer(StringComparer.Ordinal);

    /// <summary>An empty state seeded at <paramref name="initialRevision"/>.</summary>
    public static DatastoreMetaState Empty(long initialRevision) =>
        new() { HeadRevision = initialRevision, ProjectedThrough = initialRevision };

    /// <summary>
    /// The schema version effective at <paramref name="atRevision"/>: the last one with
    /// <c>Revision &lt;= atRevision</c>, or null in the seed-only window.
    /// </summary>
    public SchemaVersionWire? SchemaVersionAt(long atRevision)
    {
        SchemaVersionWire? result = null;
        foreach (var schema in Schemas)
        {
            if (schema.Revision <= atRevision)
                result = schema;
            else
                break;
        }
        return result;
    }

    /// <summary>The schema hash effective at <paramref name="atRevision"/>, or null in the seed-only window.</summary>
    public string? SchemaHashAt(long atRevision) => SchemaVersionAt(atRevision)?.Hash;
}

/// <summary>
/// One entry of the sequencer's journal. <see cref="Committed"/> carries a canonical
/// <see cref="LogEvent"/> (the datastore's source of truth, and what Watch and the shard grains tail);
/// <see cref="Projected"/> records that the shard projection has caught up through a revision, which
/// is what lets the pending tail shrink.
/// </summary>
[GenerateSerializer]
public abstract record DatastoreJournalEntry
{
    /// <summary>
    /// The store's first entry: fixes the head before any write at the revision the first activation
    /// minted, so the pre-first-write head is stable across reactivation.
    /// </summary>
    [GenerateSerializer, Immutable]
    public sealed record Seeded([property: Id(0)] long Revision) : DatastoreJournalEntry;

    /// <summary>A committed datastore event.</summary>
    [GenerateSerializer, Immutable]
    public sealed record Committed([property: Id(0)] LogEvent Event) : DatastoreJournalEntry;

    /// <summary>
    /// Every pending event at or below <paramref name="Through"/> has been persisted by each shard grain
    /// it touches and its keys added to the shard index.
    /// </summary>
    [GenerateSerializer, Immutable]
    public sealed record Projected([property: Id(0)] long Through) : DatastoreJournalEntry;
}

/// <summary>
/// The mutable holder <c>JournaledGrain</c> folds into in place (its state type must be a class with a
/// parameterless constructor); the immutable <see cref="DatastoreMetaState"/> is replaced whole.
/// </summary>
[GenerateSerializer]
public sealed class DatastoreMetaHolder
{
    /// <summary>The current state.</summary>
    [Id(0)]
    public DatastoreMetaState Value { get; set; } = new();
}

/// <summary>
/// The sequencer's reply to a shard grain hydrating itself: the pending events that touch the shard's
/// key, plus the head and floor to relabel and collect at. A shard's state is its own projected state
/// with these events folded over it (those above its projected label), relabeled to the head.
/// </summary>
[GenerateSerializer, Immutable]
public sealed record ShardTailWire(
    [property: Id(0)] ImmutableList<LogEvent> Events,
    [property: Id(1)] long HeadRevision,
    [property: Id(2)] long GcFloor);
