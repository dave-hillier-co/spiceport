using System.Collections.Immutable;

namespace Spiceport.Grains.Abstractions;

/// <summary>
/// The SMALL state of the thin-sequencer datastore grain: everything the sequencer materializes EXCEPT
/// the relationship rows. Rows live in per-key <see cref="GraphShardState"/> storage rows (one per
/// adjacency slice, both directions) plus the grain's in-memory dirty buffer; this record carries only
/// the head revision, the schema/counter version histories, the GC floor, and the KEY INDEX — a map
/// per direction from each populated shard key (escaped <c>direction/type/id</c> key string, the
/// <c>GraphShardGrainKey</c> form) to the ROW VERSION its current durable row is stored under: shard
/// rows are VERSION-QUALIFIED and write-once per version (<c>shard/{rowVersion}/{dir}/{type}/{id}</c>,
/// where the row version is the flush's meta version), so the index is the ONLY path to a row. A key
/// absent from both maps has no stored row worth reading (a physically leaked row — a crashed
/// best-effort clear or an abandoned flush attempt — is unreferenced dead history and must be
/// ignored), so "not indexed and not dirty" resolves to the empty shard without a storage read.
/// </summary>
/// <remarks>
/// The index is add-mostly and its VERSIONS only move at flushes: folding an event adds its touched
/// keys at <see cref="NoRowVersion"/> (or keeps an existing entry's version — the pure fold never
/// knows a storage version); the flush that writes a key's row bumps its entry to the flush version
/// on the meta entry it persists, and keys whose shard state becomes EMPTY are pruned there too.
/// Between flushes a key indexed at <see cref="NoRowVersion"/> is always covered by the grain's dirty
/// buffer, so the sentinel is never dereferenced as a storage row.
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

    /// <summary>
    /// The revision below which MVCC history has been garbage-collected (0 = nothing collected yet).
    /// Advanced by folding a GC <see cref="LogEvent"/>; never decreases. Reads pinned strictly below
    /// this floor are rejected. Row-level collection is LAZY: stored shard rows compact when next
    /// dirtied and flushed, and every serve path re-applies the floor, so lazily-retained dead rows are
    /// never visible.
    /// </summary>
    [Id(3)]
    public long GcFloor { get; init; }

    /// <summary>
    /// The sentinel row version of a key that is indexed but has NO durable row yet (first touched
    /// after the last flush; its state lives only in the dirty buffer and the log tail).
    /// </summary>
    public const int NoRowVersion = -1;

    /// <summary>
    /// The populated FORWARD shard keys (escaped <c>f/type/id</c> strings), each mapped to the row
    /// version its current durable <c>shard/{rowVersion}/{key}</c> row is stored under
    /// (<see cref="NoRowVersion"/> = no durable row yet; see the type remarks).
    /// </summary>
    [Id(4)]
    public ImmutableDictionary<string, int> ForwardKeys { get; init; } = ImmutableDictionary<string, int>.Empty;

    /// <summary>
    /// The populated REVERSE shard keys (escaped <c>r/type/id</c> strings), mapped like
    /// <see cref="ForwardKeys"/>.
    /// </summary>
    [Id(5)]
    public ImmutableDictionary<string, int> ReverseKeys { get; init; } = ImmutableDictionary<string, int>.Empty;

    /// <summary>An empty small state seeded at the given initial revision.</summary>
    public static DatastoreMetaState Empty(long initialRevision) => new() { HeadRevision = initialRevision };

    /// <summary>
    /// Returns the schema version effective at the given revision (the last version with
    /// <c>Revision &lt;= atRevision</c>, the write-order fold), or null if none was persisted at or
    /// before it. Mirrors <c>DatastoreGrainState.SchemaVersionAt</c> / the in-memory
    /// <c>DatastoreState.SchemaAt</c> scan.
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

    /// <summary>Returns the schema hash effective at the given revision, or null if none.</summary>
    public string? SchemaHashAt(long atRevision) => SchemaVersionAt(atRevision)?.Hash;
}

/// <summary>
/// The durable <c>meta/{version}</c> row of the thin-sequencer layout (write-once per version, like the
/// retired whole-state snapshots): the <see cref="DatastoreMetaState"/> as of the flush at
/// <see cref="FlushedThroughLogVersion"/>. The version in the row key equals
/// <see cref="FlushedThroughLogVersion"/> (carried inline too so the row is self-describing), and the
/// durable head pointer's <see cref="LogHeadEntry.SnapshotVersion"/> names which meta row is current.
/// Every shard row on disk is complete through this log version: recovery replays only the log tail
/// above it, seeding touched keys from their stored rows.
/// </summary>
[GenerateSerializer, Immutable]
public sealed record DatastoreMetaEntry(
    [property: Id(0)] DatastoreMetaState Meta,
    [property: Id(1)] int FlushedThroughLogVersion);

/// <summary>
/// A mutable holder for the immutable <see cref="DatastoreMetaState"/>, required because
/// <c>JournaledGrain&lt;TState,TEvent&gt;</c> mutates its state object in place via
/// <c>TransitionState</c>, whereas the small state is an immutable record. The fold replaces
/// <see cref="Value"/> with a new immutable state per applied event.
/// </summary>
[GenerateSerializer]
public sealed class DatastoreMetaHolder
{
    /// <summary>The current confirmed small state.</summary>
    [Id(0)]
    public DatastoreMetaState Value { get; set; } = new();
}
