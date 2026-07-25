using System.Collections.Immutable;
using Spiceport.Grains;

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
/// The durable layout descriptor of the CHUNKED key index (durable layout v2,
/// docs/scalability-program.md 3.4): the in-memory <see cref="DatastoreMetaState.ForwardKeys"/>/
/// <see cref="DatastoreMetaState.ReverseKeys"/> maps are NOT serialized into the meta row; instead each
/// direction's index is split across <see cref="BucketCount"/> buckets
/// (<c>bucket = FNV-1a-64(key) % BucketCount</c>, fixed at store creation — the bucket of a key is part
/// of the durable layout and must never change), persisted as write-once
/// <c>indexb/{version}/{dir}/{bucket}</c> rows (<see cref="KeyIndexBucketEntry"/>) rewritten one bucket
/// per direction per flush in round-robin rotation, plus per-flush <c>indexd/{version}</c> DELTA rows
/// (<see cref="KeyIndexDeltaEntry"/>) carrying exactly the entries that flush dirtied. Recovery
/// reconstructs the full maps as (all bucket rows at their recorded versions) overlaid with (the
/// pending delta rows in ascending version order). The meta row's size is therefore
/// CARDINALITY-INDEPENDENT: this descriptor is O(BucketCount), never O(keys).
/// </summary>
[GenerateSerializer, Immutable]
public sealed record KeyIndexLayout(
    [property: Id(0)] int BucketCount,
    [property: Id(1)] ImmutableArray<int> ForwardBucketVersions,
    [property: Id(2)] ImmutableArray<int> ReverseBucketVersions,
    [property: Id(3)] int NextBucket,
    [property: Id(4)] int DeltaFloorVersion,
    [property: Id(5)] ImmutableList<int> DeltaVersions)
{
    /// <summary>The bucket count of a newly created (or migrated) store.</summary>
    public const int DefaultBucketCount = 256;

    /// <summary>
    /// The sentinel version of a bucket that has never had a row written (a young store before the
    /// rotation first reaches it): recovery reads no row for it and treats it as empty.
    /// </summary>
    public const int NoBucketRow = 0;

    /// <summary>
    /// The bucket a key string belongs to: <c>FNV-1a-64(key) % bucketCount</c>. Depends only on the
    /// stable, process-independent hash and the store's fixed bucket count, so the assignment is part
    /// of the durable contract (a changed bucket function would strand every stored bucket row).
    /// </summary>
    public static int BucketOf(string key, int bucketCount) =>
        (int)(StableHash.Fnv1a64(key) % (ulong)bucketCount);

    /// <summary>A fresh layout with no bucket rows written and no pending deltas.</summary>
    public static KeyIndexLayout CreateEmpty(int bucketCount)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(bucketCount, 1);
        var none = Enumerable.Repeat(NoBucketRow, bucketCount).ToImmutableArray();
        return new KeyIndexLayout(bucketCount, none, none, NextBucket: 0, DeltaFloorVersion: 0,
            ImmutableList<int>.Empty);
    }
}

/// <summary>
/// One durable <c>indexb/{version}/{dir}/{bucket}</c> row: the FULL current key->rowVersion entries of
/// one bucket of one direction's key index, as of the flush at <c>{version}</c>. Write-once per version
/// with the same ETag-tolerant read-then-write discipline the shard rows use (a boundary retry
/// overwrites its own crashed attempt's orphan). Entries are ABSOLUTE (each value is the key's current
/// shard-row version), which is what makes the recovery overlay idempotent.
/// </summary>
[GenerateSerializer, Immutable]
public sealed record KeyIndexBucketEntry(
    [property: Id(0)] ImmutableDictionary<string, int> Entries);

/// <summary>
/// One durable <c>indexd/{version}</c> row: the key-index entries DIRTIED by the flush at
/// <c>{version}</c>, both directions. A key whose row was (re)written maps to that flush version; a key
/// DROPPED at the flush (its shard state emptied) is recorded as an explicit <see cref="Tombstone"/>
/// entry — replaying the delta at recovery must delete it from the reconstructed index, because the
/// bucket row still carrying it may be many rotations old. Like bucket entries, values are ABSOLUTE, so
/// overlaying deltas in ascending version order is idempotent last-wins.
/// </summary>
[GenerateSerializer, Immutable]
public sealed record KeyIndexDeltaEntry(
    [property: Id(0)] ImmutableDictionary<string, int> ForwardEntries,
    [property: Id(1)] ImmutableDictionary<string, int> ReverseEntries)
{
    /// <summary>
    /// The delta-entry value marking a key REMOVED from the index at this flush. Distinct from
    /// <see cref="DatastoreMetaState.NoRowVersion"/> (-1, "indexed but no durable row yet"), which never
    /// appears in a durable row.
    /// </summary>
    public const int Tombstone = -2;
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
/// <remarks>
/// LAYOUT VERSIONS. Under the current (v2) layout <see cref="IndexLayout"/> is non-null and
/// <see cref="Meta"/> is persisted with EMPTY <see cref="DatastoreMetaState.ForwardKeys"/>/
/// <see cref="DatastoreMetaState.ReverseKeys"/> maps — the index lives in the bucket/delta rows the
/// layout describes, making the meta row's size independent of graph cardinality. A row written by the
/// retired v1 layout carries the full maps INLINE and has a null <see cref="IndexLayout"/> (the field
/// did not exist; Orleans deserializes the absent field as null) — activation detects that shape and
/// migrates in place. The in-memory <see cref="DatastoreMetaState"/> always holds the full maps
/// regardless; only the durable representation changed.
/// </remarks>
[GenerateSerializer, Immutable]
public sealed record DatastoreMetaEntry(
    [property: Id(0)] DatastoreMetaState Meta,
    [property: Id(1)] int FlushedThroughLogVersion,
    [property: Id(2)] KeyIndexLayout? IndexLayout = null);

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
