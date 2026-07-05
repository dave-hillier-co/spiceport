using System.Collections.Immutable;

namespace Spiceport.Grains.Abstractions;

/// <summary>
/// The Orleans-serializable mirror of the in-memory MVCC <c>DatastoreState</c>: the full committed
/// state of the datastore at a head revision. The <see cref="DatastoreGrain"/> persists this as its
/// grain state and the <c>GrainBackedDatastore</c> converts it to/from the in-memory state to reuse the
/// fold/transaction mechanics. Every relationship row carries its created/deleted revision stamps so
/// snapshot reads at any past revision are exact.
/// </summary>
[GenerateSerializer]
public sealed record DatastoreGrainState
{
    /// <summary>The head (freshest committed) revision.</summary>
    [Id(0)]
    public long HeadRevision { get; init; }

    /// <summary>All relationship rows ever written, each with its MVCC visibility window.</summary>
    [Id(1)]
    public ImmutableList<StoredRelationshipWire> Relationships { get; init; } = ImmutableList<StoredRelationshipWire>.Empty;

    /// <summary>All schema versions, in write order.</summary>
    [Id(2)]
    public ImmutableList<SchemaVersionWire> Schemas { get; init; } = ImmutableList<SchemaVersionWire>.Empty;

    /// <summary>All counter versions, in write order (tombstones included).</summary>
    [Id(3)]
    public ImmutableList<CounterVersionWire> Counters { get; init; } = ImmutableList<CounterVersionWire>.Empty;

    /// <summary>
    /// The revision below which MVCC history has been garbage-collected (0 = nothing collected yet). Set
    /// by folding a GC <see cref="LogEvent"/> (never decreases). Reads pinned strictly below this floor
    /// are rejected.
    /// </summary>
    [Id(4)]
    public long GcFloor { get; init; }

    /// <summary>An empty state seeded at the given initial revision.</summary>
    public static DatastoreGrainState Empty(long initialRevision) => new() { HeadRevision = initialRevision };

    /// <summary>Returns the schema hash effective at the given revision, or null if none.</summary>
    public string? SchemaHashAt(long atRevision)
    {
        string? result = null;
        foreach (var schema in Schemas)
        {
            if (schema.Revision <= atRevision)
                result = schema.Hash;
            else
                break;
        }
        return result;
    }
}
