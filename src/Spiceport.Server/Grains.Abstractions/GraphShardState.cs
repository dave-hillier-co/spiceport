using System.Collections.Immutable;
using Orleans.Concurrency;

namespace Spiceport.Grains.Abstractions;

/// <summary>
/// The state of one graph shard: the MVCC relationship rows of a single adjacency slice (one
/// <see cref="GraphShardKeyWire"/>), folded from the datastore's event log restricted to that key.
/// <see cref="AppliedRevision"/> is the shard's watermark — the revision of the last log event folded,
/// which advances on EVERY event (matching or not), so "watermark covers the pinned revision" is the
/// closed-timestamp gate. <see cref="GcFloor"/> mirrors the
/// whole state's floor: reads pinned strictly below it are invalid.
/// </summary>
[GenerateSerializer, Immutable]
public sealed record GraphShardState(
    [property: Id(0)] long AppliedRevision,
    [property: Id(1)] long GcFloor,
    [property: Id(2)] ImmutableList<StoredRelationshipWire> Rows)
{
    /// <summary>The empty shard, before any log event has been folded.</summary>
    public static GraphShardState Empty { get; } = new(0, 0, ImmutableList<StoredRelationshipWire>.Empty);
}
