using Orleans.Concurrency;

namespace Spiceport.Grains.Abstractions;

/// <summary>Which adjacency slice of the graph a shard key names.</summary>
public enum GraphShardDirection
{
    /// <summary>The forward slice: all rows whose RESOURCE is the shard's object.</summary>
    Forward = 0,

    /// <summary>The reverse slice: all rows whose SUBJECT is the shard's object.</summary>
    Reverse = 1,
}

/// <summary>
/// A shard key names one adjacency slice of the graph: the forward slice (all rows whose RESOURCE is
/// this object) or the reverse slice (all rows whose SUBJECT is this object). Every relationship row
/// belongs to exactly one forward and one reverse slice, so the per-key restriction of the log fold
/// partitions the whole state (the sharding lemma pinned by <c>ShardFoldLemmaTests</c>).
/// </summary>
[GenerateSerializer, Immutable]
public sealed record GraphShardKeyWire(
    [property: Id(0)] GraphShardDirection Direction,
    [property: Id(1)] string ObjectType,
    [property: Id(2)] string ObjectId)
{
    /// <summary>True if the relationship belongs to this shard's slice (ordinal comparison).</summary>
    public bool Matches(RelationshipWire rel) => Direction == GraphShardDirection.Forward
        ? string.Equals(rel.ResourceType, ObjectType, StringComparison.Ordinal)
            && string.Equals(rel.ResourceId, ObjectId, StringComparison.Ordinal)
        : string.Equals(rel.SubjectType, ObjectType, StringComparison.Ordinal)
            && string.Equals(rel.SubjectId, ObjectId, StringComparison.Ordinal);

    /// <summary>The forward shard key for the object: all rows with this resource.</summary>
    public static GraphShardKeyWire ForResource(string type, string id) =>
        new(GraphShardDirection.Forward, type, id);

    /// <summary>The reverse shard key for the object: all rows with this subject.</summary>
    public static GraphShardKeyWire ForSubject(string type, string id) =>
        new(GraphShardDirection.Reverse, type, id);
}
