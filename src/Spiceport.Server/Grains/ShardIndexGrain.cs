using System.Collections.Immutable;
using System.Globalization;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// The shard index's key scheme: one <see cref="IShardIndexGrain"/> per (direction, object type,
/// bucket), with an object id's bucket a stable hash of the id. The bucket count is fixed because it
/// is part of the persisted layout — changing it would strand every stored bucket.
/// </summary>
internal static class ShardIndexKey
{
    /// <summary>Buckets per (direction, type): enumerating a type costs this many grain calls.</summary>
    public const int BucketCount = 16;

    /// <summary>The bucket an object id hashes into.</summary>
    public static int BucketOf(string objectId) =>
        (int)(StableHash.Fnv1a64(objectId) % BucketCount);

    /// <summary>The index grain key for one bucket.</summary>
    public static string Build(GraphShardDirection direction, string objectType, int bucket) =>
        GrainKeyCodec.Join(
            direction == GraphShardDirection.Forward ? "f" : "r",
            objectType,
            bucket.ToString(CultureInfo.InvariantCulture));

    /// <summary>The index grain key that lists <paramref name="key"/>.</summary>
    public static string For(GraphShardKeyWire key) =>
        Build(key.Direction, key.ObjectType, BucketOf(key.ObjectId));

    /// <summary>
    /// Every shard key of <paramref name="direction"/> and <paramref name="objectType"/> the index lists
    /// (a superset of the keys holding rows: a key stays listed after its rows are deleted).
    /// </summary>
    public static async Task<IReadOnlyList<GraphShardKeyWire>> ListAsync(
        IGrainFactory grainFactory, GraphShardDirection direction, string objectType)
    {
        var buckets = await Task.WhenAll(Enumerable.Range(0, BucketCount).Select(bucket =>
            grainFactory.GetGrain<IShardIndexGrain>(Build(direction, objectType, bucket)).ReadIds()));
        return buckets
            .SelectMany(ids => ids)
            .Select(id => new GraphShardKeyWire(direction, objectType, id))
            .ToList();
    }
}

/// <summary>
/// One bucket of the shard index (see <see cref="IShardIndexGrain"/>): a persisted set of object ids.
/// The sequencer adds the keys each projection touched; the set only grows, so a key whose rows were
/// all deleted stays listed and resolves to an empty shard.
/// </summary>
public sealed class ShardIndexGrain(
    [PersistentState("index", "datastore")] IPersistentState<ShardIndexState> state)
    : Grain, IShardIndexGrain
{
    /// <inheritdoc />
    public async Task Add(ImmutableList<string> ids)
    {
        var current = state.State.Ids;
        var next = current.Union(ids);
        if (next.Count == current.Count)
            return;
        // Restore the previous ids if the write fails, or a retry would see nothing new to persist.
        var previous = state.State;
        state.State = new ShardIndexState(next);
        try
        {
            await state.WriteStateAsync();
        }
        catch
        {
            state.State = previous;
            throw;
        }
    }

    /// <inheritdoc />
    public Task<ImmutableList<string>> ReadIds() => Task.FromResult(state.State.Ids.ToImmutableList());
}

/// <summary>The persisted ids of one shard index bucket.</summary>
[GenerateSerializer, Immutable]
public sealed record ShardIndexState([property: Id(0)] ImmutableSortedSet<string> Ids)
{
    /// <summary>An empty bucket (Orleans constructs persisted state through this).</summary>
    public ShardIndexState()
        : this(ImmutableSortedSet<string>.Empty.WithComparer(StringComparer.Ordinal))
    {
    }
}
