using System.Collections.Concurrent;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Storage;

namespace Spiceport.Grains.Tests.FaultInjection;

/// <summary>
/// Grain storage that outlives any one silo: every in-process silo of a <c>TestCluster</c> reads and
/// writes one process-wide store, so killing a silo loses only its activations, never its writes, as
/// with a real database. The memory provider cannot stand in here, because it keeps state in grains of
/// its own that die with the silo hosting them. State is stored serialized with the Orleans binary
/// serializer (the production grain-storage serializer), so no two silos ever share an object, and
/// writes are checked against the stored ETag.
/// </summary>
internal sealed class SharedMemoryGrainStorage(Serializer serializer) : IGrainStorage
{
    private static readonly ConcurrentDictionary<string, (byte[] State, string ETag)> Store = new();

    /// <summary>Empties the store, at the start of each harness run.</summary>
    public static void Reset() => Store.Clear();

    public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        if (Store.TryGetValue(Key(stateName, grainId), out var stored))
        {
            grainState.State = serializer.Deserialize<T>(stored.State);
            grainState.ETag = stored.ETag;
            grainState.RecordExists = true;
        }
        else
        {
            // Nothing stored: leave the caller's freshly constructed state in place, as the memory
            // provider does.
            grainState.ETag = null;
            grainState.RecordExists = false;
        }

        return Task.CompletedTask;
    }

    public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        var key = Key(stateName, grainId);
        var etag = Guid.NewGuid().ToString("N");
        var bytes = serializer.SerializeToArray(grainState.State);
        lock (Store)
        {
            var current = Store.TryGetValue(key, out var stored) ? stored.ETag : null;
            if (current != grainState.ETag)
                throw new InconsistentStateException($"ETag mismatch writing {key}", current, grainState.ETag);
            Store[key] = (bytes, etag);
        }

        grainState.ETag = etag;
        grainState.RecordExists = true;
        return Task.CompletedTask;
    }

    public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        var key = Key(stateName, grainId);
        lock (Store)
        {
            var current = Store.TryGetValue(key, out var stored) ? stored.ETag : null;
            if (current != grainState.ETag)
                throw new InconsistentStateException($"ETag mismatch clearing {key}", current, grainState.ETag);
            Store.TryRemove(key, out _);
        }

        grainState.ETag = null;
        grainState.RecordExists = false;
        return Task.CompletedTask;
    }

    private static string Key(string stateName, GrainId grainId) => $"{stateName}|{grainId}";
}
