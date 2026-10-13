using Orleans.Runtime;
using Orleans.Storage;

namespace Spiceport.Grains.Tests.FaultInjection;

/// <summary>
/// The faults <see cref="FaultInjectingGrainStorage"/> injects. Process-wide because the storage provider
/// is built by the silo's container; safe because every cluster-using test class is serialized by
/// <see cref="MeshClusterCollection"/>.
/// </summary>
internal static class StorageFaults
{
    private static int _failSequencerWrites;
    private static int _failShardWrites;
    private static int _injectedSequencerFaults;
    private static int _injectedShardFaults;

    /// <summary>When set, shard-projection writes report success without persisting (a lying store).</summary>
    public static volatile bool LoseShardWrites;

    /// <summary>Makes the next <paramref name="count"/> sequencer state writes throw.</summary>
    public static void FailSequencerWrites(int count) => Volatile.Write(ref _failSequencerWrites, count);

    /// <summary>Makes the next <paramref name="count"/> shard-projection writes throw.</summary>
    public static void FailShardWrites(int count) => Volatile.Write(ref _failShardWrites, count);

    /// <summary>How many sequencer write failures actually fired since the last reset.</summary>
    public static int InjectedSequencerFaults => Volatile.Read(ref _injectedSequencerFaults);

    /// <summary>How many shard-projection write failures actually fired since the last reset.</summary>
    public static int InjectedShardFaults => Volatile.Read(ref _injectedShardFaults);

    public static void Reset()
    {
        FailSequencerWrites(0);
        FailShardWrites(0);
        LoseShardWrites = false;
        Volatile.Write(ref _injectedSequencerFaults, 0);
        Volatile.Write(ref _injectedShardFaults, 0);
    }

    internal static bool TakeSequencerFault() => TakeOne(ref _failSequencerWrites, ref _injectedSequencerFaults);

    internal static bool TakeShardFault() => TakeOne(ref _failShardWrites, ref _injectedShardFaults);

    private static bool TakeOne(ref int remaining, ref int injected)
    {
        while (true)
        {
            var current = Volatile.Read(ref remaining);
            if (current <= 0)
                return false;
            if (Interlocked.CompareExchange(ref remaining, current - 1, current) == current)
            {
                Interlocked.Increment(ref injected);
                return true;
            }
        }
    }
}

/// <summary>
/// Wraps the "datastore" grain-storage provider (the sequencer's journaled state, the shard projections
/// and the shard index) so the harness can fail writes or silently drop them. Reads are never faulted:
/// a failed read surfaces as an activation failure, which the deactivation faults already cover.
/// </summary>
internal sealed class FaultInjectingGrainStorage(IGrainStorage inner) : IGrainStorage
{
    public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
        inner.ReadStateAsync(stateName, grainId, grainState);

    public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        var grainType = grainId.Type.ToString();
        if (grainType == "datastore" && StorageFaults.TakeSequencerFault())
            throw new InvalidOperationException("injected sequencer write failure (fault harness)");

        if (grainType == "graphshard")
        {
            if (StorageFaults.TakeShardFault())
                throw new InvalidOperationException("injected shard-projection write failure (fault harness)");
            if (StorageFaults.LoseShardWrites)
                return Task.CompletedTask;
        }

        return inner.WriteStateAsync(stateName, grainId, grainState);
    }

    public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState) =>
        inner.ClearStateAsync(stateName, grainId, grainState);
}
