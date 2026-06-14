using Orleans.Runtime;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// The cluster-singleton datastore grain (<see cref="IDatastoreGrain.Key"/> = 0): the single source of
/// truth for the whole MVCC datastore, persisted as <see cref="DatastoreGrainState"/>. It is a single
/// non-reentrant activation, so each turn runs to completion before the next — which is what makes
/// <see cref="CompareAndSwap"/> atomic (the head-compare and the persist cannot interleave with another
/// write). The grain holds no evaluation logic; the MVCC fold/transaction mechanics live in
/// <c>GrainBackedDatastore</c> (which converts this state to the in-memory state and reuses it).
/// </summary>
public sealed class DatastoreGrain : Grain, IDatastoreGrain
{
    // Orleans grain code must not ConfigureAwait(false); keep the captured context.
    private const ConfigureAwaitOptions ContinueOnCapturedContext = ConfigureAwaitOptions.ContinueOnCapturedContext;

    private readonly IPersistentState<DatastoreGrainState> _state;

    public DatastoreGrain(
        [PersistentState("state", "datastore")] IPersistentState<DatastoreGrainState> state) =>
        _state = state;

    public override async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        // A fresh persistent state has HeadRevision 0; seed an empty state at a monotonic timestamp so the
        // head is a real revision (mirrors InMemoryDatastore's initial Empty(NowNanos)) and PERSIST it.
        // Persisting the seed keeps the pre-first-write revision floor stable across re-activation: without
        // it, an idle-collected grain would re-seed with a fresh, larger NowNanos and silently move the
        // observable head (and could invalidate a zedtoken minted against the in-memory seed).
        if (_state.State.HeadRevision == 0)
        {
            _state.State = DatastoreGrainState.Empty(NowNanos());
            await _state.WriteStateAsync().ConfigureAwait(ContinueOnCapturedContext);
        }
        await base.OnActivateAsync(cancellationToken).ConfigureAwait(ContinueOnCapturedContext);
    }

    /// <inheritdoc />
    public Task<DatastoreGrainState> ReadState() => Task.FromResult(_state.State);

    /// <inheritdoc />
    public Task<DatastoreHeadWire> GetHead() =>
        Task.FromResult(new DatastoreHeadWire(
            _state.State.HeadRevision, _state.State.SchemaHashAt(_state.State.HeadRevision)));

    /// <inheritdoc />
    public async Task<bool> CompareAndSwap(long expectedHead, DatastoreGrainState newState)
    {
        ArgumentNullException.ThrowIfNull(newState);
        if (_state.State.HeadRevision != expectedHead)
            return false;
        _state.State = newState;
        await _state.WriteStateAsync().ConfigureAwait(ContinueOnCapturedContext);
        return true;
    }

    private static long NowNanos() => (DateTimeOffset.UtcNow - DateTimeOffset.UnixEpoch).Ticks * 100L;
}
