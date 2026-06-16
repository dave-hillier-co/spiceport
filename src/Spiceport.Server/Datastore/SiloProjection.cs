using Spiceport.Datastore;
using Spiceport.Datastore.Memory;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// A per-silo materialized read projection of the datastore, folded incrementally from the event log (the
/// same <see cref="LogEvent"/> feed the grain persists). It replaces the per-Check full-state fetch
/// (<c>LazyGrainReader</c>): the projection bootstraps ONCE per activation from a snapshot
/// (<see cref="IDatastoreGrain.ReadState"/>) and thereafter advances only by pulling the log tail
/// (<see cref="IDatastoreLog.ReadFrom"/>), so reads serve from local in-process state with no grain hop per
/// Check.
/// </summary>
/// <remarks>
/// CLOSED-TIMESTAMP CONSISTENCY: the projection is the fold of ONE ordered log, so once its
/// <see cref="AppliedWatermark"/> &gt;= <c>rev</c>, ALL commits &lt;= <c>rev</c> are present (read-your-writes /
/// no "new enemy"). <see cref="StateAtLeast"/> BLOCKS (catch-up-on-demand via <c>ReadFrom</c>) until the
/// watermark reaches <c>rev</c> before snapshotting, so a reader pinned at an exact / at-least-as-fresh token
/// is never served state derived from a stale prefix. The pinned <c>rev</c> is always &lt;= the grain head,
/// so the catch-up always terminates. Single-flight: one catch-up runs at a time; concurrent readers await it
/// and then observe the advanced state. If the projection falls below the grain's retained log window
/// (compaction GC'd past our watermark, e.g. after a long idle), <c>ReadFrom</c> throws
/// <see cref="RevisionNotFoundException"/> and we re-bootstrap from a full snapshot and continue.
/// </remarks>
internal sealed class SiloProjection
{
    /// <summary>Log-tail page size for catch-up pulls.</summary>
    private const int BatchSize = 256;

    private readonly IDatastoreGrain _grain;

    // Single-flight gate: only one bootstrap/catch-up runs at a time. Readers that find the watermark already
    // at/after their revision skip the gate entirely (the fast path).
    private readonly SemaphoreSlim _gate = new(1, 1);

    private bool _bootstrapped;

    // The folded grain-space state (the canonical fold via LogFold, matching the grain exactly) and its
    // memory-space projection, rebuilt only when _grainState advances (not per read). _memoryState is the
    // immutable snapshot readers query; it is assigned BEFORE _watermark advances so a reader that observes
    // watermark >= rev is guaranteed to see the corresponding (or fresher) snapshot.
    private DatastoreGrainState _grainState = DatastoreGrainState.Empty(0);
    private volatile DatastoreState? _memoryState;
    private long _watermark;

    public SiloProjection(IDatastoreGrain grain) => _grain = grain;

    /// <summary>The highest revision the projection has applied (all commits &lt;= it are materialized).</summary>
    public long AppliedWatermark => Interlocked.Read(ref _watermark);

    /// <summary>
    /// Ensures every commit with revision &lt;= <paramref name="rev"/> is folded into the projection, then
    /// returns the immutable memory snapshot to read against. Blocks (pulling the log tail) until the
    /// watermark reaches <paramref name="rev"/>. The returned state may be FRESHER than <paramref name="rev"/>
    /// (a concurrent catch-up advanced it); the caller's <see cref="InMemoryDatastoreReader"/> filters by
    /// <c>IsVisibleAt(rev)</c>, so the over-shoot is invisible to the read.
    /// </summary>
    public async Task<DatastoreState> StateAtLeast(long rev, CancellationToken cancellationToken = default)
    {
        // Fast path: already caught up. _memoryState is published before _watermark advances, so observing
        // watermark >= rev implies a snapshot at least as fresh as rev is visible.
        if (_bootstrapped && Interlocked.Read(ref _watermark) >= rev && _memoryState is { } fast)
            return fast;

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (!_bootstrapped)
                await Bootstrap().ConfigureAwait(false);

            while (Interlocked.Read(ref _watermark) < rev)
            {
                if (!await PullOnce().ConfigureAwait(false))
                    break; // drained to head (head >= rev), so the watermark now covers rev
            }

            return _memoryState!;
        }
        finally
        {
            _gate.Release();
        }
    }

    // Full-snapshot bootstrap: ONE ReadState per activation (or per re-bootstrap after falling below the GC
    // window). Sets the watermark to the snapshot head so subsequent catch-up only pulls the tail.
    private async Task Bootstrap()
    {
        var snapshot = await _grain.ReadState().ConfigureAwait(false);
        _grainState = snapshot;
        _memoryState = DatastoreStateConverters.ToMemory(snapshot);
        Interlocked.Exchange(ref _watermark, snapshot.HeadRevision);
        _bootstrapped = true;
    }

    // Pulls one log-tail page and folds it. Returns true if a full page came back (more may be pending);
    // false once a short/empty page proves we drained to the observed head (watermark jumps to head, which
    // covers the seed/change-free-head case where head > last event revision). Caller holds _gate.
    private async Task<bool> PullOnce()
    {
        LogSegment seg;
        try
        {
            seg = await _grain.ReadFrom(Interlocked.Read(ref _watermark), BatchSize).ConfigureAwait(false);
        }
        catch (RevisionNotFoundException)
        {
            // Fell below the grain's retained log window; rebuild from a full snapshot and continue.
            await Bootstrap().ConfigureAwait(false);
            return true;
        }

        if (seg.Events.Count > 0)
        {
            var folded = _grainState;
            foreach (var ev in seg.Events)
                folded = LogFold.ApplyEvent(folded, ev);
            _grainState = folded;
            _memoryState = DatastoreStateConverters.ToMemory(folded);
            Interlocked.Exchange(ref _watermark, seg.Events[^1].Revision);
        }

        if (seg.Events.Count < BatchSize)
        {
            Interlocked.Exchange(ref _watermark, Math.Max(Interlocked.Read(ref _watermark), seg.HeadRevision));
            return false;
        }

        return true;
    }
}
