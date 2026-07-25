using System.Collections.Immutable;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// One graph shard as a grain: the activation state IS the shard cache. The state is the per-key
/// restriction of the datastore fold (<see cref="ShardFold"/>) for the <see cref="GraphShardKeyWire"/>
/// named by this grain's string key — hydrated once from <see cref="IDatastoreGrain.ReadShard"/> and
/// thereafter advanced by tailing the same <see cref="LogEvent"/> feed the sequencer persists.
/// Cold keys never activate; Orleans idle collection is the eviction policy — silo memory is O(hot
/// working set), not O(graph) (<c>docs/graph-sharded-datastore.md</c> §2).
/// </summary>
/// <remarks>
/// The incremental bootstrap-then-tail-fold shape (the pattern the retired per-silo whole-graph
/// projection carried, restricted here to one key): a single-flight <see cref="SemaphoreSlim"/> gate, a
/// fast path (serve when the watermark already covers the pinned revision), and a catch-up-on-demand
/// pull loop with re-bootstrap on <see cref="RevisionNotFoundException"/> (compaction GC'd past our
/// watermark). The per-shard <see cref="GraphShardState.AppliedRevision"/> watermark is the
/// closed-timestamp gate — valid because every shard folds EVERY log event (the watermark advances even
/// when nothing matches the key; see <see cref="ShardFold.ApplyEvent"/>) — so "watermark &gt;= rev"
/// proves all commits &lt;= rev are present in this slice (<c>docs/graph-sharded-datastore.md</c> §2).
/// A cold shard ALWAYS hydrates via <see cref="IDatastoreGrain.ReadShard"/> first, never by replaying
/// <c>ReadFrom(0)</c> — the log's retained tail starts at the compaction floor, so a from-zero replay
/// would silently miss compacted history.
/// <para>
/// GC-floor stance: a shard enforces the floor IT has folded/hydrated — never a per-read probe of the
/// singleton — so at the floor boundary a cold shard (hydrated after a GC run) may reject a pinned
/// revision that a warm shard which has not yet folded the GC event still serves, and vice versa. Data
/// is never wrong on either side: a state that has not folded the GC event still retains every row live
/// at the pinned revision, while a state that has collected them carries the advanced floor and throws.
/// Only the stale-token ERROR surfaces earlier or later — bounded by the shard's own catch-up.
/// </para>
/// </remarks>
[GraphLocalityPlacement] // First-activation locality hint only (director no-ops to a random pick unless
                         // GraphPlacementOptions.CoLocateWithShards is enabled); the grain directory
                         // stays the sole authority for identity/dedup. See GraphLocalityPlacement.
public sealed class GraphShardGrain : Grain, IGraphShardGrain
{
    /// <summary>Log-tail page size for catch-up pulls (matches the Watch feed's page size).</summary>
    private const int BatchSize = 256;

    // Single-flight gate: only one hydration/catch-up runs at a time. RowsAt is [AlwaysInterleave], so
    // concurrent readers whose revision the watermark already covers skip the gate entirely (the fast
    // path); the rest queue here and re-check after acquiring.
    private readonly SemaphoreSlim _gate = new(1, 1);

    private GraphShardState _state = GraphShardState.Empty;
    private bool _hydrated;

    /// <summary>The shard key parsed once from the grain's string key.</summary>
    private GraphShardKeyWire _key = null!;

    private IDatastoreGrain _datastore = null!;

    /// <inheritdoc />
    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        _key = GraphShardGrainKey.Parse(this.GetPrimaryKeyString());
        _datastore = GrainFactory.GetGrain<IDatastoreGrain>(IDatastoreGrain.Key);
        return base.OnActivateAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async Task<GraphShardRowsReply> RowsAt(long revision, CancellationToken cancellationToken)
    {
        // Fast path: hydrated and the watermark already covers the pinned revision. _state is an
        // immutable record replaced whole, so serving off it with no gate is safe; the interleaved
        // reader sees either the previous fold or the new one, never a partial.
        if (_hydrated && _state.AppliedRevision >= revision && ShardFold.IsReadableAt(_state, revision))
            return Serve(revision);

        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (!_hydrated)
            {
                // ALWAYS hydrate via the per-key snapshot read first — never rely on ReadFrom(0)
                // semantics for a cold shard (see the class remarks).
                _state = await _datastore.ReadShard(_key);
                _hydrated = true;
            }

            // Watermark as of the last re-hydrate, or null when no re-hydrate has happened yet in
            // this catch-up. Guards the compaction-window spin: right after the singleton commits,
            // its post-commit compaction can briefly leave the hydrated head below the retained
            // recent-tail floor, so ReadFrom keeps rejecting the freshly re-hydrated watermark.
            long? lastRehydrateWatermark = null;

            while (_state.AppliedRevision < revision)
            {
                cancellationToken.ThrowIfCancellationRequested();

                LogSegment seg;
                try
                {
                    seg = await _datastore.ReadFrom(_state.AppliedRevision, BatchSize);
                }
                catch (RevisionNotFoundException)
                {
                    // Fell below the grain's retained log window; re-hydrate from a fresh per-key
                    // snapshot and continue. If we
                    // must re-hydrate AGAIN without the watermark having advanced, we are inside the
                    // compaction window described above — back off briefly instead of hot-spinning
                    // against the singleton until its compaction settles.
                    if (lastRehydrateWatermark == _state.AppliedRevision)
                        await Task.Delay(TimeSpan.FromMilliseconds(10), cancellationToken);
                    _state = await _datastore.ReadShard(_key);
                    lastRehydrateWatermark = _state.AppliedRevision;
                    continue;
                }

                foreach (var ev in seg.Events)
                    _state = ShardFold.ApplyEvent(_state, ev, _key);

                if (seg.Events.Count < BatchSize)
                {
                    // A short page proves we drained to the observed head; jump the watermark to it
                    // (covers the seed/change-free-head case where head > the last event's revision).
                    // The pinned revision is always <= the grain head, so the loop condition now
                    // releases us.
                    _state = _state with { AppliedRevision = Math.Max(_state.AppliedRevision, seg.HeadRevision) };
                    break;
                }
            }

            return Serve(revision);
        }
        finally
        {
            _gate.Release();
        }
    }

    private GraphShardRowsReply Serve(long revision)
    {
        // A revision below the shard's GC floor cannot be read exactly (rows already collected below the
        // floor would be silently missing) — reject, mirroring the MvccSnapshotReader constructor guard.
        // RevisionNotFoundException round-trips the grain boundary via RevisionNotFoundSurrogate.
        if (!ShardFold.IsReadableAt(_state, revision))
            throw new RevisionNotFoundException(new TimestampRevision(revision));

        return new GraphShardRowsReply(ShardFold.VisibleAt(_state, revision).ToImmutableList());
    }
}
