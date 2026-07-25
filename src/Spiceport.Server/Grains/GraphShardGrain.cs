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
/// <para>
/// Filtered serves (the 3.2 subject-filter pushdown) are answered from a lazily-built subject-keyed
/// index over the served state so a point-membership probe is O(matches), not an O(userset) scan
/// inside this single activation. Memory: the index holds REFERENCES to the same
/// <see cref="StoredRelationshipWire"/> rows the state already holds (dictionary/list overhead only,
/// no row copies), lives only on hot activations, and is dropped with the activation — the
/// silo-memory-is-O(hot-working-set) principle is unchanged.
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

    // --- Subject-keyed index over the served state (scalability-program 3.2, serve-side half) ---
    // Rebuilt lazily at serve time whenever the served GraphShardState INSTANCE changes (the state is
    // an immutable record replaced whole, so reference identity is the correct staleness signal).
    // Safe under [AlwaysInterleave]: Orleans turns are single-threaded and interleave only at awaits;
    // Serve is fully synchronous, so a rebuild can never be observed half-built. Buckets stay
    // multi-version (ALL stored versions of a subject's rows); visibility filters at serve time.
    private GraphShardState? _indexedState;
    private Dictionary<(string SubjectType, string SubjectId), List<StoredRelationshipWire>>? _subjectIndex;
    private List<StoredRelationshipWire>? _nonTerminalRows;

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
    public async Task<GraphShardRowsReply> RowsAt(
        long revision, FullRelationshipsFilterWire? filter, CancellationToken cancellationToken)
    {
        // Fast path: hydrated and the watermark already covers the pinned revision. _state is an
        // immutable record replaced whole, so serving off it with no gate is safe; the interleaved
        // reader sees either the previous fold or the new one, never a partial.
        if (_hydrated && _state.AppliedRevision >= revision && ShardFold.IsReadableAt(_state, revision))
            return Serve(revision, filter);

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

            return Serve(revision, filter);
        }
        finally
        {
            _gate.Release();
        }
    }

    private GraphShardRowsReply Serve(long revision, FullRelationshipsFilterWire? filter)
    {
        // A revision below the shard's GC floor cannot be read exactly (rows already collected below the
        // floor would be silently missing) — reject, mirroring the MvccSnapshotReader constructor guard.
        // RevisionNotFoundException round-trips the grain boundary via RevisionNotFoundSurrogate.
        if (!ShardFold.IsReadableAt(_state, revision))
            throw new RevisionNotFoundException(new TimestampRevision(revision));

        if (filter is null)
            return new GraphShardRowsReply(ShardFold.VisibleAt(_state, revision).ToImmutableList());

        // Subject-filter pushdown (scalability-program 3.2): apply the filter server-side so the reply
        // is O(matches), not O(userset). Converted ONCE per call; the row conversion reuses the same
        // WireConvert mapping the reader applies client-side, so server-side and client-side Matches
        // can never disagree on a row. Expiration deliberately stays a caller-side, caller-clock
        // concern (see IGraphShardGrain.RowsAt). When every selector is index-servable the candidates
        // come from the subject-keyed index (O(matches) work, not an O(userset) scan serialized on
        // this activation); the FULL pipeline — IsVisibleAt + convert + Matches — still runs over the
        // candidates, so an index-served answer is byte-identical to the scan. Any other selector
        // shape falls the whole call back to the scan.
        var coreFilter = WireConvert.ToCoreFilter(filter);

        if (TryCollectIndexCandidates(coreFilter, out var candidates))
        {
            var served = ImmutableList.CreateBuilder<RelationshipWire>();
            foreach (var row in candidates)
            {
                if (ShardFold.IsVisibleAt(row, revision) && coreFilter.Matches(WireConvert.ToRelationship(row.Relationship)))
                    served.Add(row.Relationship);
            }
            return new GraphShardRowsReply(served.ToImmutable());
        }

        return new GraphShardRowsReply(
            ShardFold.VisibleAt(_state, revision)
                .Where(row => coreFilter.Matches(WireConvert.ToRelationship(row)))
                .ToImmutableList());
    }

    /// <summary>
    /// Collects the candidate rows for <paramref name="filter"/> from the subject-keyed index, when
    /// every selector is index-servable; returns false (whole call falls back to the full scan) when
    /// any selector shape the index cannot serve appears — no selectors at all, a type without ids,
    /// ids without a type, or a relation filter other than the exact non-terminal shape. Per-branch
    /// superset arguments:
    ///   - explicit type + explicit ids: a bucket holds EVERY stored row of that (type, id) subject
    ///     regardless of subject relation — a superset of any relation-filtered selector over it, so
    ///     the final Matches narrows and can never miss;
    ///   - OnlyNonEllipsisRelations with no type/ids (and no other relation constraint): the
    ///     non-terminals list is EXACTLY that selector's domain (every stored row whose subject
    ///     relation is not the ellipsis).
    /// Candidates are deduplicated by REFERENCE: a non-terminal row appears in both its subject's
    /// bucket and the non-terminals list, and multiple stored versions of one identity are distinct
    /// instances that must each survive to the visibility check.
    /// </summary>
    private bool TryCollectIndexCandidates(
        RelationshipsFilter filter, out IReadOnlyCollection<StoredRelationshipWire> candidates)
    {
        candidates = [];
        if (filter.OptionalSubjectsSelectors is not { Count: > 0 } selectors)
            return false;

        foreach (var selector in selectors)
        {
            var servableBucketProbe =
                selector.OptionalSubjectType is not null && selector.OptionalSubjectIds is { Count: > 0 };
            var servableNonTerminal =
                selector.OptionalSubjectType is null
                && selector.OptionalSubjectIds is not { Count: > 0 }
                && selector.RelationFilter is
                {
                    NonEllipsisRelation: null,
                    IncludeEllipsisRelation: false,
                    OnlyNonEllipsisRelations: true,
                };
            if (!servableBucketProbe && !servableNonTerminal)
                return false;
        }

        EnsureIndex();

        var collected = new HashSet<StoredRelationshipWire>(ReferenceEqualityComparer.Instance);
        foreach (var selector in selectors)
        {
            if (selector.OptionalSubjectType is { } subjectType)
            {
                foreach (var id in selector.OptionalSubjectIds!)
                {
                    if (_subjectIndex!.TryGetValue((subjectType, id), out var bucket))
                        collected.UnionWith(bucket);
                }
            }
            else
            {
                collected.UnionWith(_nonTerminalRows!);
            }
        }

        candidates = collected;
        return true;
    }

    /// <summary>Rebuilds the subject-keyed index iff the served state instance changed (see the field remarks).</summary>
    private void EnsureIndex()
    {
        if (ReferenceEquals(_indexedState, _state))
            return;

        var subjectIndex = new Dictionary<(string, string), List<StoredRelationshipWire>>();
        var nonTerminals = new List<StoredRelationshipWire>();
        foreach (var row in _state.Rows)
        {
            var rel = row.Relationship;
            var key = (rel.SubjectType, rel.SubjectId);
            if (!subjectIndex.TryGetValue(key, out var bucket))
                subjectIndex[key] = bucket = [];
            bucket.Add(row);

            // The stored subject relation is normalized on fold (empty => ellipsis; see
            // ShardFold.ApplyEvent), so the ellipsis comparison is exact.
            if (rel.SubjectRelation != CoreConstants.Ellipsis)
                nonTerminals.Add(row);
        }

        _subjectIndex = subjectIndex;
        _nonTerminalRows = nonTerminals;
        _indexedState = _state;
    }
}
