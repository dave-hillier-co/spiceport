using System.Runtime.CompilerServices;
using Microsoft.Extensions.Options;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// An <see cref="IDatastore"/> that delegates all state to the cluster-singleton
/// <see cref="IDatastoreGrain"/> (the single source of truth) and reuses the in-memory MVCC mechanics
/// (<see cref="MvccReadWriteTransaction"/>, <see cref="MvccSnapshotReader"/>, the
/// <c>DatastoreState</c> fold) by converting the grain wire state to/from the in-memory state. Snapshot
/// reads fetch the sequencer's materialized state (<see cref="IDatastoreGrain.ReadState"/>) once per
/// reader; the engines' per-Check graph reads do NOT come through here — they go through the
/// <see cref="IGraphReaderSource"/> shard mesh (<see cref="ShardedGraphReader"/>). What remains on this
/// facade is revision resolution (head/optimized/check), token minting, Watch, and the compatibility
/// write path (<see cref="ReadWriteTx"/>) that tests/BulkImport/SeedData drive. Writes use an optimistic
/// compare-and-swap retry loop. This is a DI service, not a grain, so <c>ConfigureAwait(false)</c> is
/// correct here.
/// </summary>
/// <remarks>
/// This instance never owns the per-silo <see cref="LogWatchHub"/> it pulses and parks Watch streams on;
/// the hub is a DI singleton (registered by
/// <see cref="ServiceCollectionExtensions.AddSpiceportGrainServices"/>) whose lifetime belongs to the
/// container, which disposes it (<see cref="LogWatchHub.DisposeAsync"/>) on silo teardown. This type
/// therefore has nothing of its own to dispose.
/// </remarks>
public sealed class GrainBackedDatastore : IDatastore
{
    /// <summary>Bound on CAS retries before surfacing a serialization conflict.</summary>
    private const int MaxCasAttempts = 50;

    /// <summary>Log-tail page size for the Watch changefeed.</summary>
    private const int WatchBatchSize = 256;

    /// <summary>
    /// A stable, cluster-wide datastore id. It must be identical on every silo so a token minted on one
    /// silo decodes Valid on another (a per-instance Guid would make every cross-silo token mismatch).
    /// There is exactly one logical datastore (the singleton grain), so a fixed id is correct.
    /// </summary>
    private const string UniqueId = "grain-backed-datastore";

    private readonly IGrainFactory _grainFactory;
    private readonly long _quantizationNanos;
    private readonly long _gcWindowNanos;

    // Per-silo Watch notifier. The local write path pulses it on commit for instant same-silo Watch
    // latency; cross-silo commits arrive by observer push from the datastore grain, with the hub's slow
    // heartbeat as the missed-push backstop. The DI container owns its lifecycle (see the class remarks).
    private readonly LogWatchHub _hub;

    // Cached optimized-revision candidate (mirrors ReferenceDatastore's CachedOptimizedRevisions): a real
    // head sampled when a window opens, held stable until the bucket boundary so near-in-time
    // minimize-latency checks WITHIN THIS SILO share one revision (and therefore one dispatch cache key).
    // NOTE: this cache is per-silo (one GrainBackedDatastore per DI container), so two silos can sample the
    // grain head at slightly different times in the same window and key under different revisions — bounded
    // min-latency staleness, never a stale-under-fresh-token serve (each value is a real committed head).
    // Cross-mesh parity would need a quantized GetHead on the grain; left as a later optimization.
    // Guarded by _optLock.
    private readonly object _optLock = new();
    private RevisionWithSchemaHash? _optimizedCache;
    private long _optimizedValidThroughNanos;

    // The freshest committed head THIS instance has observed (its own successful commits). A stale
    // duplicate activation of the sequencer during membership churn can serve a ReadState below it,
    // and a write base folded from such a state would violate read-your-writes (the lambda would stage
    // against a snapshot missing this instance's own committed writes). The base fetch gates on it via
    // SequencerStateFetch — the closed-timestamp gate, formerly the projection watermark wait.
    private long _observedCommittedHead;

    /// <summary>
    /// Creates a grain-backed datastore over the cluster-singleton datastore grain, sharing the per-silo
    /// <paramref name="hub"/> for Watch wake-ups. This instance never owns the hub's lifetime; see the
    /// class remarks.
    /// </summary>
    /// <param name="grainFactory">The Orleans grain factory used to reach the singleton datastore grain.</param>
    /// <param name="hub">The per-silo Watch notifier (a DI singleton the container disposes).</param>
    /// <param name="quantization">Quantization window for <see cref="OptimizedRevision"/> (default 5s).</param>
    /// <param name="gcWindow">
    /// How long old revisions remain valid. Takes priority over <paramref name="gcOptions"/> when supplied
    /// (a test seam for pinning an exact value); otherwise falls back to <c>gcOptions.Value.Window</c>, and
    /// only defaults to 24h when neither is given. This MUST track the same
    /// <see cref="DatastoreGcOptions.Window"/> the singleton <see cref="DatastoreGrain"/> was configured
    /// with — that value is what actually drives the grain's real <c>GcFloor</c> (see
    /// <see cref="DatastoreGrain.RunGc"/>), so a caller that configures a non-default window for the grain
    /// must pass the SAME <see cref="IOptions{TOptions}"/> here (see the production wiring in
    /// <c>Spiceport.Api</c>/<c>Spiceport.Silo</c>'s <c>Program.cs</c>). A mismatched, independently-hardcoded
    /// window here would wrongly reject (or wrongly accept) a still-valid revision relative to the real
    /// per-host retention policy.
    /// </param>
    /// <param name="gcOptions">
    /// The same <see cref="DatastoreGcOptions"/> the singleton <see cref="DatastoreGrain"/> is configured
    /// with. Optional so a host/test that never registers it still gets the 24h default (matching
    /// <see cref="DatastoreGcOptions"/>'s own default), consistent with the grain's own optional-options
    /// pattern.
    /// </param>
    public GrainBackedDatastore(
        IGrainFactory grainFactory, LogWatchHub hub, TimeSpan? quantization = null,
        TimeSpan? gcWindow = null, IOptions<DatastoreGcOptions>? gcOptions = null)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(hub);
        _grainFactory = grainFactory;
        _quantizationNanos = ComputeQuantizationNanos(quantization);
        _gcWindowNanos = ComputeGcWindowNanos(gcWindow, gcOptions);
        _hub = hub;
    }

    private static long ComputeQuantizationNanos(TimeSpan? quantization) =>
        (long)((quantization ?? TimeSpan.FromSeconds(5)).TotalMilliseconds) * 1_000_000L;

    private static long ComputeGcWindowNanos(TimeSpan? gcWindow, IOptions<DatastoreGcOptions>? gcOptions)
    {
        var window = gcWindow ?? gcOptions?.Value.Window ?? TimeSpan.FromHours(24);
        return (long)(window.TotalMilliseconds) * 1_000_000L;
    }

    private IDatastoreGrain Grain => _grainFactory.GetGrain<IDatastoreGrain>(IDatastoreGrain.Key);

    public IDatastoreReader SnapshotReader(IRevision revision)
    {
        var rev = ToNanos(revision);
        // The (async) state acquisition defers to the first read; every subsequent query is served
        // in-process via one MvccSnapshotReader over the sequencer's materialized state
        // (IDatastoreGrain.ReadState — a full-state fetch, deliberately: this reader serves snapshot-wide
        // reads such as the fold-equivalence oracle and BulkExport, never the per-Check hot path, which
        // goes through the IGraphShardGrain mesh). The state is at the grain's confirmed head, which is
        // at or past any resolvable pinned revision (revisions are minted by the grain and confirmed
        // before their token exists), and MVCC row visibility (IsVisibleAt) makes the head state exact
        // for any retained pinned revision.
        //
        // GC-floor rejection (MvccSnapshotReader's ctor guard) is enforced from the FETCHED state's own
        // floor. Unlike the retired per-silo projection (whose locally-folded floor could lag the
        // singleton by a bounded catch-up window), the fetched state always carries the sequencer's
        // current floor, so a below-floor pin is rejected on the reader's first query.
        //
        // Closed-timestamp gate on the fetch itself: a stale duplicate activation during membership
        // churn can serve an old head, and a reader pinned at R must never be built over state whose
        // head < R (rows committed at or below R could be silently missing). SequencerStateFetch
        // refetches until the head covers the pin — the successor of the retired projection's
        // watermark wait.
        return new DeferredReader(async ct =>
            new MvccSnapshotReader(
                DatastoreStateConverters.ToMemory(
                    await SequencerStateFetch.StateCovering(Grain, rev, ct).ConfigureAwait(false)),
                rev, _ => true));
    }

    public async Task<RevisionWithSchemaHash> HeadRevision(CancellationToken cancellationToken = default)
    {
        var head = await Grain.GetHead().ConfigureAwait(false);
        return new RevisionWithSchemaHash(new TimestampRevision(head.Head), head.SchemaHash);
    }

    public async Task<RevisionWithSchemaHash> OptimizedRevision(CancellationToken cancellationToken = default)
    {
        var now = NowNanos();
        lock (_optLock)
        {
            if (_quantizationNanos > 0 && _optimizedCache is { } cached && now < _optimizedValidThroughNanos)
                return cached;
        }

        // Cache miss: sample the real head from the grain (await OUTSIDE the lock).
        var head = await Grain.GetHead().ConfigureAwait(false);
        var sampled = new RevisionWithSchemaHash(new TimestampRevision(head.Head), head.SchemaHash);
        // Recompute now AFTER the grain hop so the window boundary is not skewed past its bucket by hop
        // latency (ReferenceDatastore samples under its lock with no intervening await).
        var nowAfter = NowNanos();

        lock (_optLock)
        {
            // Re-check: another caller may have populated an in-window candidate while we fetched. Keep it
            // so all callers in one window (within this silo) share a single value.
            if (_quantizationNanos > 0 && _optimizedCache is { } c && nowAfter < _optimizedValidThroughNanos)
                return c;
            if (_quantizationNanos > 0)
            {
                _optimizedCache = sampled;
                _optimizedValidThroughNanos = nowAfter - (nowAfter % _quantizationNanos) + _quantizationNanos;
            }
            return sampled;
        }
    }

    public async Task<IRevision> ReadWriteTx(
        Func<IReadWriteTransaction, Task> transaction,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);

        // COMPATIBILITY PATH — honest cost note: each attempt fetches the sequencer's FULL materialized
        // state (ReadState) as its write base. That is acceptable because production writes are
        // declarative through IDatastoreGrain.Commit (preconditions/updates/deleteByFilter evaluated at
        // the sequencer); this lambda-shaped path survives only for tests, BulkImport and SeedData, where
        // per-write cost is not a scaling concern. The grain's CAS append remains the sole serialization
        // point.
        for (var attempt = 0; ; attempt++)
        {
            // 1. Fetch the write base. ReadState returns the grain's confirmed fold, so its HeadRevision
            //    is the freshest committed head this attempt can observe; taking expectedHead from the
            //    SAME state keeps the CAS and the diff base exactly in agreement. Read-your-writes across
            //    successive calls on this same instance holds because the grain's Commit only returns
            //    after it has confirmed the new head (see DatastoreGrain), so the next attempt's
            //    ReadState observes it. The fetch is gated on the freshest head this instance has itself
            //    committed (see _observedCommittedHead): a stale duplicate activation during membership
            //    churn must not hand us a base missing our own writes.
            var baseState = DatastoreStateConverters.ToMemory(
                await SequencerStateFetch
                    .StateCovering(Grain, Volatile.Read(ref _observedCommittedHead), cancellationToken)
                    .ConfigureAwait(false));
            var expectedHead = baseState.HeadRevision;

            // 2. Mint a provisional revision monotonically over the observed head (mirrors ReferenceDatastore).
            //    This revision pins the local tx so the staged view and preconditions are evaluated at a
            //    fixed point; the grain mints the AUTHORITATIVE revision when it appends the event.
            var now = NowNanos();
            var newRevision = now > expectedHead ? now : expectedHead + 1;

            // 3. Run the caller lambda over an in-memory tx pinned to this base. Preconditions and
            //    SchemaChangeValidator read the tx reader (the staged-view over the snapshot). Any
            //    exception thrown by the lambda (create-conflict, precondition, schema-validation, counter
            //    conflict) propagates AS-IS and aborts the whole call — it is NOT a retry.
            var tx = new MvccReadWriteTransaction(baseState, newRevision);
            await transaction(tx).ConfigureAwait(false);

            // 4. Derive the declarative commit from the committed state: the net relationship/schema/counter
            //    diff at this revision (reusing the single per-revision diff definition). The grain re-mints
            //    the revision and stamps it, so the request carries no final revision.
            var committed = tx.Commit();
            var request = CommitFromState(committed, newRevision, expectedHead);

            // 5. Submit to the sequencer's Commit with the compatibility CAS (ExpectedHead): applies only if
            //    the grain head still equals expectedHead. No preconditions ride along — the lambda already
            //    evaluated its own reads client-side against this exact base, and the ExpectedHead compare
            //    keeps them race-free. Returns the AUTHORITATIVE revision the grain minted.
            var reply = await Grain.Commit(request).ConfigureAwait(false);
            if (reply.Revision is { } revision)
            {
                // Record our own committed head so the next call's base fetch cannot be served below it
                // (read-your-writes across calls on this instance). Monotonic max under contention.
                InterlockedMax(ref _observedCommittedHead, revision);
                // Wake any local Watch stream immediately (same-silo commits skip the poll latency).
                _hub.Pulse(revision);
                return new TimestampRevision(revision);
            }

            // Anything other than a lost CAS is unexpected on this path: the lambda staged every guarded
            // operation client-side against the very base the ExpectedHead pins, so the grain's re-execution
            // can only reach the same outcome. Surface it loudly rather than retry.
            if (reply.Failure is { Kind: not CommitFailureKind.HeadMoved } failure)
                throw new InvalidOperationException(
                    $"unexpected commit failure on the compatibility write path: {failure.Kind}: {failure.Detail}");

            // 6. Head moved under us. Reload and re-run the WHOLE lambda (so preconditions and validation
            //    re-evaluate against the new base — race-free). Bounded retries; on exhaustion surface the
            //    same exception type ReferenceDatastore throws on a concurrent write.
            if (attempt + 1 >= MaxCasAttempts)
                throw new SerializationException();
        }
    }

    public async Task<bool> CheckRevision(IRevision revision, CancellationToken cancellationToken = default)
    {
        var head = await Grain.GetHead().ConfigureAwait(false);
        var rev = ToNanos(revision);
        // The REAL GC floor is the hard bound (below it, MVCC rows are actually gone); the nominal window
        // is kept as an additional, stricter-or-equal bound for API-parity with ReferenceDatastore/SpiceDB
        // even before GC has caught up to it (e.g. right after activation, when head.GcFloor is still 0).
        return rev <= head.Head && rev >= head.Head - _gcWindowNanos && rev >= head.GcFloor;
    }

    public async IAsyncEnumerable<RevisionChange> Watch(
        IRevision afterRevision,
        WatchOptions options,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(afterRevision);
        ArgumentNullException.ThrowIfNull(options);

        var cursor = ToNanos(afterRevision);

        // Validate the cursor against the REAL GC floor (mirror ReferenceDatastore: RevisionNotFoundException).
        // The nominal window is kept alongside it as a stricter-or-equal bound for API-parity even before
        // GC has caught up (head0.GcFloor starts at 0 on a fresh datastore).
        var head0 = await Grain.GetHead().ConfigureAwait(false);
        if (!(cursor <= head0.Head && cursor >= head0.Head - _gcWindowNanos && cursor >= head0.GcFloor))
            throw new RevisionNotFoundException(afterRevision);

        _hub.EnsureStarted();

        while (!cancellationToken.IsCancellationRequested)
        {
            // Pull only the changes since the cursor straight from the log (the diff), not the whole state.
            var segment = await Grain.ReadFrom(cursor, WatchBatchSize).ConfigureAwait(false);

            if (segment.Events.Count > 0)
            {
                foreach (var ev in segment.Events)
                {
                    var change = BuildChange(ev, options);
                    if (change is not null)
                        yield return change;
                    cursor = ev.Revision;
                }

                // The checkpoint rides the revision the feed has now progressed through, so a consumer
                // filtering to a content subset still observes liveness even if nothing matched its filter.
                if ((options.Content & WatchContent.Checkpoints) != 0)
                    yield return new RevisionChange(
                        new TimestampRevision(cursor), Array.Empty<RelationshipUpdate>(),
                        SchemaChanged: false, IsCheckpoint: true);

                // Drain any further already-committed events before parking on the signal.
                continue;
            }

            // Caught up: park until a commit advances the head past the cursor (a local commit pulses the hub
            // directly; cross-silo commits arrive by observer push, backstopped by the hub's heartbeat). No
            // per-stream timer.
            try
            {
                await _hub.WaitForChangeAfter(cursor, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                yield break;
            }
        }
    }

    public Task<string> GetUniqueId(CancellationToken cancellationToken = default) => Task.FromResult(UniqueId);

    public Task<IRevisionParser> GetRevisionParser(CancellationToken cancellationToken = default) =>
        Task.FromResult<IRevisionParser>(new TimestampRevisionParser(UniqueId));

    /// <summary>
    /// A no-op: this instance owns no lifetime of its own (see the class remarks) — the shared hub belongs
    /// entirely to the DI container it was registered in.
    /// </summary>
    public Task Close() => Task.CompletedTask;

    // --- internals ---

    /// <summary>
    /// Maps a single <see cref="LogEvent"/> to the <see cref="RevisionChange"/> the Watch feed emits, honoring
    /// the requested content flags. Returns null when nothing in the event matches the requested content (the
    /// caller still rides a checkpoint at the revision if checkpoints were requested). This is the Stage-0
    /// payload equivalence (the log carries the same per-revision diff the state-derived feed produced).
    /// </summary>
    private static RevisionChange? BuildChange(LogEvent ev, WatchOptions options)
    {
        var includeRels = (options.Content & WatchContent.Relationships) != 0;
        var includeSchema = (options.Content & WatchContent.Schema) != 0;

        var relChanges = includeRels && ev.RelationshipChanges.Count > 0
            ? ev.RelationshipChanges.Select(WireConvert.ToUpdate).ToList()
            : (IReadOnlyList<RelationshipUpdate>)Array.Empty<RelationshipUpdate>();
        var schemaChanged = includeSchema && ev.SchemaChange is not null;

        if (relChanges.Count == 0 && !schemaChanged)
            return null;

        return new RevisionChange(new TimestampRevision(ev.Revision), relChanges, schemaChanged);
    }

    /// <summary>
    /// Builds the compatibility-path <see cref="CommitRequest"/> for a committed transaction by reusing the
    /// single per-revision diff (<see cref="LogEventFactory.EventFromState"/>) over the committed state: the
    /// resolved relationship Touch/Delete changes, the schema bytes written at this revision (if any), and
    /// the counter deltas — with <paramref name="expectedHead"/> as the CAS and no preconditions (the
    /// lambda already evaluated its own reads against this base). The grain re-mints the authoritative
    /// revision, so the request carries no revision — only the net diff. This is the inverse of the grain's
    /// <c>ApplyEvent</c> fold, keeping the write path and the fold provably equal.
    /// </summary>
    private static CommitRequest CommitFromState(DatastoreState committed, long revision, long expectedHead)
    {
        var ev = LogEventFactory.EventFromState(committed, revision);
        return new CommitRequest(
            Array.Empty<CommitPreconditionWire>(),
            ev.RelationshipChanges,
            DeleteByFilter: null,
            ev.SchemaChange?.Bytes,
            ExpectedSchemaHash: null,
            ev.CounterChanges,
            expectedHead);
    }

    private static long ToNanos(IRevision revision) => revision switch
    {
        TimestampRevision t => t.TimestampNanosSinceEpoch,
        _ => throw new InvalidRevisionException($"unsupported revision type: {revision.GetType().Name}"),
    };

    private static long NowNanos() => (DateTimeOffset.UtcNow - DateTimeOffset.UnixEpoch).Ticks * 100L;

    /// <summary>Lock-free monotonic max: never lets a concurrent smaller value regress the field.</summary>
    private static void InterlockedMax(ref long location, long value)
    {
        var current = Volatile.Read(ref location);
        while (value > current)
        {
            var seen = Interlocked.CompareExchange(ref location, value, current);
            if (seen == current)
                return;
            current = seen;
        }
    }

    /// <summary>
    /// An <see cref="IDatastoreReader"/> that acquires its inner <see cref="MvccSnapshotReader"/> ONCE,
    /// lazily on the first query (via <paramref name="acquire"/>), then serves all subsequent reads in-process.
    /// Because <see cref="IDatastore.SnapshotReader"/> is synchronous but acquiring the state is async, the
    /// acquisition is deferred to the first (async) read. Callers pin one reader per operation and query it
    /// many times, so the acquisition (one sequencer full-state fetch) happens exactly once per reader. The
    /// MVCC fold via <c>IsVisibleAt</c> is exact for any revision AT OR ABOVE the fetched state's collected
    /// floor, and <c>MvccSnapshotReader</c>'s own constructor guard rejects
    /// (<see cref="RevisionNotFoundException"/>) a revision below it, so a permissive <c>isValid</c>
    /// delegate here is sound — the hard floor check lives in the reader itself, not this wrapper.
    /// </summary>
    private sealed class DeferredReader(Func<CancellationToken, ValueTask<MvccSnapshotReader>> acquire)
        : IDatastoreReader
    {
        private readonly SemaphoreSlim _gate = new(1, 1);
        private MvccSnapshotReader? _inner;

        private async ValueTask<MvccSnapshotReader> Inner(CancellationToken ct)
        {
            if (_inner is { } r)
                return r;
            await _gate.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                return _inner ??= await acquire(ct).ConfigureAwait(false);
            }
            finally
            {
                _gate.Release();
            }
        }

        public async IAsyncEnumerable<Relationship> QueryRelationships(
            RelationshipsFilter filter,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var inner = await Inner(cancellationToken).ConfigureAwait(false);
            await foreach (var rel in inner.QueryRelationships(filter, cancellationToken).ConfigureAwait(false))
                yield return rel;
        }

        public async IAsyncEnumerable<Relationship> ReverseQueryRelationships(
            SubjectsFilter subjectsFilter,
            ReverseQueryOptions? options = null,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var inner = await Inner(cancellationToken).ConfigureAwait(false);
            await foreach (var rel in inner.ReverseQueryRelationships(subjectsFilter, options, cancellationToken).ConfigureAwait(false))
                yield return rel;
        }

        public async Task<byte[]?> ReadStoredSchema(CancellationToken cancellationToken = default)
        {
            var inner = await Inner(cancellationToken).ConfigureAwait(false);
            return await inner.ReadStoredSchema(cancellationToken).ConfigureAwait(false);
        }

        public async Task<RelationshipsFilter?> ReadCounterFilter(string name, CancellationToken cancellationToken = default)
        {
            var inner = await Inner(cancellationToken).ConfigureAwait(false);
            return await inner.ReadCounterFilter(name, cancellationToken).ConfigureAwait(false);
        }

        public async Task<ulong> CountRelationships(string name, CancellationToken cancellationToken = default)
        {
            var inner = await Inner(cancellationToken).ConfigureAwait(false);
            return await inner.CountRelationships(name, cancellationToken).ConfigureAwait(false);
        }

        public async IAsyncEnumerable<RegisteredCounter> LookupCounters(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var inner = await Inner(cancellationToken).ConfigureAwait(false);
            await foreach (var counter in inner.LookupCounters(cancellationToken).ConfigureAwait(false))
                yield return counter;
        }

        public bool IsValid => _inner is null || _inner.IsValid;
    }
}
