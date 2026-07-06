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
/// <c>DatastoreState</c> fold) by converting the grain wire state to/from the in-memory state. Reads
/// serve from the per-silo <see cref="SiloProjection"/> (incremental log-tail fold), gated by the
/// closed-timestamp watermark so pinned reads are never stale. Writes use an optimistic
/// compare-and-swap retry loop. This is a DI service, not a grain, so <c>ConfigureAwait(false)</c> is
/// correct here.
/// </summary>
/// <remarks>
/// This instance never owns the shared <see cref="SiloProjection"/>/<see cref="LogWatchHub"/> pair it
/// reads and pulses through <paramref name="projectionHost"/> (see the constructor); their lifecycle is
/// entirely the host contract's responsibility (<see cref="IDatastoreProjectionHost"/>) — production's
/// silo-lifecycle-managed <see cref="DatastoreProjectionService"/>, or a test fixture's own host. This
/// type therefore has nothing of its own to dispose.
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

    // Per-silo materialized read projection (see IDatastoreProjectionHost.Projection): reads serve from the
    // incrementally-folded local projection (one ReadState bootstrap + log-tail catch-up), never a per-Check
    // full-state fetch. Shared with every other GrainBackedDatastore on this silo — the host owns it.
    private readonly SiloProjection _projection;

    // Per-silo Watch notifier (see IDatastoreProjectionHost.Hub). The local write path pulses it on commit
    // for instant same-silo Watch latency; cross-silo commits arrive by observer push from the datastore
    // grain, with the hub's slow heartbeat as the missed-push backstop. The host owns its lifecycle.
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

    /// <summary>
    /// Creates a grain-backed datastore. Reads and Watch use the per-silo SHARED <see cref="SiloProjection"/>/
    /// <see cref="LogWatchHub"/> owned by <paramref name="projectionHost"/> — in production, the same
    /// instances the silo-lifecycle-managed <see cref="DatastoreProjectionService"/> bootstraps before the
    /// silo accepts traffic and disposes on silo shutdown; in tests, a fixture-owned host (e.g.
    /// <c>PrivateProjectionHost</c> in <c>Spiceport.Grains.Tests</c>) when a genuinely isolated hub is needed
    /// (e.g. proving PUSH-driven Watch is real — see <c>Stage3WatchPushMeshTests</c> — rather than a shared
    /// in-process shortcut). This <see cref="GrainBackedDatastore"/> instance never owns the projection/hub
    /// lifetime; see the class remarks.
    /// </summary>
    /// <param name="grainFactory">The Orleans grain factory used to reach the singleton datastore grain.</param>
    /// <param name="projectionHost">The shared projection/hub pair (see <see cref="IDatastoreProjectionHost"/>).</param>
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
        IGrainFactory grainFactory, IDatastoreProjectionHost projectionHost, TimeSpan? quantization = null,
        TimeSpan? gcWindow = null, IOptions<DatastoreGcOptions>? gcOptions = null)
    {
        ArgumentNullException.ThrowIfNull(projectionHost);
        _grainFactory = grainFactory;
        _quantizationNanos = ComputeQuantizationNanos(quantization);
        _gcWindowNanos = ComputeGcWindowNanos(gcWindow, gcOptions);
        _projection = projectionHost.Projection;
        _hub = projectionHost.Hub;
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
        // in-process via one MvccSnapshotReader. The projection catches up on demand (and blocks
        // until watermark >= rev, the closed-timestamp gate).
        return new DeferredReader(async ct =>
            new MvccSnapshotReader(await _projection.StateAtLeast(rev, ct).ConfigureAwait(false), rev, _ => true));
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

        for (var attempt = 0; ; attempt++)
        {
            // 1. Read the current grain state (the write base).
            var grainState = await Grain.ReadState().ConfigureAwait(false);
            var baseState = DatastoreStateConverters.ToMemory(grainState);
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

            // 4. Derive the proposed change from the committed state: the net relationship/schema/counter
            //    diff at this revision (reusing the single per-revision diff definition). The grain re-mints
            //    the revision and stamps it, so the proposal carries no final revision.
            var committed = tx.Commit();
            var proposal = ProposalFromCommit(committed, newRevision);

            // 5. Append into the grain: applies only if the grain head still equals expectedHead. Returns the
            //    AUTHORITATIVE revision the grain minted, or null if the head moved.
            var minted = await Grain.AppendCommit(expectedHead, proposal).ConfigureAwait(false);
            if (minted is { } revision)
            {
                // Wake any local Watch stream immediately (same-silo commits skip the poll latency).
                _hub?.Pulse(revision);
                return new TimestampRevision(revision);
            }

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
    /// A no-op: this instance owns no lifetime of its own (see the class remarks) — the shared projection/hub
    /// belongs entirely to the <see cref="IDatastoreProjectionHost"/> it was constructed with.
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
    /// Builds the <see cref="ProposedWrite"/> for a committed transaction by reusing the single per-revision
    /// diff (<see cref="LogEventFactory.EventFromState"/>) over the committed state: the resolved
    /// relationship Touch/Delete changes, the schema bytes written at this revision (if any), and the
    /// counter deltas. The grain re-mints the authoritative revision, so the proposal carries no revision —
    /// only the net diff. This is the inverse of the grain's <c>ApplyEvent</c> fold, keeping the write path
    /// and the fold provably equal.
    /// </summary>
    private static ProposedWrite ProposalFromCommit(DatastoreState committed, long revision)
    {
        var ev = LogEventFactory.EventFromState(committed, revision);
        return new ProposedWrite(ev.RelationshipChanges, ev.SchemaChange?.Bytes, ev.CounterChanges);
    }

    private static long ToNanos(IRevision revision) => revision switch
    {
        TimestampRevision t => t.TimestampNanosSinceEpoch,
        _ => throw new InvalidRevisionException($"unsupported revision type: {revision.GetType().Name}"),
    };

    private static long NowNanos() => (DateTimeOffset.UtcNow - DateTimeOffset.UnixEpoch).Ticks * 100L;

    /// <summary>
    /// An <see cref="IDatastoreReader"/> that acquires its inner <see cref="MvccSnapshotReader"/> ONCE,
    /// lazily on the first query (via <paramref name="acquire"/>), then serves all subsequent reads in-process.
    /// Because <see cref="IDatastore.SnapshotReader"/> is synchronous but acquiring the state is async, the
    /// acquisition is deferred to the first (async) read. The local dispatcher pins one reader per Check and
    /// queries it many times, so the acquisition happens exactly once per Check. The acquire delegate
    /// catches up the per-silo projection; the MVCC fold via <c>IsVisibleAt</c> is exact for any revision
    /// AT OR ABOVE the projection's collected floor, and <c>MvccSnapshotReader</c>'s own constructor guard
    /// rejects (<see cref="RevisionNotFoundException"/>) a revision below it, so a permissive <c>isValid</c>
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
