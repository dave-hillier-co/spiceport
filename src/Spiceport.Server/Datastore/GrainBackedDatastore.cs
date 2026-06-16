using System.Runtime.CompilerServices;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Datastore.Memory;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// An <see cref="IDatastore"/> that delegates all state to the cluster-singleton
/// <see cref="IDatastoreGrain"/> (the single source of truth) and reuses the in-memory MVCC mechanics
/// (<see cref="InMemoryReadWriteTransaction"/>, <see cref="InMemoryDatastoreReader"/>, the
/// <c>DatastoreState</c> fold) by converting the grain wire state to/from the in-memory state. It holds
/// NO persistent local state cache: every read reads the current grain state (correct multi-silo reads
/// with zero replica lag). Writes use an optimistic compare-and-swap retry loop. This is a DI service,
/// not a grain, so <c>ConfigureAwait(false)</c> is correct here.
/// </summary>
public sealed class GrainBackedDatastore : IDatastore, IAsyncDisposable
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

    // Per-silo materialized read projection (add-before-remove behind a flag). When non-null, reads serve
    // from the incrementally-folded local projection (one ReadState bootstrap + log-tail catch-up) instead
    // of a per-Check full-state fetch; when null, the legacy LazyGrainReader fetches the whole state per
    // Check. This GrainBackedDatastore is itself the per-silo singleton, so the owned projection is per-silo.
    private readonly SiloProjection? _projection;

    // Per-silo Watch notifier (created lazily on the first Watch). The local write path pulses it on commit
    // for instant same-silo Watch latency; its background loop covers cross-silo commits. Guarded by _hubLock.
    private readonly object _hubLock = new();
    private LogWatchHub? _hub;

    // Cached optimized-revision candidate (mirrors InMemoryDatastore's CachedOptimizedRevisions): a real
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

    /// <summary>Creates a grain-backed datastore.</summary>
    /// <param name="grainFactory">The Orleans grain factory used to reach the singleton datastore grain.</param>
    /// <param name="quantization">Quantization window for <see cref="OptimizedRevision"/> (default 5s).</param>
    /// <param name="gcWindow">How long old revisions remain valid (default 24h).</param>
    /// <param name="useProjection">
    /// When true, reads serve from a per-silo <see cref="SiloProjection"/> (incremental log-tail fold) instead
    /// of fetching the whole grain state per Check. Defaults to false (the legacy full-fetch reader) so the
    /// projection can soak behind a flag before becoming the default.
    /// </param>
    public GrainBackedDatastore(
        IGrainFactory grainFactory, TimeSpan? quantization = null, TimeSpan? gcWindow = null, bool useProjection = false)
    {
        _grainFactory = grainFactory;
        _quantizationNanos = (long)((quantization ?? TimeSpan.FromSeconds(5)).TotalMilliseconds) * 1_000_000L;
        _gcWindowNanos = (long)((gcWindow ?? TimeSpan.FromHours(24)).TotalMilliseconds) * 1_000_000L;
        _projection = useProjection
            ? new SiloProjection(grainFactory.GetGrain<IDatastoreGrain>(IDatastoreGrain.Key))
            : null;
    }

    private IDatastoreGrain Grain => _grainFactory.GetGrain<IDatastoreGrain>(IDatastoreGrain.Key);

    public IDatastoreReader SnapshotReader(IRevision revision)
    {
        var rev = ToNanos(revision);
        // Both paths defer the (async) state acquisition to the first read and then serve every subsequent
        // query in-process via one InMemoryDatastoreReader. The projection path catches up on demand (and
        // blocks until watermark >= rev, the closed-timestamp gate); the legacy path fetches the whole state.
        return _projection is { } projection
            ? new DeferredReader(async ct =>
                new InMemoryDatastoreReader(await projection.StateAtLeast(rev, ct).ConfigureAwait(false), rev, _ => true))
            : new DeferredReader(async ct =>
                new InMemoryDatastoreReader(
                    DatastoreStateConverters.ToMemory(await Grain.ReadState().ConfigureAwait(false)), rev, _ => true));
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
        // latency (InMemoryDatastore samples under its lock with no intervening await).
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

            // 2. Mint a provisional revision monotonically over the observed head (mirrors InMemoryDatastore).
            //    This revision pins the local tx so the staged view and preconditions are evaluated at a
            //    fixed point; the grain mints the AUTHORITATIVE revision when it appends the event.
            var now = NowNanos();
            var newRevision = now > expectedHead ? now : expectedHead + 1;

            // 3. Run the caller lambda over an in-memory tx pinned to this base. Preconditions and
            //    SchemaChangeValidator read the tx reader (the staged-view over the snapshot). Any
            //    exception thrown by the lambda (create-conflict, precondition, schema-validation, counter
            //    conflict) propagates AS-IS and aborts the whole call — it is NOT a retry.
            var tx = new InMemoryReadWriteTransaction(baseState, newRevision);
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
            //    same exception type InMemoryDatastore throws on a concurrent write.
            if (attempt + 1 >= MaxCasAttempts)
                throw new SerializationException();
        }
    }

    public async Task<bool> CheckRevision(IRevision revision, CancellationToken cancellationToken = default)
    {
        var head = await Grain.GetHead().ConfigureAwait(false);
        var rev = ToNanos(revision);
        return rev <= head.Head && rev >= head.Head - _gcWindowNanos;
    }

    public async IAsyncEnumerable<RevisionChange> Watch(
        IRevision afterRevision,
        WatchOptions options,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(afterRevision);
        ArgumentNullException.ThrowIfNull(options);

        var cursor = ToNanos(afterRevision);

        // Validate the cursor is within the GC window (mirror InMemoryDatastore: RevisionNotFoundException).
        var head0 = await Grain.GetHead().ConfigureAwait(false);
        if (!(cursor <= head0.Head && cursor >= head0.Head - _gcWindowNanos))
            throw new RevisionNotFoundException(afterRevision);

        var hub = Hub();
        hub.EnsureStarted();

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
            // directly; cross-silo commits are picked up by the hub's poll). No per-stream timer.
            try
            {
                await hub.WaitForChangeAfter(cursor, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                yield break;
            }
        }
    }

    private LogWatchHub Hub()
    {
        if (_hub is { } existing)
            return existing;
        lock (_hubLock)
        {
            return _hub ??= new LogWatchHub(Grain);
        }
    }

    public Task<string> GetUniqueId(CancellationToken cancellationToken = default) => Task.FromResult(UniqueId);

    public Task<IRevisionParser> GetRevisionParser(CancellationToken cancellationToken = default) =>
        Task.FromResult<IRevisionParser>(new InMemoryRevisionParser(UniqueId));

    public Task Close() => DisposeAsync().AsTask();

    public async ValueTask DisposeAsync()
    {
        LogWatchHub? hub;
        lock (_hubLock)
        {
            hub = _hub;
            _hub = null;
        }
        if (hub is not null)
            await hub.DisposeAsync().ConfigureAwait(false);
    }

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
            ? ev.RelationshipChanges.Select(MapUpdate).ToList()
            : (IReadOnlyList<RelationshipUpdate>)Array.Empty<RelationshipUpdate>();
        var schemaChanged = includeSchema && ev.SchemaChange is not null;

        if (relChanges.Count == 0 && !schemaChanged)
            return null;

        return new RevisionChange(new TimestampRevision(ev.Revision), relChanges, schemaChanged);
    }

    private static RelationshipUpdate MapUpdate(RelationshipUpdateWire u) =>
        new(WireConvert.ToRelationship(u.Relationship),
            u.Operation == RelationshipUpdateOpWire.Delete ? UpdateOperation.Delete : UpdateOperation.Touch);

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
    /// An <see cref="IDatastoreReader"/> that acquires its inner <see cref="InMemoryDatastoreReader"/> ONCE,
    /// lazily on the first query (via <paramref name="acquire"/>), then serves all subsequent reads in-process.
    /// Because <see cref="IDatastore.SnapshotReader"/> is synchronous but acquiring the state is async, the
    /// acquisition is deferred to the first (async) read. The local dispatcher pins one reader per Check and
    /// queries it many times, so the acquisition happens exactly once per Check. The acquire delegate either
    /// fetches the whole grain state (legacy) or catches up the per-silo projection — the grain never GCs
    /// rows, so the MVCC fold via <c>IsVisibleAt</c> is exact at any revision and a permissive validator is
    /// sound (validity is enforced upstream by <see cref="CheckRevision"/>).
    /// </summary>
    private sealed class DeferredReader(Func<CancellationToken, ValueTask<InMemoryDatastoreReader>> acquire)
        : IDatastoreReader
    {
        private readonly SemaphoreSlim _gate = new(1, 1);
        private InMemoryDatastoreReader? _inner;

        private async ValueTask<InMemoryDatastoreReader> Inner(CancellationToken ct)
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
