using System.Collections.Immutable;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.EventSourcing;
using Orleans.EventSourcing.CustomStorage;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.Utilities;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// The cluster-singleton datastore grain (<see cref="IDatastoreGrain.Key"/> = 0): the single source of
/// truth for the whole MVCC datastore. It is EVENT-SOURCED — the append-only log of <see cref="LogEvent"/>s
/// is the source of truth — and, under the THIN-SEQUENCER layout, it no longer materializes the whole
/// fold: the journaled state is only the SMALL state (<see cref="DatastoreMetaState"/> — head, schemas,
/// counters, GC floor, key index), relationship rows are persisted per adjacency key as
/// VERSION-QUALIFIED <c>shard/{rowVersion}/{direction}/{type}/{id}</c> <see cref="GraphShardState"/> rows
/// (write-once per version, reachable only through the meta's key->version index maps; both directions —
/// the 2x row storage is the two-family design's own model), and the grain holds a DIRTY BUFFER of the keys touched
/// since their rows were last flushed. It is a single non-reentrant activation; WRITES
/// (<see cref="Commit"/>, <see cref="RunGc"/> — no interleave attribute) never interleave EACH OTHER, so
/// a write's turn still runs to completion before the next write's, keeping the head-compare-and-append
/// atomic. The PURE reads (<see cref="ReadState"/>, <see cref="GetHead"/>, <see cref="ReadSchemaAt"/>,
/// <see cref="ReadShard"/>, <see cref="IDatastoreLog.ReadFrom"/>) are explicitly marked
/// <c>[AlwaysInterleave]</c> on the grain interface and may run DURING a write's await — never
/// concurrently with it (the scheduler is still single-threaded) and never observing a write's
/// intermediate state; see the publication discipline in <see cref="ApplyUpdatesToStorage"/>.
/// </summary>
/// <remarks>
/// <para>
/// Persistence is OUR responsibility via <see cref="ICustomStorageInterface{TState,TDelta}"/> over an
/// Orleans grain-storage provider (NO application SQL): each event is a per-version <c>log/{version}</c>
/// entry, a <c>head</c> pointer tracks the contiguous log version + the timestamp head revision + the
/// current flush (meta) version, and every <see cref="FlushInterval"/> events the dirty buffer is FLUSHED
/// (per-key shard rows written under the new flush version + a <c>meta/{version}</c> row) and the log compacted, bounding
/// replay cost. Orleans' <c>RetrieveConfirmedEvents</c> is NOT supported under CustomStorage; the grain
/// keeps its own in-memory recent-events window so <see cref="ReadFrom"/> serves the live tail with no
/// storage reads.
/// </para>
/// <para>
/// THE LOAD-BEARING SHARDING FACT (pinned by <c>ShardFoldLemmaTests</c>): a stored shard row's CONTENT is
/// complete unless an event touched its key — any touching event dirties the key and the next flush
/// rewrites the row — so a clean key's stored row is current except for its <c>AppliedRevision</c> label,
/// which may safely be relabeled to the current head. That is why serving a clean key needs NO per-key
/// log replay. Row-level GC is LAZY: a GC event advances the small state's floor immediately, but a clean
/// key's stored row keeps its sub-floor dead rows until it is next dirtied and flushed. Sound because
/// reads pinned below the floor are rejected outright and every serve path re-applies the current floor
/// (<see cref="ShardFold.CollectBelow"/>) before answering, so lazily-retained dead rows are invisible.
/// </para>
/// </remarks>
[LogConsistencyProvider(ProviderName = "CustomStorage")]
public sealed class DatastoreGrain :
    JournaledGrain<DatastoreMetaHolder, LogEvent>,
    IDatastoreGrain,
    ICustomStorageInterface<DatastoreMetaHolder, LogEvent>,
    IRemindable
{
    /// <summary>The name of the periodic MVCC-GC reminder registered in <see cref="OnActivateAsync"/>.</summary>
    private const string GcReminderName = "mvcc-gc";

    // Orleans grain code must not ConfigureAwait(false); keep the captured context.
    private const ConfigureAwaitOptions ContinueOnCapturedContext = ConfigureAwaitOptions.ContinueOnCapturedContext;

    /// <summary>Storage state-name of the durable head pointer (one row, rewritten in place — the commit point).</summary>
    private const string HeadStateName = "head";

    /// <summary>Per-version small-state entry prefix: <c>meta/{version}</c> (write-once, so crash-safe).</summary>
    private const string MetaStatePrefix = "meta/";

    /// <summary>
    /// Per-key shard row prefix. Shard rows are VERSION-QUALIFIED and write-once per version:
    /// <c>shard/{rowVersion}/</c> + the escaped <c>GraphShardGrainKey</c> string (<c>f/type/id</c> or
    /// <c>r/type/id</c>), where <c>rowVersion</c> is the meta (flush) version the row was written
    /// under. A flush never rewrites a row any durable meta references — it writes each dirty key's
    /// row under the NEW flush version and only the meta+head commit makes those rows reachable (the
    /// key index maps key -> rowVersion). Writes still go read-then-write for the provider ETag,
    /// because a crashed flush attempt can leave an orphan row at the same versioned name that the
    /// boundary's retry must overwrite.
    /// </summary>
    private const string ShardStatePrefix = "shard/";

    /// <summary>The storage state-name of one version-qualified shard row.</summary>
    private static string ShardRowName(int rowVersion, string shardKey) =>
        $"{ShardStatePrefix}{rowVersion}/{shardKey}";

    /// <summary>
    /// Resolves a shard key through the meta key index (forward or reverse — a key string is
    /// direction-prefixed, so it lives in exactly one map). Returns false for an unindexed key; a true
    /// result may still carry <see cref="DatastoreMetaState.NoRowVersion"/> (indexed, but no durable
    /// row yet — the dirty buffer covers it).
    /// </summary>
    private static bool TryGetRowVersion(DatastoreMetaState meta, string shardKey, out int rowVersion) =>
        meta.ForwardKeys.TryGetValue(shardKey, out rowVersion)
        || meta.ReverseKeys.TryGetValue(shardKey, out rowVersion);

    /// <summary>
    /// The RETIRED whole-state snapshot prefix (<c>snapshot/{version}</c>). Only read during the one-time
    /// activation MIGRATION of a store written by the previous layout, then cleared best-effort.
    /// </summary>
    private const string LegacySnapshotStatePrefix = "snapshot/";

    /// <summary>Per-event log entry state-name prefix: <c>log/{version}</c>.</summary>
    private const string LogStatePrefix = "log/";

    /// <summary>Flush the dirty buffer (and compact the log) every N appended events — the same 64-event
    /// cadence the retired whole-state snapshots used, so the <see cref="ReadFrom"/> retention contract
    /// (its floor is the flush boundary) is unchanged for shard grains and Watch.</summary>
    private const int FlushInterval = 64;

    /// <summary>
    /// How long a watcher registration lives without a <see cref="SubscribeWatch"/> refresh. 10x the hubs'
    /// heartbeat interval, so a silo must miss many heartbeats before its watcher is dropped.
    /// </summary>
    private static readonly TimeSpan WatcherExpiry = TimeSpan.FromSeconds(10);

    private readonly IGrainStorage _storage;
    private readonly ILogger<DatastoreGrain> _logger;
    private readonly DatastoreGcOptions _gcOptions;

    /// <summary>
    /// The sequencer-side inbound-call counters (docs/scalability-program.md Phase 0), or null on a
    /// host that never registered them — every record site is null-conditional, so the grain works
    /// unchanged without the seam (the same optional-dependency stance as <see cref="_gcOptions"/>).
    /// </summary>
    private readonly ISequencerMetrics? _metrics;

    /// <summary>
    /// How long old revisions stay served by <see cref="ReadFrom"/> / retained in the in-memory window,
    /// and the retention window <see cref="RunGc"/> collects MVCC history beyond. Older cursors throw
    /// <c>RevisionNotFoundException</c>. Derived from <see cref="DatastoreGcOptions.Window"/>.
    /// </summary>
    private readonly long _gcWindowNanos;

    /// <summary>
    /// The registered head-advance observers (one per silo hub). Deliberately in-memory only — observer
    /// references are not durable, so the set empties on reactivation; the hubs' heartbeat resubscribe (which
    /// also returns the head) makes that safe.
    /// </summary>
    private readonly ObserverManager<IDatastoreWatcher> _watchers;

    /// <summary>The contiguous append-only log version (= number of confirmed events) currently in storage.</summary>
    private int _logVersion;

    /// <summary>The log version of the current <c>meta/{version}</c> row (log entries at or below it are compacted).</summary>
    private int _metaVersion;

    /// <summary>
    /// The small state as currently PERSISTED-or-published (the meta fold of all stored events). Tracked
    /// here so the storage methods never touch <c>JournaledGrain.State</c> — accessing the confirmed view
    /// from inside a confirm round-trip would re-enter the log-consistency adaptor — and so every serve
    /// path reads head/floor/schemas/counters/index from the SAME publication unit as
    /// <see cref="_dirty"/>/<see cref="_recent"/> (they are only ever assigned together in one synchronous
    /// block; see <see cref="ApplyUpdatesToStorage"/>).
    /// </summary>
    private DatastoreMetaState _storedMeta = DatastoreMetaState.Empty(0);

    /// <summary>
    /// THE DIRTY BUFFER: the folded CURRENT <see cref="GraphShardState"/> of every shard key touched since
    /// its storage row was last flushed, keyed by the escaped <c>GraphShardGrainKey</c> string. Seeding
    /// rule: before an event is folded into the buffer, every key it touches must already hold an entry
    /// seeded from its storage row (or <see cref="GraphShardState.Empty"/>) — seeding happens in ASYNC
    /// contexts only (<see cref="Commit"/> pre-stages the keys it touches; recovery seeds while replaying);
    /// the synchronous fold paths only apply <see cref="ShardFold.ApplyEvent"/> to entries guaranteed
    /// present. Between flushes the buffer only grows (bounded by write rate x <see cref="FlushInterval"/>
    /// events); a flush rewrites every entry's row and clears it. An entry left by a REJECTED commit's
    /// pre-seeding is content-correct (it is the key's current state) and merely rides to the next flush.
    /// </summary>
    private Dictionary<string, GraphShardState> _dirty = new(StringComparer.Ordinal);

    /// <summary>
    /// Serializes the flush's shard-row rewrites against <see cref="CurrentShardState"/>'s clean-key
    /// storage reads. Without it, an interleaved read racing a flush could fetch a key's PRE-flush row
    /// (missing the dirty-window events), then observe the buffer already cleared and wrongly relabel that
    /// stale content to the new head. Readers hold it only across one row read; the flush holds it across
    /// its whole write-rows/meta/head/clear section. Never held across <c>RaiseConditionalEvent</c>.
    /// </summary>
    private readonly SemaphoreSlim _shardIo = new(1, 1);

    /// <summary>
    /// The live grain-state wrapper for the <c>head</c> entry — the only singleton entry rewritten in
    /// place. Held as a field so its storage ETag persists across writes (the Orleans grain-storage
    /// providers enforce optimistic concurrency per entry; a fresh empty-ETag wrapper would be rejected on
    /// the second write). Log entries (<c>log/{version}</c>), meta entries
    /// (<c>meta/{version}</c>) and version-qualified shard rows are write-once per COMMITTED version but
    /// a crashed attempt can orphan one at the same versioned name (a crashed append for log rows, a
    /// crashed flush for shard/meta rows), so they go read-then-write so the wrapper carries the CURRENT
    /// ETag (see <see cref="WriteLogEntry"/>/<see cref="WriteShardRow"/>).
    /// </summary>
    private readonly GrainState<LogHeadEntry> _headState = new();

    /// <summary>
    /// The in-memory recent-events window (ascending by revision) that <see cref="ReadFrom"/> tails. It
    /// retains exactly the events with revision &gt; <see cref="_recentFloorRevision"/>; it is rebuilt from
    /// storage on activation (the post-flush tail) and appended to on each confirmed write.
    /// </summary>
    private readonly List<LogEvent> _recent = new();

    /// <summary>
    /// The exclusive lower bound of <see cref="_recent"/>: events at or below this revision have been
    /// flushed into per-key shard rows (or aged past the GC window) and are no longer individually
    /// retained, so <see cref="ReadFrom"/> cannot serve a cursor older than this and throws (the consumer
    /// re-bootstraps from a snapshot via <see cref="ReadState"/> / <see cref="ReadShard"/>). Kept
    /// consistent live and after reactivation.
    /// </summary>
    private long _recentFloorRevision;

    public DatastoreGrain(
        [FromKeyedServices("datastore")] IGrainStorage storage,
        ILogger<DatastoreGrain> logger,
        IOptions<DatastoreGcOptions>? gcOptions = null,
        ISequencerMetrics? metrics = null)
    {
        _storage = storage;
        _logger = logger;
        _metrics = metrics;
        _watchers = new ObserverManager<IDatastoreWatcher>(WatcherExpiry, logger);
        // Optional so a host/test that never registers DatastoreGcOptions in DI still activates the grain
        // with sane defaults (24h window, 1h reminder, enabled) — only a host that wants non-default GC
        // behaviour needs to configure the options.
        _gcOptions = gcOptions?.Value ?? new DatastoreGcOptions();
        _gcWindowNanos = (long)_gcOptions.Window.TotalMilliseconds * 1_000_000L;
    }

    /// <inheritdoc />
    public override async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        await base.OnActivateAsync(cancellationToken).ConfigureAwait(ContinueOnCapturedContext);

        if (!_gcOptions.ReminderEnabled)
            return;

        try
        {
            var period = _gcOptions.ReminderPeriod < DatastoreGcOptions.MinimumReminderPeriod
                ? DatastoreGcOptions.MinimumReminderPeriod
                : _gcOptions.ReminderPeriod;
            await this.RegisterOrUpdateReminder(GcReminderName, period, period).ConfigureAwait(ContinueOnCapturedContext);
        }
        catch (Exception ex)
        {
            // A host with no reminder service configured (many tests, some dev hosts) must still activate;
            // losing the periodic reminder is safe because the singleton re-registers it on every future
            // activation, and RunGc remains directly callable regardless (the test seam).
            _logger.LogWarning(
                ex,
                "datastore grain: failed to register the '{ReminderName}' reminder; MVCC GC will not run periodically on this activation",
                GcReminderName);
        }
    }

    /// <inheritdoc />
    public Task ReceiveReminder(string reminderName, TickStatus status) =>
        reminderName == GcReminderName ? RunGc() : Task.CompletedTask;

    // --- Fold (JournaledGrain) ---

    /// <summary>
    /// Folds a confirmed event into the in-place holder — the SMALL state only (the adaptor's confirmed
    /// view, which <see cref="Commit"/>/<see cref="RunGc"/> read at the top of a write turn). The dirty
    /// buffer's per-key fold deliberately does NOT happen here: it happens in
    /// <see cref="ApplyUpdatesToStorage"/> (into locals, published atomically with
    /// <see cref="_storedMeta"/>), because a flush inside that method must write rows that already include
    /// the in-flight batch, and folding the buffer twice (here and there) would corrupt it while folding
    /// it only here would leave the flush's rows missing the batch. This method stays deterministic and
    /// synchronous, exactly as the adaptor requires.
    /// </summary>
    protected override void TransitionState(DatastoreMetaHolder holder, LogEvent ev) =>
        holder.Value = MetaFold.ApplyEvent(holder.Value, ev);

    // --- serve-path core -------------------------------------------------------------------------

    /// <summary>
    /// current-state(key): the exact per-key fold at the current head, served without any per-key log
    /// replay. Resolution order: the dirty buffer entry (folded through every confirmed touching event);
    /// otherwise the key's STORAGE row relabeled to head (sound by the sharding fact — a clean key's row
    /// content is complete, only its label is stale) — read at the row version the key index maps the key
    /// to, so an unindexed key resolves to empty-at-head with no storage read (this also shields leaked or
    /// orphaned rows the index never references); otherwise empty-at-head. Every branch re-applies the
    /// current GC floor via <see cref="ShardFold.CollectBelow"/> (the lazy row-GC contract), which also
    /// performs the relabel.
    /// </summary>
    /// <remarks>
    /// Interleave safety: the dirty-hit and unindexed branches are fully synchronous over one publication
    /// unit. The storage-read branch awaits under <see cref="_shardIo"/> so it cannot race a flush's row
    /// rewrite, and re-checks the buffer afterwards — if a commit dirtied the key during the read, it
    /// loops and serves the (authoritative) buffer entry instead; unrelated commits landing during the
    /// read are safe without a retry because the key being clean at the re-check proves no event through
    /// the re-read head touched it.
    /// </remarks>
    private async Task<GraphShardState> CurrentShardState(string shardKey)
    {
        while (true)
        {
            var meta = _storedMeta;
            if (_dirty.TryGetValue(shardKey, out var entry))
            {
                return ShardFold.CollectBelow(
                    entry, Math.Max(entry.AppliedRevision, meta.HeadRevision), meta.GcFloor);
            }

            // Index miss => empty-at-head with no storage read. Rows are reachable ONLY through the
            // index maps (shard rows are version-qualified), so a leaked physical row — a crashed
            // best-effort clear, or an abandoned flush attempt's orphan — can never be served. A key
            // indexed at NoRowVersion but NOT dirty is unreachable in practice (a key first touched
            // after the last flush stays in the dirty buffer until the flush that assigns it a row
            // version); treat it like an index miss defensively rather than dereference the sentinel.
            if (!TryGetRowVersion(meta, shardKey, out var rowVersion) || rowVersion < 0)
                return new GraphShardState(meta.HeadRevision, meta.GcFloor, ImmutableList<StoredRelationshipWire>.Empty);

            GraphShardState? stored;
            await _shardIo.WaitAsync().ConfigureAwait(ContinueOnCapturedContext);
            try
            {
                stored = await ReadShardRow(rowVersion, shardKey).ConfigureAwait(ContinueOnCapturedContext);
            }
            finally
            {
                _shardIo.Release();
            }

            // Synchronous re-validation (no await between here and the return): a commit may have landed
            // during the read. If it touched THIS key the buffer now holds the authority — loop (the next
            // iteration returns it synchronously). If a FLUSH landed instead (possible while parked on
            // the semaphore), the key's current row may have moved to a newer version and the row just
            // read may already be post-commit-cleared — loop and resolve through the fresh index. If
            // neither happened, the row content is still complete through the CURRENT head and the
            // relabel below is exact.
            if (_dirty.ContainsKey(shardKey))
                continue;
            if (!TryGetRowVersion(_storedMeta, shardKey, out var currentVersion) || currentVersion != rowVersion)
                continue;

            // Serve at the MAX of the small state's floor and the stored row's own stamped floor. On this
            // activation they agree (rows are stamped with the flush-time floor, and the meta floor never
            // regresses past it); they can diverge only when a STALE DUPLICATE activation (membership
            // churn) reads a row a newer activation has GC-compacted further — taking the max turns what
            // would be a silently-incomplete answer for a pin between the two floors into the standard
            // below-floor rejection at the consumer (fail loud, never wrong).
            var current = _storedMeta;
            return ShardFold.CollectBelow(
                stored ?? GraphShardState.Empty,
                current.HeadRevision,
                Math.Max(current.GcFloor, stored?.GcFloor ?? 0));
        }
    }

    // --- IDatastoreGrain ---

    /// <inheritdoc />
    /// <remarks>
    /// [AlwaysInterleave]. ADMIN-PLANE ASSEMBLY: the thin sequencer no longer holds the whole fold, so
    /// this rebuilds a <see cref="DatastoreGrainState"/> from the small state plus the current state of
    /// every indexed FORWARD key (each row belongs to exactly one forward key, so the union is exact and
    /// duplicate-free; the dirty overlay wins per key inside <see cref="CurrentShardState"/>). Semantics
    /// are identical to the retired materialized fold; the cost is O(graph) storage reads + transfer per
    /// call, which is acceptable ONLY for its admin-plane consumers (the scan seam, the compatibility
    /// ReadWriteTx base, the equivalence gates) — the per-Check hot path never comes here.
    /// Consistency: rows are collected against one captured small state; a commit landing mid-assembly
    /// can only contribute rows STAMPED ABOVE the captured head, which MVCC visibility filters for every
    /// pin at or below it; a GC-floor advance mid-assembly restarts the assembly (rare — floors move once
    /// per GC run), so the returned state's floor always covers its rows.
    /// </remarks>
    public async Task<DatastoreGrainState> ReadState()
    {
        _metrics?.RecordReadState();
        while (true)
        {
            var meta = _storedMeta;
            var rows = new List<StoredRelationshipWire>();
            var floor = meta.GcFloor;
            foreach (var shardKey in meta.ForwardKeys.Keys)
            {
                var state = await CurrentShardState(shardKey).ConfigureAwait(ContinueOnCapturedContext);
                rows.AddRange(state.Rows);
                // Normally equal to the small state's floor; a HIGHER per-key floor can only come from
                // the stale-duplicate divergence documented in CurrentShardState — propagate it so a pin
                // between the floors is rejected (fail loud) instead of served incomplete.
                floor = Math.Max(floor, state.GcFloor);
            }

            if (_storedMeta.GcFloor != meta.GcFloor)
                continue; // a GC event landed mid-assembly; restart so floor and rows agree.

            // Order by created revision to approximate the retired fold's append order (ties — rows of
            // one commit — land in per-key iteration order). Order is cosmetic: every consumer is an
            // MVCC reader, and anything order-sensitive (keyset cursors, the limited delete's canonical
            // pick) imposes its own deterministic order downstream.
            rows.Sort(static (a, b) => a.CreatedRevision.CompareTo(b.CreatedRevision));

            return new DatastoreGrainState
            {
                HeadRevision = meta.HeadRevision,
                Relationships = rows.ToImmutableList(),
                Schemas = meta.Schemas,
                Counters = meta.Counters,
                GcFloor = floor,
            };
        }
    }

    /// <inheritdoc />
    /// <remarks>
    /// [AlwaysInterleave]. Reads <see cref="_storedMeta"/> — the storage-side publication unit (see
    /// <see cref="ApplyUpdatesToStorage"/>) — so an interleaved call landing mid-write sees, AT WORST,
    /// the previous commit; it can never observe a write's in-flight intermediate state.
    /// </remarks>
    public Task<DatastoreHeadWire> GetHead()
    {
        _metrics?.RecordGetHead();
        var s = _storedMeta;
        return Task.FromResult(new DatastoreHeadWire(s.HeadRevision, s.SchemaHashAt(s.HeadRevision), s.GcFloor));
    }

    /// <inheritdoc />
    /// <remarks>[AlwaysInterleave]. Small state only; same publication guarantee as <see cref="GetHead"/>.</remarks>
    public Task<byte[]?> ReadSchemaAt(long revision)
    {
        _metrics?.RecordReadSchemaAt();
        var s = _storedMeta;
        // Below the GC floor the schema history has been collected, so "no schema at this revision" is
        // indistinguishable from "collected away" — a silent null here would make callers fall back to
        // the seed schema and evaluate WRONG results under a stale token. Reject loudly instead (the
        // MvccSnapshotReader constructor's guard, which the retired projection-backed reader enforced);
        // RevisionNotFoundException round-trips the grain boundary via RevisionNotFoundSurrogate and the
        // front door maps it to InvalidArgument like any other GC'd pinned revision.
        if (revision < s.GcFloor)
            throw new RevisionNotFoundException(new TimestampRevision(revision));

        return Task.FromResult(s.SchemaVersionAt(revision)?.Bytes);
    }

    /// <inheritdoc />
    /// <remarks>
    /// [AlwaysInterleave]. One <see cref="CurrentShardState"/> resolution: the dirty entry, or the stored
    /// row relabeled to the CURRENT head (the sharding fact), or empty-at-head — with the current GC
    /// floor applied in every branch. The reply's <see cref="GraphShardState.AppliedRevision"/> is the
    /// current head, exactly the watermark contract a hydrating shard grain expects.
    /// </remarks>
    public Task<GraphShardState> ReadShard(GraphShardKeyWire key)
    {
        _metrics?.RecordReadShard();
        ArgumentNullException.ThrowIfNull(key);
        return CurrentShardState(GraphShardGrainKey.Build(key));
    }

    /// <inheritdoc />
    /// <remarks>
    /// The RELOCATION of the client-side write execution (formerly <c>GrainBackedDatastore.ReadWriteTx</c>'s
    /// base-transact-CAS loop) into the sequencer: because writes never interleave writes, the head read at
    /// the top of this method cannot move before the append at the bottom — the transaction executor IS the
    /// serialization point, so a declarative commit (null <see cref="CommitRequest.ExpectedHead"/>) needs
    /// no retry. Under the thin-sequencer layout the write base is no longer the whole fold: candidate-key
    /// resolution assembles a PARTIAL base from exactly the shard keys the request can read (see the
    /// soundness argument inline). The storage protocol (<see cref="ApplyUpdatesToStorage"/> and the
    /// event-application path through <see cref="TransitionState"/>) is byte-for-byte the one the retired
    /// whole-state path drove. All rejections return as reply data (<see cref="CommitReply.Failure"/>)
    /// with NOTHING applied — the staged <see cref="MvccReadWriteTransaction"/> is simply abandoned,
    /// exactly as the caller-side lambda abandonment worked.
    /// </remarks>
    public async Task<CommitReply> Commit(CommitRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        // Duration measured around the WHOLE serialized write turn (candidate loads, staging, append,
        // confirm) and recorded in a finally so every inbound call — accepted, rejected, or thrown —
        // counts exactly once. GetTimestamp/GetElapsedTime rather than Stopwatch.StartNew: the same
        // clock with no per-commit Stopwatch allocation on the sequencer's hot path. The
        // candidate-key-count buckets are recorded inside CommitCore at the point candidate
        // resolution completes.
        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            return await CommitCore(request).ConfigureAwait(ContinueOnCapturedContext);
        }
        finally
        {
            _metrics?.RecordCommit((long)Stopwatch.GetElapsedTime(startTimestamp).TotalMicroseconds);
        }
    }

    /// <summary>The whole Commit body (see the <see cref="Commit"/> remarks); split out only so the
    /// public entry can bracket it with the Phase 0 duration measurement.</summary>
    private async Task<CommitReply> CommitCore(CommitRequest request)
    {
        // The confirmed small state: in a write turn this equals _storedMeta (all prior confirms have
        // fully completed), and it is the view RaiseConditionalEvent's own version check pairs with.
        var meta = State.Value;
        var head = meta.HeadRevision;

        // 1. Compatibility CAS (the lambda ReadWriteTx path evaluated its preconditions caller-side
        //    against this head): reject if the head moved — the load-bearing CAS compare, atomic with the
        //    append because writes never interleave writes.
        if (request.ExpectedHead is { } expectedHead && head != expectedHead)
            return Rejected(CommitFailureKind.HeadMoved, $"expected head {expectedHead} but head is {head}");

        // 2. Schema-write serializability: the caller validated its schema change against the schema it
        //    saw; reject if another schema landed since. A null current hash is the pre-first-schema seed
        //    window and matches only a null/empty expected hash.
        if (request.ExpectedSchemaHash is { } expectedSchemaHash)
        {
            var currentHash = meta.SchemaHashAt(head);
            var unchanged = currentHash is null
                ? expectedSchemaHash.Length == 0
                : string.Equals(currentHash, expectedSchemaHash, StringComparison.Ordinal);
            if (!unchanged)
                return Rejected(
                    CommitFailureKind.SchemaHashMoved,
                    $"expected schema hash '{expectedSchemaHash}' but schema hash at head is '{currentHash}'");
        }

        // 3. Mint the new revision monotonically over the observed head (mirrors ReferenceDatastore).
        var now = NowNanos();
        var newRevision = now > head ? now : head + 1;

        // 4. CANDIDATE-KEY RESOLUTION replaces the whole-state base: the forward key of every update's
        //    resource, plus the keys matching each precondition filter and the deleteByFilter (a filter
        //    naming resource type/ids/prefix resolves against the forward key index; one constraining
        //    ONLY subjects resolves the matching reverse keys; one constraining neither resolves every
        //    forward key). Rows are deduped by (six-tuple identity, CreatedRevision) because a row
        //    contributed by both its forward and its reverse key is the identical stored row.
        //
        //    SOUNDNESS: the partial base is sufficient because every read the staged transaction can
        //    perform is bounded by the request's own filters and updates — precondition probes and the
        //    deleteByFilter scan only rows matching their filters (all of whose keys are in the candidate
        //    set by the resolution rules above, which are deliberately SUPERSET-inclusive), and the
        //    create-conflict check reads only the updated resources' forward keys. The sharding lemma
        //    (fold(log) == merge of fold(log|key); ShardFoldLemmaTests) guarantees each candidate key's
        //    current state equals the whole fold restricted to that key, so the assembled partial state
        //    is exactly the whole state restricted to the candidate keys — no probe can distinguish them.
        //    Schemas/counters/floor/head come whole from the small state. ChangesAt(newRevision) over the
        //    committed partial state yields exactly the commit's own changes (only rows stamped at the
        //    minted revision), so the appended event is identical to the whole-state derivation.
        var candidates = new HashSet<string>(StringComparer.Ordinal);
        foreach (var update in request.Updates)
            candidates.Add(MetaFold.ForwardKeyOf(update.Relationship));
        foreach (var precondition in request.Preconditions)
            AddFilterCandidates(precondition.Filter, meta, candidates);
        if (request.DeleteByFilter is { } df)
            AddFilterCandidates(df.Filter, meta, candidates);
        _metrics?.RecordCommitCandidates(candidates.Count);

        var loaded = new Dictionary<string, GraphShardState>(StringComparer.Ordinal);
        var candidateRows = new List<StoredRelationshipWire>();
        var seen = new Dictionary<CandidateRowIdentity, long?>();
        foreach (var candidate in candidates)
        {
            var shard = await CurrentShardState(candidate).ConfigureAwait(ContinueOnCapturedContext);
            loaded[candidate] = shard;
            foreach (var row in shard.Rows)
            {
                var identity = CandidateRowIdentity.From(row);
                if (seen.TryGetValue(identity, out var deletedRevision))
                {
                    // The INTENDED collision: the same stored row contributed by both its forward and
                    // its reverse key — identical stamps by the sharding lemma, so dropping the
                    // duplicate is exact. With version-qualified write-once shard rows a crashed flush
                    // can no longer leave a same-revision live row AND tombstone for one identity in
                    // the reachable rows (the old in-place-rewrite crash window this dedup used to
                    // collapse WRONGLY — a rolled-back tombstone silently shadowing, or being
                    // shadowed by, the live row). If the stamps nonetheless disagree, that state is
                    // impossible by construction: it means storage corruption or a fold bug, and
                    // silently preferring either row would reintroduce the collapsed-verdict blocker.
                    // Fail loudly instead of guessing.
                    if (deletedRevision != row.DeletedRevision)
                    {
                        throw new InvalidOperationException(
                            "datastore invariant violation: two reachable shard rows share identity "
                            + $"{row.Relationship.ResourceType}:{row.Relationship.ResourceId}"
                            + $"#{row.Relationship.ResourceRelation}"
                            + $"@{row.Relationship.SubjectType}:{row.Relationship.SubjectId}"
                            + $"#{row.Relationship.SubjectRelation}"
                            + $" and created revision {row.CreatedRevision} but differ in deleted revision"
                            + $" ({deletedRevision?.ToString() ?? "live"} vs {row.DeletedRevision?.ToString() ?? "live"});"
                            + " forward/reverse contributions of one stored row must be identical");
                    }
                    continue;
                }

                seen.Add(identity, row.DeletedRevision);
                candidateRows.Add(row);
            }
        }
        candidateRows.Sort(static (a, b) => a.CreatedRevision.CompareTo(b.CreatedRevision));

        var partial = new DatastoreGrainState
        {
            HeadRevision = head,
            Relationships = candidateRows.ToImmutableList(),
            Schemas = meta.Schemas,
            Counters = meta.Counters,
            GcFloor = meta.GcFloor,
        };

        // 5. Stage the whole request through the SAME in-memory MVCC transaction the client path used,
        //    pinned directly at the authoritative revision (minting and staging are the same activation,
        //    so no re-stamp).
        var baseState = DatastoreStateConverters.ToMemory(partial);
        var tx = new MvccReadWriteTransaction(baseState, newRevision);

        // 6. Preconditions in request order, BEFORE any mutation — the semantics the client-side
        //    write path historically evaluated inline (existence-only probe per filter; MustMatch requires
        //    at least one row, MustNotMatch requires none), with the shared message text
        //    (PreconditionMessages) so the client-side rethrown PreconditionFailedException message is
        //    unchanged.
        for (var i = 0; i < request.Preconditions.Count; i++)
        {
            var precondition = request.Preconditions[i];
            var filter = WireConvert.ToCoreFilter(precondition.Filter);

            var matched = false;
            await foreach (var _ in tx.QueryRelationships(filter))
            {
                matched = true;
                break; // existence check only; one row is enough.
            }

            if (precondition.MustMatch && !matched)
                return Rejected(CommitFailureKind.PreconditionFailed, PreconditionMessages.MustMatchFailed(i, filter));
            if (!precondition.MustMatch && matched)
                return Rejected(CommitFailureKind.PreconditionFailed, PreconditionMessages.MustNotMatchFailed(i, filter));
        }

        // 7. Relationship updates, preserving Create so the create-conflict check fires here (the reply
        //    carries the canonical relationship string; the client reconstructs the exact exception).
        if (request.Updates.Count > 0)
        {
            var updates = request.Updates.Select(WireConvert.ToWriteUpdate).ToList();
            try
            {
                await tx.WriteRelationships(updates).ConfigureAwait(ContinueOnCapturedContext);
            }
            catch (CreateRelationshipExistsException ex)
            {
                return Rejected(CommitFailureKind.CreateAlreadyExists, ex.Relationship);
            }
        }

        // 8. Bulk delete-by-filter (after the updates, like the request declares them).
        ulong deletedCount = 0;
        var reachedLimit = false;
        if (request.DeleteByFilter is { } deleteByFilter)
        {
            (deletedCount, reachedLimit) = await tx
                .DeleteRelationships(WireConvert.ToCoreFilter(deleteByFilter.Filter), deleteByFilter.Limit)
                .ConfigureAwait(ContinueOnCapturedContext);
        }

        // 9. Schema bytes (compile/type/diff validation is the caller's job — RelationshipsGrain.WriteSchema
        //    — guarded here by the ExpectedSchemaHash gate above).
        if (request.SchemaBytes is { } schemaBytes)
            await tx.WriteStoredSchema(schemaBytes).ConfigureAwait(ContinueOnCapturedContext);

        // 10. Counter deltas. The two request shapes carry DIFFERENT counter semantics, keyed (like the
        //    CAS itself) off ExpectedHead:
        //    - Declarative (ExpectedHead null, the counter RPCs): each delta is a guarded INTENT, run
        //      through tx.WriteCounter/DeleteCounter so the register/unregister preconditions are
        //      enforced here and rejected as typed reply failures.
        //    - Compatibility (ExpectedHead set, the ReadWriteTx lambda path): each delta is the already-
        //      RESOLVED net CounterVersion the lambda's own guarded ops produced client-side against
        //      this exact base (race-free under the CAS). It must NOT replay through the guards — a
        //      same-commit register+unregister nets to a tombstone whose DeleteCounter guard is false in
        //      the base (and the inverse trips AlreadyRegistered) — so it is appended directly, exactly
        //      the direct-append stance LogFold.ApplyEvent/MetaFold take for the same reason. The deltas
        //      are instead carried straight onto the appended event below.
        if (request.ExpectedHead is null)
        {
            foreach (var counter in request.CounterChanges)
            {
                try
                {
                    if (counter.Filter is { } counterFilter)
                        await tx.WriteCounter(counter.Name, WireConvert.ToCoreFilter(counterFilter))
                            .ConfigureAwait(ContinueOnCapturedContext);
                    else
                        await tx.DeleteCounter(counter.Name).ConfigureAwait(ContinueOnCapturedContext);
                }
                catch (CounterAlreadyRegisteredException ex)
                {
                    return Rejected(CommitFailureKind.CounterAlreadyRegistered, ex.CounterName);
                }
                catch (CounterNotRegisteredException ex)
                {
                    return Rejected(CommitFailureKind.CounterNotRegistered, ex.CounterName);
                }
            }
        }

        // 11. Commit the staged transaction and derive the canonical event. EventFromState(committed,
        //     newRevision) is exactly what the fold will re-derive over the same per-key bases (see the
        //     soundness argument at step 4), so fold == commit by the existing equivalence argument.
        var committed = tx.Commit();
        var ev = LogEventFactory.EventFromState(committed, newRevision);

        // Compatibility path: the net counter deltas ride the event directly (see step 10) — the fold
        // applies them by direct append, which is exactly how the retired client-side derivation shipped
        // them.
        if (request.ExpectedHead is not null && request.CounterChanges.Count > 0)
            ev = ev with { CounterChanges = request.CounterChanges };

        // 12. PRE-SEED the dirty buffer for every key the event touches (the seeding rule): the forward
        //     candidates are already loaded; reverse keys of updated/deleted rows' subjects may not be —
        //     load the missing ones now (async is fine, still before the append). The merge into the
        //     shared buffer is SYNCHRONOUS (no await between the merge and RaiseConditionalEvent's own
        //     bookkeeping), so an interleaved reader sees either no entry or a complete, content-correct
        //     seed — never a partial one. Seeds left behind by a later failure are content-correct.
        var touched = MetaFold.TouchedKeys(ev);
        foreach (var key in touched)
        {
            if (!loaded.ContainsKey(key) && !_dirty.ContainsKey(key))
                loaded[key] = await CurrentShardState(key).ConfigureAwait(ContinueOnCapturedContext);
        }
        foreach (var key in touched)
        {
            if (!_dirty.ContainsKey(key))
                _dirty[key] = loaded[key];
        }

        // RaiseConditionalEvent's own version check is a secondary guard behind the CAS above. It
        // cannot lose here — this activation is the only writer and no write interleaves a write — but
        // keep the identical false-branch handling (reported as a moved head) rather than assume.
        var raised = await RaiseConditionalEvent(ev).ConfigureAwait(ContinueOnCapturedContext);
        if (!raised)
            return Rejected(CommitFailureKind.HeadMoved, "conditional append reported a concurrent head advance");
        await ConfirmEvents().ConfigureAwait(ContinueOnCapturedContext);

        // Push the new head to the per-silo watch hubs. Best-effort and isolated:
        // HeadAdvanced is [OneWay] and any failure is swallowed — the commit result must never depend on
        // the notify; a missed push is recovered by the hubs' heartbeat.
        try
        {
            await _watchers.Notify(w => w.HeadAdvanced(newRevision)).ConfigureAwait(ContinueOnCapturedContext);
        }
        catch
        {
            // Defunct observers are pruned by ObserverManager; nothing else to do.
        }

        return new CommitReply(newRevision, null, deletedCount, reachedLimit);

        static CommitReply Rejected(CommitFailureKind kind, string? detail) =>
            new(null, new CommitFailureWire(kind, detail), 0, false);
    }

    /// <summary>
    /// Resolves a filter to the shard keys able to hold ANY row it matches — deliberately a SUPERSET
    /// (over-inclusion only adds rows the filter then ignores; under-inclusion would silently break
    /// MustNotMatch/delete completeness). Resource-side constraints (type/ids/prefix) resolve against the
    /// forward index; a filter constraining ONLY subjects resolves its selectors against the reverse
    /// index; a filter constraining neither (relation-only or empty) resolves every forward key, because
    /// every row lives in exactly one forward key. Row-level constraints (relation, caveat, expiration,
    /// subject-relation) never narrow KEYS and are left to the transaction's own filter evaluation.
    /// </summary>
    private static void AddFilterCandidates(
        FullRelationshipsFilterWire filter, DatastoreMetaState meta, HashSet<string> candidates)
    {
        var resourceConstrained = filter.OptionalResourceType is not null
            || filter.OptionalResourceIds is { Count: > 0 }
            || filter.OptionalResourceIdPrefix is not null;

        if (resourceConstrained)
        {
            // Exact-key fast path: a filter naming an explicit resource type + explicit ids (no prefix)
            // determines its candidate keys by CONSTRUCTION — O(#ids) dictionary probes, never a scan.
            // The index scan below is O(all keys) with a string parse per key, which the measurement
            // harness showed making an exact-key precondition cost scale linearly with graph cardinality
            // (docs/scalability-program.md 3.3 trigger note); it remains only for the shapes that
            // genuinely cannot name their keys (type-only, prefix). An id absent from the index simply
            // contributes no candidate: no key means no rows, which evaluates preconditions identically.
            if (filter is { OptionalResourceType: { } exactType, OptionalResourceIds.Count: > 0, OptionalResourceIdPrefix: null })
            {
                foreach (var id in filter.OptionalResourceIds)
                {
                    var key = GraphShardGrainKey.Build(GraphShardKeyWire.ForResource(exactType, id));
                    if (meta.ForwardKeys.ContainsKey(key))
                        candidates.Add(key);
                }
                return;
            }

            foreach (var key in meta.ForwardKeys.Keys)
            {
                var parsed = GraphShardGrainKey.Parse(key);
                if (filter.OptionalResourceType is { } type
                    && !string.Equals(parsed.ObjectType, type, StringComparison.Ordinal))
                    continue;
                if (filter.OptionalResourceIds is { Count: > 0 } ids && !ids.Contains(parsed.ObjectId))
                    continue;
                if (filter.OptionalResourceIdPrefix is { } prefix
                    && !parsed.ObjectId.StartsWith(prefix, StringComparison.Ordinal))
                    continue;
                candidates.Add(key);
            }
            return;
        }

        if (filter.OptionalSubjectsSelectors is { Count: > 0 } selectors)
        {
            // Same fast path on the reverse side: when EVERY selector names an explicit subject type +
            // explicit ids, the reverse keys are constructible; one broader selector falls the whole
            // filter back to the scan (selectors are OR'd, so a scan-shaped one covers the others too).
            if (selectors.All(static s => s is { OptionalSubjectType: not null, OptionalSubjectIds.Count: > 0 }))
            {
                foreach (var selector in selectors)
                foreach (var id in selector.OptionalSubjectIds!)
                {
                    var key = GraphShardGrainKey.Build(GraphShardKeyWire.ForSubject(selector.OptionalSubjectType!, id));
                    if (meta.ReverseKeys.ContainsKey(key))
                        candidates.Add(key);
                }
                return;
            }

            foreach (var key in meta.ReverseKeys.Keys)
            {
                var parsed = GraphShardGrainKey.Parse(key);
                foreach (var selector in selectors)
                {
                    if (selector.OptionalSubjectType is { } subjectType
                        && !string.Equals(parsed.ObjectType, subjectType, StringComparison.Ordinal))
                        continue;
                    if (selector.OptionalSubjectIds is { Count: > 0 } subjectIds
                        && !subjectIds.Contains(parsed.ObjectId))
                        continue;
                    candidates.Add(key);
                    break;
                }
            }
            return;
        }

        candidates.UnionWith(meta.ForwardKeys.Keys);
    }

    /// <summary>
    /// The dedupe identity for candidate-row assembly: the six-tuple storage identity plus the created
    /// revision. A row contributed by both its forward and its reverse key carries identical stamps
    /// (both folds restrict the same log — the sharding lemma), so this suffices to union them.
    /// </summary>
    private readonly record struct CandidateRowIdentity(
        string ResourceType,
        string ResourceId,
        string ResourceRelation,
        string SubjectType,
        string SubjectId,
        string SubjectRelation,
        long CreatedRevision)
    {
        public static CandidateRowIdentity From(StoredRelationshipWire row) => new(
            row.Relationship.ResourceType, row.Relationship.ResourceId, row.Relationship.ResourceRelation,
            row.Relationship.SubjectType, row.Relationship.SubjectId, row.Relationship.SubjectRelation,
            row.CreatedRevision);
    }

    /// <inheritdoc />
    public async Task<long?> RunGc()
    {
        var head = State.Value.HeadRevision;
        var currentFloor = State.Value.GcFloor;
        var now = NowNanos();
        // Never above head (a floor beyond the head would be meaningless) and never inside the retained
        // window — mirrors Commit's own revision-minting arithmetic style.
        var floor = Math.Min(head, now - _gcWindowNanos);

        if (floor <= currentFloor)
            return null; // GC only ever moves forward; nothing new to collect yet.

        // Mint the new revision exactly like Commit (monotonic over the observed head) — a GC event
        // is minted by the grain itself, not by a caller-supplied expectedHead CAS. It touches no shard
        // keys, so no dirty-buffer seeding is needed: the fold applies it to every entry already present,
        // and clean keys compact lazily (their next dirty+flush; every serve path re-applies the floor).
        var newRevision = now > head ? now : head + 1;
        var ev = new LogEvent(
            newRevision, Array.Empty<RelationshipUpdateWire>(), SchemaChange: null,
            Array.Empty<CounterDeltaWire>(), GcFloor: floor);

        var raised = await RaiseConditionalEvent(ev).ConfigureAwait(ContinueOnCapturedContext);
        if (!raised)
            return null; // lost a race with a concurrent turn; the next reminder tick (or caller) retries.
        await ConfirmEvents().ConfigureAwait(ContinueOnCapturedContext);

        // Keep the in-memory log-tail retention (ReadFrom) in lockstep with the collected state floor: a
        // cursor below the floor is no longer a meaningful re-bootstrap point either.
        _recentFloorRevision = Math.Max(_recentFloorRevision, floor);
        _recent.RemoveAll(e => e.Revision <= _recentFloorRevision);

        // Same best-effort notify pattern as Commit: the GC result never depends on this succeeding,
        // and the GC event itself carries no relationship/schema content so a Watch consumer parked before
        // it just observes the head advancing with nothing to emit.
        try
        {
            await _watchers.Notify(w => w.HeadAdvanced(newRevision)).ConfigureAwait(ContinueOnCapturedContext);
        }
        catch
        {
            // Defunct observers are pruned by ObserverManager; nothing else to do.
        }

        return floor;
    }

    /// <inheritdoc />
    public Task<DatastoreHeadWire> SubscribeWatch(IDatastoreWatcher watcher)
    {
        ArgumentNullException.ThrowIfNull(watcher);
        _watchers.Subscribe(watcher, watcher);
        return GetHead();
    }

    /// <inheritdoc />
    public Task UnsubscribeWatch(IDatastoreWatcher watcher)
    {
        ArgumentNullException.ThrowIfNull(watcher);
        _watchers.Unsubscribe(watcher);
        return Task.CompletedTask;
    }

    // --- IDatastoreLog ---

    /// <inheritdoc />
    /// <remarks>
    /// This is one of the [AlwaysInterleave] pure reads, so it can run while a write is parked at an
    /// await. It reads the head from <c>_storedMeta.HeadRevision</c> — so the served head and
    /// <see cref="_recent"/>/<see cref="_recentFloorRevision"/> all come from the SAME publication unit
    /// (see <see cref="ApplyUpdatesToStorage"/>): the returned <see cref="LogSegment"/> is internally
    /// consistent (its head is &gt;= every event it serves) even if this call lands mid-write.
    /// </remarks>
    public Task<LogSegment> ReadFrom(long afterRevision, int maxCount)
    {
        _metrics?.RecordReadFrom();
        var head = _storedMeta.HeadRevision;

        // The in-memory window retains only events strictly above the flush/GC floor; an older cursor
        // cannot be served COMPLETELY (some events in (afterRevision, floor] were compacted), so we must
        // reject rather than silently return a short tail — the consumer re-bootstraps via ReadState /
        // ReadShard.
        if (afterRevision < _recentFloorRevision)
            throw new RevisionNotFoundException(new TimestampRevision(afterRevision));

        var events = _recent
            .Where(e => e.Revision > afterRevision)
            .OrderBy(e => e.Revision)
            .Take(maxCount < 0 ? int.MaxValue : maxCount)
            .ToList();

        return Task.FromResult(new LogSegment(events, head));
    }

    // --- ICustomStorageInterface (WE own persistence; no application SQL) ---

    /// <summary>
    /// PUBLICATION DISCIPLINE (both this method and <see cref="ApplyUpdatesToStorage"/>): storage reads/writes
    /// are awaited into purely LOCAL variables; the shared fields (<see cref="_storedMeta"/>, <see cref="_dirty"/>,
    /// <see cref="_recent"/>, <see cref="_recentFloorRevision"/>, <see cref="_logVersion"/>,
    /// <see cref="_metaVersion"/>) are only ever assigned in a single synchronous block with no <c>await</c>
    /// in between. Because an <c>[AlwaysInterleave]</c> read can only run while THIS call is parked at an
    /// await, a read landing mid-method sees either the fields wholly untouched (still whatever the previous
    /// publish left) or, once this method's synchronous tail runs, the fully-updated fields — never a
    /// partially-applied batch.
    /// </summary>
    /// <remarks>
    /// RECOVERY is O(tail + touched keys), never O(graph): load the head row, load the
    /// <c>meta/{SnapshotVersion}</c> small state, then replay the log tail
    /// (<c>FlushedThroughLogVersion</c>, <c>logVersion</c>] — folding the small state via the SAME
    /// <see cref="MetaFold"/>, rebuilding the dirty buffer (each touched key's row seeded from storage as
    /// first encountered, then <see cref="ShardFold"/>), and rebuilding <see cref="_recent"/> so
    /// <see cref="ReadFrom"/> serves the tail exactly as before (its floor is the flush boundary — the
    /// same cadence the retired snapshot boundary had, so shard grains and Watch see an unchanged
    /// retention contract). MIGRATION: a store written by the retired whole-state layout has a
    /// <c>snapshot/{version}</c> row where the meta row is missing; it is split ONCE into per-key shard
    /// rows (both directions) + key index + meta, persisted, and the legacy snapshot cleared best-effort
    /// (a crash mid-migration simply re-migrates — shard-row writes go read-then-write, so re-writing an
    /// existing row is ETag-correct; a crash between the meta write and the legacy clear leaks the one
    /// legacy row, harmlessly, exactly like a failed compaction clear).
    /// </remarks>
    /// <inheritdoc />
    public async Task<KeyValuePair<int, DatastoreMetaHolder>> ReadStateFromStorage()
    {
        await _storage.ReadStateAsync(HeadStateName, this.GetGrainId(), _headState)
            .ConfigureAwait(ContinueOnCapturedContext);

        if (!_headState.RecordExists || _headState.State is null)
        {
            // No durable head yet: seed an empty state at a monotonic timestamp (mirrors ReferenceDatastore's
            // initial Empty(NowNanos)) and PERSIST the seed, so the pre-first-write revision floor is stable
            // across reactivation (a re-seed with a fresh, larger NowNanos would silently move the head).
            var seeded = DatastoreMetaState.Empty(NowNanos());
            await WriteMeta(0, new DatastoreMetaEntry(seeded, 0)).ConfigureAwait(ContinueOnCapturedContext);
            await WriteHead(new LogHeadEntry(0, seeded.HeadRevision, 0)).ConfigureAwait(ContinueOnCapturedContext);

            // Publish (no await below this point in the branch).
            _logVersion = 0;
            _metaVersion = 0;
            _storedMeta = seeded;
            _dirty = new Dictionary<string, GraphShardState>(StringComparer.Ordinal);
            _recent.Clear();
            _recentFloorRevision = seeded.HeadRevision;
            return new KeyValuePair<int, DatastoreMetaHolder>(0, new DatastoreMetaHolder { Value = seeded });
        }

        var headEntry = _headState.State;
        var logVersion = headEntry.LogVersion;
        var metaVersion = headEntry.SnapshotVersion;

        // Load the small state the head points at — or MIGRATE a legacy whole-state snapshot in place.
        var metaEntry = await ReadMeta(metaVersion).ConfigureAwait(ContinueOnCapturedContext);
        if (metaEntry is null)
        {
            var legacy = await ReadLegacySnapshot(metaVersion).ConfigureAwait(ContinueOnCapturedContext);
            metaEntry = legacy is not null
                ? await MigrateLegacySnapshot(metaVersion, legacy).ConfigureAwait(ContinueOnCapturedContext)
                // Mirrors the retired layout's missing-snapshot tolerance (?? Empty(0)); an in-range
                // missing LOG entry below still fails loudly as corruption.
                : new DatastoreMetaEntry(DatastoreMetaState.Empty(0), metaVersion);
        }

        // Replay the log tail (metaVersion .. logVersion] folding the small state AND the dirty buffer.
        // The range is contiguous by construction (head is written last, the commit point), so a missing
        // in-range entry is corruption — fail loudly rather than silently fold a lossy state (which would
        // pass the durability negative control while having lost data).
        var meta = metaEntry.Meta;
        var dirty = new Dictionary<string, GraphShardState>(StringComparer.Ordinal);
        var recent = new List<LogEvent>();
        var floor = meta.HeadRevision;
        for (var v = metaVersion + 1; v <= logVersion; v++)
        {
            var ev = await ReadLogEvent(v).ConfigureAwait(ContinueOnCapturedContext)
                ?? throw new InvalidOperationException(
                    $"datastore log corruption: missing log entry {v} in [{metaVersion + 1}..{logVersion}]");

            if (ev.GcFloor is null)
            {
                // SEEDING RULE (recovery form): every key the event touches gets an entry seeded from
                // its storage row as FIRST encountered — resolved through the CURRENT fold point's
                // index map, exactly like the serve path. A key that is unindexed (or indexed at
                // NoRowVersion) seeds Empty even if a physical row exists somewhere: rows are
                // version-qualified, so a leaked row (crashed best-effort clear) or an abandoned flush
                // attempt's orphan is unreferenced dead history and must not resurrect.
                //
                // NO DOUBLE-FOLD IS POSSIBLE: the seeds come from rows the OLD meta references, and the
                // index versions used here stay the OLD meta's throughout the replay (MetaFold only
                // ADDS keys at NoRowVersion and never bumps a version — bumps happen only at flushes).
                // Every referenced row was written by the flush that produced that meta, so it contains
                // NOTHING above the meta's FlushedThroughLogVersion — in particular no content from the
                // tail events replayed here, even when a later flush crashed after writing ahead-of-head
                // rows (those live at a version the old meta never references). Each tail event
                // therefore folds into a base that strictly predates it, exactly once.
                foreach (var key in MetaFold.TouchedKeys(ev))
                {
                    if (dirty.ContainsKey(key))
                        continue;
                    var stored = TryGetRowVersion(meta, key, out var rowVersion) && rowVersion >= 0
                        ? await ReadShardRow(rowVersion, key).ConfigureAwait(ContinueOnCapturedContext)
                        : null;
                    dirty[key] = stored ?? GraphShardState.Empty;
                }
                foreach (var key in MetaFold.TouchedKeys(ev))
                    dirty[key] = ShardFold.ApplyEvent(dirty[key], ev, GraphShardGrainKey.Parse(key));
            }
            else
            {
                // A GC event touches no keys; it folds into every entry already present (clean keys
                // compact lazily — see the class remarks).
                foreach (var key in dirty.Keys.ToList())
                    dirty[key] = ShardFold.ApplyEvent(dirty[key], ev, GraphShardGrainKey.Parse(key));
            }

            meta = MetaFold.ApplyEvent(meta, ev);
            AddPending(recent, ref floor, ev, meta.HeadRevision);
        }

        // Publish (no await below this point in the branch).
        _logVersion = logVersion;
        _metaVersion = metaVersion;
        _storedMeta = meta;
        _dirty = dirty;
        _recent.Clear();
        _recent.AddRange(recent);
        _recentFloorRevision = floor;
        return new KeyValuePair<int, DatastoreMetaHolder>(logVersion, new DatastoreMetaHolder { Value = meta });
    }

    /// <summary>
    /// One-time in-place migration of a store written by the retired whole-state layout: split the
    /// legacy snapshot's rows into per-key <see cref="GraphShardState"/> rows (BOTH directions), build
    /// the key index, persist the <c>meta/{version}</c> row, then clear the legacy snapshot best-effort.
    /// Crash-safe: shard rows go read-then-write (re-migration after a crash overwrites them), the meta
    /// row is the migration's commit point (once it exists this method is never entered again), and a
    /// crash between meta write and legacy clear leaks one unreferenced row, exactly like a failed
    /// compaction clear.
    /// </summary>
    private async Task<DatastoreMetaEntry> MigrateLegacySnapshot(int version, DatastoreGrainState legacy)
    {
        _logger.LogInformation(
            "datastore grain: migrating legacy whole-state snapshot v{Version} ({RowCount} rows) to the per-key shard layout",
            version, legacy.Relationships.Count);

        var byKey = new Dictionary<string, List<StoredRelationshipWire>>(StringComparer.Ordinal);
        // The migrated rows are written under the migration's own meta version, and the index maps
        // point every key at it — the same version-qualified discipline as a flush.
        var forward = ImmutableDictionary.CreateBuilder<string, int>(StringComparer.Ordinal);
        var reverse = ImmutableDictionary.CreateBuilder<string, int>(StringComparer.Ordinal);
        foreach (var row in legacy.Relationships)
        {
            var forwardKey = MetaFold.ForwardKeyOf(row.Relationship);
            var reverseKey = MetaFold.ReverseKeyOf(row.Relationship);
            forward[forwardKey] = version;
            reverse[reverseKey] = version;
            AddRow(byKey, forwardKey, row);
            AddRow(byKey, reverseKey, row);
        }

        static void AddRow(
            Dictionary<string, List<StoredRelationshipWire>> byKey, string key, StoredRelationshipWire row)
        {
            if (!byKey.TryGetValue(key, out var rows))
                byKey[key] = rows = new List<StoredRelationshipWire>();
            rows.Add(row);
        }

        foreach (var (key, rows) in byKey)
        {
            await WriteShardRow(version, key, new GraphShardState(legacy.HeadRevision, legacy.GcFloor, rows.ToImmutableList()))
                .ConfigureAwait(ContinueOnCapturedContext);
        }

        var meta = new DatastoreMetaState
        {
            HeadRevision = legacy.HeadRevision,
            Schemas = legacy.Schemas,
            Counters = legacy.Counters,
            GcFloor = legacy.GcFloor,
            ForwardKeys = forward.ToImmutable(),
            ReverseKeys = reverse.ToImmutable(),
        };
        var entry = new DatastoreMetaEntry(meta, version);
        await WriteMeta(version, entry).ConfigureAwait(ContinueOnCapturedContext);

        try
        {
            await ClearLegacySnapshot(version).ConfigureAwait(ContinueOnCapturedContext);
        }
        catch
        {
            // Best-effort, like compaction: the meta row already supersedes the legacy snapshot, so a
            // failed clear only leaks one unreferenced storage row.
        }

        return entry;
    }

    /// <inheritdoc />
    public async Task<bool> ApplyUpdatesToStorage(IReadOnlyList<LogEvent> updates, int expectedVersion)
    {
        ArgumentNullException.ThrowIfNull(updates);

        // Optimistic concurrency on the CONTIGUOUS LOG VERSION (distinct from the timestamp head revision):
        // refuse if storage has moved on. The grain-storage providers enforce a per-entry ETag check too;
        // this contiguous-version gate is the log-level guard the adaptor relies on.
        if (_logVersion != expectedVersion)
            return false;

        // Fold forward into LOCALS only (never touch JournaledGrain.State here — that would re-enter the
        // consistency adaptor mid-confirm — and never touch the shared _dirty/_recent/_storedMeta fields
        // yet either; see the publication-discipline remark on ReadStateFromStorage). The dirty buffer is
        // folded here (not in TransitionState) so a flush below can write rows that already include this
        // batch, and so the buffer and the small state publish as ONE unit at the commit point.
        var foldedMeta = _storedMeta;
        var foldedDirty = updates.Count > 0 ? new Dictionary<string, GraphShardState>(_dirty, StringComparer.Ordinal) : null;
        var version = _logVersion;
        var pending = new List<LogEvent>();
        foreach (var ev in updates)
        {
            version++;
            // Log entries are write-once per COMMITTED version, but a crashed append attempt (row
            // written, head never advanced) leaves an orphan at this exact versioned name that the
            // boundary's retry must overwrite — WriteLogEntry attempts the single-RTT fresh write and
            // falls back to ETag-tolerant read-then-write only on that orphan collision.
            await WriteLogEntry(version, ev).ConfigureAwait(ContinueOnCapturedContext);

            if (ev.GcFloor is null)
            {
                foreach (var key in MetaFold.TouchedKeys(ev))
                {
                    if (!foldedDirty!.TryGetValue(key, out var entry))
                    {
                        // Belt-and-braces: the seeding rule says Commit pre-seeded every touched key, so
                        // this read should never run; if a future caller breaches the contract, seeding
                        // here (from the pre-event index, exactly like recovery) is still correct.
                        var stored = TryGetRowVersion(foldedMeta, key, out var rowVersion) && rowVersion >= 0
                            ? await ReadShardRow(rowVersion, key).ConfigureAwait(ContinueOnCapturedContext)
                            : null;
                        entry = stored ?? GraphShardState.Empty;
                    }
                    foldedDirty[key] = ShardFold.ApplyEvent(entry, ev, GraphShardGrainKey.Parse(key));
                }
            }
            else
            {
                foreach (var key in foldedDirty!.Keys.ToList())
                    foldedDirty[key] = ShardFold.ApplyEvent(foldedDirty[key], ev, GraphShardGrainKey.Parse(key));
            }

            foldedMeta = MetaFold.ApplyEvent(foldedMeta, ev);
            pending.Add(ev);
        }

        // Periodically FLUSH + compact: bound the replay tail on reactivation and the in-memory window.
        //
        // FLUSH WRITE ORDER AND CRASH WINDOWS. Shard rows are VERSION-QUALIFIED and write-once per
        // version — shard/{flushVersion}/{key}, where flushVersion is the meta version being written
        // (the log-version boundary) — and only the key index maps in the meta row make a row
        // reachable. The order is:
        //   1. log rows            — already written above, one per event (unchanged by the flush).
        //   2. dirty shard rows    — each written under the NEW flushVersion, never over any row a
        //                            durable meta references. ETag-tolerant read-then-write, like the
        //                            other per-version entries' clears: a crashed flush attempt's
        //                            orphan at this exact versioned name is cleanly overwritten when
        //                            this same boundary is retried (log versions are contiguous, so
        //                            the boundary version recurs).
        //   3. meta/{flushVersion} — carries the index maps updated to the new row versions (also
        //                            ETag-tolerant read-then-write, for the same orphan-retry reason).
        //   4. head                — THE commit point (the only in-place row).
        //   5. post-commit, best-effort: clear each rewritten/dropped key's PREVIOUS version row
        //      (versions captured from the PRE-flush index), the superseded meta row, and the log
        //      rows at or below the new FlushedThroughLogVersion; then clear the dirty buffer.
        //
        // CRASH BEFORE STEP 4: the durable head still names the OLD meta, whose index references only
        // OLD-version rows — and step 2 never touched those (it wrote new storage names only). So the
        // in-flight batch's content is UNREACHABLE: recovery restores the old meta and replays the old
        // tail over exactly the pre-flush rows, and no later head advance can ever surface a
        // rolled-back, unacknowledged write (the in-place layout's crash window: rows rewritten under
        // a stable name BEFORE the commit point became visible once head moved past their stamps —
        // including a same-revision tombstone+live pair for one identity, which candidate dedup then
        // collapsed wrongly). The new-version shard/meta rows the crash orphaned are referenced by
        // nothing; the retry of the same boundary overwrites them via the read-then-write in steps
        // 2-3, and if the boundary is never retried they remain bounded, unreferenced garbage.
        //
        // CRASH AT/AFTER STEP 4: committed. A crash inside step 5 at worst leaks the superseded
        // rows/meta/log entries — unreferenced by the new head+index and invisible to every read path
        // (rows resolve only through the index maps) — exactly like any other failed best-effort
        // clear. Physical clears must never run before step 4: clearing a previous-version row first
        // would LOSE data on crash (the old meta still references it, and the events that superseded
        // it may already be compacted).
        var oldMetaVersion = _metaVersion;
        var newMetaVersion = _metaVersion;
        var flushBoundaryRevision = -1L;
        List<(string Key, int RowVersion)>? supersededRows = null;
        var flushing = updates.Count > 0 && version / FlushInterval > _metaVersion / FlushInterval;
        if (flushing)
            await _shardIo.WaitAsync().ConfigureAwait(ContinueOnCapturedContext);
        try
        {
            if (flushing)
            {
                // Lazy GC materializes here: every dirty entry is collected at the CURRENT floor before
                // its row is written, so flushed rows never carry sub-floor dead history.
                var flushVersion = version;
                var forwardIndex = foldedMeta.ForwardKeys;
                var reverseIndex = foldedMeta.ReverseKeys;
                supersededRows = new List<(string, int)>();
                foreach (var (key, entry) in foldedDirty!)
                {
                    var collected = ShardFold.CollectBelow(entry, foldedMeta.HeadRevision, foldedMeta.GcFloor);

                    // The key's previous durable row (if any) is superseded whether the key is
                    // rewritten under the new version or dropped as empty. Its version comes from the
                    // PRE-flush index — the only place it survives the bump/removal below — and its
                    // physical clear is deferred to step 5 (post-commit).
                    var hadRow = TryGetRowVersion(foldedMeta, key, out var previousVersion) && previousVersion >= 0;
                    if (hadRow)
                        supersededRows.Add((key, previousVersion));

                    if (collected.Rows.IsEmpty)
                    {
                        // The key's current state is empty: drop it from the index (making any old row
                        // unreachable at the commit point) and write nothing at the new version.
                        forwardIndex = forwardIndex.Remove(key);
                        reverseIndex = reverseIndex.Remove(key);
                        continue;
                    }

                    await WriteShardRow(flushVersion, key, collected).ConfigureAwait(ContinueOnCapturedContext);

                    // The flush is where index VERSIONS move (the fold only adds keys at NoRowVersion):
                    // bump the written key's entry to the flush version on the meta persisted below.
                    if (GraphShardGrainKey.Parse(key).Direction == GraphShardDirection.Forward)
                        forwardIndex = forwardIndex.SetItem(key, flushVersion);
                    else
                        reverseIndex = reverseIndex.SetItem(key, flushVersion);
                }

                foldedMeta = foldedMeta with { ForwardKeys = forwardIndex, ReverseKeys = reverseIndex };
                await WriteMeta(flushVersion, new DatastoreMetaEntry(foldedMeta, flushVersion))
                    .ConfigureAwait(ContinueOnCapturedContext);
                newMetaVersion = flushVersion;
                flushBoundaryRevision = foldedMeta.HeadRevision;

                // Phase 0 observability: one flush boundary written (dirty rows + meta durable) — the
                // every-64th-commit sawtooth observable (docs/scalability-program.md P5).
                _metrics?.RecordFlush();
            }

            // The head pointer is the commit point: write it AFTER the log entries (+ flushed rows/meta)
            // so a crash mid-write never advertises a version whose log entries are missing or a flush
            // not yet durable.
            await WriteHead(new LogHeadEntry(version, foldedMeta.HeadRevision, newMetaVersion))
                .ConfigureAwait(ContinueOnCapturedContext);

            // COMMITTED. Publish in one synchronous block — no await from here until the next statement
            // with an await — so an interleaved [AlwaysInterleave] read either sees the pre-commit fields
            // (still the PREVIOUS commit) or, once this block runs, the fully-applied new commit: the
            // small state, the dirty buffer and the recent window advance as ONE unit. This IS the
            // atomicity argument for the read side: there is no yield point inside the block for a read
            // to land on.
            _logVersion = version;
            _storedMeta = foldedMeta;
            if (foldedDirty is not null)
                _dirty = foldedDirty;
            _recent.AddRange(pending);
            TrimRecent(foldedMeta.HeadRevision);

            // Post-commit compaction: the flush subsumes log entries up to AND INCLUDING its own version,
            // the previous meta row is no longer referenced, and rows whose keys emptied are unindexed.
            // Clearing after the commit point keeps a crash recoverable (the worst case leaks an entry,
            // never loses one). This runs AFTER publication, so it has its own awaits — an interleaved
            // read landing between publication and here still observes the fully-committed state above;
            // compaction only ever shrinks in-memory retention and storage, it never changes what's
            // already been published as committed.
            if (newMetaVersion > oldMetaVersion)
            {
                // BEST-EFFORT, same stance as the post-commit watch notify in Commit: the head entry
                // written above already references the new meta version, so a failed clear only leaks a
                // storage row — it must NEVER surface as a failed append. An exception escaping here would
                // make the log-consistency adaptor retry ApplyUpdatesToStorage, hit the version guard, and
                // report the (fully committed) conditional event as failed — the caller would then re-run
                // its whole transaction against a base that already contains its own write.
                try
                {
                    for (var v = oldMetaVersion + 1; v <= newMetaVersion; v++)
                        await ClearLogEntry(v).ConfigureAwait(ContinueOnCapturedContext);
                    await ClearMeta(oldMetaVersion).ConfigureAwait(ContinueOnCapturedContext);
                    foreach (var (key, rowVersion) in supersededRows!)
                        await ClearShardRow(rowVersion, key).ConfigureAwait(ContinueOnCapturedContext);
                }
                catch
                {
                    // A leaked entry/meta/row is unreferenced by the committed head+index and harmless to
                    // recovery (unindexed rows are never read — see CurrentShardState / the seeding rule).
                }

                // Its own contiguous synchronous field group. Unconditional: the committed head already
                // points at the new meta, so in-memory retention advances whether or not the clears
                // succeeded. The dirty buffer clears because every entry's row (or absence) is now
                // durable and current — the buffer's content is byte-identical to what a fresh read
                // would serve, so dropping it only releases memory.
                _metaVersion = newMetaVersion;
                // Retention now starts at the flush boundary (consistent with the post-reactivation
                // rebuild, which can only replay the post-flush tail).
                _recentFloorRevision = Math.Max(_recentFloorRevision, flushBoundaryRevision);
                _recent.RemoveAll(e => e.Revision <= _recentFloorRevision);
                _dirty = new Dictionary<string, GraphShardState>(StringComparer.Ordinal);
            }
        }
        finally
        {
            if (flushing)
                _shardIo.Release();
        }

        return true;
    }

    /// <inheritdoc />
    public Task ClearStoredState() =>
        _storage.ClearStateAsync(HeadStateName, this.GetGrainId(), _headState);

    // --- storage helpers ---

    private async Task<LogEvent?> ReadLogEvent(int version)
    {
        var entry = new GrainState<LogEvent>();
        await _storage.ReadStateAsync($"{LogStatePrefix}{version}", this.GetGrainId(), entry)
            .ConfigureAwait(ContinueOnCapturedContext);
        return entry.RecordExists ? entry.State : null;
    }

    // The head entry is rewritten in place through the held wrapper so its ETag carries across writes
    // (the commit point). Log entries, meta entries and shard rows are version-qualified — write-once
    // per COMMITTED version — but a crashed attempt can orphan an entry at a versioned name a retry
    // must write again (a crashed append leaves a log row with no head advance; a crashed flush leaves
    // shard/meta rows no meta references), so all three tolerate an existing row with no long-lived
    // wrapper (a per-row wrapper cache would grow O(touched keys) memory). Meta and shard rows go
    // read-then-write; log entries — the per-commit hot path — attempt the single-RTT fresh write
    // first and pay the read only on the rare orphan collision (see WriteLogEntry) — the same
    // providers-enforce-etags lesson as the read-then-clear helpers below.

    private async Task WriteLogEntry(int version, LogEvent ev)
    {
        var name = $"{LogStatePrefix}{version}";
        // HOT PATH — one storage RTT: in the overwhelmingly common case this versioned name has never
        // been written, so a fresh empty-ETag wrapper is a valid insert and the write succeeds first
        // try. Only when a CRASHED prior append attempt orphaned a row at this exact name (row written,
        // head never advanced) does the provider reject the empty ETag; then, and only then, fall back
        // to the ETag-tolerant read-then-write so the retry overwrites the orphan. Any other failure
        // propagates unchanged.
        var entry = new GrainState<LogEvent> { State = ev };
        try
        {
            await _storage.WriteStateAsync(name, this.GetGrainId(), entry).ConfigureAwait(ContinueOnCapturedContext);
        }
        catch (InconsistentStateException)
        {
            var existing = new GrainState<LogEvent>();
            await _storage.ReadStateAsync(name, this.GetGrainId(), existing).ConfigureAwait(ContinueOnCapturedContext);
            existing.State = ev;
            await _storage.WriteStateAsync(name, this.GetGrainId(), existing).ConfigureAwait(ContinueOnCapturedContext);
        }
    }

    private Task WriteHead(LogHeadEntry entry)
    {
        _headState.State = entry;
        return _storage.WriteStateAsync(HeadStateName, this.GetGrainId(), _headState);
    }

    private async Task WriteMeta(int version, DatastoreMetaEntry entry)
    {
        var name = $"{MetaStatePrefix}{version}";
        var state = new GrainState<DatastoreMetaEntry>();
        await _storage.ReadStateAsync(name, this.GetGrainId(), state).ConfigureAwait(ContinueOnCapturedContext);
        state.State = entry;
        await _storage.WriteStateAsync(name, this.GetGrainId(), state).ConfigureAwait(ContinueOnCapturedContext);
    }

    private async Task<DatastoreMetaEntry?> ReadMeta(int version)
    {
        var entry = new GrainState<DatastoreMetaEntry>();
        await _storage.ReadStateAsync($"{MetaStatePrefix}{version}", this.GetGrainId(), entry)
            .ConfigureAwait(ContinueOnCapturedContext);
        return entry.RecordExists ? entry.State : null;
    }

    private async Task<GraphShardState?> ReadShardRow(int rowVersion, string shardKey)
    {
        var entry = new GrainState<GraphShardState>();
        await _storage.ReadStateAsync(ShardRowName(rowVersion, shardKey), this.GetGrainId(), entry)
            .ConfigureAwait(ContinueOnCapturedContext);
        return entry.RecordExists ? entry.State : null;
    }

    private async Task WriteShardRow(int rowVersion, string shardKey, GraphShardState state)
    {
        var name = ShardRowName(rowVersion, shardKey);
        var entry = new GrainState<GraphShardState>();
        await _storage.ReadStateAsync(name, this.GetGrainId(), entry).ConfigureAwait(ContinueOnCapturedContext);
        entry.State = state;
        await _storage.WriteStateAsync(name, this.GetGrainId(), entry).ConfigureAwait(ContinueOnCapturedContext);
    }

    private async Task<DatastoreGrainState?> ReadLegacySnapshot(int version)
    {
        var entry = new GrainState<DatastoreGrainState>();
        await _storage.ReadStateAsync($"{LegacySnapshotStatePrefix}{version}", this.GetGrainId(), entry)
            .ConfigureAwait(ContinueOnCapturedContext);
        return entry.RecordExists ? entry.State : null;
    }

    // Clears go through a read-then-clear so the wrapper carries the CURRENT storage ETag: providers
    // enforce the etag on delete too (MemoryStorage throws MemoryStorageEtagMismatchException for a
    // null-etag delete of an existing row), and these version-qualified entries were written through
    // transient wrappers whose minted etags were discarded. Reading a missing/already-cleared entry
    // yields a null etag, which every provider accepts for a no-op clear.

    private async Task ClearMeta(int version)
    {
        var entry = new GrainState<DatastoreMetaEntry>();
        await _storage.ReadStateAsync($"{MetaStatePrefix}{version}", this.GetGrainId(), entry)
            .ConfigureAwait(ContinueOnCapturedContext);
        await _storage.ClearStateAsync($"{MetaStatePrefix}{version}", this.GetGrainId(), entry)
            .ConfigureAwait(ContinueOnCapturedContext);
    }

    private async Task ClearShardRow(int rowVersion, string shardKey)
    {
        var name = ShardRowName(rowVersion, shardKey);
        var entry = new GrainState<GraphShardState>();
        await _storage.ReadStateAsync(name, this.GetGrainId(), entry).ConfigureAwait(ContinueOnCapturedContext);
        await _storage.ClearStateAsync(name, this.GetGrainId(), entry).ConfigureAwait(ContinueOnCapturedContext);
    }

    private async Task ClearLegacySnapshot(int version)
    {
        var entry = new GrainState<DatastoreGrainState>();
        await _storage.ReadStateAsync($"{LegacySnapshotStatePrefix}{version}", this.GetGrainId(), entry)
            .ConfigureAwait(ContinueOnCapturedContext);
        await _storage.ClearStateAsync($"{LegacySnapshotStatePrefix}{version}", this.GetGrainId(), entry)
            .ConfigureAwait(ContinueOnCapturedContext);
    }

    private async Task ClearLogEntry(int version)
    {
        var entry = new GrainState<LogEvent>();
        await _storage.ReadStateAsync($"{LogStatePrefix}{version}", this.GetGrainId(), entry)
            .ConfigureAwait(ContinueOnCapturedContext);
        await _storage.ClearStateAsync($"{LogStatePrefix}{version}", this.GetGrainId(), entry)
            .ConfigureAwait(ContinueOnCapturedContext);
    }

    // --- recent-events window ---

    /// <summary>
    /// The LOCAL-list counterpart of <see cref="TrimRecent"/>, used by <see cref="ReadStateFromStorage"/>
    /// while it is still folding into locals (before its single publish block) — never touches the shared
    /// <see cref="_recent"/>/<see cref="_recentFloorRevision"/> fields.
    /// </summary>
    private void AddPending(List<LogEvent> pending, ref long floor, LogEvent ev, long head)
    {
        pending.Add(ev);
        var newFloor = Math.Max(floor, head - _gcWindowNanos);
        if (newFloor <= floor)
            return;
        floor = newFloor;
        var floorValue = newFloor;
        pending.RemoveAll(e => e.Revision <= floorValue);
    }

    // Raise the floor for anything aged past the GC window (never lower it — compaction may have set it
    // higher), and drop the now-unretained events. _recent always holds exactly the events above the floor.
    // Only ever called from within a synchronous publish block (see ApplyUpdatesToStorage) or from RunGc's
    // own append (which mints and confirms its own event via the normal write path, same guarantee).
    private void TrimRecent(long head)
    {
        var floor = Math.Max(_recentFloorRevision, head - _gcWindowNanos);
        if (floor <= _recentFloorRevision)
            return;
        _recentFloorRevision = floor;
        _recent.RemoveAll(e => e.Revision <= _recentFloorRevision);
    }

    private static long NowNanos() => (DateTimeOffset.UtcNow - DateTimeOffset.UnixEpoch).Ticks * 100L;
}
