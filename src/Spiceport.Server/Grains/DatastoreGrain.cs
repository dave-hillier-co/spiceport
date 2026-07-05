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
/// is the source of truth and the materialized <see cref="DatastoreGrainState"/> is the fold over that log
/// (held in a mutable <see cref="DatastoreStateHolder"/> because <c>JournaledGrain</c> mutates state in
/// place). It is a single non-reentrant activation, so each turn runs to completion before the next — which
/// is what makes <see cref="AppendCommit"/> atomic (the head-compare and the append cannot interleave).
/// </summary>
/// <remarks>
/// Persistence is OUR responsibility via <see cref="ICustomStorageInterface{TState,TDelta}"/> over an
/// Orleans grain-storage provider (NO application SQL): each event is a per-version <c>log/{version}</c>
/// entry, a <c>head</c> pointer tracks the contiguous log version + the timestamp head revision + the
/// snapshot version, and a periodic <c>snapshot</c> entry plus log compaction bound replay cost. Orleans'
/// <c>RetrieveConfirmedEvents</c> is NOT supported under CustomStorage; the grain keeps its own in-memory
/// recent-events window so <see cref="ReadFrom"/> serves the live tail with no storage reads.
/// </remarks>
[LogConsistencyProvider(ProviderName = "CustomStorage")]
public sealed class DatastoreGrain :
    JournaledGrain<DatastoreStateHolder, LogEvent>,
    IDatastoreGrain,
    ICustomStorageInterface<DatastoreStateHolder, LogEvent>,
    IRemindable
{
    /// <summary>The name of the periodic MVCC-GC reminder registered in <see cref="OnActivateAsync"/>.</summary>
    private const string GcReminderName = "mvcc-gc";

    // Orleans grain code must not ConfigureAwait(false); keep the captured context.
    private const ConfigureAwaitOptions ContinueOnCapturedContext = ConfigureAwaitOptions.ContinueOnCapturedContext;

    /// <summary>Storage state-name of the durable head pointer (one row, rewritten in place — the commit point).</summary>
    private const string HeadStateName = "head";

    /// <summary>Per-version snapshot state-name prefix: <c>snapshot/{version}</c> (write-once, so crash-safe).</summary>
    private const string SnapshotStatePrefix = "snapshot/";

    /// <summary>Per-event log entry state-name prefix: <c>log/{version}</c>.</summary>
    private const string LogStatePrefix = "log/";

    /// <summary>Take a snapshot (and compact) every N appended events.</summary>
    private const int SnapshotInterval = 64;

    /// <summary>
    /// How long a watcher registration lives without a <see cref="SubscribeWatch"/> refresh. 10x the hubs'
    /// heartbeat interval, so a silo must miss many heartbeats before its watcher is dropped.
    /// </summary>
    private static readonly TimeSpan WatcherExpiry = TimeSpan.FromSeconds(10);

    private readonly IGrainStorage _storage;
    private readonly ILogger<DatastoreGrain> _logger;
    private readonly DatastoreGcOptions _gcOptions;

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

    /// <summary>The log version at which the latest snapshot was taken (entries below it are compacted).</summary>
    private int _snapshotVersion;

    /// <summary>
    /// The materialized state as currently PERSISTED (the fold of all stored events). Tracked here so the
    /// storage methods never touch <c>JournaledGrain.State</c> — accessing the confirmed view from inside a
    /// confirm round-trip would re-enter the log-consistency adaptor.
    /// </summary>
    private DatastoreGrainState _stored = DatastoreGrainState.Empty(0);

    /// <summary>
    /// The live grain-state wrapper for the <c>head</c> entry — the only entry rewritten in place. Held as a
    /// field so its storage ETag persists across writes (the Orleans grain-storage providers enforce
    /// optimistic concurrency per entry; a fresh empty-ETag wrapper would be rejected on the second write).
    /// Log entries (<c>log/{version}</c>) and snapshots (<c>snapshot/{version}</c>) are write-once.
    /// </summary>
    private readonly GrainState<LogHeadEntry> _headState = new();

    /// <summary>
    /// The in-memory recent-events window (ascending by revision) that <see cref="ReadFrom"/> tails. It
    /// retains exactly the events with revision &gt; <see cref="_recentFloorRevision"/>; it is rebuilt from
    /// storage on activation (the post-snapshot tail) and appended to on each confirmed write.
    /// </summary>
    private readonly List<LogEvent> _recent = new();

    /// <summary>
    /// The exclusive lower bound of <see cref="_recent"/>: events at or below this revision have been
    /// compacted into a snapshot (or aged past the GC window) and are no longer individually retained, so
    /// <see cref="ReadFrom"/> cannot serve a cursor older than this and throws (the consumer re-bootstraps
    /// from a full snapshot via <see cref="ReadState"/>). Kept consistent live and after reactivation.
    /// </summary>
    private long _recentFloorRevision;

    public DatastoreGrain(
        [FromKeyedServices("datastore")] IGrainStorage storage,
        ILogger<DatastoreGrain> logger,
        IOptions<DatastoreGcOptions>? gcOptions = null)
    {
        _storage = storage;
        _logger = logger;
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

    /// <summary>Folds a confirmed event into the in-place holder (replay + live append).</summary>
    protected override void TransitionState(DatastoreStateHolder holder, LogEvent ev) =>
        holder.Value = LogFold.ApplyEvent(holder.Value, ev);

    // --- IDatastoreGrain ---

    /// <inheritdoc />
    public Task<DatastoreGrainState> ReadState() => Task.FromResult(State.Value);

    /// <inheritdoc />
    public Task<DatastoreHeadWire> GetHead()
    {
        var s = State.Value;
        return Task.FromResult(new DatastoreHeadWire(s.HeadRevision, s.SchemaHashAt(s.HeadRevision), s.GcFloor));
    }

    /// <inheritdoc />
    public async Task<long?> AppendCommit(long expectedHead, ProposedWrite write)
    {
        ArgumentNullException.ThrowIfNull(write);

        // The load-bearing CAS: the single-activation non-reentrant turn makes this compare atomic with the
        // append, so caller-side preconditions (evaluated against expectedHead) stay race-free.
        if (State.Value.HeadRevision != expectedHead)
            return null;

        // Mint the new revision monotonically over the observed head (mirrors ReferenceDatastore).
        var now = NowNanos();
        var newRevision = now > expectedHead ? now : expectedHead + 1;

        var ev = LogFold.EventFromProposal(write, newRevision);

        // RaiseConditionalEvent's own version check is a secondary guard; the head==expectedHead compare
        // above is the load-bearing CAS. Confirm so the append is persisted (ApplyUpdatesToStorage) before
        // returning the token.
        var raised = await RaiseConditionalEvent(ev).ConfigureAwait(ContinueOnCapturedContext);
        if (!raised)
            return null;
        await ConfirmEvents().ConfigureAwait(ContinueOnCapturedContext);

        // Push the new head to the per-silo watch hubs. Best-effort and isolated: HeadAdvanced is [OneWay]
        // (the await completes at send, no round-trip) and any failure is swallowed — the commit result must
        // never depend on the notify; a missed push is recovered by the hubs' heartbeat.
        try
        {
            await _watchers.Notify(w => w.HeadAdvanced(newRevision)).ConfigureAwait(ContinueOnCapturedContext);
        }
        catch
        {
            // Defunct observers are pruned by ObserverManager; nothing else to do.
        }

        return newRevision;
    }

    /// <inheritdoc />
    public async Task<long?> RunGc()
    {
        var head = State.Value.HeadRevision;
        var currentFloor = State.Value.GcFloor;
        var now = NowNanos();
        // Never above head (a floor beyond the head would be meaningless) and never inside the retained
        // window — mirrors AppendCommit's own revision-minting arithmetic style.
        var floor = Math.Min(head, now - _gcWindowNanos);

        if (floor <= currentFloor)
            return null; // GC only ever moves forward; nothing new to collect yet.

        // Mint the new revision exactly like AppendCommit (monotonic over the observed head) — a GC event
        // is minted by the grain itself, not by a caller-supplied expectedHead CAS.
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

        // Same best-effort notify pattern as AppendCommit: the GC result never depends on this succeeding,
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
    public Task<LogSegment> ReadFrom(long afterRevision, int maxCount)
    {
        var head = State.Value.HeadRevision;

        // The in-memory window retains only events strictly above the snapshot/GC floor; an older cursor
        // cannot be served COMPLETELY (some events in (afterRevision, floor] were compacted), so we must
        // reject rather than silently return a short tail — the consumer re-bootstraps via ReadState.
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

    /// <inheritdoc />
    public async Task<KeyValuePair<int, DatastoreStateHolder>> ReadStateFromStorage()
    {
        await _storage.ReadStateAsync(HeadStateName, this.GetGrainId(), _headState)
            .ConfigureAwait(ContinueOnCapturedContext);

        if (!_headState.RecordExists || _headState.State is null)
        {
            // No durable head yet: seed an empty state at a monotonic timestamp (mirrors ReferenceDatastore's
            // initial Empty(NowNanos)) and PERSIST the seed, so the pre-first-write revision floor is stable
            // across reactivation (a re-seed with a fresh, larger NowNanos would silently move the head).
            var seeded = DatastoreGrainState.Empty(NowNanos());
            await WriteSnapshot(0, seeded).ConfigureAwait(ContinueOnCapturedContext);
            await WriteHead(new LogHeadEntry(0, seeded.HeadRevision, 0)).ConfigureAwait(ContinueOnCapturedContext);
            _logVersion = 0;
            _snapshotVersion = 0;
            _stored = seeded;
            _recent.Clear();
            _recentFloorRevision = seeded.HeadRevision;
            return new KeyValuePair<int, DatastoreStateHolder>(0, new DatastoreStateHolder { Value = seeded });
        }

        var headEntry = _headState.State;
        _logVersion = headEntry.LogVersion;
        _snapshotVersion = headEntry.SnapshotVersion;

        // Load the snapshot the head points at, then replay the log tail (snapshotVersion+1 .. logVersion)
        // folding into it. The range is contiguous by construction (head is written last, the commit point),
        // so a missing in-range entry is corruption — fail loudly rather than silently fold a lossy state
        // (which would pass the durability negative control while having lost data).
        var value = await ReadSnapshot(_snapshotVersion).ConfigureAwait(ContinueOnCapturedContext)
            ?? DatastoreGrainState.Empty(0);

        _recent.Clear();
        _recentFloorRevision = value.HeadRevision;
        for (var v = _snapshotVersion + 1; v <= _logVersion; v++)
        {
            var ev = await ReadLogEvent(v).ConfigureAwait(ContinueOnCapturedContext)
                ?? throw new InvalidOperationException(
                    $"datastore log corruption: missing log entry {v} in [{_snapshotVersion + 1}..{_logVersion}]");
            value = LogFold.ApplyEvent(value, ev);
            AddRecent(ev, value.HeadRevision);
        }

        _stored = value;
        return new KeyValuePair<int, DatastoreStateHolder>(_logVersion, new DatastoreStateHolder { Value = value });
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

        // Materialize the head revision after applying these events (fold forward from the persisted state;
        // never touch JournaledGrain.State here — that would re-enter the consistency adaptor mid-confirm).
        var folded = _stored;
        var version = _logVersion;
        foreach (var ev in updates)
        {
            version++;
            // Log entries are write-once: a fresh wrapper (empty ETag) is correct for an insert.
            await _storage.WriteStateAsync($"{LogStatePrefix}{version}", this.GetGrainId(), new GrainState<LogEvent>(ev))
                .ConfigureAwait(ContinueOnCapturedContext);
            folded = LogFold.ApplyEvent(folded, ev);
            AddRecent(ev, folded.HeadRevision);
        }

        // Periodically snapshot + compact: bound the replay tail on reactivation and the in-memory window.
        // The snapshot is written to a NEW version-qualified key BEFORE the head commit, so a crash between
        // the two leaves the head still pointing at the previous (untouched) snapshot — never at a
        // half-written one. (Overwriting a single snapshot row before the commit point would let replay
        // double-apply events already folded into the new snapshot.)
        var oldSnapshotVersion = _snapshotVersion;
        var newSnapshotVersion = _snapshotVersion;
        var snapshotBoundaryRevision = -1L;
        if (updates.Count > 0 && version / SnapshotInterval > _snapshotVersion / SnapshotInterval)
        {
            await WriteSnapshot(version, folded).ConfigureAwait(ContinueOnCapturedContext);
            newSnapshotVersion = version;
            snapshotBoundaryRevision = folded.HeadRevision;
        }

        // The head pointer is the commit point: write it AFTER the log entries (+ new snapshot) so a crash
        // mid-write never advertises a version whose log entries are missing or a snapshot not yet durable.
        await WriteHead(new LogHeadEntry(version, folded.HeadRevision, newSnapshotVersion))
            .ConfigureAwait(ContinueOnCapturedContext);

        // Post-commit compaction: the new snapshot subsumes log entries up to AND INCLUDING its own version,
        // and the previous snapshot is no longer referenced. Clearing after the commit point keeps a crash
        // recoverable (the worst case leaks an entry, never loses one).
        if (newSnapshotVersion > oldSnapshotVersion)
        {
            for (var v = oldSnapshotVersion + 1; v <= newSnapshotVersion; v++)
                await ClearLogEntry(v).ConfigureAwait(ContinueOnCapturedContext);
            await ClearSnapshot(oldSnapshotVersion).ConfigureAwait(ContinueOnCapturedContext);
            _snapshotVersion = newSnapshotVersion;
            // Retention now starts at the snapshot boundary (consistent with the post-reactivation rebuild,
            // which can only replay the post-snapshot tail).
            _recentFloorRevision = Math.Max(_recentFloorRevision, snapshotBoundaryRevision);
            _recent.RemoveAll(e => e.Revision <= _recentFloorRevision);
        }

        _logVersion = version;
        _stored = folded;
        TrimRecent(folded.HeadRevision);
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
    // (the commit point). Snapshots are write-once per version, so they use a fresh wrapper.
    private Task WriteHead(LogHeadEntry entry)
    {
        _headState.State = entry;
        return _storage.WriteStateAsync(HeadStateName, this.GetGrainId(), _headState);
    }

    private Task WriteSnapshot(int version, DatastoreGrainState snapshot) =>
        _storage.WriteStateAsync($"{SnapshotStatePrefix}{version}", this.GetGrainId(),
            new GrainState<DatastoreGrainState>(snapshot));

    private async Task<DatastoreGrainState?> ReadSnapshot(int version)
    {
        var entry = new GrainState<DatastoreGrainState>();
        await _storage.ReadStateAsync($"{SnapshotStatePrefix}{version}", this.GetGrainId(), entry)
            .ConfigureAwait(ContinueOnCapturedContext);
        return entry.RecordExists ? entry.State : null;
    }

    private Task ClearSnapshot(int version) =>
        _storage.ClearStateAsync($"{SnapshotStatePrefix}{version}", this.GetGrainId(),
            new GrainState<DatastoreGrainState>());

    private Task ClearLogEntry(int version) =>
        _storage.ClearStateAsync($"{LogStatePrefix}{version}", this.GetGrainId(), new GrainState<LogEvent>());

    // --- recent-events window ---

    private void AddRecent(LogEvent ev, long head)
    {
        _recent.Add(ev);
        TrimRecent(head);
    }

    // Raise the floor for anything aged past the GC window (never lower it — compaction may have set it
    // higher), and drop the now-unretained events. _recent always holds exactly the events above the floor.
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
