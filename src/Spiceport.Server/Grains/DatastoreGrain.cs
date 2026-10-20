using System.Collections.Immutable;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.EventSourcing;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Utilities;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// The cluster-singleton datastore grain (<see cref="IDatastoreGrain.Key"/> = 0): the sequencer whose
/// journal is the source of truth for the whole MVCC datastore. It is an ordinary JOURNALED grain over
/// a standard log-consistency provider: each commit journals a canonical <see cref="LogEvent"/>, and
/// the journaled state is only the SMALL state (<see cref="DatastoreMetaState"/> — head, schemas,
/// counters, GC floor, the types seen, and the pending tail of events not yet projected). Relationship
/// rows are owned by the per-key <see cref="GraphShardGrain"/> projections, which persist their own
/// state, and listed by the <see cref="ShardIndexGrain"/> buckets. It is a single activation whose
/// WRITES (commit batches, <see cref="RunGc"/>, <see cref="FlushProjection"/>) never interleave EACH
/// OTHER: each is a work item in the one write queue, which a single <see cref="BatchWorker"/> drains
/// in arrival order, so a write runs to completion before the next, keeping every
/// head-compare-and-append atomic. The PURE reads (<see cref="ReadState"/>,
/// <see cref="GetHead"/>, <see cref="ReadSchemaAt"/>, <see cref="ReadShard"/>,
/// <see cref="ReadShardTail"/>, <see cref="IDatastoreLog.ReadFrom"/>) are <c>[AlwaysInterleave]</c> on
/// the grain interface and may run DURING a write's await — never concurrently with it, and never
/// observing a write's intermediate state, because they read the confirmed view.
/// </summary>
/// <remarks>
/// <para>
/// GROUP COMMIT. <see cref="Commit"/> interleaves: it queues its request and awaits its reply. The
/// writer takes each run of adjacent queued commits as one batch, evaluating each request in arrival order
/// against the confirmed state with the events of the requests before it in the batch folded on
/// (<see cref="MetaFold.ApplyEntry"/>), so each sees exactly the state a serial run would have shown
/// it. The accepted events are then appended with ONE conditional raise and ONE confirm, so a batch
/// costs one storage write however many commits it carries. Every commit still mints its own
/// revision and gets its own reply, sent only once the batch is durable; rejected requests append
/// nothing. Commits that arrive while a batch is being written form the next batch.
/// </para>
/// <para>
/// THE PROJECTION. Every <see cref="FlushInterval"/> pending events, the sequencer projects them: each
/// shard grain an event touches folds its events over its persisted state and persists the result
/// (<see cref="IGraphShardGrain.Project"/>), the touched keys are added to the shard index, and only
/// then is a <see cref="DatastoreJournalEntry.Projected"/> entry journaled, which drops those events
/// from the pending tail. A crash anywhere before that entry leaves the events pending, and the next
/// flush replays them; shard projection is idempotent by its label. So every committed event is, at
/// every instant, either projected into each shard it touches or pending in the journaled state —
/// which is what makes a key's current state exactly its projected state with its pending events
/// folded over (<see cref="ShardFold.Overlay"/>), relabeled to the head.
/// </para>
/// <para>
/// THE ORDER RULE for reading a key's current state while a flush may interleave: take the pending
/// tail FIRST, read the projected state SECOND. A projection that lands in between is then already in
/// the projected state and its events skipped by the label; one that has not landed is still in the
/// captured tail.
/// </para>
/// <para>
/// Row-level GC is LAZY: a GC event advances the small state's floor immediately, but a projected row
/// keeps its sub-floor dead rows until its key is next projected. Sound because reads pinned below the
/// floor are rejected outright and every serve path re-applies the current floor
/// (<see cref="ShardFold.CollectBelow"/>) before answering.
/// </para>
/// </remarks>
[LogConsistencyProvider(ProviderName = "StateStorage")]
[StorageProvider(ProviderName = "datastore")]
public sealed class DatastoreGrain :
    JournaledGrain<DatastoreMetaHolder, DatastoreJournalEntry>,
    IDatastoreGrain,
    IRemindable
{
    /// <summary>The name of the periodic MVCC-GC reminder registered in <see cref="OnActivateAsync"/>.</summary>
    private const string GcReminderName = "mvcc-gc";

    // Orleans grain code must not ConfigureAwait(false); keep the captured context.
    private const ConfigureAwaitOptions ContinueOnCapturedContext = ConfigureAwaitOptions.ContinueOnCapturedContext;

    /// <summary>
    /// Project the pending tail once it holds this many events. It bounds the journaled state (and so
    /// the cost of each commit's state write) and is the retention of <see cref="ReadFrom"/>, whose
    /// floor is the projection boundary.
    /// </summary>
    private const int FlushInterval = 64;

    /// <summary>
    /// The most shard or index grain calls the sequencer has in flight at once. A flush touches up
    /// to two keys per event, and every one of those grains persists through the storage provider:
    /// unbounded, a flush opens as many storage connections as it touches keys and exhausts the
    /// database's connection limit, failing the projection.
    /// </summary>
    private const int MaxConcurrentShardCalls = 16;

    /// <summary>
    /// How long a watcher registration lives without a <see cref="SubscribeWatch"/> refresh. 10x the hubs'
    /// heartbeat interval, so a silo must miss many heartbeats before its watcher is dropped.
    /// </summary>
    private static readonly TimeSpan WatcherExpiry = TimeSpan.FromSeconds(10);

    private readonly ILogger<DatastoreGrain> _logger;
    private readonly DatastoreGcOptions _gcOptions;

    /// <summary>
    /// The sequencer-side inbound-call counters (docs/scalability-program.md Phase 0), or null on a
    /// host that never registered them — every record site is null-conditional, so the grain works
    /// unchanged without the seam (the same optional-dependency stance as <see cref="_gcOptions"/>).
    /// </summary>
    private readonly ISequencerMetrics? _metrics;

    /// <summary>The retention window <see cref="RunGc"/> collects MVCC history beyond.</summary>
    private readonly long _gcWindowNanos;

    /// <summary>
    /// The registered head-advance observers (one per silo hub). Deliberately in-memory only — observer
    /// references are not durable, so the set empties on reactivation; the hubs' heartbeat resubscribe (which
    /// also returns the head) makes that safe.
    /// </summary>
    private readonly ObserverManager<IDatastoreWatcher> _watchers;

    /// <summary>
    /// Writes waiting their turn, in arrival order: commits, GC rounds and projection flushes. Every write
    /// call interleaves and only enqueues; <see cref="_writer"/> is the one thing that executes writes, so
    /// no two writes interleave and none waits in the activation's own request queue.
    /// </summary>
    private readonly List<QueuedWrite> _writes = [];

    /// <summary>The single writer: one work cycle at a time, on the activation's scheduler.</summary>
    private readonly BatchWorkerFromDelegate _writer;

    public DatastoreGrain(
        ILogger<DatastoreGrain> logger,
        IOptions<DatastoreGcOptions>? gcOptions = null,
        ISequencerMetrics? metrics = null)
    {
        _logger = logger;
        _metrics = metrics;
        _watchers = new ObserverManager<IDatastoreWatcher>(WatcherExpiry, logger);
        _writer = new BatchWorkerFromDelegate(DrainWrites);
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

        if (Version == 0)
        {
            // A fresh store: seed the head at a monotonic timestamp (mirrors ReferenceDatastore's initial
            // Empty(NowNanos)) and JOURNAL the seed, so the pre-first-write head is stable across
            // reactivation (a re-seed with a fresh, larger NowNanos would silently move the head).
            RaiseEvent(new DatastoreJournalEntry.Seeded(NowNanos()));
            await ConfirmEvents().ConfigureAwait(ContinueOnCapturedContext);
        }

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

    /// <summary>Folds a journal entry into the in-place holder — the small state only.</summary>
    protected override void TransitionState(DatastoreMetaHolder holder, DatastoreJournalEntry entry) =>
        holder.Value = MetaFold.ApplyEntry(holder.Value, entry);

    // --- serve-path core -------------------------------------------------------------------------

    /// <summary>
    /// current-state(key) against the captured state <paramref name="meta"/>: the key's shard grain's
    /// projected state with <paramref name="meta"/>'s pending events for the key folded over, relabeled
    /// to its head with its floor applied. <paramref name="meta"/> must be captured BEFORE this call
    /// reads the projected state (the order rule in the class remarks).
    /// </summary>
    private async Task<GraphShardState> CurrentShardState(DatastoreMetaState meta, string shardKey)
    {
        var key = GraphShardGrainKey.Parse(shardKey);
        var projected = await GrainFactory.GetGrain<IGraphShardGrain>(shardKey).ReadProjected()
            .ConfigureAwait(ContinueOnCapturedContext);
        return ShardFold.Overlay(projected, meta.Pending, key, meta.HeadRevision, meta.GcFloor);
    }

    /// <summary>
    /// Every key of <paramref name="direction"/> and <paramref name="objectType"/> that may hold rows:
    /// the shard index's listing plus the keys the pending tail touches (not yet indexed).
    /// </summary>
    private async Task<IEnumerable<GraphShardKeyWire>> KeysOf(
        DatastoreMetaState meta, GraphShardDirection direction, string objectType)
    {
        var listed = await ShardIndexKey.ListAsync(GrainFactory, direction, objectType)
            .ConfigureAwait(ContinueOnCapturedContext);
        return listed.Concat(PendingKeys(meta, direction).Where(key =>
            string.Equals(key.ObjectType, objectType, StringComparison.Ordinal)));
    }

    /// <summary>The keys of <paramref name="direction"/> the pending tail touches.</summary>
    private static IEnumerable<GraphShardKeyWire> PendingKeys(DatastoreMetaState meta, GraphShardDirection direction) =>
        meta.Pending
            .Where(ev => ev.GcFloor is null)
            .SelectMany(ev => ev.RelationshipChanges)
            .Select(change => direction == GraphShardDirection.Forward
                ? GraphShardKeyWire.ForResource(change.Relationship.ResourceType, change.Relationship.ResourceId)
                : GraphShardKeyWire.ForSubject(change.Relationship.SubjectType, change.Relationship.SubjectId));

    // --- IDatastoreGrain ---

    /// <inheritdoc />
    /// <remarks>
    /// [AlwaysInterleave]. ADMIN-PLANE ASSEMBLY: rebuilds a <see cref="DatastoreGrainState"/> from the
    /// captured small state plus the current state of every FORWARD key the shard index or the pending
    /// tail names (each row belongs to exactly one forward key, so the union is exact and
    /// duplicate-free). The cost is O(graph) grain calls + transfer per call, acceptable ONLY for its
    /// admin-plane consumers (the scan seam, the compatibility ReadWriteTx base, the equivalence gates).
    /// Consistency: every key is read against the one captured state (the order rule), so a projection
    /// landing mid-assembly is never double-applied or lost; a key whose projection already includes
    /// events above the captured head contributes rows stamped above it, which MVCC visibility filters
    /// for every pin at or below it.
    /// </remarks>
    public async Task<DatastoreGrainState> ReadState()
    {
        _metrics?.RecordReadState();
        var meta = State.Value;

        var keys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var type in meta.ForwardTypes)
        {
            foreach (var key in await KeysOf(meta, GraphShardDirection.Forward, type).ConfigureAwait(ContinueOnCapturedContext))
                keys.Add(GraphShardGrainKey.Build(key));
        }

        var states = await InBatches(keys, key => CurrentShardState(meta, key))
            .ConfigureAwait(ContinueOnCapturedContext);

        var rows = new List<StoredRelationshipWire>();
        var floor = meta.GcFloor;
        foreach (var state in states)
        {
            rows.AddRange(state.Rows);
            // Normally equal to the small state's floor; a HIGHER projected floor (a GC and a projection
            // landed mid-assembly) is propagated so a pin between the floors is rejected (fail loud)
            // instead of served incomplete.
            floor = Math.Max(floor, state.GcFloor);
        }

        // Order by created revision to approximate the whole fold's append order. Order is cosmetic:
        // every consumer is an MVCC reader, and anything order-sensitive (keyset cursors, the limited
        // delete's canonical pick) imposes its own deterministic order downstream.
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

    /// <inheritdoc />
    /// <remarks>
    /// [AlwaysInterleave]. Reads the confirmed state, so an interleaved call landing mid-write sees, AT
    /// WORST, the previous commit; it can never observe a write's in-flight intermediate state.
    /// </remarks>
    public Task<DatastoreHeadWire> GetHead()
    {
        _metrics?.RecordGetHead();
        var s = State.Value;
        return Task.FromResult(new DatastoreHeadWire(s.HeadRevision, s.SchemaHashAt(s.HeadRevision), s.GcFloor));
    }

    /// <inheritdoc />
    /// <remarks>[AlwaysInterleave]. Small state only; same publication guarantee as <see cref="GetHead"/>.</remarks>
    public Task<byte[]?> ReadSchemaAt(long revision)
    {
        _metrics?.RecordReadSchemaAt();
        var s = State.Value;
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
    /// <remarks>[AlwaysInterleave]. One <see cref="CurrentShardState"/> resolution at the current head.</remarks>
    public Task<GraphShardState> ReadShard(GraphShardKeyWire key)
    {
        ArgumentNullException.ThrowIfNull(key);
        return CurrentShardState(State.Value, GraphShardGrainKey.Build(key));
    }

    /// <inheritdoc />
    /// <remarks>[AlwaysInterleave]. Synchronous over the confirmed state.</remarks>
    public Task<ShardTailWire> ReadShardTail(GraphShardKeyWire key)
    {
        _metrics?.RecordReadShard();
        ArgumentNullException.ThrowIfNull(key);
        var meta = State.Value;
        var events = meta.Pending.Where(ev => ShardFold.Touches(ev, key)).ToImmutableList();
        return Task.FromResult(new ShardTailWire(events, meta.HeadRevision, meta.GcFloor));
    }

    /// <inheritdoc />
    /// <remarks>
    /// The RELOCATION of the client-side write execution (formerly <c>GrainBackedDatastore.ReadWriteTx</c>'s
    /// base-transact-CAS loop) into the sequencer: because writes never interleave writes, the head read at
    /// the top of this method cannot move before the append at the bottom — the transaction executor IS the
    /// serialization point, so a declarative commit (null <see cref="CommitRequest.ExpectedHead"/>) needs
    /// no retry. The write base is not the whole fold: candidate-key resolution assembles a PARTIAL base
    /// from exactly the shard keys the request can read (see the soundness argument inline), each the
    /// key's projected state with the pending tail folded over. All rejections return as reply data
    /// (<see cref="CommitReply.Failure"/>)
    /// with NOTHING applied — the staged <see cref="MvccReadWriteTransaction"/> is simply abandoned,
    /// exactly as the caller-side lambda abandonment worked.
    /// </remarks>
    public async Task<CommitReply> Commit(CommitRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        // Duration measured from arrival to reply — queueing, evaluation, the batch's append and
        // confirm — and recorded in a finally so every inbound call, accepted, rejected, or thrown,
        // counts exactly once. GetTimestamp/GetElapsedTime rather than Stopwatch.StartNew: the same
        // clock with no per-commit Stopwatch allocation on the sequencer's hot path. The
        // candidate-key-count buckets are recorded inside EvaluateCommit at the point candidate
        // resolution completes.
        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var queued = new QueuedCommit(request);
            Enqueue(queued);
            return await queued.Reply.Task.ConfigureAwait(ContinueOnCapturedContext);
        }
        finally
        {
            _metrics?.RecordCommit((long)Stopwatch.GetElapsedTime(startTimestamp).TotalMicroseconds);
        }
    }

    /// <summary>A write waiting in <see cref="_writes"/>.</summary>
    private abstract record QueuedWrite;

    /// <summary>A commit waiting for its batch, and where its reply goes.</summary>
    private sealed record QueuedCommit(CommitRequest Request) : QueuedWrite
    {
        public TaskCompletionSource<CommitReply> Reply { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    /// <summary>A GC round or a projection flush, run alone between batches; it completes its own caller.</summary>
    private sealed record QueuedRun(Func<Task> Run) : QueuedWrite;

    /// <summary>Queues a write and wakes the writer.</summary>
    private void Enqueue(QueuedWrite write)
    {
        _writes.Add(write);
        _writer.Notify();
    }

    /// <summary>Queues a GC round or a projection flush and waits for its outcome.</summary>
    private Task<T> EnqueueRun<T>(Func<Task<T>> run)
    {
        var done = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
        Enqueue(new QueuedRun(async () =>
        {
            try
            {
                done.TrySetResult(await run().ConfigureAwait(ContinueOnCapturedContext));
            }
            catch (Exception ex)
            {
                done.TrySetException(ex);
            }
        }));
        return done.Task;
    }

    /// <summary>
    /// The writer's work cycle: drains <see cref="_writes"/> in arrival order. Each run of adjacent
    /// commits is one batch; a GC round or a flush runs alone. Runs on the activation's scheduler, so
    /// taking from the queue cannot race an enqueue, and the worker never runs two cycles at once.
    /// </summary>
    private async Task DrainWrites()
    {
        while (_writes.Count > 0)
        {
            if (_writes[0] is QueuedCommit)
            {
                var batch = _writes.TakeWhile(w => w is QueuedCommit).Cast<QueuedCommit>().ToArray();
                _writes.RemoveRange(0, batch.Length);
                await CommitBatch(batch).ConfigureAwait(ContinueOnCapturedContext);
            }
            else
            {
                var run = (QueuedRun)_writes[0];
                _writes.RemoveAt(0);
                await run.Run().ConfigureAwait(ContinueOnCapturedContext);
            }
        }
    }

    /// <summary>
    /// Evaluates a batch in order, appends its accepted events in one write, and
    /// only then replies to every member. Never throws: every outcome, including a failed append, goes
    /// to the members' replies.
    /// </summary>
    private async Task CommitBatch(IReadOnlyList<QueuedCommit> batch)
    {
        // The confirmed small state, with each accepted request's event folded on in turn, so a later
        // request sees the earlier ones exactly as a serial run would have shown them.
        var meta = State.Value;
        var evaluated = new List<(QueuedCommit Commit, CommitReply Reply, LogEvent? Event)>(batch.Count);
        foreach (var queued in batch)
        {
            try
            {
                var (reply, ev) = await EvaluateCommit(queued.Request, meta).ConfigureAwait(ContinueOnCapturedContext);
                if (ev is not null)
                    meta = MetaFold.ApplyEntry(meta, new DatastoreJournalEntry.Committed(ev));
                evaluated.Add((queued, reply, ev));
            }
            catch (Exception ex)
            {
                // An evaluation fault (an invariant violation) fails only its own request, which
                // appended nothing, so it cannot have influenced the requests after it.
                queued.Reply.TrySetException(ex);
            }
        }

        var events = evaluated.Where(e => e.Event is not null).Select(e => e.Event!).ToList();
        if (events.Count > 0)
        {
            // Append: the batch's events join the pending tail in one write, from which the shard
            // grains and Watch read them until a projection flush persists them into the shard grains
            // they touch. The conditional raise is a secondary guard behind the per-request CAS: this
            // activation is the only writer and the write queue runs one write at a time, so it
            // cannot lose here, but a loss is still reported as a moved head rather than assumed away.
            //
            // EVERY member's reply waits for this write, rejections included: a rejection may rest on
            // an earlier member's event (two creates of one relationship), so it is only true once
            // that event is durable. A failed write fails the whole batch.
            bool raised;
            try
            {
                raised = await RaiseConditionalEvents(
                        events.Select(ev => (DatastoreJournalEntry)new DatastoreJournalEntry.Committed(ev)))
                    .ConfigureAwait(ContinueOnCapturedContext);
                if (raised)
                    await ConfirmEvents().ConfigureAwait(ContinueOnCapturedContext);
            }
            catch (Exception ex)
            {
                foreach (var e in evaluated)
                    e.Commit.Reply.TrySetException(ex);
                return;
            }

            if (!raised)
            {
                foreach (var e in evaluated)
                    e.Commit.Reply.TrySetResult(Rejected(
                        CommitFailureKind.HeadMoved, "conditional append reported a concurrent head advance"));
                return;
            }

            await NotifyCommitted(events).ConfigureAwait(ContinueOnCapturedContext);
        }

        // The flush a full tail triggers runs BEFORE the replies, so a caller that sees its reply sees
        // the flush attempted. TryFlushProjection never throws (a failed flush leaves the events
        // pending), so the replies below always complete.
        if (State.Value.Pending.Count >= FlushInterval)
            await TryFlushProjection().ConfigureAwait(ContinueOnCapturedContext);

        foreach (var e in evaluated)
            e.Commit.Reply.TrySetResult(e.Reply);
    }

    /// <summary>
    /// Pushes a confirmed batch to the per-silo watch hubs: the new head once, and every schema change in
    /// revision order. Best-effort and isolated: the notifications are [OneWay] and any failure is
    /// swallowed — the commit result must never depend on the notify; a missed push is recovered by the
    /// hubs' heartbeat.
    /// </summary>
    private async Task NotifyCommitted(IReadOnlyList<LogEvent> committed)
    {
        try
        {
            await _watchers.Notify(w => w.HeadAdvanced(committed[^1].Revision)).ConfigureAwait(ContinueOnCapturedContext);
        }
        catch
        {
            // Defunct observers are pruned by ObserverManager; nothing else to do.
        }

        // A schema change committed: push the bytes to every silo's watch hub alongside the head advance.
        // Without this, a WriteSchema landing on THIS silo's grain activation only swaps ISchemaProvider
        // locally on the silo that ran it — every other silo keeps serving its stale Current schema hash
        // (a live divergence, not just a cache-miss latency cost) until its own hub heartbeat notices the
        // stored hash moved.
        foreach (var schemaChange in committed.Select(ev => ev.SchemaChange).OfType<SchemaVersionWire>())
        {
            try
            {
                await _watchers.Notify(w => w.SchemaAdvanced(schemaChange.Bytes, schemaChange.Hash))
                    .ConfigureAwait(ContinueOnCapturedContext);
            }
            catch
            {
                // Defunct observers are pruned by ObserverManager; nothing else to do.
            }
        }
    }

    private static CommitReply Rejected(CommitFailureKind kind, string? detail) =>
        new(null, new CommitFailureWire(kind, detail), 0, false);

    /// <summary>
    /// Evaluates one commit (see the <see cref="Commit"/> remarks) against <paramref name="meta"/> — the
    /// confirmed state with the earlier members of its batch folded on — and returns its reply and the
    /// event to append, or a rejection and no event. Appends nothing itself.
    /// </summary>
    private async Task<(CommitReply Reply, LogEvent? Event)> EvaluateCommit(CommitRequest request, DatastoreMetaState meta)
    {
        var head = meta.HeadRevision;

        // 1. Compatibility CAS (the lambda ReadWriteTx path evaluated its preconditions caller-side
        //    against this head): reject if the head moved — the load-bearing CAS compare, atomic with the
        //    append because writes never interleave writes.
        if (request.ExpectedHead is { } expectedHead && head != expectedHead)
            return (Rejected(CommitFailureKind.HeadMoved, $"expected head {expectedHead} but head is {head}"), null);

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
                return (Rejected(
                    CommitFailureKind.SchemaHashMoved,
                    $"expected schema hash '{expectedSchemaHash}' but schema hash at head is '{currentHash}'"), null);
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
            await AddFilterCandidates(precondition.Filter, meta, candidates).ConfigureAwait(ContinueOnCapturedContext);
        if (request.DeleteByFilter is { } df)
            await AddFilterCandidates(df.Filter, meta, candidates).ConfigureAwait(ContinueOnCapturedContext);
        _metrics?.RecordCommitCandidates(candidates.Count);

        var shards = await InBatches(candidates, candidate => CurrentShardState(meta, candidate))
            .ConfigureAwait(ContinueOnCapturedContext);
        var candidateRows = new List<StoredRelationshipWire>();
        var seen = new Dictionary<CandidateRowIdentity, long?>();
        foreach (var shard in shards)
        {
            foreach (var row in shard.Rows)
            {
                var identity = CandidateRowIdentity.From(row);
                if (seen.TryGetValue(identity, out var deletedRevision))
                {
                    // The INTENDED collision: the same stored row contributed by both its forward and
                    // its reverse key — identical stamps by the sharding lemma, so dropping the
                    // duplicate is exact. If the stamps disagree, that state is impossible by
                    // construction: it means storage corruption or a fold bug, and silently preferring
                    // either row would reintroduce the collapsed-verdict blocker. Fail loudly instead
                    // of guessing.
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
                return (Rejected(CommitFailureKind.PreconditionFailed, PreconditionMessages.MustMatchFailed(i, filter)), null);
            if (!precondition.MustMatch && matched)
                return (Rejected(CommitFailureKind.PreconditionFailed, PreconditionMessages.MustNotMatchFailed(i, filter)), null);
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
                return (Rejected(CommitFailureKind.CreateAlreadyExists, ex.Relationship), null);
            }
        }

        // 8. Bulk delete-by-filter (after the updates, like the request declares them).
        ulong deletedCount = 0;
        var reachedLimit = false;
        if (request.DeleteByFilter is { } deleteByFilter)
        {
            // A transactional delete (SpiceDB: a limit without partial deletion) counts at most limit+1
            // matches after the preconditions and rejects, applying nothing, when the limit is exceeded.
            if (deleteByFilter is { RejectOverLimit: true, Limit: { } cap })
            {
                var matches = 0UL;
                await foreach (var _ in tx.QueryRelationships(WireConvert.ToCoreFilter(deleteByFilter.Filter)))
                {
                    if (++matches > cap)
                    {
                        return (Rejected(CommitFailureKind.DeleteLimitExceeded,
                            cap.ToString(System.Globalization.CultureInfo.InvariantCulture)), null);
                    }
                }
            }

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
        //      the direct-append stance MetaFold (and the tests' whole-state LogFold) take for the same reason. The deltas
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
                    return (Rejected(CommitFailureKind.CounterAlreadyRegistered, ex.CounterName), null);
                }
                catch (CounterNotRegisteredException ex)
                {
                    return (Rejected(CommitFailureKind.CounterNotRegistered, ex.CounterName), null);
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

        // The request's transaction metadata belongs to the revision it mints, on either path; an empty
        // one is stored as none (SpiceDB common.Changes.AddRevisionMetadata skips an empty metadata).
        if (request.TransactionMetadata is { Length: > 0 } metadata)
            ev = ev with { TransactionMetadata = metadata };

        return (new CommitReply(newRevision, null, deletedCount, reachedLimit), ev);
    }

    /// <summary>
    /// Resolves a filter to the shard keys able to hold ANY row it matches — deliberately a SUPERSET
    /// (over-inclusion only adds rows the filter then ignores; under-inclusion would silently break
    /// MustNotMatch/delete completeness). A filter naming an explicit resource type and ids determines
    /// its keys by construction; other resource-side constraints (type/ids/prefix) enumerate the forward
    /// keys of the constrained type (every type when none is named); a filter constraining ONLY subjects
    /// resolves its selectors the same way on the reverse side; a filter constraining neither
    /// (relation-only or empty) resolves every forward key, because every row lives in exactly one
    /// forward key. Enumeration reads the shard index plus the pending tail's keys. Row-level
    /// constraints (relation, caveat, expiration, subject-relation) never narrow KEYS and are left to the
    /// transaction's own filter evaluation.
    /// </summary>
    private async Task AddFilterCandidates(
        FullRelationshipsFilterWire filter, DatastoreMetaState meta, HashSet<string> candidates)
    {
        var resourceConstrained = filter.OptionalResourceType is not null
            || filter.OptionalResourceIds is { Count: > 0 }
            || filter.OptionalResourceIdPrefix is not null;

        if (resourceConstrained)
        {
            // Exact-key fast path: a filter naming an explicit resource type + explicit ids (no prefix)
            // determines its candidate keys by CONSTRUCTION — O(#ids), never an enumeration. A key with
            // no rows resolves to an empty shard, which evaluates preconditions identically.
            if (filter is { OptionalResourceType: { } exactType, OptionalResourceIds.Count: > 0, OptionalResourceIdPrefix: null })
            {
                foreach (var id in filter.OptionalResourceIds)
                    candidates.Add(GraphShardGrainKey.Build(GraphShardKeyWire.ForResource(exactType, id)));
                return;
            }

            IEnumerable<string> types = filter.OptionalResourceType is { } type ? [type] : meta.ForwardTypes;
            foreach (var t in types)
            {
                foreach (var key in await KeysOf(meta, GraphShardDirection.Forward, t).ConfigureAwait(ContinueOnCapturedContext))
                {
                    if (filter.OptionalResourceIds is { Count: > 0 } ids && !ids.Contains(key.ObjectId))
                        continue;
                    if (filter.OptionalResourceIdPrefix is { } prefix
                        && !key.ObjectId.StartsWith(prefix, StringComparison.Ordinal))
                        continue;
                    candidates.Add(GraphShardGrainKey.Build(key));
                }
            }
            return;
        }

        if (filter.OptionalSubjectsSelectors is { Count: > 0 } selectors)
        {
            // Same fast path on the reverse side: when EVERY selector names an explicit subject type +
            // explicit ids, the reverse keys are constructible; one broader selector falls the whole
            // filter back to enumeration (selectors are OR'd, so an enumerated one covers the others).
            if (selectors.All(static s => s is { OptionalSubjectType: not null, OptionalSubjectIds.Count: > 0 }))
            {
                foreach (var selector in selectors)
                foreach (var id in selector.OptionalSubjectIds!)
                    candidates.Add(GraphShardGrainKey.Build(GraphShardKeyWire.ForSubject(selector.OptionalSubjectType!, id)));
                return;
            }

            IEnumerable<string> subjectTypes = selectors.Any(static s => s.OptionalSubjectType is null)
                ? meta.ReverseTypes
                : selectors.Select(static s => s.OptionalSubjectType!).Distinct(StringComparer.Ordinal);
            foreach (var t in subjectTypes)
            {
                foreach (var key in await KeysOf(meta, GraphShardDirection.Reverse, t).ConfigureAwait(ContinueOnCapturedContext))
                {
                    foreach (var selector in selectors)
                    {
                        if (selector.OptionalSubjectType is { } subjectType
                            && !string.Equals(key.ObjectType, subjectType, StringComparison.Ordinal))
                            continue;
                        if (selector.OptionalSubjectIds is { Count: > 0 } subjectIds
                            && !subjectIds.Contains(key.ObjectId))
                            continue;
                        candidates.Add(GraphShardGrainKey.Build(key));
                        break;
                    }
                }
            }
            return;
        }

        foreach (var t in meta.ForwardTypes)
        {
            foreach (var key in await KeysOf(meta, GraphShardDirection.Forward, t).ConfigureAwait(ContinueOnCapturedContext))
                candidates.Add(GraphShardGrainKey.Build(key));
        }
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
    public Task<long?> RunGc() => EnqueueRun(RunGcCore);

    /// <summary>The GC round <see cref="RunGc"/> queues as one write.</summary>
    private async Task<long?> RunGcCore()
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
        // keys: projected rows compact lazily (at their key's next projection), and every serve path
        // re-applies the floor.
        var newRevision = now > head ? now : head + 1;
        var ev = new LogEvent(
            newRevision, Array.Empty<RelationshipUpdateWire>(), SchemaChange: null,
            Array.Empty<CounterDeltaWire>(), GcFloor: floor);

        var raised = await RaiseConditionalEvent(new DatastoreJournalEntry.Committed(ev))
            .ConfigureAwait(ContinueOnCapturedContext);
        if (!raised)
            return null; // lost a race with a concurrent turn; the next reminder tick (or caller) retries.
        await ConfirmEvents().ConfigureAwait(ContinueOnCapturedContext);

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

        if (State.Value.Pending.Count >= FlushInterval)
            await TryFlushProjection().ConfigureAwait(ContinueOnCapturedContext);

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
    /// One of the [AlwaysInterleave] pure reads. The tail it serves is the pending tail of the
    /// confirmed state, so the served head and events come from one immutable state and the returned
    /// <see cref="LogSegment"/> is internally consistent (its head is &gt;= every event it serves) even
    /// if this call lands mid-write. Retention starts at the projection boundary (and the GC floor): an
    /// older cursor cannot be served COMPLETELY, so it is rejected rather than silently answered with a
    /// short tail — a shard re-bootstraps from its projection, a Watch consumer from a fresh snapshot.
    /// </remarks>
    public Task<LogSegment> ReadFrom(long afterRevision, int maxCount)
    {
        _metrics?.RecordReadFrom();
        var meta = State.Value;

        if (afterRevision < Math.Max(meta.ProjectedThrough, meta.GcFloor))
            throw new RevisionNotFoundException(new TimestampRevision(afterRevision));

        var events = meta.Pending
            .Where(e => e.Revision > afterRevision)
            .Take(maxCount < 0 ? int.MaxValue : maxCount)
            .ToList();

        return Task.FromResult(new LogSegment(events, meta.HeadRevision));
    }

    // --- projection ---

    /// <inheritdoc />
    public Task FlushProjection() => EnqueueRun(async () =>
    {
        await FlushProjectionCore().ConfigureAwait(ContinueOnCapturedContext);
        return true;
    });

    /// <summary>
    /// The flush a write runs when the pending tail reaches <see cref="FlushInterval"/>. A failure is
    /// logged and left for the next flush to retry — the events stay pending, which is always correct —
    /// because the write that triggered it has already committed and must not be reported as failed.
    /// </summary>
    private async Task TryFlushProjection()
    {
        try
        {
            await FlushProjectionCore().ConfigureAwait(ContinueOnCapturedContext);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "datastore grain: projecting {Count} pending events failed; they stay pending and the next flush retries",
                State.Value.Pending.Count);
        }
    }

    /// <summary>
    /// Projects the whole pending tail (see the class remarks): every touched shard grain persists its
    /// events, the populated keys are added to the shard index, and only then is the projection
    /// journaled. Runs as or inside a queued write, so no commit batch interleaves it.
    /// </summary>
    private async Task FlushProjectionCore()
    {
        var meta = State.Value;
        if (meta.Pending.IsEmpty)
            return;
        var through = meta.Pending[^1].Revision;

        var eventsByKey = new Dictionary<string, ImmutableList<LogEvent>.Builder>(StringComparer.Ordinal);
        foreach (var ev in meta.Pending)
        {
            foreach (var key in MetaFold.TouchedKeys(ev))
            {
                if (!eventsByKey.TryGetValue(key, out var events))
                    eventsByKey[key] = events = ImmutableList.CreateBuilder<LogEvent>();
                events.Add(ev);
            }
        }

        var projected = await InBatches(eventsByKey.ToList(), async pair =>
        {
            var populated = await GrainFactory.GetGrain<IGraphShardGrain>(pair.Key)
                .Project(pair.Value.ToImmutable(), through, meta.GcFloor)
                .ConfigureAwait(ContinueOnCapturedContext);
            return (Key: GraphShardGrainKey.Parse(pair.Key), Populated: populated);
        }).ConfigureAwait(ContinueOnCapturedContext);

        var buckets = projected
            .Where(p => p.Populated)
            .GroupBy(p => ShardIndexKey.For(p.Key), StringComparer.Ordinal)
            .ToList();
        await InBatches(buckets, async bucket =>
        {
            await GrainFactory.GetGrain<IShardIndexGrain>(bucket.Key)
                .Add(bucket.Select(p => p.Key.ObjectId).ToImmutableList())
                .ConfigureAwait(ContinueOnCapturedContext);
            return true;
        }).ConfigureAwait(ContinueOnCapturedContext);

        RaiseEvent(new DatastoreJournalEntry.Projected(through));
        await ConfirmEvents().ConfigureAwait(ContinueOnCapturedContext);
    }

    /// <summary>
    /// Runs <paramref name="call"/> over <paramref name="items"/> with at most
    /// <see cref="MaxConcurrentShardCalls"/> in flight, on the grain's own scheduler.
    /// </summary>
    private static async Task<TResult[]> InBatches<T, TResult>(IReadOnlyCollection<T> items, Func<T, Task<TResult>> call)
    {
        var results = new List<TResult>(items.Count);
        foreach (var batch in items.Chunk(MaxConcurrentShardCalls))
            results.AddRange(await Task.WhenAll(batch.Select(call)).ConfigureAwait(ContinueOnCapturedContext));
        return results.ToArray();
    }

    private static long NowNanos() => (DateTimeOffset.UtcNow - DateTimeOffset.UnixEpoch).Ticks * 100L;
}
