using System.Threading;

namespace Spiceport.Grains;

/// <summary>
/// Silo-singleton sequencer counters (the <see cref="IDispatchMetrics"/> idiom: plain interface +
/// atomic-counter implementation + summable snapshot struct; the <c>System.Diagnostics.Metrics</c> route
/// was assessed and rejected for this repo's metrics seams): inbound <see cref="DatastoreGrain"/> calls
/// decomposed by method, plus the Commit-only duration and candidate-key-count observables the
/// scalability program's Phase 0 needs (docs/scalability-program.md — the direct observables for the
/// P1/P2/P5/P6 pressure points). Registered on every silo; only the silo hosting the single sequencer
/// activation ever records, so summing every silo's snapshot yields the cluster-wide totals (the same
/// aggregation contract as <see cref="DispatchMetricsSnapshot"/>).
/// </summary>
public interface ISequencerMetrics
{
    /// <summary>
    /// One inbound <see cref="DatastoreGrain.Commit"/> call completed (accepted, rejected, or thrown —
    /// recorded in a finally so every inbound call counts exactly once), carrying the call's own
    /// wall-clock duration as measured by the grain's Stopwatch. Feeds the Commit count and the
    /// total/max duration observables.
    /// </summary>
    /// <param name="elapsedMicroseconds">The whole Commit turn's duration in microseconds.</param>
    void RecordCommit(long elapsedMicroseconds);

    /// <summary>
    /// The candidate-key set of one Commit resolved to <paramref name="candidateKeyCount"/> shard keys
    /// (the per-commit candidate I/O breadth — pressure point P1). Recorded at the point candidate
    /// resolution completes, so commits rejected BEFORE resolution (head/schema-hash CAS failures)
    /// count in <see cref="RecordCommit"/> but not in any bucket. Buckets: at most 1, 2-8, 9-64, 65+.
    /// </summary>
    void RecordCommitCandidates(int candidateKeyCount);

    /// <summary>One inbound <see cref="DatastoreGrain.ReadFrom"/> (log-tail) call — the P2 fan-in observable.</summary>
    void RecordReadFrom();

    /// <summary>One inbound <see cref="DatastoreGrain.ReadShard"/> (cold-shard hydration) call.</summary>
    void RecordReadShard();

    /// <summary>One inbound <see cref="DatastoreGrain.GetHead"/> call.</summary>
    void RecordGetHead();

    /// <summary>One inbound <see cref="DatastoreGrain.ReadState"/> (admin-plane whole-state assembly) call — the P6 observable.</summary>
    void RecordReadState();

    /// <summary>One inbound <see cref="DatastoreGrain.ReadSchemaAt"/> call.</summary>
    void RecordReadSchemaAt();

    /// <summary>
    /// One flush boundary written (the every-<c>FlushInterval</c> dirty-buffer flush + meta write inside
    /// <see cref="DatastoreGrain"/>'s storage apply) — the P5 sawtooth observable, recorded at the
    /// flush-write site once the boundary's meta row is durable.
    /// </summary>
    void RecordFlush();

    /// <summary>An immutable point-in-time snapshot of the counters.</summary>
    SequencerMetricsSnapshot Snapshot();

    /// <summary>Resets all counters to zero (to bracket one benchmark workload).</summary>
    void Reset();
}

/// <summary>An immutable snapshot of <see cref="ISequencerMetrics"/> counters.</summary>
/// <param name="Commit">Inbound <c>Commit</c> calls (accepted, rejected, or thrown).</param>
/// <param name="ReadFrom">Inbound <c>ReadFrom</c> log-tail calls.</param>
/// <param name="ReadShard">Inbound <c>ReadShard</c> cold-hydration calls.</param>
/// <param name="GetHead">Inbound <c>GetHead</c> calls.</param>
/// <param name="ReadState">Inbound <c>ReadState</c> admin-plane assembly calls.</param>
/// <param name="ReadSchemaAt">Inbound <c>ReadSchemaAt</c> calls.</param>
/// <param name="CommitMicrosTotal">Sum of every Commit call's duration, in microseconds.</param>
/// <param name="CommitMicrosMax">
/// The single longest Commit call's duration in microseconds. Combined via max (not sum) by the
/// aggregation operator, so the cluster-wide value is still "the longest commit anywhere".
/// </param>
/// <param name="CommitCandidates1">Commits whose candidate-key set resolved to at most one key.</param>
/// <param name="CommitCandidates2To8">Commits whose candidate-key set resolved to 2-8 keys.</param>
/// <param name="CommitCandidates9To64">Commits whose candidate-key set resolved to 9-64 keys.</param>
/// <param name="CommitCandidates65Plus">Commits whose candidate-key set resolved to 65 or more keys.</param>
/// <param name="Flush">Flush boundaries written (dirty-buffer flush + meta row durable).</param>
public readonly record struct SequencerMetricsSnapshot(
    long Commit,
    long ReadFrom = 0,
    long ReadShard = 0,
    long GetHead = 0,
    long ReadState = 0,
    long ReadSchemaAt = 0,
    long CommitMicrosTotal = 0,
    long CommitMicrosMax = 0,
    long CommitCandidates1 = 0,
    long CommitCandidates2To8 = 0,
    long CommitCandidates9To64 = 0,
    long CommitCandidates65Plus = 0,
    long Flush = 0)
{
    /// <summary>
    /// Aggregation across silos: component-wise sum, except <see cref="CommitMicrosMax"/> which combines
    /// via max (a maximum is not additive).
    /// </summary>
    public static SequencerMetricsSnapshot operator +(SequencerMetricsSnapshot a, SequencerMetricsSnapshot b) =>
        new(
            a.Commit + b.Commit,
            a.ReadFrom + b.ReadFrom,
            a.ReadShard + b.ReadShard,
            a.GetHead + b.GetHead,
            a.ReadState + b.ReadState,
            a.ReadSchemaAt + b.ReadSchemaAt,
            a.CommitMicrosTotal + b.CommitMicrosTotal,
            Math.Max(a.CommitMicrosMax, b.CommitMicrosMax),
            a.CommitCandidates1 + b.CommitCandidates1,
            a.CommitCandidates2To8 + b.CommitCandidates2To8,
            a.CommitCandidates9To64 + b.CommitCandidates9To64,
            a.CommitCandidates65Plus + b.CommitCandidates65Plus,
            a.Flush + b.Flush);
}

/// <summary>Thread-safe atomic-counter <see cref="ISequencerMetrics"/> (the <see cref="DispatchMetrics"/> mechanism).</summary>
public sealed class SequencerMetrics : ISequencerMetrics
{
    private long _commit;
    private long _readFrom;
    private long _readShard;
    private long _getHead;
    private long _readState;
    private long _readSchemaAt;
    private long _commitMicrosTotal;
    private long _commitMicrosMax;
    private long _commitCandidates1;
    private long _commitCandidates2To8;
    private long _commitCandidates9To64;
    private long _commitCandidates65Plus;
    private long _flush;

    /// <inheritdoc />
    public void RecordCommit(long elapsedMicroseconds)
    {
        Interlocked.Increment(ref _commit);
        Interlocked.Add(ref _commitMicrosTotal, elapsedMicroseconds);
        // Lock-free running max: retry only while our observation is still the larger one.
        long current;
        while (elapsedMicroseconds > (current = Interlocked.Read(ref _commitMicrosMax)))
        {
            if (Interlocked.CompareExchange(ref _commitMicrosMax, elapsedMicroseconds, current) == current)
                break;
        }
    }

    /// <inheritdoc />
    public void RecordCommitCandidates(int candidateKeyCount)
    {
        if (candidateKeyCount <= 1)
            Interlocked.Increment(ref _commitCandidates1);
        else if (candidateKeyCount <= 8)
            Interlocked.Increment(ref _commitCandidates2To8);
        else if (candidateKeyCount <= 64)
            Interlocked.Increment(ref _commitCandidates9To64);
        else
            Interlocked.Increment(ref _commitCandidates65Plus);
    }

    /// <inheritdoc />
    public void RecordReadFrom() => Interlocked.Increment(ref _readFrom);

    /// <inheritdoc />
    public void RecordReadShard() => Interlocked.Increment(ref _readShard);

    /// <inheritdoc />
    public void RecordGetHead() => Interlocked.Increment(ref _getHead);

    /// <inheritdoc />
    public void RecordReadState() => Interlocked.Increment(ref _readState);

    /// <inheritdoc />
    public void RecordReadSchemaAt() => Interlocked.Increment(ref _readSchemaAt);

    /// <inheritdoc />
    public void RecordFlush() => Interlocked.Increment(ref _flush);

    /// <inheritdoc />
    public SequencerMetricsSnapshot Snapshot() => new(
        Interlocked.Read(ref _commit),
        Interlocked.Read(ref _readFrom),
        Interlocked.Read(ref _readShard),
        Interlocked.Read(ref _getHead),
        Interlocked.Read(ref _readState),
        Interlocked.Read(ref _readSchemaAt),
        Interlocked.Read(ref _commitMicrosTotal),
        Interlocked.Read(ref _commitMicrosMax),
        Interlocked.Read(ref _commitCandidates1),
        Interlocked.Read(ref _commitCandidates2To8),
        Interlocked.Read(ref _commitCandidates9To64),
        Interlocked.Read(ref _commitCandidates65Plus),
        Interlocked.Read(ref _flush));

    /// <inheritdoc />
    public void Reset()
    {
        Interlocked.Exchange(ref _commit, 0);
        Interlocked.Exchange(ref _readFrom, 0);
        Interlocked.Exchange(ref _readShard, 0);
        Interlocked.Exchange(ref _getHead, 0);
        Interlocked.Exchange(ref _readState, 0);
        Interlocked.Exchange(ref _readSchemaAt, 0);
        Interlocked.Exchange(ref _commitMicrosTotal, 0);
        Interlocked.Exchange(ref _commitMicrosMax, 0);
        Interlocked.Exchange(ref _commitCandidates1, 0);
        Interlocked.Exchange(ref _commitCandidates2To8, 0);
        Interlocked.Exchange(ref _commitCandidates9To64, 0);
        Interlocked.Exchange(ref _commitCandidates65Plus, 0);
        Interlocked.Exchange(ref _flush, 0);
    }
}
