using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using Spiceport.Core;

namespace Spiceport.Datastore.Memory;

/// <summary>
/// An in-memory MVCC datastore. Revisions are monotonically increasing nanosecond timestamps.
/// Each committed transaction produces a new immutable <see cref="DatastoreState"/>; snapshot
/// readers capture a state reference and read correctly regardless of subsequent writes.
/// Writes are serialized by a single write lock.
/// </summary>
public sealed class InMemoryDatastore : IDatastore
{
    private readonly object _writeLock = new();
    private readonly string _uniqueId = Guid.NewGuid().ToString("n");
    private readonly long _quantizationNanos;
    private readonly long _gcWindowNanos;

    private long _lastRevision;
    private DatastoreState _current;

    // Cached optimized-revision candidate (SpiceDB's CachedOptimizedRevisions): a real head snapshot held
    // stable for the quantization window so near-in-time minimize-latency checks share a single revision —
    // and therefore a single cache key. Guarded by _writeLock.
    private RevisionWithSchemaHash? _optimizedCache;
    private long _optimizedValidThroughNanos;

    // Ordered list of revisions that have been committed by a write transaction (the changefeed). The
    // datastore's initial empty revision is not a write and is not listed. Used by Watch to enumerate
    // which revisions carry changes after a cursor. Bounded by the GC window in lockstep with reads.
    private ImmutableList<long> _committedRevisions = ImmutableList<long>.Empty;

    // A signal completed whenever a new revision commits, so live Watch tailers wake without polling.
    // Swapped under _writeLock on every commit; waiters capture the current instance before re-checking.
    private TaskCompletionSource _commitSignal =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Creates an in-memory datastore.</summary>
    /// <param name="quantization">Quantization window for <see cref="OptimizedRevision"/> (default 5s).</param>
    /// <param name="gcWindow">How long old revisions remain valid (default 24h). Snapshots before this window are rejected.</param>
    public InMemoryDatastore(TimeSpan? quantization = null, TimeSpan? gcWindow = null)
    {
        _quantizationNanos = (long)((quantization ?? TimeSpan.FromSeconds(5)).TotalMilliseconds) * 1_000_000L;
        _gcWindowNanos = (long)((gcWindow ?? TimeSpan.FromHours(24)).TotalMilliseconds) * 1_000_000L;
        _lastRevision = NowNanos();
        _current = DatastoreState.Empty(_lastRevision);
    }

    public IDatastoreReader SnapshotReader(IRevision revision)
    {
        var rev = ToNanos(revision);
        DatastoreState state;
        lock (_writeLock)
        {
            if (!IsRevisionValid(rev))
                throw new RevisionNotFoundException(revision);
            state = _current;
        }
        return new InMemoryDatastoreReader(state, rev, IsRevisionValidSnapshot);
    }

    public Task<RevisionWithSchemaHash> HeadRevision(CancellationToken cancellationToken = default)
    {
        lock (_writeLock)
        {
            var head = _current.HeadRevision;
            return Task.FromResult(new RevisionWithSchemaHash(new TimestampRevision(head), _current.SchemaHashAt(head)));
        }
    }

    public Task<RevisionWithSchemaHash> OptimizedRevision(CancellationToken cancellationToken = default)
    {
        lock (_writeLock)
        {
            // SpiceDB's optimized revision (revisions/optimized.go CachedOptimizedRevisions) is a CACHED
            // candidate: the real HEAD revision sampled when a window opens, then returned UNCHANGED for
            // the whole quantization window. It is a genuine committed snapshot (never floored BELOW head,
            // so minimize-latency never silently drops already-committed writes) AND it is stable for the
            // window so near-in-time checks share it. Crucially this exact value is used as BOTH the read
            // snapshot AND the cache key's AtRevision (the key is NOT separately time-floored): all
            // requests in one window read at, and key by, the SAME cached head, so a write that advances
            // head mid-window cannot be served an older-snapshot result under a colliding bucket.
            var now = NowNanos();
            if (_quantizationNanos <= 0 ||
                _optimizedCache is not { } cached ||
                now >= _optimizedValidThroughNanos)
            {
                var head = _current.HeadRevision;
                cached = new RevisionWithSchemaHash(new TimestampRevision(head), _current.SchemaHashAt(head));
                if (_quantizationNanos > 0)
                {
                    _optimizedCache = cached;
                    // Hold this candidate until the end of the current bucket window so the cached value
                    // aligns to a stable boundary that is shared across the mesh.
                    _optimizedValidThroughNanos = now - (now % _quantizationNanos) + _quantizationNanos;
                }
            }
            return Task.FromResult(cached);
        }
    }

    public async Task<IRevision> ReadWriteTx(
        Func<IReadWriteTransaction, Task> transaction,
        CancellationToken cancellationToken = default)
    {
        DatastoreState baseState;
        long newRevision;
        lock (_writeLock)
        {
            baseState = _current;
            newRevision = NextRevision();
        }

        var tx = new InMemoryReadWriteTransaction(baseState, newRevision);
        await transaction(tx).ConfigureAwait(false);

        lock (_writeLock)
        {
            if (!ReferenceEquals(_current, baseState))
                throw new SerializationException();
            _current = tx.Commit();
            _committedRevisions = _committedRevisions.Add(newRevision);
            // Wake any live Watch tailers, then arm a fresh signal for the next commit.
            var signal = _commitSignal;
            _commitSignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            signal.TrySetResult();
        }

        return new TimestampRevision(newRevision);
    }

    public Task<bool> CheckRevision(IRevision revision, CancellationToken cancellationToken = default)
    {
        lock (_writeLock)
        {
            return Task.FromResult(IsRevisionValid(ToNanos(revision)));
        }
    }

    public async IAsyncEnumerable<RevisionChange> Watch(
        IRevision afterRevision,
        WatchOptions options,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(afterRevision);
        ArgumentNullException.ThrowIfNull(options);

        var cursor = ToNanos(afterRevision);
        lock (_writeLock)
        {
            // Cannot resume from a revision older than the retained GC window.
            if (!IsRevisionValid(cursor))
                throw new RevisionNotFoundException(afterRevision);
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            // Snapshot, under the write lock, the state to read changes from, the revisions committed
            // after the cursor, and the signal to await if there is nothing new. Doing all three under
            // one lock makes the "check then wait" race-free: any commit after this point either is in
            // our list, or will complete the signal we captured.
            DatastoreState state;
            List<long> pending;
            TaskCompletionSource signal;
            lock (_writeLock)
            {
                state = _current;
                signal = _commitSignal;
                pending = new List<long>();
                foreach (var rev in _committedRevisions)
                {
                    if (rev > cursor)
                        pending.Add(rev);
                }
            }

            // pending is already in ascending order (revisions are appended monotonically).
            foreach (var rev in pending)
            {
                var change = BuildChange(state, rev, options);
                if (change is not null)
                    yield return change;
                cursor = rev;
            }

            if (pending.Count == 0)
            {
                // Nothing new: wait for the next commit or cancellation.
                var cancelTask = Task.Delay(Timeout.Infinite, cancellationToken);
                var completed = await Task.WhenAny(signal.Task, cancelTask).ConfigureAwait(false);
                if (completed == cancelTask)
                    yield break;
            }
        }
    }

    private static RevisionChange? BuildChange(DatastoreState state, long revision, WatchOptions options)
    {
        var rev = new TimestampRevision(revision);
        var includeRels = (options.Content & WatchContent.Relationships) != 0;
        var includeSchema = (options.Content & WatchContent.Schema) != 0;

        var relChanges = includeRels
            ? state.ChangesAt(revision)
            : Array.Empty<RelationshipUpdate>();
        var schemaChanged = includeSchema && state.SchemaChangedAt(revision);

        // Suppress empty checkpoints: only emit a revision that actually carries requested content.
        if (relChanges.Count == 0 && !schemaChanged)
            return null;

        return new RevisionChange(rev, relChanges, schemaChanged);
    }

    public Task<string> GetUniqueId(CancellationToken cancellationToken = default) => Task.FromResult(_uniqueId);

    public Task<IRevisionParser> GetRevisionParser(CancellationToken cancellationToken = default) =>
        Task.FromResult<IRevisionParser>(new InMemoryRevisionParser(_uniqueId));

    public Task Close() => Task.CompletedTask;

    // --- internals ---

    private long NextRevision()
    {
        var now = NowNanos();
        var next = now > _lastRevision ? now : _lastRevision + 1;
        _lastRevision = next;
        return next;
    }

    private bool IsRevisionValid(long rev)
    {
        if (rev > _current.HeadRevision)
            return false;
        return rev >= _current.HeadRevision - _gcWindowNanos;
    }

    private bool IsRevisionValidSnapshot(long rev)
    {
        lock (_writeLock)
        {
            return IsRevisionValid(rev);
        }
    }

    private static long ToNanos(IRevision revision) => revision switch
    {
        TimestampRevision t => t.TimestampNanosSinceEpoch,
        _ => throw new InvalidRevisionException($"unsupported revision type: {revision.GetType().Name}"),
    };

    private static long NowNanos() => (DateTimeOffset.UtcNow - DateTimeOffset.UnixEpoch).Ticks * 100L;
}
