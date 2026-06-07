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
            var head = _current.HeadRevision;
            // Quantize the clock DOWN to a bucket so near-in-time checks share a revision, but never
            // below the latest committed revision: a snapshot at an older bucket would silently drop
            // already-committed writes. When the bucket floor falls before head's commit (the common
            // case immediately after a write), pin to head — which is still snapshot-able and reflects
            // all committed data. Cache bucketing across the mesh is handled separately by the
            // IRevisionQuantizer over this revision, so correctness here costs no cache locality.
            var quantized = _quantizationNanos > 0 ? head - (head % _quantizationNanos) : head;
            if (quantized < head || !IsRevisionValid(quantized))
                quantized = head;
            return Task.FromResult(new RevisionWithSchemaHash(new TimestampRevision(quantized), _current.SchemaHashAt(quantized)));
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

    public Task<string> GetUniqueId(CancellationToken cancellationToken = default) => Task.FromResult(_uniqueId);

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
