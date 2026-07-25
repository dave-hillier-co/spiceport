using System.Runtime.CompilerServices;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// The storage-direct scan seam of the graph-sharded design (<c>docs/graph-sharded-datastore.md</c> §3):
/// broad/admin reads — ReadRelationships with loose filters, bulk export, counter evaluation, the
/// schema-change data guards — fetch the sequencer snapshot per scan instead of keeping a per-silo
/// whole-graph replica. Scans are the workload actors are worst at, so this seam routes them AROUND the
/// shard-grain mesh; it is deliberately OFF the per-Check hot path, which reads through
/// <see cref="IGraphReaderSource"/>.
/// </summary>
public interface ISnapshotScanner
{
    /// <summary>Streams the relationships matching the filter, visible at the pinned revision.</summary>
    /// <remarks>
    /// The scan seam carries exactly the shapes production needs: forward filter scans and the counter
    /// reads below. There is deliberately no reverse-shaped scan — reverse reads are the shard mesh's
    /// workload (<see cref="IGraphReaderSource"/>), and the one reverse-shaped admin check became a
    /// forward-filter precondition.
    /// </remarks>
    IAsyncEnumerable<Relationship> Scan(RelationshipsFilter filter, IRevision revision, CancellationToken ct);

    /// <summary>
    /// Counts the relationships matching the named registered counter's filter at the pinned revision.
    /// Throws <see cref="CounterNotRegisteredException"/> when the counter is not registered there.
    /// </summary>
    Task<ulong> CountRelationships(string counterName, IRevision revision, CancellationToken ct);

    /// <summary>
    /// Returns the filter registered for the named counter at the pinned revision, or null when the
    /// counter is not registered there.
    /// </summary>
    Task<RelationshipsFilter?> ReadCounterFilter(string name, IRevision revision, CancellationToken ct);

    /// <summary>Streams the counters registered (not tombstoned) at the pinned revision.</summary>
    IAsyncEnumerable<RegisteredCounter> LookupCounters(IRevision revision, CancellationToken ct);
}

/// <summary>
/// The grain-backed scanner: per CALL, one <see cref="IDatastoreGrain.ReadState"/> snapshot fetch from
/// the cluster-singleton sequencer, folded to memory form and served through the same
/// <see cref="MvccSnapshotReader"/> the reference model uses — so scan semantics (MVCC visibility,
/// expiration, counter folds) cannot drift from the reference reader semantics.
/// State at head always covers any resolvable pinned revision (the grain minted it at or below that
/// head); a revision below the GC floor is rejected by <see cref="MvccSnapshotReader"/>'s own
/// constructor guard (<see cref="RevisionNotFoundException"/>).
/// </summary>
internal sealed class GrainSnapshotScanner(IGrainFactory grainFactory) : ISnapshotScanner
{
    private IDatastoreGrain Grain => grainFactory.GetGrain<IDatastoreGrain>(IDatastoreGrain.Key);

    private async Task<MvccSnapshotReader> ReaderAt(IRevision revision, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(revision);
        ct.ThrowIfCancellationRequested();

        // Closed-timestamp gate: a stale duplicate activation during membership churn can serve an old
        // head, and a scan pinned at R must never fold over state whose head < R (rows committed at or
        // below R could be silently missing). SequencerStateFetch refetches until the head covers the
        // pin — the successor of the retired projection's watermark wait.
        var pinned = ToNanos(revision);
        var state = await SequencerStateFetch.StateCovering(Grain, pinned, ct).ConfigureAwait(false);
        // A permissive isValid is sound here for the same reason as GrainBackedDatastore's readers: the
        // hard floor check lives in MvccSnapshotReader's constructor, not the validity delegate.
        return new MvccSnapshotReader(DatastoreStateConverters.ToMemory(state), pinned, _ => true);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<Relationship> Scan(
        RelationshipsFilter filter, IRevision revision, [EnumeratorCancellation] CancellationToken ct)
    {
        var reader = await ReaderAt(revision, ct).ConfigureAwait(false);
        await foreach (var rel in reader.QueryRelationships(filter, ct).ConfigureAwait(false))
            yield return rel;
    }

    /// <inheritdoc />
    public async Task<ulong> CountRelationships(string counterName, IRevision revision, CancellationToken ct)
    {
        var reader = await ReaderAt(revision, ct).ConfigureAwait(false);
        return await reader.CountRelationships(counterName, ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<RelationshipsFilter?> ReadCounterFilter(string name, IRevision revision, CancellationToken ct)
    {
        var reader = await ReaderAt(revision, ct).ConfigureAwait(false);
        return await reader.ReadCounterFilter(name, ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RegisteredCounter> LookupCounters(
        IRevision revision, [EnumeratorCancellation] CancellationToken ct)
    {
        var reader = await ReaderAt(revision, ct).ConfigureAwait(false);
        await foreach (var counter in reader.LookupCounters(ct).ConfigureAwait(false))
            yield return counter;
    }

    private static long ToNanos(IRevision revision) => revision switch
    {
        TimestampRevision t => t.TimestampNanosSinceEpoch,
        _ => throw new InvalidRevisionException($"unsupported revision type: {revision.GetType().Name}"),
    };
}
