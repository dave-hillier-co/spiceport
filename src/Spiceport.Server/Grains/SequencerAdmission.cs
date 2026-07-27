using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// The per-silo sequencer write admission gate (issue #36): bounds how many commits this silo may have
/// in flight to the cluster-singleton sequencer grain at once, so offered write load beyond the
/// sequencer's capacity degrades as deliberate load shedding (<see cref="SequencerOverloadedException"/>
/// -> gRPC <c>RESOURCE_EXHAUSTED</c>, a retryable signal) instead of an unbounded activation queue whose
/// requests die as opaque Orleans response timeouts. A DI singleton — one gate per silo container, the
/// same scope as the stateless-worker relationships grain activations that submit production commits —
/// so the sequencer's total queue is bounded by (silo count) x <see
/// cref="SequencerAdmissionOptions.MaxInFlightCommits"/>. Admission is non-blocking: a commit either
/// takes a slot immediately or is shed; there is no waiting tier, because waiting is exactly the latency
/// ramp the gate exists to cut off (the admitted in-flight commits already queue, bounded, at the
/// sequencer itself).
/// </summary>
/// <remarks>
/// Only the production declarative write path (<see cref="RelationshipsGrain"/>) enters the gate. The
/// compatibility lambda path (<c>GrainBackedDatastore.ReadWriteTx</c> — tests, SeedData) bypasses it,
/// matching its documented standing as a non-scaling-concern path. Shed commits are counted via
/// <see cref="ISequencerMetrics.RecordCommitShed"/> on the SUBMITTING silo (unlike the sequencer-side
/// counters, which only the sequencer's silo records); the snapshot sum is still the cluster total.
/// </remarks>
public sealed class SequencerAdmission(SequencerAdmissionOptions options, ISequencerMetrics metrics)
{
    private readonly int _limit = options.MaxInFlightCommits;
    private readonly SemaphoreSlim? _slots = options.MaxInFlightCommits > 0
        ? new SemaphoreSlim(options.MaxInFlightCommits, options.MaxInFlightCommits)
        : null;

    /// <summary>
    /// Takes an in-flight commit slot, or sheds the commit (<see cref="SequencerOverloadedException"/>)
    /// when all slots are taken. Dispose the returned slot when the sequencer call completes (success,
    /// rejection, or throw). With the gate disabled (non-positive limit) every entry succeeds.
    /// </summary>
    public IDisposable Enter()
    {
        if (_slots is null)
            return NoopSlot.Instance;

        if (_slots.Wait(0))
            return new Slot(_slots);

        metrics.RecordCommitShed();
        throw new SequencerOverloadedException(
            $"the sequencer write queue is full on this silo ({_limit} commits in flight); " +
            "the write was shed to keep overload retryable — back off and retry");
    }

    private sealed class Slot(SemaphoreSlim slots) : IDisposable
    {
        private int _released;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _released, 1) == 0)
                slots.Release();
        }
    }

    private sealed class NoopSlot : IDisposable
    {
        public static readonly NoopSlot Instance = new();

        public void Dispose()
        {
        }
    }
}
