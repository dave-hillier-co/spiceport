using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// A per-silo notifier that lets every <see cref="GrainBackedDatastore.Watch"/> stream on the silo learn when
/// the datastore head advances WITHOUT each stream polling the grain on its own timer. ONE background loop per
/// silo samples the head (a cheap <see cref="IDatastoreGrain.GetHead"/>, not a whole-state fetch) and pulses a
/// shared async signal; in addition the local write path pulses it directly on commit, so a write committed on
/// this silo is observed by a local watcher immediately (the poll only covers writes committed via other
/// silos). Streams await the signal and then pull their own diffs from the log
/// (<see cref="IDatastoreLog.ReadFrom"/>) from their own cursor — so the per-stream cost is one log-tail read
/// per change, never a full-state fetch and never a private timer.
/// </summary>
internal sealed class LogWatchHub : IAsyncDisposable
{
    /// <summary>Head-sampling cadence (covers cross-silo writes; same-silo writes pulse directly on commit).</summary>
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(25);

    private readonly IDatastoreGrain _grain;
    private readonly CancellationTokenSource _cts = new();
    private readonly object _lock = new();

    private TaskCompletionSource _signal = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private long _observedHead;
    private Task? _loop;

    public LogWatchHub(IDatastoreGrain grain) => _grain = grain;

    /// <summary>Starts the per-silo head-sampling loop on first use (idempotent).</summary>
    public void EnsureStarted()
    {
        if (_loop is not null)
            return;
        lock (_lock)
        {
            _loop ??= Task.Run(() => PollLoop(_cts.Token));
        }
    }

    /// <summary>
    /// Records that the head advanced to <paramref name="head"/> and wakes every waiter. Called by the local
    /// write path on commit (instant same-silo latency) and by the poll loop (cross-silo writes).
    /// </summary>
    public void Pulse(long head)
    {
        lock (_lock)
        {
            if (head <= _observedHead)
                return;
            _observedHead = head;
            var prior = _signal;
            _signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            prior.TrySetResult();
        }
    }

    /// <summary>
    /// Completes once the observed head is known to be strictly greater than <paramref name="cursor"/> (a new
    /// commit may be visible past the cursor). Spurious wake-ups are harmless: the caller re-reads the log and
    /// simply finds nothing past its cursor, then waits again.
    /// </summary>
    public async Task WaitForChangeAfter(long cursor, CancellationToken cancellationToken)
    {
        while (true)
        {
            Task signal;
            lock (_lock)
            {
                if (_observedHead > cursor)
                    return;
                signal = _signal.Task;
            }
            await signal.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task PollLoop(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                // WaitAsync(ct) makes cancellation unblock the await IMMEDIATELY even if the grain call is
                // in-flight (e.g. the silo is shutting down) — so disposal never waits on a hung hop.
                Pulse((await _grain.GetHead().WaitAsync(ct).ConfigureAwait(false)).Head);
                await Task.Delay(PollInterval, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return; // Normal shutdown.
            }
            catch
            {
                // The grain may be momentarily unavailable (membership change, deactivation). Back off and
                // retry; a transient failure must not tear down every Watch stream on the silo.
                try { await Task.Delay(PollInterval, ct).ConfigureAwait(false); }
                catch (OperationCanceledException) { return; }
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync().ConfigureAwait(false);
        if (_loop is { } loop)
        {
            // The loop observes cancellation between/within hops and exits promptly; bound the wait anyway so
            // a pathological in-flight hop can never deadlock silo teardown.
            await Task.WhenAny(loop, Task.Delay(TimeSpan.FromSeconds(5))).ConfigureAwait(false);
        }
        // Release any stragglers so their WaitAsync observes cancellation via their own token.
        lock (_lock)
            _signal.TrySetResult();
        _cts.Dispose();
    }
}
