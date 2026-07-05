using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// A per-silo notifier that lets every <see cref="GrainBackedDatastore.Watch"/> stream on the silo learn when
/// the datastore head advances WITHOUT each stream polling the grain on its own timer. The primary signal is
/// PUSH: the hub registers itself as an <see cref="IDatastoreWatcher"/> grain observer, so a commit on any
/// silo notifies it directly; in addition the local write path pulses it on commit (zero-hop same-silo
/// latency). Because observer delivery is best-effort (non-durable references, dropped on grain
/// reactivation), ONE slow background loop per silo calls <see cref="IDatastoreGrain.SubscribeWatch"/> as a
/// combined heartbeat: it refreshes the registration, resubscribes after grain reactivation, and pulses the
/// returned head — so a missed push costs at most one heartbeat of latency, never a lost event. Streams await
/// the signal and then pull their own diffs from the log (<see cref="IDatastoreLog.ReadFrom"/>) from their own
/// cursor — so the per-stream cost is one log-tail read per change, never a full-state fetch and never a
/// private timer.
/// </summary>
internal sealed class LogWatchHub : IDatastoreWatcher, IAsyncDisposable
{
    /// <summary>
    /// Default heartbeat cadence: a liveness backstop for missed pushes and the observer-registration
    /// refresh (the grain expires registrations not refreshed within several heartbeats).
    /// </summary>
    private static readonly TimeSpan DefaultHeartbeatInterval = TimeSpan.FromSeconds(1);

    private readonly IDatastoreGrain _grain;
    private readonly IGrainFactory _grainFactory;
    private readonly TimeSpan _heartbeatInterval;
    private readonly CancellationTokenSource _cts = new();
    private readonly object _lock = new();

    private TaskCompletionSource _signal = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private long _observedHead;
    private Task? _loop;
    private IDatastoreWatcher? _selfRef;

    public LogWatchHub(IDatastoreGrain grain, IGrainFactory grainFactory, TimeSpan? heartbeatInterval = null)
    {
        _grain = grain;
        _grainFactory = grainFactory;
        _heartbeatInterval = heartbeatInterval ?? DefaultHeartbeatInterval;
    }

    /// <summary>Push delivery from the datastore grain: a commit advanced the head.</summary>
    public Task HeadAdvanced(long head)
    {
        Pulse(head);
        return Task.CompletedTask;
    }

    /// <summary>Registers the observer reference and starts the heartbeat loop on first use (idempotent).</summary>
    public void EnsureStarted()
    {
        if (_loop is not null)
            return;
        lock (_lock)
        {
            _selfRef ??= _grainFactory.CreateObjectReference<IDatastoreWatcher>(this);
            _loop ??= Task.Run(() => HeartbeatLoop(_selfRef, _cts.Token));
        }
    }

    /// <summary>
    /// Records that the head advanced to <paramref name="head"/> and wakes every waiter. Called by the local
    /// write path on commit (instant same-silo latency), by the observer push (cross-silo commits), and by
    /// the heartbeat (missed-push backstop). Monotonic, so racing sources are harmless.
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

    private async Task HeartbeatLoop(IDatastoreWatcher selfRef, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                // One hop doing three jobs: refresh the observer registration (so it never expires while
                // this hub lives), resubscribe after a grain reactivation dropped it, and read the head as
                // the missed-push backstop. WaitAsync(ct) makes cancellation unblock the await IMMEDIATELY
                // even if the grain call is in-flight (e.g. the silo is shutting down) — so disposal never
                // waits on a hung hop.
                Pulse((await _grain.SubscribeWatch(selfRef).WaitAsync(ct).ConfigureAwait(false)).Head);
                await Task.Delay(_heartbeatInterval, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return; // Normal shutdown.
            }
            catch
            {
                // The grain may be momentarily unavailable (membership change, deactivation). Back off and
                // retry; a transient failure must not tear down every Watch stream on the silo.
                try { await Task.Delay(_heartbeatInterval, ct).ConfigureAwait(false); }
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

        // Best-effort deregistration (expiry would drop it anyway) + release the client-side reference.
        if (_selfRef is { } selfRef)
        {
            try
            {
                await _grain.UnsubscribeWatch(selfRef).WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(false);
            }
            catch
            {
                // The registration expires on its own; never let teardown fail on it.
            }
            _grainFactory.DeleteObjectReference<IDatastoreWatcher>(selfRef);
        }

        // Release any stragglers so their WaitAsync observes cancellation via their own token.
        lock (_lock)
            _signal.TrySetResult();
        _cts.Dispose();
    }
}
