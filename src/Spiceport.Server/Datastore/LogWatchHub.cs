using System.Text;
using Microsoft.Extensions.Logging;
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
///
/// The SAME channel also propagates schema changes to this silo's live schema provider (constructor's
/// <c>applySchema</c> callback): a <c>WriteSchema</c> commit anywhere in the cluster pushes
/// <see cref="IDatastoreWatcher.SchemaAdvanced"/> to every registered hub, and the heartbeat's
/// <see cref="IDatastoreGrain.SubscribeWatch"/> reply carries the CURRENT stored schema hash
/// (<see cref="DatastoreHeadWire.SchemaHash"/>) as the same missed-push backstop — a hash
/// mismatch against the last one applied triggers one <see cref="IDatastoreGrain.ReadSchemaAt"/> fetch to
/// repair it. Without this, a silo that never ran the WriteSchema RPC itself would keep serving its stale
/// <c>ISchemaProvider.Current</c> forever (a live wrong-verdict divergence, not just added latency).
/// </summary>
public sealed class LogWatchHub : IDatastoreWatcher, IAsyncDisposable
{
    /// <summary>
    /// Default heartbeat cadence: a liveness backstop for missed pushes and the observer-registration
    /// refresh (the grain expires registrations not refreshed within several heartbeats).
    /// </summary>
    private static readonly TimeSpan DefaultHeartbeatInterval = TimeSpan.FromSeconds(1);

    private readonly IDatastoreGrain _grain;
    private readonly IGrainFactory _grainFactory;
    private readonly TimeSpan _heartbeatInterval;
    private readonly Action<string>? _applySchema;
    private readonly ILogger? _logger;
    private readonly CancellationTokenSource _cts = new();
    private readonly object _lock = new();

    private TaskCompletionSource _signal = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private long _observedHead;
    private string? _lastAppliedSchemaHash;
    private Task? _loop;
    private IDatastoreWatcher? _selfRef;

    /// <param name="grain">The cluster-singleton sequencer grain this hub subscribes to.</param>
    /// <param name="grainFactory">Used to mint this hub's own <see cref="IDatastoreWatcher"/> object reference.</param>
    /// <param name="heartbeatInterval">Overrides the default heartbeat cadence (tests only).</param>
    /// <param name="applySchema">
    /// Applies a pushed/heartbeat-repaired schema text to this silo's live <see cref="ISchemaProvider"/>
    /// (wired in <c>ServiceCollectionExtensions</c> to <c>ISchemaProvider.Update</c>). Null in contexts
    /// that never propagate schema changes (e.g. a hub built only to observe head advances). Kept as an
    /// injected callback rather than a direct <c>ISchemaProvider</c> dependency: this class lives in the
    /// Datastore layer and must not depend on the Grains-layer schema-compilation seam.
    /// </param>
    /// <param name="logger">Optional; used only to log-and-swallow a schema apply failure.</param>
    public LogWatchHub(
        IDatastoreGrain grain,
        IGrainFactory grainFactory,
        TimeSpan? heartbeatInterval = null,
        Action<string>? applySchema = null,
        ILogger? logger = null)
    {
        _grain = grain;
        _grainFactory = grainFactory;
        _heartbeatInterval = heartbeatInterval ?? DefaultHeartbeatInterval;
        _applySchema = applySchema;
        _logger = logger;
    }

    /// <summary>Push delivery from the datastore grain: a commit advanced the head.</summary>
    public Task HeadAdvanced(long head)
    {
        Pulse(head);
        return Task.CompletedTask;
    }

    /// <summary>Push delivery from the datastore grain: a commit changed the schema. See the interface doc.</summary>
    public Task SchemaAdvanced(byte[] schemaBytes, string storedHash)
    {
        ApplySchema(schemaBytes, storedHash);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Applies <paramref name="schemaBytes"/> to the live schema provider, monotonically: a
    /// <paramref name="storedHash"/> already applied (by an earlier push or heartbeat) is a no-op, so a
    /// racing push and heartbeat repair can never double-apply or thrash. A poison schema cannot reach
    /// here in practice (it would have failed <c>WriteSchema</c>'s own validation before ever committing),
    /// but <see cref="ISchemaProvider.Update"/> can still throw — caught and logged so a bad push can
    /// never kill the heartbeat loop or this observer callback.
    /// </summary>
    private void ApplySchema(byte[] schemaBytes, string storedHash)
    {
        lock (_lock)
        {
            if (storedHash == _lastAppliedSchemaHash)
                return;
            _lastAppliedSchemaHash = storedHash;
        }

        if (_applySchema is null)
            return;

        try
        {
            _applySchema(Encoding.UTF8.GetString(schemaBytes));
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Discarding schema apply (stored hash {StoredHash}): update failed", storedHash);
        }
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

    /// <summary>Cheap pre-check so the heartbeat only pays a <c>ReadSchemaAt</c> hop when the hash actually moved.</summary>
    private bool NeedsApply(string storedHash)
    {
        lock (_lock)
            return storedHash != _lastAppliedSchemaHash;
    }

    private async Task HeartbeatLoop(IDatastoreWatcher selfRef, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                // One hop doing four jobs: refresh the observer registration (so it never expires while
                // this hub lives), resubscribe after a grain reactivation dropped it, read the head as the
                // missed-push backstop, and — the schema counterpart of that same backstop — compare the
                // reply's current stored schema hash against the last one applied. WaitAsync(ct) makes
                // cancellation unblock the await IMMEDIATELY even if the grain call is in-flight (e.g. the
                // silo is shutting down) — so disposal never waits on a hung hop.
                var reply = await _grain.SubscribeWatch(selfRef).WaitAsync(ct).ConfigureAwait(false);
                Pulse(reply.Head);

                if (reply.SchemaHash is { } schemaHash && NeedsApply(schemaHash))
                {
                    // A missed SchemaAdvanced push (or a hub that only just started, and so never saw the
                    // original commit's push at all): fetch the bytes at head and apply them. This is the
                    // one place the hub reads schema bytes itself rather than being handed them — the
                    // heartbeat has no push payload to fall back on, only the hash.
                    var bytes = await _grain.ReadSchemaAt(reply.Head).WaitAsync(ct).ConfigureAwait(false);
                    if (bytes is not null)
                        ApplySchema(bytes, schemaHash);
                }

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

            try
            {
                _grainFactory.DeleteObjectReference<IDatastoreWatcher>(selfRef);
            }
            catch
            {
                // DisposeAsync runs at container disposal, possibly AFTER the Orleans runtime has already
                // stopped — deleting an object reference against a stopped runtime can throw. The
                // reference dies with the runtime anyway, so this stays inside the same bounded
                // best-effort teardown discipline as the unsubscribe above: never throw, never stall.
            }
        }

        // Release any stragglers so their WaitAsync observes cancellation via their own token.
        lock (_lock)
            _signal.TrySetResult();
        _cts.Dispose();
    }
}
