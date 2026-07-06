using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;
using Spiceport.Schema;

namespace Spiceport.Grains;

/// <summary>
/// An <see cref="IDispatcher"/> that turns each sub-problem into a grain call: it derives the
/// canonical grain key from the request, resolves the keyed <see cref="ICheckGrain"/> via
/// <see cref="IGrainFactory"/>, and invokes <see cref="ICheckGrain.DispatchCheck"/>.
/// </summary>
/// <remarks>
/// This is how ALL recursion crosses grain boundaries: a grain computing one sub-problem dispatches each
/// of its children through an <see cref="OrleansDispatcher"/>, so every child becomes a call to a
/// (potentially same-process, but always grain-addressed) <see cref="ICheckGrain"/> activation keyed by
/// that child's identity. There is no in-process local-recurse shortcut — every sub-problem, local or
/// remote, is a grain call; Orleans' own placement director and grain directory are the only router. The
/// dispatcher maps the engine's <see cref="DispatchCheckRequest"/> / <see cref="DispatchCheckResult"/> to
/// and from the serializable grain <see cref="DispatchCheckReply"/>, threading the depth budget and
/// traversal-bloom cycle guard ambiently via <see cref="DispatchContext"/> rather than a method argument.
/// </remarks>
public sealed class OrleansDispatcher : IDispatcher
{
    private readonly IGrainFactory _grains;
    private readonly ISchemaHashSource _schemaHash;
    private readonly IDispatchMetrics? _metrics;

    /// <summary>Creates an Orleans dispatcher.</summary>
    /// <param name="grains">The grain factory used to resolve keyed check grains.</param>
    /// <param name="schemaHash">Supplies the live schema hash embedded in every grain key (scopes identity to the current schema).</param>
    /// <param name="metrics">Optional silo-wide counters (loop-bypass hits, activation-memo hit/miss).</param>
    public OrleansDispatcher(
        IGrainFactory grains,
        ISchemaHashSource schemaHash,
        IDispatchMetrics? metrics = null)
    {
        ArgumentNullException.ThrowIfNull(grains);
        ArgumentNullException.ThrowIfNull(schemaHash);
        _grains = grains;
        _schemaHash = schemaHash;
        _metrics = metrics;
    }

    /// <summary>
    /// The canonical grain key for a sub-problem, identical to the key used to address its
    /// <see cref="ICheckGrain"/>.
    /// </summary>
    public string KeyFor(ObjectAndRelation resource, ObjectAndRelation subject, string revision) =>
        GrainKey.Build(resource, subject, revision, _schemaHash.CurrentSchemaHash);

    /// <inheritdoc/>
    public async Task<DispatchCheckResult> DispatchCheck(DispatchCheckRequest request, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(request);
        ct.ThrowIfCancellationRequested();

        var key = GrainKey.Build(
            request.Resource,
            request.Subject,
            request.Meta.Revision.ToString(),
            _schemaHash.CurrentSchemaHash);

        // SINGLEFLIGHT-STYLE LOOP BYPASS (SpiceDB singleflight.go:69-81): if the traversal bloom already
        // contains this sub-problem's (resource, subject) key, this is a LIKELY loop back to a grain key
        // that is (or would be) busy on the path above us. The grain call still happens NORMALLY — the
        // grain is [Reentrant], so a genuine same-key re-entry is accepted and terminates deterministically
        // on the depth budget (MaxDepthExceededException), exactly as a fresh call would. What changes is
        // the RETURNED result: it is force-tagged CycleCut so no CheckGrain activation memo up the call
        // chain ever stores this path-dependent branch (see CheckGrain's CACHING remarks). A bloom
        // false-positive is harmless here — same verdict, just an uncached hop.
        var visit = VisitKey.Of(request.Resource, request.Subject);
        var loopBypass = request.Meta.Bloom.Contains(visit);
        if (loopBypass)
            _metrics?.RecordLoopBypass();

        var grain = _grains.GetGrain<ICheckGrain>(key);

        // The depth budget and traversal-bloom cycle guard are call-chain context, not sub-problem
        // identity (already pinned by `key` above), so they ride ambiently via DispatchContext /
        // Orleans RequestContext rather than as a method argument — see the scoping guarantee documented
        // on DispatchContext. Set immediately before the grain call so it is exactly what flows down into
        // THIS hop (never a stale value from a previous sibling dispatch).
        DispatchContext.Set(request.Meta.DepthRemaining, request.Meta.Bloom.ToBytes(), request.Meta.Bloom.Hashes);

        // Deliberate cross-silo error mapping (cf. SpiceDB rewriteError / remote cluster boundary) now
        // happens INSIDE the grain call itself, via CheckDispatchOutgoingCallFilter: known typed domain
        // exceptions keep their own semantics and pass through; transport/availability failures become a
        // RETRIABLE Unavailable; anything else collapses to Internal. The mapped failure travels back as
        // a [GenerateSerializer] DispatchFailedException so its code survives any further grain hops up
        // the recursion. This dispatcher therefore no longer wraps the call in its own catch(Exception) —
        // only the cancellation bridging below remains, because that ONE case (the caller's own `ct`
        // firing first inside Task.WaitAsync) never reaches the filter: WaitAsync raises its own
        // OperationCanceledException locally, against an abandoned await, without observing whatever the
        // still-running (filter-wrapped) grain call itself eventually faults with.
        DispatchCheckReply reply;
        using var grainCancellation = new GrainCancellationTokenSource();
        var grainCall = grain.DispatchCheck(grainCancellation.Token);
        try
        {
            reply = await grainCall.WaitAsync(ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException ex) when (ct.IsCancellationRequested)
        {
            // GrainCancellationTokenSource.Cancel is asynchronous: await delivery so cancellation
            // has reached the remote activation before unwinding this hop. The receiver converts
            // the Orleans token back to the engine's System.Threading.CancellationToken.
            try
            {
                await grainCancellation.Cancel().ConfigureAwait(false);
            }
            catch
            {
                // Cancellation is already the primary outcome. A silo disappearing while the
                // cancellation notification is in flight must not replace it with a transport error.
            }

            // Mirror the SAME classification the outgoing filter applies to every other dispatch
            // failure, so caller-cancellation still surfaces as the DispatchFailedException(Cancelled)
            // callers have always seen — even though this exception never touched the filter.
            throw DispatchErrorMapper.Translate(ex);
        }

        var result = new DispatchCheckResult(
            reply.Member, CaveatWire.FromWire(reply.Caveat), reply.CycleCut, reply.DepthRequired);

        // Force the loop-bypassed subtree CycleCut so it is never memoized upstream, regardless of what
        // the (correctly computed) callee itself decided about its own memo.
        return loopBypass ? result with { CycleCut = true } : result;
    }
}
