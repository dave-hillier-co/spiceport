using System.Reflection;
using Orleans;
using Spiceport.Core;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// Shared method-matching for the two check-dispatch grain-call filters below: both act ONLY on
/// <see cref="ICheckGrain.DispatchCheck"/>, so every other grain call in the mesh (the datastore grain,
/// the relationships grain, the reverse-ops worker, ...) passes through untouched with zero overhead
/// beyond this one type check.
/// </summary>
internal static class CheckDispatchFilter
{
    /// <summary>Whether the intercepted call is <see cref="ICheckGrain.DispatchCheck"/>.</summary>
    public static bool Matches(MethodInfo method) =>
        method.DeclaringType == typeof(ICheckGrain) && method.Name == nameof(ICheckGrain.DispatchCheck);
}

/// <summary>
/// Cross-silo exception classification for every <see cref="ICheckGrain.DispatchCheck"/> hop, lifted out
/// of <see cref="OrleansDispatcher"/> so the dispatcher itself carries only genuinely dispatch-semantic
/// logic (the cancellation bridging, the loop-bypass tag). Wraps <see cref="IOutgoingGrainCallContext.Invoke"/>
/// in the SAME try/catch that used to sit directly around the grain call in <see cref="OrleansDispatcher"/>:
/// a known domain exception (or an already-classified <see cref="DispatchFailedException"/>) is re-thrown
/// unchanged; everything else is collapsed via <see cref="DispatchErrorMapper.Translate"/>.
/// </summary>
/// <remarks>
/// This filter never sees the ONE cancellation case the caller's own <see cref="CancellationToken"/>
/// produces: <see cref="OrleansDispatcher"/> races the grain call against that token with
/// <see cref="Task.WaitAsync(CancellationToken)"/>, which — if the caller's token fires first — raises
/// its OWN <see cref="OperationCanceledException"/> locally, against an abandoned await, without ever
/// observing a fault from the still-running grain call this filter wraps. That specific case is
/// translated directly by the dispatcher itself, via the same <see cref="DispatchErrorMapper.Translate"/>;
/// this filter only ever classifies a fault the underlying grain call genuinely produced (a domain
/// exception, a transport failure, or e.g. a grain-side cancellation unrelated to that caller token).
/// </remarks>
public sealed class CheckDispatchOutgoingCallFilter : IOutgoingGrainCallFilter
{
    /// <inheritdoc/>
    public async Task Invoke(IOutgoingGrainCallContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (!CheckDispatchFilter.Matches(context.InterfaceMethod))
        {
            await context.Invoke().ConfigureAwait(false);
            return;
        }

        try
        {
            await context.Invoke().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw DispatchErrorMapper.Translate(ex);
        }
    }
}

/// <summary>
/// The depth-budget BOUNDARY guard for every <see cref="ICheckGrain.DispatchCheck"/> call, enforced at
/// the silo before the grain body runs at all: a caller offering a depth budget (read from the ambient
/// <see cref="DispatchContext"/>) &lt;= 0 is rejected immediately with a
/// <see cref="MaxDepthExceededException"/>, with no activation-memo lookup and no relation-graph
/// expansion.
/// </summary>
/// <remarks>
/// This is a BOUNDARY check, not a substitute for <see cref="Spiceport.Engine.LocalDispatcher"/>'s own
/// in-step guard (which still governs recursion WITHIN one grain's own expansion step, decrementing the
/// budget as it dispatches each child); this filter only rejects whatever arrives at the grain already
/// exhausted. It also records every matched call as a real dispatch hop
/// (<see cref="IDispatchMetrics.RecordDispatch"/>) — the one place in the mesh a grain-call boundary
/// crossing is counted independent of the callee's own activation-memo outcome: a rejected call is still
/// counted, because a message genuinely did cross into this grain.
/// </remarks>
public sealed class CheckDispatchIncomingCallFilter : IIncomingGrainCallFilter
{
    private readonly IDispatchMetrics? _metrics;

    /// <summary>Creates the incoming boundary filter.</summary>
    /// <param name="metrics">Optional silo-wide dispatch counters.</param>
    public CheckDispatchIncomingCallFilter(IDispatchMetrics? metrics = null) => _metrics = metrics;

    /// <inheritdoc/>
    public async Task Invoke(IIncomingGrainCallContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        if (!CheckDispatchFilter.Matches(context.InterfaceMethod))
        {
            await context.Invoke().ConfigureAwait(false);
            return;
        }

        _metrics?.RecordDispatch();

        // Orleans imports the sender's RequestContext before any incoming call filter runs (see
        // DispatchContext's remarks), so the depth budget the caller set via DispatchContext.Set is
        // already visible here — reading it (rather than a method argument) throws loudly via
        // RequireDepthRemaining if some caller reached this grain without going through that seam.
        if (DispatchContext.RequireDepthRemaining() <= 0)
            throw new MaxDepthExceededException();

        await context.Invoke().ConfigureAwait(false);
    }
}
