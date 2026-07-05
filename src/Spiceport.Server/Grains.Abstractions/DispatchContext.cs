using Orleans.Runtime;
using Spiceport.Grains;

namespace Spiceport.Grains.Abstractions;

/// <summary>
/// The call-chain context threaded ambiently across every <see cref="ICheckGrain.DispatchCheck"/> hop
/// via Orleans <see cref="RequestContext"/>, rather than as an explicit method argument.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why these fields are context, not identity.</b> The grain's own string key already pins the
/// canonical sub-problem (resource, subject, quantized revision, schema hash) — see the remarks on
/// <see cref="ICheckGrain"/>. The remaining recursion budget and the traversal-bloom cycle guard are
/// NOT part of that identity: two callers asking the exact same sub-problem on different recursion
/// paths address the same grain but may carry a different depth budget / bloom state. Since the wire
/// contract for a dispatch call should be exactly the canonical sub-problem, these cross-cutting fields
/// ride in <see cref="RequestContext"/> instead of a request argument.
/// </para>
/// <para>
/// <b>Scoping guarantee (verified against this repo's Orleans 10.1.0).</b>
/// <see cref="RequestContext"/> is backed by an <c>AsyncLocal&lt;ContextProperties&gt;</c>
/// (<c>Orleans.Runtime.RequestContext.CallContextData</c>), and every <see cref="Set"/> replaces the
/// value with a freshly copied dictionary rather than mutating the existing one in place
/// (copy-on-write). Combined with the .NET async-method-builder guarantee that an
/// <c>AsyncLocal</c> mutation made inside an async method's synchronous prefix is restored on the
/// calling flow once that method yields at (or returns from) an <c>await</c>, this means:
/// <list type="bullet">
/// <item>a value <see cref="Set"/> immediately before an awaited grain call flows DOWN into that call
/// (Orleans' own <c>MessageFactory</c> snapshots the ambient context via
/// <c>RequestContextExtensions.Export</c> at message-construction time, i.e. synchronously, before any
/// await intervenes) — see Orleans' <c>src/Orleans.Core/Messaging/MessageFactory.cs</c>;</item>
/// <item>it never leaks back UP to the caller once the awaited call completes (the caller's own ambient
/// context is restored) — proven by Orleans' own
/// <c>RequestContextTests_Local.RequestContext_CrossThread</c> test, where 1000 concurrent
/// <c>Task.Run</c> children each mutate their own copy of a key and the parent's value is unchanged
/// after they all complete;</item>
/// <item>and it never leaks ACROSS sibling dispatches: each call to <see cref="Set"/> overwrites the
/// values for that one outgoing call only, so as long as every dispatch site calls
/// <see cref="Set"/> immediately before its own grain call (never relying on a value set by a
/// previous sibling), each hop's own values are exactly what THAT hop set.</item>
/// </list>
/// On the receiving side, Orleans imports the message's context (a wholesale replace, see
/// <c>RequestContextExtensions.Import</c>) BEFORE running incoming grain-call filters or invoking the
/// grain body (see <c>InsideRuntimeClient.Invoke</c>), so <see cref="TryGetDepthRemaining"/> /
/// <see cref="RequireBloom"/> observe the sender's values from the very first incoming filter onward.
/// </para>
/// <para>
/// A missing key when reading is treated as a bug, not a default: every legitimate call path (the
/// dispatcher, or a test's <c>SetDispatchContext</c> helper) sets all three values before the grain
/// call, so an absence means some caller reached <see cref="ICheckGrain.DispatchCheck"/> without going
/// through that seam. The <c>Require*</c> accessors throw a clear <see cref="InvalidOperationException"/>
/// naming the missing key rather than silently defaulting, so such a bug surfaces immediately instead of
/// masquerading as (say) a depth-budget of zero.
/// </para>
/// </remarks>
public static class DispatchContext
{
    private const string DepthRemainingKey = "spiceport.dispatch.depthRemaining";
    private const string BloomBitsKey = "spiceport.dispatch.bloomBits";
    private const string BloomKKey = "spiceport.dispatch.bloomK";

    /// <summary>
    /// Sets the depth budget and traversal-bloom cycle guard for the NEXT outgoing
    /// <see cref="ICheckGrain.DispatchCheck"/> call made from this point in the current async flow.
    /// Call this immediately before that call — see the scoping guarantee on <see cref="DispatchContext"/>.
    /// </summary>
    public static void Set(int depthRemaining, byte[] bloomBits, int bloomK)
    {
        ArgumentNullException.ThrowIfNull(bloomBits);
        RequestContext.Set(DepthRemainingKey, depthRemaining);
        RequestContext.Set(BloomBitsKey, bloomBits);
        RequestContext.Set(BloomKKey, bloomK);
    }

    /// <summary>Attempts to read the remaining recursion depth budget from the ambient context.</summary>
    public static bool TryGetDepthRemaining(out int depthRemaining)
    {
        if (RequestContext.Get(DepthRemainingKey) is int value)
        {
            depthRemaining = value;
            return true;
        }

        depthRemaining = default;
        return false;
    }

    /// <summary>
    /// Reads the remaining recursion depth budget from the ambient context, throwing if it is absent
    /// (a lost context is a bug that must surface loudly, never silently default).
    /// </summary>
    public static int RequireDepthRemaining() =>
        TryGetDepthRemaining(out var depthRemaining) ? depthRemaining : throw Missing(DepthRemainingKey);

    /// <summary>
    /// Reads the traversal-bloom cycle guard (its raw bits and hash count) from the ambient context,
    /// throwing if either half is absent.
    /// </summary>
    public static (byte[] BloomBits, int BloomK) RequireBloom()
    {
        var bits = RequestContext.Get(BloomBitsKey) as byte[] ?? throw Missing(BloomBitsKey);
        var k = RequestContext.Get(BloomKKey) is int value ? value : throw Missing(BloomKKey);
        return (bits, k);
    }

    private static InvalidOperationException Missing(string key) => new(
        $"DispatchContext key '{key}' is missing from the Orleans RequestContext. Every " +
        $"{nameof(ICheckGrain)}.{nameof(ICheckGrain.DispatchCheck)} call must set it beforehand — via " +
        $"{nameof(OrleansDispatcher)} in production, or the test SetDispatchContext helper in a direct " +
        "grain call — never silently default a lost call-chain context.");
}
