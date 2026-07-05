using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Test-only convenience wrapper over <see cref="DispatchContext.Set"/>. Production callers only ever
/// reach <see cref="ICheckGrain.DispatchCheck"/> through <see cref="OrleansDispatcher"/>, which sets the
/// ambient depth budget / traversal-bloom cycle guard immediately before each grain call. Tests that
/// resolve an <see cref="ICheckGrain"/> directly (bypassing the dispatcher, to isolate the grain's own
/// behaviour) must do the same — this is the honest cost of moving those fields out of the method
/// signature and into ambient context. See <see cref="DispatchContext"/>'s remarks for the scoping
/// guarantee this relies on: set it immediately before each direct grain call, never once up front for
/// several calls.
/// </summary>
internal static class DispatchContextTestHelper
{
    /// <summary>
    /// Sets the depth budget and traversal-bloom cycle guard for the NEXT direct
    /// <see cref="ICheckGrain.DispatchCheck"/> call. <paramref name="bloom"/> defaults to
    /// <see cref="TraversalBloom.Empty"/> (no in-flight visit keys).
    /// </summary>
    public static void SetDispatchContext(int depthRemaining, TraversalBloom? bloom = null)
    {
        var traversalBloom = bloom ?? TraversalBloom.Empty;
        DispatchContext.Set(depthRemaining, traversalBloom.ToBytes(), traversalBloom.Hashes);
    }
}
