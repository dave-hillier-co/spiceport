using System.Collections.Immutable;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Test-only convenience wrapper over <see cref="DispatchContext.Set"/>. Production callers only ever
/// reach <see cref="ICheckGrain.DispatchCheck"/> through <see cref="OrleansDispatcher"/>, which sets the
/// ambient depth budget / exact visited-set cycle guard immediately before each grain call. Tests that
/// resolve an <see cref="ICheckGrain"/> directly (bypassing the dispatcher, to isolate the grain's own
/// behaviour) must do the same — this is the honest cost of moving those fields out of the method
/// signature and into ambient context. See <see cref="DispatchContext"/>'s remarks for the scoping
/// guarantee this relies on: set it immediately before each direct grain call, never once up front for
/// several calls.
/// </summary>
internal static class DispatchContextTestHelper
{
    /// <summary>
    /// Sets the depth budget and exact visited-set cycle guard for the NEXT direct
    /// <see cref="ICheckGrain.DispatchCheck"/> call. <paramref name="visited"/> defaults to the empty
    /// set (no in-flight visit keys).
    /// </summary>
    public static void SetDispatchContext(int depthRemaining, ImmutableHashSet<VisitKey>? visited = null)
    {
        var set = visited ?? ImmutableHashSet<VisitKey>.Empty;
        DispatchContext.Set(depthRemaining, set.Select(v => v.ToCanonicalString()).ToArray());
    }
}
