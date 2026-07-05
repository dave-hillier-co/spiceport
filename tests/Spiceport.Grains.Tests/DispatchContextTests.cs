using Orleans.Runtime;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Pure unit tests for <see cref="DispatchContext"/>'s ambient <see cref="RequestContext"/> plumbing,
/// isolated from any real grain call. Each test clears <see cref="RequestContext"/> first so a value left
/// behind by a previous test on the same async flow can never leak in and mask a missing-key bug.
/// </summary>
public class DispatchContextTests
{
    [Fact]
    public void RequireDepthRemaining_throws_loudly_when_never_set()
    {
        RequestContext.Clear();

        var ex = Assert.Throws<InvalidOperationException>(() => DispatchContext.RequireDepthRemaining());

        Assert.Contains("depthRemaining", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void RequireBloom_throws_loudly_when_never_set()
    {
        RequestContext.Clear();

        var ex = Assert.Throws<InvalidOperationException>(() => DispatchContext.RequireBloom());

        Assert.Contains("bloomBits", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void RequireBloom_throws_when_bits_are_set_but_k_is_missing()
    {
        // Guards against a partial Set (a future refactor that split the two RequestContext.Set calls):
        // either half missing must still throw, not silently default the other.
        RequestContext.Clear();
        DispatchContext.Set(10, TraversalBloom.Empty.ToBytes(), TraversalBloom.Empty.Hashes);
        RequestContext.Remove("spiceport.dispatch.bloomK");

        var ex = Assert.Throws<InvalidOperationException>(() => DispatchContext.RequireBloom());

        Assert.Contains("bloomK", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Set_then_Require_round_trips_the_depth_and_bloom()
    {
        RequestContext.Clear();
        var bloom = TraversalBloom.Empty;

        DispatchContext.Set(42, bloom.ToBytes(), bloom.Hashes);

        Assert.Equal(42, DispatchContext.RequireDepthRemaining());
        var (bits, k) = DispatchContext.RequireBloom();
        Assert.Equal(bloom.ToBytes(), bits);
        Assert.Equal(bloom.Hashes, k);
    }
}
