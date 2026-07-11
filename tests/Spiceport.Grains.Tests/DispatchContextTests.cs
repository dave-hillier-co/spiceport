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
    public void RequireVisited_throws_loudly_when_never_set()
    {
        RequestContext.Clear();

        var ex = Assert.Throws<InvalidOperationException>(() => DispatchContext.RequireVisited());

        Assert.Contains("visited", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Set_then_Require_round_trips_the_depth_and_visited_set()
    {
        RequestContext.Clear();
        var key = new VisitKey("document", "doc1", "view", "user", "alice", "...");
        var visited = new[] { key.ToCanonicalString() };

        DispatchContext.Set(42, visited);

        Assert.Equal(42, DispatchContext.RequireDepthRemaining());
        var round = DispatchContext.RequireVisited();
        Assert.Equal(visited.ToHashSet(), round.ToHashSet());
    }
}
