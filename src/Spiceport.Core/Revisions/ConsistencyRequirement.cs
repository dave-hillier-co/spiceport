namespace Spiceport.Core;

/// <summary>
/// The consistency a read request demands, mirroring <c>authzed.api.v1.Consistency</c>.
/// A discriminated union over the four supported modes. Construct via the static factories.
/// </summary>
public abstract record ConsistencyRequirement
{
    private ConsistencyRequirement() { }

    /// <summary>The default: read at the optimized (quantized, head-pinned) revision for best latency/cache hit rate.</summary>
    public static ConsistencyRequirement MinimizeLatency { get; } = new MinimizeLatencyRequirement();

    /// <summary>Read at the freshest committed (head) revision.</summary>
    public static ConsistencyRequirement FullyConsistent { get; } = new FullyConsistentRequirement();

    /// <summary>Read at least as fresh as the supplied token (read-your-writes); picks max(token, optimized).</summary>
    public static ConsistencyRequirement AtLeastAsFresh(ZedToken token) =>
        new AtLeastAsFreshRequirement(token ?? throw new ArgumentNullException(nameof(token)));

    /// <summary>Read exactly at the snapshot encoded by the supplied token.</summary>
    public static ConsistencyRequirement AtExactSnapshot(ZedToken token) =>
        new AtExactSnapshotRequirement(token ?? throw new ArgumentNullException(nameof(token)));

    /// <summary>Minimize-latency variant.</summary>
    public sealed record MinimizeLatencyRequirement : ConsistencyRequirement;

    /// <summary>Fully-consistent variant.</summary>
    public sealed record FullyConsistentRequirement : ConsistencyRequirement;

    /// <summary>At-least-as-fresh variant carrying the floor token.</summary>
    public sealed record AtLeastAsFreshRequirement(ZedToken Token) : ConsistencyRequirement;

    /// <summary>At-exact-snapshot variant carrying the exact token.</summary>
    public sealed record AtExactSnapshotRequirement(ZedToken Token) : ConsistencyRequirement;
}
