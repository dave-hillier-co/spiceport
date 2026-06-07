using Spiceport.Core;

namespace Spiceport.Grains.Abstractions;

/// <summary>The selected consistency mode on the wire.</summary>
public enum ConsistencyModeWire
{
    /// <summary>Read at the optimized revision (default).</summary>
    MinimizeLatency = 0,

    /// <summary>Read at least as fresh as <see cref="ConsistencyWire.Token"/>.</summary>
    AtLeastAsFresh = 1,

    /// <summary>Read at head.</summary>
    FullyConsistent = 2,

    /// <summary>Read exactly at <see cref="ConsistencyWire.Token"/>.</summary>
    AtExactSnapshot = 3,
}

/// <summary>
/// The serializable consistency requirement carried across grain calls. Maps to and from the
/// domain <see cref="ConsistencyRequirement"/>. A null wire (or default) means minimize-latency.
/// </summary>
[GenerateSerializer]
public sealed record ConsistencyWire(
    [property: Id(0)] ConsistencyModeWire Mode,
    [property: Id(1)] string? Token = null)
{
    /// <summary>The minimize-latency requirement (the server default).</summary>
    public static ConsistencyWire MinimizeLatency { get; } = new(ConsistencyModeWire.MinimizeLatency);

    /// <summary>Converts to the domain <see cref="ConsistencyRequirement"/>.</summary>
    public ConsistencyRequirement ToRequirement() => Mode switch
    {
        ConsistencyModeWire.MinimizeLatency => ConsistencyRequirement.MinimizeLatency,
        ConsistencyModeWire.FullyConsistent => ConsistencyRequirement.FullyConsistent,
        ConsistencyModeWire.AtLeastAsFresh => ConsistencyRequirement.AtLeastAsFresh(RequireToken()),
        ConsistencyModeWire.AtExactSnapshot => ConsistencyRequirement.AtExactSnapshot(RequireToken()),
        _ => ConsistencyRequirement.MinimizeLatency,
    };

    /// <summary>Builds the wire form from a domain requirement.</summary>
    public static ConsistencyWire FromRequirement(ConsistencyRequirement requirement) => requirement switch
    {
        ConsistencyRequirement.MinimizeLatencyRequirement => MinimizeLatency,
        ConsistencyRequirement.FullyConsistentRequirement => new(ConsistencyModeWire.FullyConsistent),
        ConsistencyRequirement.AtLeastAsFreshRequirement a => new(ConsistencyModeWire.AtLeastAsFresh, a.Token.Token),
        ConsistencyRequirement.AtExactSnapshotRequirement a => new(ConsistencyModeWire.AtExactSnapshot, a.Token.Token),
        _ => MinimizeLatency,
    };

    private ZedToken RequireToken() => new(
        Token ?? throw new InvalidOperationException($"consistency mode {Mode} requires a token"));
}
