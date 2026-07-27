namespace Spiceport.Grains.Abstractions;

/// <summary>
/// Thrown at the write surface when the per-silo sequencer admission gate is full: the silo already has
/// its configured maximum of commits in flight to the cluster-singleton sequencer grain, so this write
/// is shed instead of joining an unbounded activation queue (where it would eventually die as an opaque
/// Orleans response timeout). The gRPC front door maps it to <c>RESOURCE_EXHAUSTED</c> — a deliberate,
/// retryable overload signal the client backs off from, in contrast to the <c>Unknown</c> a timeout
/// storm produced. <c>[GenerateSerializer]</c> because it crosses the grain boundary (the data-plane
/// relationships grain throws it to the gRPC caller).
/// </summary>
[GenerateSerializer]
public sealed class SequencerOverloadedException : Exception
{
    /// <summary>Creates the exception with the shed diagnostic message.</summary>
    public SequencerOverloadedException(string message) : base(message)
    {
    }
}
