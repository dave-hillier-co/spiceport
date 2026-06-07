using Spiceport.Engine;

namespace Spiceport.Grains;

/// <summary>
/// Holds the silo-wide "root" dispatcher (Caching over Orleans) so that a grain's onward
/// sub-dispatch routes children through the same shared caching + Orleans path the API entry uses.
/// </summary>
/// <remarks>
/// The root <see cref="CachingDispatcher"/> wraps an <see cref="OrleansDispatcher"/>; both the gRPC
/// front door and every grain's child sub-problems dispatch through this single instance, so the
/// branch cache is shared across the whole mesh and all recursion crosses grain boundaries.
/// </remarks>
public interface ISiloDispatcher
{
    /// <summary>The shared root dispatcher (Caching over Orleans).</summary>
    IDispatcher Dispatcher { get; }
}

/// <summary>Default <see cref="ISiloDispatcher"/> holding a single shared dispatcher instance.</summary>
public sealed class SiloDispatcher(IDispatcher dispatcher) : ISiloDispatcher
{
    /// <inheritdoc />
    public IDispatcher Dispatcher { get; } = dispatcher;
}
