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

/// <summary>
/// Default <see cref="ISiloDispatcher"/> holding a single shared dispatcher instance.
/// </summary>
/// <remarks>
/// Supports late binding of the held dispatcher (set once after construction) to resolve the DI
/// chicken-and-egg between the root caching dispatcher and the local-recurse path inside the
/// <see cref="OrleansDispatcher"/> it wraps: the holder is created first, handed to the dispatcher as
/// its onward path, then the fully-built root is assigned back into it.
/// </remarks>
public sealed class SiloDispatcher : ISiloDispatcher
{
    private IDispatcher? _dispatcher;

    /// <summary>Creates a holder around an already-built dispatcher.</summary>
    public SiloDispatcher(IDispatcher dispatcher) =>
        _dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));

    /// <summary>Creates an unbound holder; the dispatcher must be set via <see cref="Bind"/> before use.</summary>
    public SiloDispatcher() { }

    /// <inheritdoc />
    public IDispatcher Dispatcher =>
        _dispatcher ?? throw new InvalidOperationException("SiloDispatcher has not been bound to a dispatcher.");

    /// <summary>Binds the held dispatcher exactly once.</summary>
    public void Bind(IDispatcher dispatcher)
    {
        ArgumentNullException.ThrowIfNull(dispatcher);
        if (_dispatcher is not null)
            throw new InvalidOperationException("SiloDispatcher is already bound.");
        _dispatcher = dispatcher;
    }
}
