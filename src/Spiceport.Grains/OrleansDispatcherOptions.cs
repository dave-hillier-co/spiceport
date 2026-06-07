namespace Spiceport.Grains;

/// <summary>
/// Tunables for <see cref="OrleansDispatcher"/>. Notably the hybrid local-recurse-vs-grain-hop toggle,
/// so a benchmark can compare the always-remote-grain path (OFF) against the consistent-hash
/// local-recurse shortcut (ON) apples-to-apples.
/// </summary>
public sealed class OrleansDispatcherOptions
{
    /// <summary>
    /// When true (default), a sub-problem whose consistent-hash owner is the LOCAL silo is served by an
    /// in-process dispatch through the silo-wide caching dispatcher (no cross-silo message), while a
    /// remotely-owned sub-problem still becomes a grain call to its owner. When false, EVERY sub-problem
    /// becomes a grain call regardless of ownership — the pre-hybrid behaviour, for benchmark baselining.
    /// </summary>
    public bool LocalRecurseEnabled { get; set; } = true;
}
