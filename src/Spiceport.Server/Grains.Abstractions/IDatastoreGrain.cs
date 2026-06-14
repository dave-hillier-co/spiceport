namespace Spiceport.Grains.Abstractions;

/// <summary>
/// The cluster-singleton datastore grain: the single source of truth for all relationship/schema/counter
/// state, held as <see cref="DatastoreGrainState"/> in a persistent grain. It is keyed by the constant
/// integer <see cref="Key"/> so every silo routes to the ONE activation (single-activation virtual actor),
/// which makes multi-silo reads correct with zero replica lag. Writes go through an optimistic
/// compare-and-swap (the non-reentrant single turn makes the head-compare atomic).
/// </summary>
public interface IDatastoreGrain : IGrainWithIntegerKey
{
    /// <summary>The fixed key of the single datastore activation.</summary>
    public const long Key = 0;

    /// <summary>Returns the full committed state (the CAS base / snapshot source).</summary>
    Task<DatastoreGrainState> ReadState();

    /// <summary>Returns the head revision and schema hash without shipping the whole state blob.</summary>
    Task<DatastoreHeadWire> GetHead();

    /// <summary>
    /// Optimistic compare-and-swap: applies and persists <paramref name="newState"/> ONLY if the grain's
    /// current head still equals <paramref name="expectedHead"/>; returns true on apply, false if the head
    /// moved (the caller must reload and retry). The non-reentrant single-activation turn makes the
    /// compare-and-apply atomic with respect to all other writes.
    /// </summary>
    Task<bool> CompareAndSwap(long expectedHead, DatastoreGrainState newState);
}
