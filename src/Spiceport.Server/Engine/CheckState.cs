using Spiceport.Core;

namespace Spiceport.Engine;

/// <summary>
/// Shared mutable state for a single top-level check, threaded across all dispatched sub-problems.
/// </summary>
/// <remarks>
/// Holds the dispatch counter (reported as <see cref="CheckResult.DispatchCount"/>). It is per-check
/// rather than per-dispatcher so that a decorator chain accumulates a single, correct count.
/// </remarks>
public sealed class CheckState
{
    /// <summary>The number of dispatch (recursive check) steps performed so far.</summary>
    public int DispatchCount { get; set; }
}

/// <summary>
/// A placeholder revision used when the engine is driven directly from an
/// <see cref="Spiceport.Datastore.IDatastoreReader"/> (the in-process path) and no concrete revision
/// identity is supplied. The dispatch request still carries a revision so it is serializable; the
/// local reader resolver maps any revision back to the single reader it closed over.
/// </summary>
internal sealed class InProcessRevision : IRevision
{
    public static readonly InProcessRevision Instance = new();

    private InProcessRevision() { }

    public bool ByteSortable => false;

    public override string ToString() => "in-process";

    public int CompareTo(IRevision? other) => ReferenceEquals(this, other) ? 0 : -1;

    public bool Equals(IRevision? other) => ReferenceEquals(this, other);

    public override bool Equals(object? obj) => ReferenceEquals(this, obj);

    public override int GetHashCode() => 0;
}
