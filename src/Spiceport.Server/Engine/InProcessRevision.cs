using Spiceport.Core;

namespace Spiceport.Engine;

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
