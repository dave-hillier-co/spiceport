namespace Spiceport.Core;

/// <summary>
/// An opaque, comparable representation of a point-in-time datastore state.
/// Implementations must be value-comparable and provide a stable string form.
/// </summary>
public interface IRevision : IComparable<IRevision>, IEquatable<IRevision>
{
    /// <summary>The opaque string form of this revision (round-trippable via the owning parser).</summary>
    string ToString();

    /// <summary>True if the string form is lexicographically byte-sortable in revision order.</summary>
    bool ByteSortable { get; }
}
