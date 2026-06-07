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

    /// <summary>
    /// True iff this revision is STRICTLY newer than <paramref name="other"/>. For totally-ordered
    /// revisions (e.g. timestamps) this is just <c>CompareTo(other) &gt; 0</c>, the default. Revisions
    /// with a partial order (e.g. Postgres snapshots, which can be CONCURRENT/incomparable) MUST
    /// override this so concurrent revisions return false — otherwise a <c>CompareTo &gt; 0</c> tiebreak
    /// would wrongly treat a concurrent revision as strictly newer, breaking read-your-writes and
    /// revision-window checks. Mirrors SpiceDB's <c>Revision.GreaterThan</c>.
    /// </summary>
    bool GreaterThan(IRevision? other) => CompareTo(other) > 0;
}
