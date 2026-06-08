using Spiceport.Core;

namespace Spiceport.Grains;

/// <summary>
/// Parses the revision string carried in a grain key back into an <see cref="IRevision"/> so the
/// grain can resolve a snapshot reader at that revision.
/// </summary>
/// <remarks>
/// This slice's datastore is timestamp-based (<see cref="TimestampRevision"/>), whose string form is
/// the integer nanosecond count. When richer revision schemes appear, this is the single place that
/// dispatches on the string form to reconstruct the right revision type.
/// </remarks>
internal static class RevisionCodec
{
    public static IRevision Parse(string revision)
    {
        ArgumentNullException.ThrowIfNull(revision);
        return TimestampRevision.Parse(revision);
    }
}
