using Orleans.Serialization.Cloning;
using Spiceport.Core;
using Spiceport.Datastore;

namespace Spiceport.Grains;

/// <summary>
/// Orleans serialization surrogate for <see cref="RevisionNotFoundException"/>.
/// </summary>
/// <remarks>
/// The datastore-layer <see cref="RevisionNotFoundException"/> is a domain exception that must NOT take an
/// Orleans dependency, yet it crosses the grain boundary when <see cref="IDatastoreLog.ReadFrom"/> rejects
/// a cursor older than the GC window. This surrogate captures the (timestamp) revision so the exception
/// round-trips with its <see cref="RevisionNotFoundException.Revision"/> intact, mirroring how SpiceDB maps
/// a stale Watch cursor.
/// </remarks>
[GenerateSerializer]
public struct RevisionNotFoundSurrogate
{
    /// <summary>The timestamp-nanos of the revision that could not be found.</summary>
    [Id(0)]
    public long RevisionNanos;
}

/// <summary>Converts <see cref="RevisionNotFoundException"/> to and from its serializable surrogate.</summary>
[RegisterConverter]
public sealed class RevisionNotFoundSurrogateConverter
    : IConverter<RevisionNotFoundException, RevisionNotFoundSurrogate>
{
    /// <inheritdoc />
    public RevisionNotFoundException ConvertFromSurrogate(in RevisionNotFoundSurrogate surrogate) =>
        new(new TimestampRevision(surrogate.RevisionNanos));

    /// <inheritdoc />
    public RevisionNotFoundSurrogate ConvertToSurrogate(in RevisionNotFoundException value) =>
        new()
        {
            RevisionNanos = value.Revision is TimestampRevision t ? t.TimestampNanosSinceEpoch : 0,
        };
}
