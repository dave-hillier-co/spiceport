using Spiceport.Core;

namespace Spiceport.Datastore.Memory;

/// <summary>
/// Decodes the in-memory datastore's revision string form (integer nanos) back into a
/// <see cref="TimestampRevision"/>. Co-located with the datastore that mints the strings.
/// </summary>
/// <param name="datastoreUniqueId">The owning datastore's <see cref="IDatastore.GetUniqueId"/>.</param>
public sealed class InMemoryRevisionParser(string datastoreUniqueId) : IRevisionParser
{
    /// <inheritdoc />
    public string DatastoreUniqueId { get; } = datastoreUniqueId;

    /// <inheritdoc />
    public IRevision ParseRevisionString(string revisionString) => TimestampRevision.Parse(revisionString);
}
