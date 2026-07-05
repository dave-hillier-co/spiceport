using Spiceport.Core;

namespace Spiceport.Datastore;

/// <summary>
/// Decodes the timestamp revision string form (integer nanos) back into a
/// <see cref="TimestampRevision"/>. Used by both <see cref="ReferenceDatastore"/> and
/// GrainBackedDatastore, which mint the same string form.
/// </summary>
/// <param name="datastoreUniqueId">The owning datastore's <see cref="IDatastore.GetUniqueId"/>.</param>
public sealed class TimestampRevisionParser(string datastoreUniqueId) : IRevisionParser
{
    /// <inheritdoc />
    public string DatastoreUniqueId { get; } = datastoreUniqueId;

    /// <inheritdoc />
    public IRevision ParseRevisionString(string revisionString) => TimestampRevision.Parse(revisionString);
}
