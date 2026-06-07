using Spiceport.Core;

namespace Spiceport.Datastore.Postgres;

/// <summary>
/// Decodes the Postgres datastore's revision string form back into a <see cref="PostgresRevision"/>,
/// so ZedTokens minted by this datastore round-trip. Co-located with the datastore that mints them.
/// </summary>
/// <param name="datastoreUniqueId">The owning datastore's <see cref="IDatastore.GetUniqueId"/>.</param>
public sealed class PostgresRevisionParser(string datastoreUniqueId) : IRevisionParser
{
    /// <inheritdoc />
    public string DatastoreUniqueId { get; } = datastoreUniqueId;

    /// <inheritdoc />
    public IRevision ParseRevisionString(string revisionString) => PostgresRevision.Parse(revisionString);
}
