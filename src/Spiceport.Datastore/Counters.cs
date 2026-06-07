namespace Spiceport.Datastore;

/// <summary>
/// A registered relationship counter: a stable name bound to the <see cref="RelationshipsFilter"/>
/// whose live matches are counted on demand. Stored MVCC-versioned, exactly like the unified schema:
/// registering writes a version, unregistering tombstones it, and a snapshot read sees the counter
/// (and filter) live at its revision.
/// </summary>
/// <param name="Name">The counter's unique name.</param>
/// <param name="Filter">The filter whose matching relationships are counted.</param>
public sealed record RegisteredCounter(string Name, RelationshipsFilter Filter);
