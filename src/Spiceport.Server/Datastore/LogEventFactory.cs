using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// Builds a <see cref="LogEvent"/> for a single committed revision from the in-memory
/// <see cref="DatastoreState"/>, reusing the existing per-revision diff logic
/// (<c>DatastoreState.ChangesAt</c> / <c>SchemaChangedAt</c>) so the event-log feed is byte-equivalent to
/// the Watch changefeed. This is the single definition of "what changed at revision R"; Watch and the
/// per-silo projection both consume the resulting events.
/// </summary>
internal static class LogEventFactory
{
    public static LogEvent EventFromState(DatastoreState state, long revision)
    {
        var relationshipChanges = state.ChangesAt(revision)
            .Select(u => new RelationshipUpdateWire(
                u.Operation == UpdateOperation.Delete ? RelationshipUpdateOpWire.Delete : RelationshipUpdateOpWire.Touch,
                WireConvert.ToWire(u.Relationship)))
            .ToList();

        var counterChanges = state.Counters
            .Where(c => c.Revision == revision)
            .Select(c => new CounterDeltaWire(c.Name, c.Filter is { } f ? WireConvert.ToFullFilter(f) : null))
            .ToList();

        // The schema written exactly at this revision (null = no schema change). Carrying the full version
        // (revision + bytes + hash) makes the event self-contained: a fold can append the schema version
        // without re-reading the source state. There is at most one schema version per revision.
        var schemaChange = state.Schemas
            .Where(s => s.Revision == revision)
            .Select(s => new SchemaVersionWire(s.Revision, s.Bytes, s.Hash))
            .FirstOrDefault();

        return new LogEvent(revision, relationshipChanges, schemaChange, counterChanges, GcFloor: null);
    }
}
