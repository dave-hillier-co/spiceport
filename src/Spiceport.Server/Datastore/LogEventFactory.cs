using Spiceport.Core;
using Spiceport.Datastore.Memory;
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

        return new LogEvent(revision, relationshipChanges, state.SchemaChangedAt(revision), counterChanges);
    }
}
