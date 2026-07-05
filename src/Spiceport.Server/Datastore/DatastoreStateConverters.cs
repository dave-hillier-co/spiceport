using System.Collections.Immutable;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// Converts between the Orleans-serializable <see cref="DatastoreGrainState"/> (read from / written to
/// the singleton datastore grain) and the in-memory MVCC <see cref="DatastoreState"/> (whose fold,
/// transaction and reader mechanics the grain-backed datastore reuses). Relationship payload and
/// counter-filter conversions delegate to <see cref="WireConvert"/> so there is one copy of that logic.
/// </summary>
internal static class DatastoreStateConverters
{
    /// <summary>Grain wire state to in-memory MVCC state.</summary>
    public static DatastoreState ToMemory(DatastoreGrainState g) =>
        new(
            g.HeadRevision,
            g.Relationships.Select(ToMemoryRow).ToImmutableList(),
            g.Schemas.Select(ToMemorySchema).ToImmutableList(),
            g.Counters.Select(ToMemoryCounter).ToImmutableList(),
            g.GcFloor);

    /// <summary>In-memory MVCC state to grain wire state (for the CAS payload).</summary>
    public static DatastoreGrainState ToGrain(DatastoreState s) =>
        new()
        {
            HeadRevision = s.HeadRevision,
            Relationships = s.Relationships.Select(ToWireRow).ToImmutableList(),
            Schemas = s.Schemas.Select(ToWireSchema).ToImmutableList(),
            Counters = s.Counters.Select(ToWireCounter).ToImmutableList(),
            GcFloor = s.GcFloor,
        };

    private static StoredRelationship ToMemoryRow(StoredRelationshipWire w) =>
        new(WireConvert.ToRelationship(w.Relationship), w.CreatedRevision, w.DeletedRevision);

    private static StoredRelationshipWire ToWireRow(StoredRelationship r) =>
        new(WireConvert.ToWire(r.Relationship), r.CreatedRevision, r.DeletedRevision);

    private static SchemaVersion ToMemorySchema(SchemaVersionWire w) => new(w.Revision, w.Bytes, w.Hash);

    private static SchemaVersionWire ToWireSchema(SchemaVersion v) => new(v.Revision, v.Bytes, v.Hash);

    private static CounterVersion ToMemoryCounter(CounterVersionWire w) =>
        new(w.Revision, w.Name, w.Filter is null ? null : WireConvert.ToCoreFilter(w.Filter));

    private static CounterVersionWire ToWireCounter(CounterVersion v) =>
        new(v.Revision, v.Name, v.Filter is null ? null : WireConvert.ToFullFilter(v.Filter));
}
