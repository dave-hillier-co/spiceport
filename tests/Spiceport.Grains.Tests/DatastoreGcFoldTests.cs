using Spiceport.Core;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Fold-level gates for GC <see cref="LogEvent"/>s (<c>ev.GcFloor</c> non-null): proves
/// <see cref="LogFold.ApplyEvent"/> on a GC event is exactly "collect below the floor, then advance the
/// head to the event's revision" — the same equivalence <see cref="LogEventEquivalenceTests"/> establishes
/// for ordinary events. A GC event carries no relationship/schema/counter changes, so any log-tail
/// consumer (the shard grains' <c>ShardFold</c>, the Watch feed) folds it harmlessly — the shard-level
/// gate is <c>ShardedReaderEquivalenceTests.Gc_Floor_Is_Enforced_Through_The_Shard_Grain</c>. The Leopard
/// membership-walk grain mesh needs no analogous fold-tolerance gate: each walk runs over a reader pinned
/// to one exact revision (see <c>Spiceport.Engine.MembershipWalk</c>), so a GC event elsewhere in the log
/// never touches it.
/// </summary>
public sealed class DatastoreGcFoldTests
{
    private const long Seed = 1_000;

    private static Relationship Rel(string rid, string sid) =>
        Relationship.Create(new ObjectAndRelation("doc", rid, "viewer"), new ObjectAndRelation("user", sid, CoreConstants.Ellipsis));

    private static LogEvent TouchEvent(long revision, string rid, string sid) =>
        new(revision,
            new[] { new RelationshipUpdateWire(RelationshipUpdateOpWire.Touch, WireConvert.ToWire(Rel(rid, sid))) },
            SchemaChange: null, Array.Empty<CounterDeltaWire>(), GcFloor: null);

    private static LogEvent DeleteEvent(long revision, string rid, string sid) =>
        new(revision,
            new[] { new RelationshipUpdateWire(RelationshipUpdateOpWire.Delete, WireConvert.ToWire(Rel(rid, sid))) },
            SchemaChange: null, Array.Empty<CounterDeltaWire>(), GcFloor: null);

    private static LogEvent GcEvent(long revision, long floor) =>
        new(revision, Array.Empty<RelationshipUpdateWire>(), SchemaChange: null,
            Array.Empty<CounterDeltaWire>(), GcFloor: floor);

    private static SortedSet<string> LiveSet(DatastoreGrainState state, long atRevision)
    {
        var set = new SortedSet<string>();
        foreach (var row in state.Relationships)
            if (row.CreatedRevision <= atRevision && (row.DeletedRevision is null || row.DeletedRevision > atRevision))
                set.Add($"{row.Relationship.ResourceId}:{row.Relationship.SubjectId}");
        return set;
    }

    [Fact]
    public void ApplyEvent_on_a_gc_event_equals_CollectBelow_plus_head_advance()
    {
        var state = DatastoreGrainState.Empty(Seed);
        state = LogFold.ApplyEvent(state, TouchEvent(Seed + 1, "a", "alice"));
        state = LogFold.ApplyEvent(state, TouchEvent(Seed + 2, "b", "bob"));
        state = LogFold.ApplyEvent(state, DeleteEvent(Seed + 3, "a", "alice")); // "a" now dead

        var floor = Seed + 3; // collects everything dead at/before this revision
        var gcRevision = Seed + 4;

        var viaFold = LogFold.ApplyEvent(state, GcEvent(gcRevision, floor));

        var viaDirect = DatastoreStateConverters.ToGrain(
            DatastoreStateConverters.ToMemory(state).CollectBelow(floor)) with
        { HeadRevision = gcRevision };

        Assert.Equal(gcRevision, viaFold.HeadRevision);
        Assert.Equal(floor, viaFold.GcFloor);
        Assert.Equal(viaDirect.GcFloor, viaFold.GcFloor);
        Assert.Equal(LiveSet(viaDirect, gcRevision), LiveSet(viaFold, gcRevision));
        // "a" (dead at Seed+3 <= floor) collected; "b" (still live) survives.
        Assert.DoesNotContain(viaFold.Relationships, r => r.Relationship.ResourceId == "a");
        Assert.Contains(viaFold.Relationships, r => r.Relationship.ResourceId == "b");
    }

    [Fact]
    public void ApplyEvent_gc_event_carries_no_relationship_schema_or_counter_changes()
    {
        var ev = GcEvent(Seed + 1, Seed);
        Assert.Empty(ev.RelationshipChanges);
        Assert.Null(ev.SchemaChange);
        Assert.Empty(ev.CounterChanges);
    }

}
