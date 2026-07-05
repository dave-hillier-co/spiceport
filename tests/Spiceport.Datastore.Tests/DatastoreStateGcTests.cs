using System.Collections.Immutable;
using Spiceport.Core;

namespace Spiceport.Datastore.Tests;

/// <summary>
/// Unit gates for <see cref="DatastoreState.CollectBelow"/> — the single shared MVCC-collection fold
/// primitive that a GC <c>LogEvent</c> applies (see <c>Spiceport.Server</c>'s <c>LogFold.ApplyEvent</c>).
/// Exercised directly against the internal MVCC state (friend access via
/// <c>InternalsVisibleTo("Spiceport.Datastore.Tests")</c>) since <see cref="ReferenceDatastore"/> never
/// calls it itself.
/// </summary>
public class DatastoreStateGcTests
{
    private static Relationship Rel(string rid, string sid) =>
        Relationship.Create(new ObjectAndRelation("doc", rid, "viewer"), new ObjectAndRelation("user", sid, CoreConstants.Ellipsis));

    private static StoredRelationship Row(string rid, string sid, long created, long? deleted, DateTimeOffset? expiration = null) =>
        new(Rel(rid, sid) with { OptionalExpiration = expiration }, created, deleted);

    private static long Nanos(DateTimeOffset dt) => (dt - DateTimeOffset.UnixEpoch).Ticks * 100L;

    private static SortedSet<string> LiveIds(DatastoreState state, long atRevision)
    {
        var set = new SortedSet<string>();
        foreach (var rel in state.LiveAt(atRevision))
        {
            var now = DateTimeOffset.UtcNow;
            if (rel.OptionalExpiration is { } exp && exp <= now)
                continue;
            set.Add($"{rel.Resource.ObjectId}:{rel.Subject.ObjectId}");
        }
        return set;
    }

    // --- structural relationship collection ---

    [Fact]
    public void CollectBelow_drops_rows_dead_below_the_floor()
    {
        var state = new DatastoreState(
            100,
            ImmutableList.Create(
                Row("a", "alice", 10, 20),  // dead at 20, well below floor 50
                Row("b", "bob", 10, null)), // still live
            ImmutableList<SchemaVersion>.Empty,
            ImmutableList<CounterVersion>.Empty);

        var collected = state.CollectBelow(50);

        Assert.Single(collected.Relationships);
        Assert.Equal("bob", collected.Relationships[0].Relationship.Subject.ObjectId);
        Assert.Equal(50, collected.GcFloor);
    }

    [Fact]
    public void CollectBelow_keeps_rows_created_at_or_below_floor_that_are_still_live_or_straddling()
    {
        var state = new DatastoreState(
            100,
            ImmutableList.Create(
                Row("a", "alice", 10, null),  // created below floor, still live: KEEP
                Row("b", "bob", 10, 60)),      // created below floor, deleted AFTER floor: KEEP (straddles)
            ImmutableList<SchemaVersion>.Empty,
            ImmutableList<CounterVersion>.Empty);

        var collected = state.CollectBelow(50);

        Assert.Equal(2, collected.Relationships.Count);
    }

    [Fact]
    public void Reads_at_or_above_floor_are_identical_before_and_after_collection()
    {
        var state = new DatastoreState(
            100,
            ImmutableList.Create(
                Row("a", "alice", 10, 20),
                Row("b", "bob", 10, null),
                Row("c", "carol", 30, 70),
                Row("d", "dave", 60, null)),
            ImmutableList<SchemaVersion>.Empty,
            ImmutableList<CounterVersion>.Empty);

        var collected = state.CollectBelow(50);

        foreach (var rev in new long[] { 50, 60, 70, 100 })
            Assert.Equal(LiveIds(state, rev), LiveIds(collected, rev));
    }

    [Fact]
    public void Reads_below_the_floor_throw_via_MvccSnapshotReader()
    {
        var state = new DatastoreState(
            100,
            ImmutableList.Create(Row("a", "alice", 10, null)),
            ImmutableList<SchemaVersion>.Empty,
            ImmutableList<CounterVersion>.Empty);
        var collected = state.CollectBelow(50);

        Assert.Throws<RevisionNotFoundException>(() => new MvccSnapshotReader(collected, 49, _ => true));
        // At/above the floor is fine.
        _ = new MvccSnapshotReader(collected, 50, _ => true);
    }

    [Fact]
    public void CollectBelow_is_a_no_op_at_or_below_the_current_floor()
    {
        var state = new DatastoreState(
            100,
            ImmutableList.Create(Row("a", "alice", 10, 20)),
            ImmutableList<SchemaVersion>.Empty,
            ImmutableList<CounterVersion>.Empty);

        var collectedOnce = state.CollectBelow(50);
        var collectedAgainLower = collectedOnce.CollectBelow(30);
        var collectedAgainSame = collectedOnce.CollectBelow(50);

        Assert.Same(collectedOnce, collectedAgainLower);
        Assert.Same(collectedOnce, collectedAgainSame);
        Assert.Equal(50, collectedOnce.GcFloor);
    }

    [Fact]
    public void CollectBelow_floor_only_ever_advances()
    {
        var state = new DatastoreState(
            100,
            ImmutableList.Create(
                Row("a", "alice", 10, 20),  // dead by revision 20
                Row("b", "bob", 60, 90),    // dead by revision 90
                Row("c", "carol", 70, null)), // never dies
            ImmutableList<SchemaVersion>.Empty,
            ImmutableList<CounterVersion>.Empty);

        var first = state.CollectBelow(30);
        Assert.Equal(30, first.GcFloor);
        Assert.Equal(2, first.Relationships.Count); // "a" collected; "b" (dead@90) and "c" survive

        var second = first.CollectBelow(95);
        Assert.Equal(95, second.GcFloor);
        Assert.Single(second.Relationships); // "b" (dead@90 <= 95) now collected too; only "c" remains
        Assert.Equal("carol", second.Relationships[0].Relationship.Subject.ObjectId);
    }

    // --- expiration sweep ---

    [Fact]
    public void CollectBelow_expiration_sweep_drops_expired_rows_without_changing_any_servable_read()
    {
        var floorTime = DateTimeOffset.UtcNow.AddDays(-1); // already elapsed
        var floor = Nanos(floorTime);

        var state = new DatastoreState(
            floor + 1_000_000_000L,
            ImmutableList.Create(
                Row("a", "alice", 10, null, expiration: floorTime.AddSeconds(-1)), // expired at/before floor
                Row("b", "bob", 10, null)),                                        // never expires
            ImmutableList<SchemaVersion>.Empty,
            ImmutableList<CounterVersion>.Empty);

        // Both rows are "live" (MVCC-visible) at every revision >= floor before collection; "a" is filtered
        // out of every QueryRelationships-style read regardless (real wall clock has already passed its
        // expiration), so removing it changes nothing observable.
        var before = LiveIds(state, state.HeadRevision);
        var collected = state.CollectBelow(floor);
        var after = LiveIds(collected, collected.HeadRevision);

        Assert.Equal(before, after);
        Assert.DoesNotContain(collected.Relationships, r => r.Relationship.Subject.ObjectId == "alice");
        Assert.Contains(collected.Relationships, r => r.Relationship.Subject.ObjectId == "bob");
    }

    [Fact]
    public void CollectBelow_keeps_rows_expiring_after_the_floor()
    {
        var floorTime = DateTimeOffset.UtcNow.AddDays(-1);
        var floor = Nanos(floorTime);

        var state = new DatastoreState(
            floor + 1_000_000_000L,
            ImmutableList.Create(Row("a", "alice", 10, null, expiration: floorTime.AddDays(30))),
            ImmutableList<SchemaVersion>.Empty,
            ImmutableList<CounterVersion>.Empty);

        var collected = state.CollectBelow(floor);

        Assert.Single(collected.Relationships);
    }

    // --- schema compaction ---

    [Fact]
    public void CollectBelow_schema_compaction_keeps_latest_at_or_below_floor_plus_everything_above()
    {
        var state = new DatastoreState(
            100,
            ImmutableList<StoredRelationship>.Empty,
            ImmutableList.Create(
                new SchemaVersion(10, "s10"u8.ToArray(), "h10"),
                new SchemaVersion(20, "s20"u8.ToArray(), "h20"), // superseded by 20 <= floor: dropped
                new SchemaVersion(60, "s60"u8.ToArray(), "h60")), // above floor: kept
            ImmutableList<CounterVersion>.Empty);

        var collected = state.CollectBelow(50);

        Assert.Equal(2, collected.Schemas.Count);
        Assert.Equal("h20", collected.SchemaHashAt(50)); // the version effective at the floor survived
        Assert.Equal("h60", collected.SchemaHashAt(100));
        Assert.Equal(state.SchemaHashAt(50), collected.SchemaHashAt(50));
        Assert.Equal(state.SchemaHashAt(100), collected.SchemaHashAt(100));
    }

    // --- counter compaction ---

    [Fact]
    public void CollectBelow_counter_compaction_keeps_latest_live_version_per_name()
    {
        var filterA = new RelationshipsFilter { OptionalResourceType = "doc" };
        var filterA2 = new RelationshipsFilter { OptionalResourceType = "doc", OptionalResourceRelation = "viewer" };

        var state = new DatastoreState(
            100,
            ImmutableList<StoredRelationship>.Empty,
            ImmutableList<SchemaVersion>.Empty,
            ImmutableList.Create(
                new CounterVersion(10, "c1", filterA),   // superseded below floor: dropped
                new CounterVersion(20, "c1", filterA2),  // latest <= floor for c1: kept
                new CounterVersion(70, "c2", filterA)));  // above floor: kept

        var collected = state.CollectBelow(50);

        Assert.Equal(2, collected.Counters.Count);
        Assert.Equal(filterA2, collected.CounterFilterAt("c1", 100));
        Assert.Equal(filterA, collected.CounterFilterAt("c2", 100));
        Assert.Equal(state.CounterFilterAt("c1", 100), collected.CounterFilterAt("c1", 100));
    }

    [Fact]
    public void CollectBelow_drops_a_tombstoned_counter_entirely_when_it_is_the_latest_at_or_below_floor()
    {
        var filter = new RelationshipsFilter { OptionalResourceType = "doc" };
        var state = new DatastoreState(
            100,
            ImmutableList<StoredRelationship>.Empty,
            ImmutableList<SchemaVersion>.Empty,
            ImmutableList.Create(
                new CounterVersion(10, "c1", filter),
                new CounterVersion(20, "c1", null))); // unregistered below the floor, never re-registered

        var collected = state.CollectBelow(50);

        Assert.Empty(collected.Counters);
        Assert.Null(collected.CounterFilterAt("c1", 100));
        Assert.Equal(state.CounterFilterAt("c1", 100), collected.CounterFilterAt("c1", 100));
    }

    [Fact]
    public void CollectBelow_keeps_a_tombstone_reregistered_above_the_floor()
    {
        var filter = new RelationshipsFilter { OptionalResourceType = "doc" };
        var state = new DatastoreState(
            100,
            ImmutableList<StoredRelationship>.Empty,
            ImmutableList<SchemaVersion>.Empty,
            ImmutableList.Create(
                new CounterVersion(10, "c1", filter),
                new CounterVersion(20, "c1", null),  // tombstone at/below floor: droppable in isolation
                new CounterVersion(70, "c1", filter))); // re-registered above the floor: must survive

        var collected = state.CollectBelow(50);

        // The tombstone itself may be dropped, but the re-registration above the floor must remain.
        Assert.Equal(filter, collected.CounterFilterAt("c1", 100));
        Assert.Null(collected.CounterFilterAt("c1", 60)); // still unregistered right at/above the floor
    }
}
