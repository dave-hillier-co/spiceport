using System.Collections.Immutable;
using System.Text;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Freezes the event-log payload contract before any consumer depends on it: for every committed
/// revision, <see cref="LogEventFactory.EventFromState"/> must reproduce exactly the per-revision diff
/// the Watch changefeed already emits (<c>DatastoreState.ChangesAt</c> / <c>SchemaChangedAt</c>). Proven
/// three ways per revision: the factory output, the in-memory <c>ChangesAt</c>, and an independent
/// re-derivation of the touch/delete rule over the public wire rows, must all agree.
/// </summary>
public sealed class LogEventEquivalenceTests
{
    [Fact]
    public void EventFromState_MatchesChangesAt_ForEveryRevision()
    {
        const string ell = CoreConstants.Ellipsis;
        var relA = new RelationshipWire("doc", "a", "viewer", "user", "alice", ell, null, null, null);
        var relB = new RelationshipWire("doc", "b", "viewer", "user", "bob", ell, null, null, null);
        var relC = new RelationshipWire("doc", "c", "editor", "user", "carol", ell, null, null, null);

        // rev10: create A and B. rev20: create C + write schema + register counter c1.
        // rev30: touch A (close A@10..30 AND re-create A@30 with the same identity) -> ChangesAt must
        //        emit a single Touch (the live result), never a Delete, for A at rev30.
        var grainState = new DatastoreGrainState
        {
            HeadRevision = 30,
            Relationships = ImmutableList.Create(
                new StoredRelationshipWire(relA, 10, 30),
                new StoredRelationshipWire(relB, 10, null),
                new StoredRelationshipWire(relC, 20, null),
                new StoredRelationshipWire(relA, 30, null)),
            Schemas = ImmutableList.Create(
                new SchemaVersionWire(20, Encoding.UTF8.GetBytes("definition doc {}"), "h20")),
            Counters = ImmutableList.Create(
                new CounterVersionWire(20, "c1",
                    new FullRelationshipsFilterWire("doc", null, null, "viewer", null, null, 0))),
        };

        var state = DatastoreStateConverters.ToMemory(grainState);

        foreach (var rev in new long[] { 10, 20, 30 })
        {
            var ev = LogEventFactory.EventFromState(state, rev);

            // 1. factory vs the in-memory ChangesAt the Watch path already uses.
            var fromFactory = ev.RelationshipChanges.Select(Canonical).OrderBy(s => s).ToList();
            var fromChangesAt = state.ChangesAt(rev).Select(Canonical).OrderBy(s => s).ToList();
            Assert.Equal(fromChangesAt, fromFactory);

            // 2. factory vs an independent re-derivation of the touch/delete rule over the wire rows.
            Assert.Equal(ExpectedFromWire(grainState, rev), fromFactory);

            // 3. schema flag matches (the bool is now derived from the self-contained SchemaChange payload).
            Assert.Equal(state.SchemaChangedAt(rev), ev.SchemaChange is not null);
        }

        // Spot-check the tricky cases explicitly.
        Assert.Equal(new[] { "Touch:doc:a#viewer@user:alice#...", "Touch:doc:b#viewer@user:bob#..." },
            LogEventFactory.EventFromState(state, 10).RelationshipChanges.Select(Canonical).OrderBy(s => s));
        Assert.NotNull(LogEventFactory.EventFromState(state, 20).SchemaChange);
        var rev30 = LogEventFactory.EventFromState(state, 30).RelationshipChanges;
        Assert.Single(rev30); // touch-over-existing yields one Touch, not a Touch + Delete
        Assert.Equal("Touch:doc:a#viewer@user:alice#...", Canonical(rev30[0]));

        // Counter delta surfaces at its revision.
        var c20 = LogEventFactory.EventFromState(state, 20).CounterChanges;
        Assert.Contains(c20, c => c.Name == "c1" && c.Filter is not null);
    }

    private static string Canonical(RelationshipUpdateWire u) =>
        $"{u.Operation}:{u.Relationship.ResourceType}:{u.Relationship.ResourceId}#{u.Relationship.ResourceRelation}" +
        $"@{u.Relationship.SubjectType}:{u.Relationship.SubjectId}#{u.Relationship.SubjectRelation}";

    private static string Canonical(RelationshipUpdate u)
    {
        var op = u.Operation == UpdateOperation.Delete ? RelationshipUpdateOpWire.Delete : RelationshipUpdateOpWire.Touch;
        return $"{op}:{u.Relationship.Resource.ObjectType}:{u.Relationship.Resource.ObjectId}#{u.Relationship.Resource.Relation}" +
               $"@{u.Relationship.Subject.ObjectType}:{u.Relationship.Subject.ObjectId}#{u.Relationship.Subject.Relation}";
    }

    private static List<string> ExpectedFromWire(DatastoreGrainState s, long rev)
    {
        static string Key(RelationshipWire r) =>
            $"{r.ResourceType}:{r.ResourceId}#{r.ResourceRelation}@{r.SubjectType}:{r.SubjectId}#{r.SubjectRelation}";

        var touched = s.Relationships.Where(r => r.CreatedRevision == rev).Select(r => Key(r.Relationship)).ToHashSet();
        var result = new List<string>();
        foreach (var r in s.Relationships.Where(r => r.CreatedRevision == rev))
            result.Add($"Touch:{Key(r.Relationship)}");
        foreach (var r in s.Relationships.Where(r => r.DeletedRevision == rev && !touched.Contains(Key(r.Relationship))))
            result.Add($"Delete:{Key(r.Relationship)}");
        return result.OrderBy(x => x).ToList();
    }
}
