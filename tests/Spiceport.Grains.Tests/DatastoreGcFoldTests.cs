using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;
using Spiceport.Schema;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Fold-level gates for GC <see cref="LogEvent"/>s (<c>ev.GcFloor</c> non-null): proves
/// <see cref="LogFold.ApplyEvent"/> on a GC event is exactly "collect below the floor, then advance the
/// head to the event's revision" — the same equivalence <see cref="LogEventEquivalenceTests"/> establishes
/// for ordinary events — and that <see cref="SiloProjection"/> / <see cref="MembershipIndexCache"/> fold a
/// GC event in their log tail harmlessly (it carries no relationship/schema/counter changes).
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

    [Fact]
    public async Task SiloProjection_folds_a_gc_event_in_its_tail_and_matches_ReadState()
    {
        var grain = new FakeLog(Seed);
        grain.Append(TouchEvent(Seed + 1, "a", "alice"));
        grain.Append(TouchEvent(Seed + 2, "b", "bob"));

        var projection = new SiloProjection(grain);
        _ = await projection.StateAtLeast(Seed + 2); // bootstrap + fold up to here

        grain.Append(DeleteEvent(Seed + 3, "a", "alice"));
        grain.Append(GcEvent(Seed + 4, Seed + 3)); // collects "a" (dead at Seed+3)

        var folded = await projection.StateAtLeast(Seed + 4);
        var readState = await grain.ReadState(); // independently folds the whole log from empty

        Assert.Equal(LiveIdsFromMemory(DatastoreStateConverters.ToMemory(readState)), LiveIdsFromMemory(folded));
        Assert.DoesNotContain(readState.Relationships, r => r.Relationship.ResourceId == "a");
        Assert.Contains(readState.Relationships, r => r.Relationship.ResourceId == "b");
    }

    private static SortedSet<string> LiveIdsFromMemory(DatastoreState state)
    {
        var set = new SortedSet<string>();
        foreach (var rel in state.LiveAt(state.HeadRevision))
            set.Add($"{rel.Resource.ObjectId}:{rel.Subject.ObjectId}");
        return set;
    }

    [Fact]
    public async Task MembershipIndexCache_CatchUp_folds_a_gc_event_in_the_tail_harmlessly()
    {
        const string schema = """
            definition user {}
            definition doc {
                relation viewer: user
                permission view = viewer
            }
            """;
        var compiled = SchemaCompiler.CompileSchema(schema);
        var store = new ReferenceDatastore();
        var grain = new FakeLog(Seed);
        var cache = new MembershipIndexCache(new MembershipIndexOptions(), grain);

        var r1 = await store.ReadWriteTx(tx => tx.WriteRelationships(new List<RelationshipUpdate>
        {
            new(Rel("a", "alice"), UpdateOperation.Create),
        }));
        var reader1 = store.SnapshotReader(r1);
        var index1 = await cache.TryGet(compiled.Namespaces, compiled.Caveats, reader1, Seed);
        Assert.NotNull(index1);

        // The log advances via a delete, THEN a GC event that carries no content — the cache must fold
        // past it without error and the candidates must be unaffected.
        grain.Append(DeleteEvent(Seed + 1, "a", "alice"));
        grain.Append(GcEvent(Seed + 2, Seed + 1));

        var reader2 = store.SnapshotReader(r1); // unused by the incremental fold path
        var index2 = await cache.TryGet(compiled.Namespaces, compiled.Caveats, reader2, Seed + 2);

        Assert.NotNull(index2);
        Assert.True(index2!.TryCoveredResources("user", "alice", CoreConstants.Ellipsis, "doc", "viewer", out var ids));
        Assert.Empty(ids); // alice's viewer relationship was deleted before the GC event
    }

    /// <summary>
    /// A minimal in-process <see cref="IDatastoreGrain"/> owning an append-only <see cref="LogEvent"/> list,
    /// mirroring <see cref="Stage2SiloProjectionTests"/>'s fake (kept separate here so this file has no
    /// dependency on that test class's private nested type).
    /// </summary>
    private sealed class FakeLog(long seed) : IDatastoreGrain
    {
        private readonly List<LogEvent> _events = new();

        public void Append(LogEvent ev) => _events.Add(ev);

        public Task<DatastoreGrainState> ReadState()
        {
            var state = DatastoreGrainState.Empty(seed);
            foreach (var ev in _events)
                state = LogFold.ApplyEvent(state, ev);
            return Task.FromResult(state);
        }

        public Task<LogSegment> ReadFrom(long afterRevision, int maxCount)
        {
            var head = _events.Count > 0 ? _events[^1].Revision : seed;
            var page = _events
                .Where(e => e.Revision > afterRevision)
                .OrderBy(e => e.Revision)
                .Take(maxCount < 0 ? int.MaxValue : maxCount)
                .ToList();
            return Task.FromResult(new LogSegment(page, head));
        }

        public Task<DatastoreHeadWire> GetHead() => throw new NotSupportedException();
        public Task<long?> AppendCommit(long expectedHead, ProposedWrite write) => throw new NotSupportedException();
        public Task<DatastoreHeadWire> SubscribeWatch(IDatastoreWatcher watcher) => throw new NotSupportedException();
        public Task UnsubscribeWatch(IDatastoreWatcher watcher) => throw new NotSupportedException();
        public Task<long?> RunGc() => throw new NotSupportedException();
    }
}
