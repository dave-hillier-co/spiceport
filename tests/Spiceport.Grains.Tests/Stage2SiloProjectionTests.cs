using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Datastore.Memory;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Stage-2 unit gates for <see cref="SiloProjection"/>: the per-silo materialized read projection folded
/// from the event log. Driven against a hand-rolled <see cref="IDatastoreGrain"/> fake (no Orleans) so the
/// grain-call behaviour is observable: it must bootstrap from a full snapshot ONCE per activation and then
/// advance only by pulling the log tail (never a full fetch per read — the per-Check fetch this stage
/// kills), catch up incrementally, and re-bootstrap from a snapshot only when it has fallen below the
/// grain's retained log window.
/// </summary>
public sealed class Stage2SiloProjectionTests
{
    private const long Seed = 1_000;

    private static Relationship Rel(string rid, string sid) =>
        Relationship.Create(new ObjectAndRelation("doc", rid, "viewer"), new ObjectAndRelation("user", sid, CoreConstants.Ellipsis));

    private static LogEvent TouchEvent(long revision, string rid, string sid) =>
        new(revision,
            new[] { new RelationshipUpdateWire(RelationshipUpdateOpWire.Touch, WireConvert.ToWire(Rel(rid, sid))) },
            SchemaChange: null,
            Array.Empty<CounterDeltaWire>());

    /// <summary>The set of live relationship identities a projection snapshot exposes at its head.</summary>
    private static SortedSet<string> LiveIdentities(DatastoreState state)
    {
        var set = new SortedSet<string>();
        foreach (var row in state.Relationships)
            if (row.CreatedRevision <= state.HeadRevision && (row.DeletedRevision is null || row.DeletedRevision > state.HeadRevision))
                set.Add($"{row.Relationship.Resource.ObjectId}:{row.Relationship.Subject.ObjectId}");
        return set;
    }

    [Fact]
    public async Task Bootstrap_ReadsFullSnapshotOnce_ThenServesEveryReadFromMemory()
    {
        var grain = new FakeLog(Seed);
        grain.Append(TouchEvent(Seed + 1, "a", "alice"));
        grain.Append(TouchEvent(Seed + 2, "b", "bob"));
        var projection = new SiloProjection(grain);

        // Many reads at (and below) the head: exactly ONE full ReadState bootstrap, no per-read fetch.
        for (var i = 0; i < 5; i++)
        {
            var state = await projection.StateAtLeast(Seed + 2);
            Assert.Equal(new SortedSet<string> { "a:alice", "b:bob" }, LiveIdentities(state));
        }
        _ = await projection.StateAtLeast(Seed + 1);

        Assert.Equal(1, grain.ReadStateCalls);
        Assert.Equal(Seed + 2, projection.AppliedWatermark);
    }

    [Fact]
    public async Task CatchesUp_ViaLogTail_WithoutRebootstrapping()
    {
        var grain = new FakeLog(Seed);
        grain.Append(TouchEvent(Seed + 1, "a", "alice"));
        var projection = new SiloProjection(grain);

        var first = await projection.StateAtLeast(Seed + 1);
        Assert.Equal(new SortedSet<string> { "a:alice" }, LiveIdentities(first));
        Assert.Equal(1, grain.ReadStateCalls);

        // Two more commits land AFTER the bootstrap. A read at the new head catches up via ReadFrom alone.
        grain.Append(TouchEvent(Seed + 2, "b", "bob"));
        grain.Append(TouchEvent(Seed + 3, "c", "carol"));

        var caughtUp = await projection.StateAtLeast(Seed + 3);
        Assert.Equal(new SortedSet<string> { "a:alice", "b:bob", "c:carol" }, LiveIdentities(caughtUp));
        Assert.Equal(Seed + 3, projection.AppliedWatermark);
        Assert.Equal(1, grain.ReadStateCalls);            // no re-bootstrap
        Assert.True(grain.ReadFromCalls >= 1);            // advanced by the tail feed
    }

    [Fact]
    public async Task Rebootstraps_FromSnapshot_WhenCursorFallsBelowGcWindow()
    {
        var grain = new FakeLog(Seed);
        grain.Append(TouchEvent(Seed + 1, "a", "alice"));
        var projection = new SiloProjection(grain);

        _ = await projection.StateAtLeast(Seed + 1); // bootstrap at watermark Seed+1
        Assert.Equal(1, grain.ReadStateCalls);

        // More commits land, then the log is compacted past the projection's watermark: ReadFrom(Seed+1) now
        // throws RevisionNotFound. The projection must recover by re-bootstrapping from a full snapshot.
        grain.Append(TouchEvent(Seed + 2, "b", "bob"));
        grain.Append(TouchEvent(Seed + 3, "c", "carol"));
        grain.CompactBelow(Seed + 2); // any cursor < Seed+2 is no longer served by ReadFrom

        var recovered = await projection.StateAtLeast(Seed + 3);
        Assert.Equal(new SortedSet<string> { "a:alice", "b:bob", "c:carol" }, LiveIdentities(recovered));
        Assert.Equal(Seed + 3, projection.AppliedWatermark);
        Assert.Equal(2, grain.ReadStateCalls); // exactly one extra snapshot read for the recovery
    }

    /// <summary>
    /// A minimal in-process <see cref="IDatastoreGrain"/> that owns an append-only <see cref="LogEvent"/>
    /// list. <see cref="ReadState"/> folds the whole log into a snapshot (the full fetch); <see cref="ReadFrom"/>
    /// serves the tail and throws <see cref="RevisionNotFoundException"/> below the compaction floor. The write
    /// surface (<see cref="GetHead"/>/<see cref="AppendCommit"/>) is unused by the projection.
    /// </summary>
    private sealed class FakeLog(long seed) : IDatastoreGrain
    {
        private readonly List<LogEvent> _events = new();
        private long _floor = seed;

        public int ReadStateCalls { get; private set; }
        public int ReadFromCalls { get; private set; }

        public void Append(LogEvent ev) => _events.Add(ev);

        public void CompactBelow(long floorExclusive) => _floor = floorExclusive;

        public Task<DatastoreGrainState> ReadState()
        {
            ReadStateCalls++;
            var state = DatastoreGrainState.Empty(seed);
            foreach (var ev in _events)
                state = LogFold.ApplyEvent(state, ev);
            return Task.FromResult(state);
        }

        public Task<LogSegment> ReadFrom(long afterRevision, int maxCount)
        {
            ReadFromCalls++;
            if (afterRevision < _floor)
                throw new RevisionNotFoundException(new TimestampRevision(afterRevision));
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
    }
}
