using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;
using Spiceport.Schema;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Cache-level gates for <see cref="MembershipIndexCache"/>'s incremental fold: mirroring
/// <see cref="Stage2SiloProjectionTests"/>, driven against a hand-rolled <see cref="IDatastoreGrain"/> fake (no
/// Orleans) so the grain-call behaviour is observable. A request for a fresher revision than the cached build
/// must advance by folding the log tail (<see cref="MembershipIndex.Apply"/>) rather than a second full scan,
/// except when the tail carries a schema change or the log has been compacted below the cache's watermark, in
/// which case a full rebuild is required.
/// </summary>
public sealed class LeopardIncrementalFoldTests
{
    private const long Seed = 1_000;

    private const string NestedSchema = """
        definition user {}

        definition group {
            relation member: user | group#member
        }

        definition document {
            relation viewer: user | group#member
            permission view = viewer
        }
        """;

    private static ObjectAndRelation Onr(string type, string id, string relation = CoreConstants.Ellipsis) =>
        new(type, id, relation);

    private static Relationship Rel(string rt, string rid, string rel, ObjectAndRelation subject) =>
        Relationship.Create(new ObjectAndRelation(rt, rid, rel), subject);

    private static LogEvent TouchEvent(long revision, Relationship rel) =>
        new(revision,
            new[] { new RelationshipUpdateWire(RelationshipUpdateOpWire.Touch, WireConvert.ToWire(rel)) },
            SchemaChange: null,
            Array.Empty<CounterDeltaWire>());

    private static LogEvent DeleteEvent(long revision, Relationship rel) =>
        new(revision,
            new[] { new RelationshipUpdateWire(RelationshipUpdateOpWire.Delete, WireConvert.ToWire(rel)) },
            SchemaChange: null,
            Array.Empty<CounterDeltaWire>());

    private static LogEvent SchemaChangeEvent(long revision) =>
        new(revision, Array.Empty<RelationshipUpdateWire>(),
            new SchemaVersionWire(revision, Array.Empty<byte>(), $"schema-{revision}"),
            Array.Empty<CounterDeltaWire>());

    [Fact]
    public async Task Incremental_CatchUp_AdvancesWithoutASecondFullScan()
    {
        var compiled = SchemaCompiler.CompileSchema(NestedSchema);
        var store = new ReferenceDatastore();
        var grain = new FakeLog(Seed);
        var cache = new MembershipIndexCache(new MembershipIndexOptions(), grain);

        var r1 = await store.ReadWriteTx(tx => tx.WriteRelationships(new List<RelationshipUpdate>
        {
            new(Rel("group", "g1", "member", Onr("user", "alice")), UpdateOperation.Create),
        }));
        var reader1 = new CountingReader(store.SnapshotReader(r1));

        var index1 = await cache.TryGet(compiled.Namespaces, compiled.Caveats, reader1, Seed);
        Assert.NotNull(index1);
        var initialScans = reader1.QueryCalls;
        Assert.True(initialScans > 0);
        Assert.True(index1!.TryCoveredResources("user", "alice", CoreConstants.Ellipsis, "group", "member", out var ids1));
        Assert.Equal(new[] { "g1" }, ids1);

        // A commit lands: bob joins g1, and alice is removed. Only the log advances (the FakeLog); the
        // reader passed to TryGet at the fresher revision must NOT be consulted for a second full scan.
        grain.Append(TouchEvent(Seed + 1, Rel("group", "g1", "member", Onr("user", "bob"))));
        grain.Append(DeleteEvent(Seed + 2, Rel("group", "g1", "member", Onr("user", "alice"))));

        var reader2 = new CountingReader(store.SnapshotReader(r1)); // must be unused by the fold path
        var index2 = await cache.TryGet(compiled.Namespaces, compiled.Caveats, reader2, Seed + 2);

        Assert.NotNull(index2);
        Assert.Equal(0, reader2.QueryCalls);          // no second full scan
        Assert.True(grain.ReadFromCalls >= 1);         // advanced via the log tail

        Assert.True(index2!.TryCoveredResources("user", "bob", CoreConstants.Ellipsis, "group", "member", out var bobIds));
        Assert.Equal(new[] { "g1" }, bobIds);
        Assert.True(index2.TryCoveredResources("user", "alice", CoreConstants.Ellipsis, "group", "member", out var aliceIds));
        Assert.Empty(aliceIds);
    }

    [Fact]
    public async Task CatchUp_ClampsExactlyAtRequestedRevision_DeleteBeyondItIsNotApplied()
    {
        var compiled = SchemaCompiler.CompileSchema(NestedSchema);
        var store = new ReferenceDatastore();
        var grain = new FakeLog(Seed);
        var cache = new MembershipIndexCache(new MembershipIndexOptions(), grain);

        var r1 = await store.ReadWriteTx(tx => tx.WriteRelationships(new List<RelationshipUpdate>
        {
            new(Rel("group", "g1", "member", Onr("user", "alice")), UpdateOperation.Create),
        }));
        var reader1 = new CountingReader(store.SnapshotReader(r1));
        var index1 = await cache.TryGet(compiled.Namespaces, compiled.Caveats, reader1, Seed);
        Assert.NotNull(index1);

        // The log advances two steps past the cached watermark in ONE page: bob joins (Seed+1), then
        // alice is removed (Seed+2). A request pinned at the INTERMEDIATE revision Seed+1 (after bob
        // joined, but strictly BEFORE alice's removal) must still see alice as a candidate: catching up
        // to satisfy this request must not silently fold in the later delete just because it arrived in
        // the same fetched page.
        grain.Append(TouchEvent(Seed + 1, Rel("group", "g1", "member", Onr("user", "bob"))));
        grain.Append(DeleteEvent(Seed + 2, Rel("group", "g1", "member", Onr("user", "alice"))));

        var readerMid = new CountingReader(store.SnapshotReader(r1));
        var indexMid = await cache.TryGet(compiled.Namespaces, compiled.Caveats, readerMid, Seed + 1);

        Assert.NotNull(indexMid);
        Assert.Equal(0, readerMid.QueryCalls); // still a fold, not a full rescan

        Assert.True(indexMid!.TryCoveredResources(
            "user", "alice", CoreConstants.Ellipsis, "group", "member", out var aliceAtMid));
        Assert.Equal(new[] { "g1" }, aliceAtMid); // alice is not yet removed as of Seed+1
        Assert.True(indexMid.TryCoveredResources(
            "user", "bob", CoreConstants.Ellipsis, "group", "member", out var bobAtMid));
        Assert.Equal(new[] { "g1" }, bobAtMid);

        // A LATER request (Seed+2) correctly observes the delete.
        var readerLater = new CountingReader(store.SnapshotReader(r1));
        var indexLater = await cache.TryGet(compiled.Namespaces, compiled.Caveats, readerLater, Seed + 2);
        Assert.NotNull(indexLater);
        Assert.True(indexLater!.TryCoveredResources(
            "user", "alice", CoreConstants.Ellipsis, "group", "member", out var aliceAtLater));
        Assert.Empty(aliceAtLater);
    }

    [Fact]
    public async Task RequestBehindTheSharedCachesWatermark_IsServedByAnIndependentScan_NeverTheAdvancedIndex()
    {
        var compiled = SchemaCompiler.CompileSchema(NestedSchema);
        var store = new ReferenceDatastore();
        var grain = new FakeLog(Seed);
        var cache = new MembershipIndexCache(new MembershipIndexOptions(), grain);

        var r1 = await store.ReadWriteTx(tx => tx.WriteRelationships(new List<RelationshipUpdate>
        {
            new(Rel("group", "g1", "member", Onr("user", "alice")), UpdateOperation.Create),
        }));
        var reader1 = new CountingReader(store.SnapshotReader(r1));
        var index1 = await cache.TryGet(compiled.Namespaces, compiled.Caveats, reader1, Seed);
        Assert.NotNull(index1);

        // Some OTHER caller advances the shared cache all the way to Seed+2, past alice's removal.
        grain.Append(TouchEvent(Seed + 1, Rel("group", "g1", "member", Onr("user", "bob"))));
        grain.Append(DeleteEvent(Seed + 2, Rel("group", "g1", "member", Onr("user", "alice"))));
        var readerAdvance = new CountingReader(store.SnapshotReader(r1));
        var indexAdvanced = await cache.TryGet(compiled.Namespaces, compiled.Caveats, readerAdvance, Seed + 2);
        Assert.NotNull(indexAdvanced);
        Assert.True(indexAdvanced!.TryCoveredResources(
            "user", "alice", CoreConstants.Ellipsis, "group", "member", out var aliceAdvanced));
        Assert.Empty(aliceAdvanced);

        // A request now arrives pinned at the OLDER revision Seed (e.g. AtExactSnapshot against a still
        // retained historical revision, or a straggling MinimizeLatency read). The shared cache's
        // watermark (Seed+2) is now AHEAD of it, and that fold has already dropped alice — the cache must
        // not hand back that too-advanced index. It must serve this caller from an independent scan
        // (over their OWN reader, pinned at Seed) instead, where alice is still a legitimate candidate,
        // and must leave the shared cache's own state untouched.
        var readerOld = new CountingReader(store.SnapshotReader(r1));
        var indexOld = await cache.TryGet(compiled.Namespaces, compiled.Caveats, readerOld, Seed);

        Assert.NotNull(indexOld);
        Assert.True(readerOld.QueryCalls > 0); // an independent full scan, not a reuse of the advanced index
        Assert.True(indexOld!.TryCoveredResources(
            "user", "alice", CoreConstants.Ellipsis, "group", "member", out var aliceAtOld));
        Assert.Equal(new[] { "g1" }, aliceAtOld);

        // The shared cache itself is unperturbed: a fresh request at Seed+2 still sees the advanced state.
        var readerRecheck = new CountingReader(store.SnapshotReader(r1));
        var indexRecheck = await cache.TryGet(compiled.Namespaces, compiled.Caveats, readerRecheck, Seed + 2);
        Assert.Equal(0, readerRecheck.QueryCalls);
        Assert.True(indexRecheck!.TryCoveredResources(
            "user", "alice", CoreConstants.Ellipsis, "group", "member", out var aliceRecheck));
        Assert.Empty(aliceRecheck);
    }

    [Fact]
    public async Task ConcurrentTryGet_AtTheSameRevision_NeverObservesATornSnapshot()
    {
        var compiled = SchemaCompiler.CompileSchema(NestedSchema);
        var store = new ReferenceDatastore();
        var grain = new FakeLog(Seed);
        var cache = new MembershipIndexCache(new MembershipIndexOptions(), grain);

        var r1 = await store.ReadWriteTx(tx => tx.WriteRelationships(new List<RelationshipUpdate>
        {
            new(Rel("group", "g1", "member", Onr("user", "alice")), UpdateOperation.Create),
        }));

        // Hammer TryGet from many concurrent tasks at an advancing sequence of revisions while the log
        // keeps growing underneath; every returned index must be internally consistent (matching schema,
        // and — since nothing here is ever deleted before the request's own revision — always reporting
        // alice as a covered candidate).
        grain.Append(TouchEvent(Seed + 1, Rel("group", "g1", "member", Onr("user", "bob"))));

        var tasks = Enumerable.Range(0, 64).Select(async i =>
        {
            var reader = new CountingReader(store.SnapshotReader(r1));
            var revision = i % 2 == 0 ? Seed : Seed + 1;
            var index = await cache.TryGet(compiled.Namespaces, compiled.Caveats, reader, revision);
            Assert.NotNull(index);
            Assert.True(index!.TryCoveredResources(
                "user", "alice", CoreConstants.Ellipsis, "group", "member", out var ids));
            Assert.Equal(new[] { "g1" }, ids);
        });

        await Task.WhenAll(tasks);
    }

    [Fact]
    public async Task SchemaChangeInTail_ForcesFullRebuild()
    {
        var compiled = SchemaCompiler.CompileSchema(NestedSchema);
        var store = new ReferenceDatastore();
        var grain = new FakeLog(Seed);
        var cache = new MembershipIndexCache(new MembershipIndexOptions(), grain);

        var r1 = await store.ReadWriteTx(tx => tx.WriteRelationships(new List<RelationshipUpdate>
        {
            new(Rel("group", "g1", "member", Onr("user", "alice")), UpdateOperation.Create),
        }));
        var reader1 = new CountingReader(store.SnapshotReader(r1));
        var index1 = await cache.TryGet(compiled.Namespaces, compiled.Caveats, reader1, Seed);
        Assert.NotNull(index1);
        var initialScans = reader1.QueryCalls;
        Assert.True(initialScans > 0);

        // The tail carries a schema-change event (even though the caller's schema hash ends up unchanged —
        // an A->B->A round trip — the cache conservatively rebuilds rather than trust the intervening fold).
        grain.Append(SchemaChangeEvent(Seed + 1));
        grain.Append(TouchEvent(Seed + 2, Rel("group", "g1", "member", Onr("user", "bob"))));

        var r2 = await store.ReadWriteTx(tx => tx.WriteRelationships(new List<RelationshipUpdate>
        {
            new(Rel("group", "g1", "member", Onr("user", "bob")), UpdateOperation.Create),
        }));
        var reader2 = new CountingReader(store.SnapshotReader(r2));
        var index2 = await cache.TryGet(compiled.Namespaces, compiled.Caveats, reader2, Seed + 2);

        Assert.NotNull(index2);
        Assert.Equal(initialScans, reader2.QueryCalls); // a fresh full scan happened via the passed reader
        Assert.True(index2!.TryCoveredResources("user", "alice", CoreConstants.Ellipsis, "group", "member", out var aliceIds));
        Assert.Equal(new[] { "g1" }, aliceIds);
        Assert.True(index2.TryCoveredResources("user", "bob", CoreConstants.Ellipsis, "group", "member", out var bobIds));
        Assert.Equal(new[] { "g1" }, bobIds);
    }

    [Fact]
    public async Task CompactionPastWatermark_ForcesFullRebuild()
    {
        var compiled = SchemaCompiler.CompileSchema(NestedSchema);
        var store = new ReferenceDatastore();
        var grain = new FakeLog(Seed);
        var cache = new MembershipIndexCache(new MembershipIndexOptions(), grain);

        var r1 = await store.ReadWriteTx(tx => tx.WriteRelationships(new List<RelationshipUpdate>
        {
            new(Rel("group", "g1", "member", Onr("user", "alice")), UpdateOperation.Create),
        }));
        var reader1 = new CountingReader(store.SnapshotReader(r1));
        var index1 = await cache.TryGet(compiled.Namespaces, compiled.Caveats, reader1, Seed);
        Assert.NotNull(index1);
        var initialScans = reader1.QueryCalls;
        Assert.True(initialScans > 0);

        grain.Append(TouchEvent(Seed + 1, Rel("group", "g1", "member", Onr("user", "bob"))));
        grain.CompactBelow(Seed + 1); // the cache's watermark (Seed) is now below the retained floor

        var r2 = await store.ReadWriteTx(tx => tx.WriteRelationships(new List<RelationshipUpdate>
        {
            new(Rel("group", "g1", "member", Onr("user", "bob")), UpdateOperation.Create),
        }));
        var reader2 = new CountingReader(store.SnapshotReader(r2));
        var index2 = await cache.TryGet(compiled.Namespaces, compiled.Caveats, reader2, Seed + 1);

        Assert.NotNull(index2);
        Assert.Equal(initialScans, reader2.QueryCalls); // recovered via a full rebuild against the fresh reader
        Assert.True(index2!.TryCoveredResources("user", "bob", CoreConstants.Ellipsis, "group", "member", out var bobIds));
        Assert.Equal(new[] { "g1" }, bobIds);
    }

    [Fact]
    public async Task Disabled_ReturnsNull_AndNeverTouchesTheGrain()
    {
        var compiled = SchemaCompiler.CompileSchema(NestedSchema);
        var store = new ReferenceDatastore();
        var grain = new FakeLog(Seed);
        var cache = new MembershipIndexCache(new MembershipIndexOptions { Enabled = false }, grain);

        var r1 = await store.ReadWriteTx(tx => tx.WriteRelationships(new List<RelationshipUpdate>
        {
            new(Rel("group", "g1", "member", Onr("user", "alice")), UpdateOperation.Create),
        }));
        var reader1 = new CountingReader(store.SnapshotReader(r1));

        var index = await cache.TryGet(compiled.Namespaces, compiled.Caveats, reader1, Seed);

        Assert.Null(index);
        Assert.Equal(0, reader1.QueryCalls);
        Assert.Equal(0, grain.ReadFromCalls);
    }

    /// <summary>Wraps an <see cref="IDatastoreReader"/>, counting <see cref="QueryRelationships"/> calls (the full-scan signal).</summary>
    private sealed class CountingReader(IDatastoreReader inner) : IDatastoreReader
    {
        public int QueryCalls { get; private set; }

        public IAsyncEnumerable<Relationship> QueryRelationships(
            RelationshipsFilter filter, CancellationToken cancellationToken = default)
        {
            QueryCalls++;
            return inner.QueryRelationships(filter, cancellationToken);
        }

        public IAsyncEnumerable<Relationship> ReverseQueryRelationships(
            SubjectsFilter subjectsFilter, ReverseQueryOptions? options = null, CancellationToken cancellationToken = default) =>
            inner.ReverseQueryRelationships(subjectsFilter, options, cancellationToken);

        public Task<byte[]?> ReadStoredSchema(CancellationToken cancellationToken = default) =>
            inner.ReadStoredSchema(cancellationToken);

        public Task<RelationshipsFilter?> ReadCounterFilter(string name, CancellationToken cancellationToken = default) =>
            inner.ReadCounterFilter(name, cancellationToken);

        public Task<ulong> CountRelationships(string name, CancellationToken cancellationToken = default) =>
            inner.CountRelationships(name, cancellationToken);

        public IAsyncEnumerable<RegisteredCounter> LookupCounters(CancellationToken cancellationToken = default) =>
            inner.LookupCounters(cancellationToken);

        public bool IsValid => inner.IsValid;
    }

    /// <summary>
    /// A minimal in-process <see cref="IDatastoreGrain"/> exposing only the log-tail read surface the cache
    /// consults (<see cref="ReadFrom"/>); every other member is unused by <see cref="MembershipIndexCache"/>
    /// and stubbed to throw, mirroring <see cref="Stage2SiloProjectionTests"/>'s fake.
    /// </summary>
    private sealed class FakeLog(long seed) : IDatastoreGrain
    {
        private readonly List<LogEvent> _events = new();
        private long _floor = seed;

        public int ReadFromCalls { get; private set; }

        public void Append(LogEvent ev) => _events.Add(ev);

        public void CompactBelow(long floorExclusive) => _floor = floorExclusive;

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

        public Task<DatastoreGrainState> ReadState() => throw new NotSupportedException();
        public Task<DatastoreHeadWire> GetHead() => throw new NotSupportedException();
        public Task<long?> AppendCommit(long expectedHead, ProposedWrite write) => throw new NotSupportedException();
        public Task<DatastoreHeadWire> SubscribeWatch(IDatastoreWatcher watcher) => throw new NotSupportedException();
        public Task UnsubscribeWatch(IDatastoreWatcher watcher) => throw new NotSupportedException();
    }
}
