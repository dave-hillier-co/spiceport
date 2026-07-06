using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.TestingHost;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Stage-1 gates for the journaled (event-sourced) datastore write path: the append-only
/// <see cref="LogEvent"/> log is the source of truth, persisted via the grain's
/// <c>ICustomStorageInterface</c> over an Orleans grain-storage provider. Drives writes through the real
/// <see cref="GrainBackedDatastore.ReadWriteTx"/> against a single-silo in-memory <see cref="TestCluster"/>,
/// then asserts the log feed (<see cref="IDatastoreLog.ReadFrom"/>) and the fold reproduce the state.
/// </summary>
[Collection("MeshCluster")]
public sealed class Stage1JournaledWritePathTests
{
    private sealed class DatastoreSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder b)
        {
            b.AddMemoryGrainStorage("datastore");
            b.AddCustomStorageBasedLogConsistencyProvider("CustomStorage");
        }
    }

    private static async Task<TestCluster> NewClusterAsync()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<DatastoreSiloConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        return cluster;
    }

    private static IDatastoreGrain Grain(TestCluster cluster) =>
        cluster.GrainFactory.GetGrain<IDatastoreGrain>(IDatastoreGrain.Key);

    private static readonly DateTimeOffset Expiry = new(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static Relationship Rel(string rt, string rid, string rel, string st, string sid) =>
        Relationship.Create(new ObjectAndRelation(rt, rid, rel), new ObjectAndRelation(st, sid, CoreConstants.Ellipsis));

    /// <summary>Runs a representative spread of writes through the grain-backed datastore.</summary>
    private static async Task RunWorkload(IDatastore ds)
    {
        // Schema + counter + creates.
        await ds.ReadWriteTx(async tx =>
        {
            await tx.WriteStoredSchema(Encoding.UTF8.GetBytes("definition user {}\ndefinition doc { relation viewer: user }"));
            await tx.WriteRelationships(new[]
            {
                new RelationshipUpdate(Rel("doc", "a", "viewer", "user", "alice"), UpdateOperation.Create),
                new RelationshipUpdate(Rel("doc", "b", "viewer", "user", "bob"), UpdateOperation.Create),
            });
            await tx.WriteCounter("doc_viewers", new RelationshipsFilter { OptionalResourceType = "doc", OptionalResourceRelation = "viewer" });
        });

        // A caveated + an expiring relationship.
        await ds.ReadWriteTx(async tx =>
        {
            var caveated = Relationship.Create(
                new ObjectAndRelation("doc", "c", "viewer"),
                new ObjectAndRelation("user", "carol", CoreConstants.Ellipsis),
                caveat: new ContextualizedCaveat("is_active", new Dictionary<string, object?> { ["level"] = 7 }));
            var expiring = Rel("doc", "d", "viewer", "user", "dave") with { OptionalExpiration = Expiry };
            await tx.WriteRelationships(new[]
            {
                new RelationshipUpdate(caveated, UpdateOperation.Create),
                new RelationshipUpdate(expiring, UpdateOperation.Create),
            });
        });

        // A touch-over-existing + a delete.
        await ds.ReadWriteTx(async tx =>
        {
            await tx.WriteRelationships(new[]
            {
                new RelationshipUpdate(Rel("doc", "a", "viewer", "user", "alice"), UpdateOperation.Touch),
                new RelationshipUpdate(Rel("doc", "b", "viewer", "user", "bob"), UpdateOperation.Delete),
            });
        });
    }

    /// <summary>
    /// Gate 1: paged ReadFrom equivalence. ReadFrom(0, inf) in page sizes 1, 7 and inf must yield ONE
    /// identical ordered event list (revision-ascending), proving paging is stable.
    /// </summary>
    [Fact]
    public async Task ReadFrom_IsPageSizeInvariant()
    {
        await using var scope = new Scope(await NewClusterAsync());
        await using var host = new PrivateProjectionHost(scope.Cluster.GrainFactory);
        IDatastore ds = new GrainBackedDatastore(scope.Cluster.GrainFactory, host);
        var grain = Grain(scope.Cluster);
        // The "from the beginning" cursor: the pre-first-write head. Revisions are timestamp-nanos, so 0 is
        // below the GC window; the seed head is the earliest valid cursor that precedes every event.
        var from = (await grain.GetHead()).Head;
        await RunWorkload(ds);

        var whole = (await grain.ReadFrom(from, int.MaxValue)).Events;
        var byOne = await DrainPaged(grain, from, 1);
        var bySeven = await DrainPaged(grain, from, 7);

        Assert.NotEmpty(whole);
        Assert.Equal(Canonical(whole), Canonical(byOne));
        Assert.Equal(Canonical(whole), Canonical(bySeven));

        // Revisions are strictly ascending (the log offset order).
        var revs = whole.Select(e => e.Revision).ToList();
        Assert.Equal(revs.OrderBy(r => r).ToList(), revs);
    }

    /// <summary>
    /// Gate 2: folding the event list from EMPTY via the same ApplyEvent the grain uses reproduces the
    /// grain's materialized state (LiveAt head, schema, counters) AND matches an independent
    /// ReferenceDatastore oracle run with the same ops.
    /// </summary>
    [Fact]
    public async Task Replay_FromEmpty_ReconstructsGrainStateAndMatchesOracle()
    {
        await using var scope = new Scope(await NewClusterAsync());
        await using var host = new PrivateProjectionHost(scope.Cluster.GrainFactory);
        IDatastore ds = new GrainBackedDatastore(scope.Cluster.GrainFactory, host);
        var grain = Grain(scope.Cluster);
        var from = (await grain.GetHead()).Head;
        await RunWorkload(ds);

        var events = (await grain.ReadFrom(from, int.MaxValue)).Events;

        // Fold from empty (seeded at the pre-first-write head) via the production fold.
        var folded = DatastoreGrainState.Empty(from);
        foreach (var ev in events)
            folded = LogFold.ApplyEvent(folded, ev);

        var grainState = await grain.ReadState();

        // The fold's live set == the grain's live set at head.
        Assert.Equal(LiveSet(folded, grainState.HeadRevision), LiveSet(grainState, grainState.HeadRevision));
        // Schema bytes effective at head match.
        Assert.Equal(
            SchemaBytesAt(grainState, grainState.HeadRevision),
            SchemaBytesAt(folded, grainState.HeadRevision));
        // Counter filter survives the fold (resource type + relation).
        Assert.Equal(
            CounterResource(grainState, "doc_viewers", grainState.HeadRevision),
            CounterResource(folded, "doc_viewers", grainState.HeadRevision));

        // Independent oracle: the SAME ops through a fresh ReferenceDatastore yield the same live set.
        var oracle = new ReferenceDatastore();
        await RunWorkload(oracle);
        var oracleHead = await oracle.HeadRevision();
        var oracleReader = oracle.SnapshotReader(oracleHead.Revision);
        var oracleSet = new SortedSet<string>();
        await foreach (var r in oracleReader.QueryRelationships(new RelationshipsFilter()))
            oracleSet.Add(Identity(r));
        // Compare identity sets (revisions differ between backends; expiration is dropped symmetrically by
        // the same QueryRelationships path, so exclude the expiring row's possibly-filtered state by using
        // the grain live set's identities too).
        Assert.Equal(oracleSet, GrainLiveIdentities(grainState));
    }

    /// <summary>Gate 4: a cursor older than the GC window throws RevisionNotFoundException.</summary>
    [Fact]
    public async Task ReadFrom_BelowGcWindow_Throws()
    {
        await using var scope = new Scope(await NewClusterAsync());
        await using var host = new PrivateProjectionHost(scope.Cluster.GrainFactory);
        IDatastore ds = new GrainBackedDatastore(scope.Cluster.GrainFactory, host);
        await RunWorkload(ds);
        var grain = Grain(scope.Cluster);

        var head = (await grain.GetHead()).Head;
        // One nanosecond past 24h before head is outside the retained window.
        var gcWindowNanos = (long)TimeSpan.FromHours(24).TotalMilliseconds * 1_000_000L;
        var tooOld = head - gcWindowNanos - 1;

        await Assert.ThrowsAsync<RevisionNotFoundException>(() => grain.ReadFrom(tooOld, int.MaxValue));
    }

    /// <summary>
    /// Gate 5: a net counter delta whose guard precondition is false in the fold base (same-commit
    /// register+unregister, and the inverse) must fold WITHOUT throwing. The fold appends the net counter
    /// version directly (matching Commit), not via the guarded WriteCounter/DeleteCounter — otherwise the
    /// journaled append (TransitionState) and reactivation replay would throw on a perfectly valid commit.
    /// </summary>
    [Fact]
    public async Task CounterNetDelta_FoldsWithoutThrowing()
    {
        await using var scope = new Scope(await NewClusterAsync());
        await using var host = new PrivateProjectionHost(scope.Cluster.GrainFactory);
        IDatastore ds = new GrainBackedDatastore(scope.Cluster.GrainFactory, host);
        var grain = Grain(scope.Cluster);
        var from = (await grain.GetHead()).Head;

        // Case 1: register then unregister "x" in ONE commit over a base where "x" was never registered ->
        // net tombstone. Pre-fix the fold replayed DeleteCounter("x") over a base with no live "x" and threw.
        await ds.ReadWriteTx(async tx =>
        {
            await tx.WriteCounter("x", new RelationshipsFilter { OptionalResourceType = "doc" });
            await tx.DeleteCounter("x");
        });

        // Case 2: "y" live, then unregister+register "y" in ONE commit -> net live over existing. Pre-fix the
        // fold replayed WriteCounter("y", ...) over a base with "y" still live and threw AlreadyRegistered.
        await ds.ReadWriteTx(tx => tx.WriteCounter("y", new RelationshipsFilter { OptionalResourceType = "doc" }));
        await ds.ReadWriteTx(async tx =>
        {
            await tx.DeleteCounter("y");
            await tx.WriteCounter("y", new RelationshipsFilter { OptionalResourceType = "folder" });
        });

        // Re-folding every event from empty must not throw, and must reproduce the grain's counter state.
        var events = (await grain.ReadFrom(from, int.MaxValue)).Events;
        var folded = DatastoreGrainState.Empty(from);
        foreach (var ev in events)
            folded = LogFold.ApplyEvent(folded, ev);

        var grainState = await grain.ReadState();
        Assert.Null(CounterResource(grainState, "x", grainState.HeadRevision));   // ends tombstoned
        Assert.Null(CounterResource(folded, "x", grainState.HeadRevision));
        Assert.Equal("folder#", CounterResource(grainState, "y", grainState.HeadRevision)); // ends live (folder)
        Assert.Equal("folder#", CounterResource(folded, "y", grainState.HeadRevision));
    }

    // --- helpers ---

    private static async Task<List<LogEvent>> DrainPaged(IDatastoreGrain grain, long from, int pageSize)
    {
        var all = new List<LogEvent>();
        long cursor = from;
        while (true)
        {
            var segment = await grain.ReadFrom(cursor, pageSize);
            if (segment.Events.Count == 0)
                break;
            all.AddRange(segment.Events);
            cursor = segment.Events[^1].Revision;
        }
        return all;
    }

    private static List<string> Canonical(IReadOnlyList<LogEvent> events) =>
        events.Select(e =>
            $"r{e.Revision}|schema={(e.SchemaChange is not null)}|" +
            string.Join(",", e.RelationshipChanges.OrderBy(u => u.Relationship.ResourceId)
                .Select(u => $"{u.Operation}:{u.Relationship.ResourceType}:{u.Relationship.ResourceId}#{u.Relationship.ResourceRelation}@{u.Relationship.SubjectType}:{u.Relationship.SubjectId}")) + "|" +
            string.Join(",", e.CounterChanges.OrderBy(c => c.Name).Select(c => $"{c.Name}:{(c.Filter is not null)}")))
            .ToList();

    private static SortedSet<string> LiveSet(DatastoreGrainState state, long atRevision)
    {
        var set = new SortedSet<string>();
        foreach (var row in state.Relationships)
        {
            if (row.CreatedRevision <= atRevision && (row.DeletedRevision is null || row.DeletedRevision > atRevision))
                set.Add(Identity(row.Relationship));
        }
        return set;
    }

    private static SortedSet<string> GrainLiveIdentities(DatastoreGrainState state) =>
        LiveSet(state, state.HeadRevision);

    private static string? SchemaBytesAt(DatastoreGrainState state, long atRevision)
    {
        string? result = null;
        foreach (var s in state.Schemas.OrderBy(s => s.Revision))
            if (s.Revision <= atRevision)
                result = Convert.ToBase64String(s.Bytes);
        return result;
    }

    private static string? CounterResource(DatastoreGrainState state, string name, long atRevision)
    {
        FullRelationshipsFilterWire? filter = null;
        var found = false;
        foreach (var c in state.Counters.OrderBy(c => c.Revision))
            if (c.Name == name && c.Revision <= atRevision)
            {
                filter = c.Filter;
                found = true;
            }
        return found && filter is not null ? $"{filter.OptionalResourceType}#{filter.OptionalResourceRelation}" : null;
    }

    private static string Identity(RelationshipWire r) =>
        $"{r.ResourceType}:{r.ResourceId}#{r.ResourceRelation}@{r.SubjectType}:{r.SubjectId}#{r.SubjectRelation}";

    private static string Identity(Relationship r) =>
        $"{r.Resource.ObjectType}:{r.Resource.ObjectId}#{r.Resource.Relation}@{r.Subject.ObjectType}:{r.Subject.ObjectId}#{r.Subject.Relation}";

    private sealed class Scope(TestCluster cluster) : IAsyncDisposable
    {
        public TestCluster Cluster { get; } = cluster;
        public async ValueTask DisposeAsync() => await Cluster.DisposeAsync();
    }
}
