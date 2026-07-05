using System.Collections.Immutable;
using System.Text;
using System.Text.Json;
using Orleans.Hosting;
using Orleans.TestingHost;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Proves the LIVE grain-backed datastore is faithful to the <see cref="ReferenceDatastore"/> oracle through
/// the <see cref="IDatastore"/> contract: an identical ordered sequence of write transactions through (a) a
/// plain <see cref="ReferenceDatastore"/> and (b) a <see cref="GrainBackedDatastore"/> over the singleton
/// <c>DatastoreGrain</c> yields equal live relationship sets at head, plus matching delete counts and
/// schema/counter state. This exercises the seam the conformance corpus does not isolate: the optimistic
/// compare-and-swap commit and the <c>DatastoreState</c> &lt;-&gt; <c>DatastoreGrainState</c> conversion
/// round-trip, including caveat context and expiration stamps.
/// </summary>
/// <remarks>
/// Uses its OWN single-silo <see cref="TestCluster"/> registering <c>AddMemoryGrainStorage("datastore")</c>
/// (the provider the grain's <c>[PersistentState]</c> binds). Revisions are timestamp-monotonic and differ
/// between the two backends; the assertion is over the live SET (identity + payload), revision-independent.
/// Both sides read via <see cref="IDatastoreReader.QueryRelationships"/>, so expiration is dropped
/// symmetrically by the same code path.
/// </remarks>
[Collection("MeshCluster")]
public sealed class GrainBackedDatastoreFidelityTests
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

    [Fact]
    public async Task GrainBackedDatastore_MatchesReferenceDatastore_AtLiveSetLevel()
    {
        var inMem = new ReferenceDatastore();
        await using var scope = new ClusterScope(await NewClusterAsync());
        IDatastore grainDs = new GrainBackedDatastore(scope.Cluster.GrainFactory);

        // Step 1: distinct creates.
        var a = Rel("doc", "a", "viewer", "user", "alice");
        var b = Rel("doc", "b", "viewer", "user", "bob");
        var c = Rel("doc", "c", "editor", "user", "carol");
        await WriteBoth(inMem, grainDs, (Op.Create, a), (Op.Create, b), (Op.Create, c));
        await AssertLiveSetsEqual(inMem, grainDs);

        // Step 2: touch over an existing key (close-old + create-new) and a brand-new touch.
        var aTouched = a with { OptionalCaveat = new ContextualizedCaveat("is_active") };
        var d = Rel("doc", "d", "viewer", "user", "dave");
        await WriteBoth(inMem, grainDs, (Op.Touch, aTouched), (Op.Touch, d));
        await AssertLiveSetsEqual(inMem, grainDs);

        // Step 3: delete a base row, and create+delete the same key within one batch.
        var e = Rel("doc", "e", "viewer", "user", "erin");
        await WriteBoth(inMem, grainDs, (Op.Delete, b), (Op.Create, e), (Op.Delete, e));
        await AssertLiveSetsEqual(inMem, grainDs);

        // Step 4: create an already-live key -> conflict on BOTH; live-set unchanged.
        await Assert.ThrowsAsync<CreateRelationshipExistsException>(
            () => inMem.ReadWriteTx(tx => tx.WriteRelationships(new[] { new RelationshipUpdate(c, UpdateOperation.Create) })));
        await Assert.ThrowsAsync<CreateRelationshipExistsException>(
            () => grainDs.ReadWriteTx(tx => tx.WriteRelationships(new[] { new RelationshipUpdate(c, UpdateOperation.Create) })));
        await AssertLiveSetsEqual(inMem, grainDs);

        // Step 5: DeleteRelationships with a limit smaller than the match count, then the remainder.
        var f = Rel("trip", "x", "viewer", "user", "fred");
        var g = Rel("trip", "y", "viewer", "user", "gina");
        var h = Rel("trip", "z", "viewer", "user", "hank");
        await WriteBoth(inMem, grainDs, (Op.Create, f), (Op.Create, g), (Op.Create, h));

        var tripFilter = new RelationshipsFilter { OptionalResourceType = "trip", OptionalResourceRelation = "viewer" };
        var inMemLimited = await Delete(inMem, tripFilter, 2);
        var grainLimited = await Delete(grainDs, tripFilter, 2);
        Assert.Equal(inMemLimited, grainLimited);
        Assert.Equal(((ulong)2, true), grainLimited);
        await AssertLiveSetsEqual(inMem, grainDs);

        var inMemRest = await Delete(inMem, tripFilter, null);
        var grainRest = await Delete(grainDs, tripFilter, null);
        Assert.Equal(inMemRest, grainRest);
        Assert.False(grainRest.ReachedLimit);
        await AssertLiveSetsEqual(inMem, grainDs);

        // Step 6: a populated caveat CONTEXT (boxed JsonElement) survives the CAS + conversion round-trip.
        var ctxRel = WithCaveatContext(Rel("doc", "ctx", "viewer", "user", "ivan"), "is_active", CaveatContext());
        await WriteBoth(inMem, grainDs, (Op.Touch, ctxRel));
        await AssertLiveSetsEqual(inMem, grainDs);

        var ctxStored = await Single(grainDs, r => r.Resource.ObjectId == "ctx");
        Assert.NotNull(ctxStored.OptionalCaveat?.Context);
        Assert.Equal("is_active", ctxStored.OptionalCaveat!.CaveatName);
        Assert.Equal("7", ((JsonElement)ctxStored.OptionalCaveat.Context!["level"]!).GetRawText());
        Assert.Equal("\"alice\"", ((JsonElement)ctxStored.OptionalCaveat.Context!["name"]!).GetRawText());
        Assert.Equal("true", ((JsonElement)ctxStored.OptionalCaveat.Context!["active"]!).GetRawText());

        // Step 7: expiration round-trips; an already-expired row is dropped symmetrically (both read via
        // QueryRelationships, which applies the same expiration filter), and a future expiration survives.
        var future = new DateTimeOffset(2999, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var past = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero);
        await WriteBoth(inMem, grainDs,
            (Op.Create, Rel("exp", "future", "viewer", "user", "nora") with { OptionalExpiration = future }),
            (Op.Create, Rel("exp", "past", "viewer", "user", "owen") with { OptionalExpiration = past }));
        await AssertLiveSetsEqual(inMem, grainDs);
        var futureStored = await Single(grainDs, r => r.Resource.ObjectId == "future");
        Assert.Equal(future, futureStored.OptionalExpiration);

        // Step 8: bulk import.
        var bulk = new[]
        {
            Rel("bulk", "1", "viewer", "user", "u1"),
            Rel("bulk", "2", "viewer", "user", "u2"),
        };
        await inMem.ReadWriteTx(tx => tx.BulkLoad(ToAsync(bulk)));
        await grainDs.ReadWriteTx(tx => tx.BulkLoad(ToAsync(bulk)));
        await AssertLiveSetsEqual(inMem, grainDs);

        // Step 9: schema write -> same effective schema bytes at head.
        var schema = Encoding.UTF8.GetBytes("definition doc { relation viewer: user }");
        await inMem.ReadWriteTx(tx => tx.WriteStoredSchema(schema));
        await grainDs.ReadWriteTx(tx => tx.WriteStoredSchema(schema));
        var inMemSchema = await ReadSchema(inMem);
        var grainSchema = await ReadSchema(grainDs);
        Assert.NotNull(inMemSchema);
        Assert.NotNull(grainSchema);
        Assert.True(inMemSchema!.SequenceEqual(grainSchema!));

        // Step 10: counter register -> filter live (non-null) at head on both.
        var counterFilter = new RelationshipsFilter { OptionalResourceType = "doc", OptionalResourceRelation = "viewer" };
        await inMem.ReadWriteTx(tx => tx.WriteCounter("c1", counterFilter));
        await grainDs.ReadWriteTx(tx => tx.WriteCounter("c1", counterFilter));
        Assert.NotNull(await ReadCounter(inMem, "c1"));
        Assert.NotNull(await ReadCounter(grainDs, "c1"));

        await AssertLiveSetsEqual(inMem, grainDs);
    }

    // --- helpers ---

    private enum Op { Create, Touch, Delete }

    private static async Task WriteBoth(IDatastore inMem, IDatastore grain, params (Op Op, Relationship Rel)[] ops)
    {
        var updates = ops.Select(o => new RelationshipUpdate(o.Rel, o.Op switch
        {
            Op.Create => UpdateOperation.Create,
            Op.Delete => UpdateOperation.Delete,
            _ => UpdateOperation.Touch,
        })).ToList();
        await inMem.ReadWriteTx(tx => tx.WriteRelationships(updates));
        await grain.ReadWriteTx(tx => tx.WriteRelationships(updates));
    }

    private static async Task<(ulong Count, bool ReachedLimit)> Delete(IDatastore ds, RelationshipsFilter filter, ulong? limit)
    {
        (ulong, bool) result = default;
        await ds.ReadWriteTx(async tx => result = await tx.DeleteRelationships(filter, limit));
        return result;
    }

    private static async Task AssertLiveSetsEqual(IDatastore inMem, IDatastore grain) =>
        Assert.Equal(await LiveSet(inMem), await LiveSet(grain));

    private static async Task<ImmutableHashSet<string>> LiveSet(IDatastore ds)
    {
        var head = await ds.HeadRevision();
        var reader = ds.SnapshotReader(head.Revision);
        var set = ImmutableHashSet.CreateBuilder<string>();
        await foreach (var rel in reader.QueryRelationships(new RelationshipsFilter()))
            set.Add(Canonicalize(rel));
        return set.ToImmutable();
    }

    private static async Task<Relationship> Single(IDatastore ds, Func<Relationship, bool> predicate)
    {
        var head = await ds.HeadRevision();
        var reader = ds.SnapshotReader(head.Revision);
        await foreach (var rel in reader.QueryRelationships(new RelationshipsFilter()))
            if (predicate(rel))
                return rel;
        throw new InvalidOperationException("no matching relationship at head");
    }

    private static async Task<byte[]?> ReadSchema(IDatastore ds)
    {
        var head = await ds.HeadRevision();
        return await ds.SnapshotReader(head.Revision).ReadStoredSchema();
    }

    private static async Task<RelationshipsFilter?> ReadCounter(IDatastore ds, string name)
    {
        var head = await ds.HeadRevision();
        return await ds.SnapshotReader(head.Revision).ReadCounterFilter(name);
    }

    private static string Canonicalize(Relationship rel)
    {
        var caveat = rel.OptionalCaveat is { } cav ? $"[{cav.CaveatName}:{ContextString(cav.Context)}]" : "";
        var exp = rel.OptionalExpiration is { } e ? $"@exp={e:O}" : "";
        return $"{rel.Resource.ObjectType}:{rel.Resource.ObjectId}#{rel.Resource.Relation}" +
               $"@{rel.Subject.ObjectType}:{rel.Subject.ObjectId}#{rel.Subject.Relation}{caveat}{exp}";
    }

    private static string ContextString(IReadOnlyDictionary<string, object?>? ctx) =>
        ctx is null || ctx.Count == 0
            ? ""
            : string.Join(",", ctx.OrderBy(kv => kv.Key).Select(kv => $"{kv.Key}={kv.Value}"));

    private static IReadOnlyDictionary<string, object?> CaveatContext()
    {
        const string json = """{ "level": 7, "name": "alice", "active": true }""";
        return JsonSerializer.Deserialize<Dictionary<string, object?>>(json)!;
    }

    private static Relationship WithCaveatContext(Relationship rel, string name, IReadOnlyDictionary<string, object?> ctx) =>
        rel with { OptionalCaveat = new ContextualizedCaveat(name, ctx) };

    private static Relationship Rel(string rt, string rid, string rrel, string st, string sid) =>
        Relationship.Create(
            new ObjectAndRelation(rt, rid, rrel),
            new ObjectAndRelation(st, sid, CoreConstants.Ellipsis));

    private static async IAsyncEnumerable<Relationship> ToAsync(IReadOnlyList<Relationship> source)
    {
        foreach (var r in source)
            yield return r;
        await Task.CompletedTask;
    }

    private sealed class ClusterScope(TestCluster cluster) : IAsyncDisposable
    {
        public TestCluster Cluster { get; } = cluster;
        public async ValueTask DisposeAsync() => await Cluster.DisposeAsync();
    }
}
