using Spiceport.Core;
using Spiceport.Datastore;

namespace Spiceport.Datastore.Postgres.Tests;

/// <summary>
/// Behaviour conformance for the Postgres datastore, ported from the in-memory reference suite:
/// write/read-back, snapshot isolation, filters, reverse queries, delete (with limit), schema
/// versioning, watch, GC window, and CREATE-conflict / serialization semantics. Each test runs on a
/// fresh database in a shared, disposable container.
/// </summary>
[Collection(PostgresCollection.Name)]
public class PostgresDatastoreTests
{
    private readonly PostgresContainerFixture _fixture;

    public PostgresDatastoreTests(PostgresContainerFixture fixture) => _fixture = fixture;

    private static Relationship Rel(string resType, string resId, string relation, string subType, string subId, string subRel = CoreConstants.Ellipsis) =>
        Relationship.Create(
            new ObjectAndRelation(resType, resId, relation),
            new ObjectAndRelation(subType, subId, subRel));

    private static async Task<List<Relationship>> Collect(IAsyncEnumerable<Relationship> source)
    {
        var list = new List<Relationship>();
        await foreach (var r in source)
            list.Add(r);
        return list;
    }

    [Fact]
    public async Task WriteThenReadBack_ReturnsRelationship()
    {
        var ds = await _fixture.NewDatastore();
        var rel = Rel("document", "doc1", "viewer", "user", "alice");

        var rev = await ds.ReadWriteTx(async tx =>
            await tx.WriteRelationships([new RelationshipUpdate(rel, UpdateOperation.Create)]));

        var results = await Collect(ds.SnapshotReader(rev).QueryRelationships(
            new RelationshipsFilter { OptionalResourceType = "document" }));

        Assert.Single(results);
        Assert.Equal(rel, results[0]);
    }

    [Fact]
    public async Task SnapshotIsolation_OldRevisionDoesNotSeeNewWrite()
    {
        var ds = await _fixture.NewDatastore();
        var relA = Rel("document", "doc1", "viewer", "user", "alice");
        var relB = Rel("document", "doc2", "viewer", "user", "bob");

        var rev1 = await ds.ReadWriteTx(async tx =>
            await tx.WriteRelationships([new RelationshipUpdate(relA, UpdateOperation.Create)]));
        var rev2 = await ds.ReadWriteTx(async tx =>
            await tx.WriteRelationships([new RelationshipUpdate(relB, UpdateOperation.Create)]));

        var atRev1 = await Collect(ds.SnapshotReader(rev1).QueryRelationships(new RelationshipsFilter()));
        var atRev2 = await Collect(ds.SnapshotReader(rev2).QueryRelationships(new RelationshipsFilter()));

        Assert.Single(atRev1);
        Assert.Equal(relA, atRev1[0]);
        Assert.Equal(2, atRev2.Count);
    }

    [Fact]
    public async Task SnapshotIsolation_DeleteNotVisibleAtOldRevision()
    {
        var ds = await _fixture.NewDatastore();
        var rel = Rel("document", "doc1", "viewer", "user", "alice");

        var rev1 = await ds.ReadWriteTx(async tx =>
            await tx.WriteRelationships([new RelationshipUpdate(rel, UpdateOperation.Create)]));
        var rev2 = await ds.ReadWriteTx(async tx =>
            await tx.WriteRelationships([new RelationshipUpdate(rel, UpdateOperation.Delete)]));

        var atRev1 = await Collect(ds.SnapshotReader(rev1).QueryRelationships(new RelationshipsFilter()));
        var atRev2 = await Collect(ds.SnapshotReader(rev2).QueryRelationships(new RelationshipsFilter()));

        Assert.Single(atRev1);
        Assert.Empty(atRev2);
    }

    [Fact]
    public async Task CreateExisting_ThrowsCreateRelationshipExists()
    {
        // A CREATE on an already-existing relationship is a permanent conflict (AlreadyExists at the
        // gRPC boundary), NOT a transient write-write serialization failure (Aborted).
        var ds = await _fixture.NewDatastore();
        var rel = Rel("document", "doc1", "viewer", "user", "alice");

        await ds.ReadWriteTx(async tx =>
            await tx.WriteRelationships([new RelationshipUpdate(rel, UpdateOperation.Create)]));

        await Assert.ThrowsAsync<CreateRelationshipExistsException>(async () =>
            await ds.ReadWriteTx(async tx =>
                await tx.WriteRelationships([new RelationshipUpdate(rel, UpdateOperation.Create)])));
    }

    [Fact]
    public async Task Touch_UpsertsWithoutThrowing()
    {
        var ds = await _fixture.NewDatastore();
        var rel = Rel("document", "doc1", "viewer", "user", "alice");
        var withCaveat = rel.WithCaveat(new ContextualizedCaveat("only_office"));

        await ds.ReadWriteTx(async tx =>
            await tx.WriteRelationships([new RelationshipUpdate(rel, UpdateOperation.Touch)]));
        var rev = await ds.ReadWriteTx(async tx =>
            await tx.WriteRelationships([new RelationshipUpdate(withCaveat, UpdateOperation.Touch)]));

        var results = await Collect(ds.SnapshotReader(rev).QueryRelationships(new RelationshipsFilter()));
        Assert.Single(results);
        Assert.Equal("only_office", results[0].OptionalCaveat?.CaveatName);
    }

    [Fact]
    public async Task FilterByResourceId_Matches()
    {
        var ds = await _fixture.NewDatastore();
        var rev = await ds.ReadWriteTx(async tx => await tx.WriteRelationships(
        [
            new RelationshipUpdate(Rel("document", "doc1", "viewer", "user", "alice"), UpdateOperation.Create),
            new RelationshipUpdate(Rel("document", "doc2", "viewer", "user", "bob"), UpdateOperation.Create),
        ]));

        var results = await Collect(ds.SnapshotReader(rev).QueryRelationships(
            new RelationshipsFilter { OptionalResourceType = "document", OptionalResourceIds = ["doc2"] }));

        Assert.Single(results);
        Assert.Equal("doc2", results[0].Resource.ObjectId);
    }

    [Fact]
    public async Task FilterByResourceIdPrefix_Matches()
    {
        var ds = await _fixture.NewDatastore();
        var rev = await ds.ReadWriteTx(async tx => await tx.WriteRelationships(
        [
            new RelationshipUpdate(Rel("document", "report-1", "viewer", "user", "alice"), UpdateOperation.Create),
            new RelationshipUpdate(Rel("document", "report-2", "viewer", "user", "bob"), UpdateOperation.Create),
            new RelationshipUpdate(Rel("document", "memo-1", "viewer", "user", "carol"), UpdateOperation.Create),
        ]));

        var results = await Collect(ds.SnapshotReader(rev).QueryRelationships(
            new RelationshipsFilter { OptionalResourceType = "document", OptionalResourceIdPrefix = "report-" }));

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.StartsWith("report-", r.Resource.ObjectId));
    }

    [Fact]
    public async Task ReverseQuery_FiltersBySubject()
    {
        var ds = await _fixture.NewDatastore();
        var rev = await ds.ReadWriteTx(async tx => await tx.WriteRelationships(
        [
            new RelationshipUpdate(Rel("document", "doc1", "viewer", "user", "alice"), UpdateOperation.Create),
            new RelationshipUpdate(Rel("document", "doc2", "viewer", "user", "bob"), UpdateOperation.Create),
            new RelationshipUpdate(Rel("folder", "f1", "viewer", "user", "alice"), UpdateOperation.Create),
        ]));

        var results = await Collect(ds.SnapshotReader(rev).ReverseQueryRelationships(
            new SubjectsFilter("user", OptionalSubjectIds: ["alice"])));

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Equal("alice", r.Subject.ObjectId));
    }

    [Fact]
    public async Task ReverseQuery_BySubject_OrdersAndResumesAfterKeyset()
    {
        var ds = await _fixture.NewDatastore();
        var rev = await ds.ReadWriteTx(async tx => await tx.WriteRelationships(
        [
            // Inserted out of order; the BySubject sort (ORDER BY ... COLLATE "C") must yield doc1, doc2, doc3.
            new RelationshipUpdate(Rel("document", "doc3", "viewer", "user", "alice"), UpdateOperation.Create),
            new RelationshipUpdate(Rel("document", "doc1", "viewer", "user", "alice"), UpdateOperation.Create),
            new RelationshipUpdate(Rel("document", "doc2", "viewer", "user", "alice"), UpdateOperation.Create),
        ]));
        var reader = ds.SnapshotReader(rev);
        var filter = new SubjectsFilter("user", OptionalSubjectIds: ["alice"]);

        var ordered = await Collect(reader.ReverseQueryRelationships(
            filter, new ReverseQueryOptions(ReverseQuerySort.BySubject)));
        Assert.Equal(["doc1", "doc2", "doc3"], ordered.Select(r => r.Resource.ObjectId).ToArray());

        // Exclusive keyset resume after the first row (the SQL row-comparison keyset) does not repeat it.
        var after = await Collect(reader.ReverseQueryRelationships(
            filter, new ReverseQueryOptions(ReverseQuerySort.BySubject, After: ordered[0].Reference)));
        Assert.Equal(["doc2", "doc3"], after.Select(r => r.Resource.ObjectId).ToArray());
    }

    [Fact]
    public async Task FilterByCaveat_Matches()
    {
        var ds = await _fixture.NewDatastore();
        var plain = Rel("document", "doc1", "viewer", "user", "alice");
        var caveated = Rel("document", "doc2", "viewer", "user", "bob").WithCaveat(new ContextualizedCaveat("biz_hours"));

        var rev = await ds.ReadWriteTx(async tx => await tx.WriteRelationships(
        [
            new RelationshipUpdate(plain, UpdateOperation.Create),
            new RelationshipUpdate(caveated, UpdateOperation.Create),
        ]));

        var hasCaveat = await Collect(ds.SnapshotReader(rev).QueryRelationships(
            new RelationshipsFilter { OptionalCaveatNameFilter = new CaveatNameFilter(CaveatFilterOption.HasMatchingCaveat, "biz_hours") }));
        var noCaveat = await Collect(ds.SnapshotReader(rev).QueryRelationships(
            new RelationshipsFilter { OptionalCaveatNameFilter = new CaveatNameFilter(CaveatFilterOption.NoCaveat) }));

        Assert.Single(hasCaveat);
        Assert.Equal("doc2", hasCaveat[0].Resource.ObjectId);
        Assert.Single(noCaveat);
        Assert.Equal("doc1", noCaveat[0].Resource.ObjectId);
    }

    [Fact]
    public async Task CaveatContext_RoundTrips()
    {
        var ds = await _fixture.NewDatastore();
        var ctx = new Dictionary<string, object?> { ["region"] = "eu", ["level"] = 3L, ["enabled"] = true };
        var caveated = Rel("document", "doc1", "viewer", "user", "alice")
            .WithCaveat(new ContextualizedCaveat("policy", ctx));

        var rev = await ds.ReadWriteTx(async tx =>
            await tx.WriteRelationships([new RelationshipUpdate(caveated, UpdateOperation.Create)]));

        var results = await Collect(ds.SnapshotReader(rev).QueryRelationships(new RelationshipsFilter()));
        Assert.Single(results);
        var readCtx = results[0].OptionalCaveat?.Context;
        Assert.NotNull(readCtx);
        Assert.Equal("eu", readCtx!["region"]);
        Assert.Equal(3L, Convert.ToInt64(readCtx["level"]));
        Assert.Equal(true, readCtx["enabled"]);
    }

    [Fact]
    public async Task ExpiredRelationship_IsExcluded()
    {
        var ds = await _fixture.NewDatastore();
        var expired = Relationship.Create(
            new ObjectAndRelation("document", "doc1", "viewer"),
            new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis),
            expiration: DateTimeOffset.UtcNow.AddMinutes(-1));
        var live = Relationship.Create(
            new ObjectAndRelation("document", "doc2", "viewer"),
            new ObjectAndRelation("user", "bob", CoreConstants.Ellipsis),
            expiration: DateTimeOffset.UtcNow.AddHours(1));

        var rev = await ds.ReadWriteTx(async tx => await tx.WriteRelationships(
        [
            new RelationshipUpdate(expired, UpdateOperation.Create),
            new RelationshipUpdate(live, UpdateOperation.Create),
        ]));

        var results = await Collect(ds.SnapshotReader(rev).QueryRelationships(new RelationshipsFilter()));
        Assert.Single(results);
        Assert.Equal("doc2", results[0].Resource.ObjectId);
    }

    [Fact]
    public async Task DeleteRelationships_RemovesMatchingAndReportsCount()
    {
        var ds = await _fixture.NewDatastore();
        await ds.ReadWriteTx(async tx => await tx.WriteRelationships(
        [
            new RelationshipUpdate(Rel("document", "doc1", "viewer", "user", "alice"), UpdateOperation.Create),
            new RelationshipUpdate(Rel("document", "doc2", "viewer", "user", "bob"), UpdateOperation.Create),
        ]));

        (ulong Count, bool ReachedLimit) deleteResult = default;
        var rev = await ds.ReadWriteTx(async tx =>
            deleteResult = await tx.DeleteRelationships(new RelationshipsFilter { OptionalResourceType = "document" }));

        Assert.Equal(2ul, deleteResult.Count);
        var remaining = await Collect(ds.SnapshotReader(rev).QueryRelationships(new RelationshipsFilter()));
        Assert.Empty(remaining);
    }

    [Fact]
    public async Task DeleteRelationships_RespectsLimit()
    {
        var ds = await _fixture.NewDatastore();
        await ds.ReadWriteTx(async tx => await tx.WriteRelationships(
        [
            new RelationshipUpdate(Rel("document", "doc1", "viewer", "user", "alice"), UpdateOperation.Create),
            new RelationshipUpdate(Rel("document", "doc2", "viewer", "user", "bob"), UpdateOperation.Create),
            new RelationshipUpdate(Rel("document", "doc3", "viewer", "user", "carol"), UpdateOperation.Create),
        ]));

        (ulong Count, bool ReachedLimit) deleteResult = default;
        var rev = await ds.ReadWriteTx(async tx =>
            deleteResult = await tx.DeleteRelationships(new RelationshipsFilter { OptionalResourceType = "document" }, limit: 2));

        Assert.Equal(2ul, deleteResult.Count);
        Assert.True(deleteResult.ReachedLimit);
        var remaining = await Collect(ds.SnapshotReader(rev).QueryRelationships(new RelationshipsFilter()));
        Assert.Single(remaining);
    }

    [Fact]
    public async Task SchemaWriteAndReadBack()
    {
        var ds = await _fixture.NewDatastore();
        var schema = "definition user {}"u8.ToArray();

        var rev = await ds.ReadWriteTx(async tx => await tx.WriteStoredSchema(schema));

        var read = await ds.SnapshotReader(rev).ReadStoredSchema();
        Assert.NotNull(read);
        Assert.Equal(schema, read);

        var head = await ds.HeadRevision();
        Assert.NotNull(head.SchemaHash);
    }

    [Fact]
    public async Task Schema_SnapshotIsolation()
    {
        var ds = await _fixture.NewDatastore();
        var v1 = "definition user {}"u8.ToArray();
        var v2 = "definition user {}\ndefinition doc {}"u8.ToArray();

        var rev1 = await ds.ReadWriteTx(async tx => await tx.WriteStoredSchema(v1));
        var rev2 = await ds.ReadWriteTx(async tx => await tx.WriteStoredSchema(v2));

        Assert.Equal(v1, await ds.SnapshotReader(rev1).ReadStoredSchema());
        Assert.Equal(v2, await ds.SnapshotReader(rev2).ReadStoredSchema());
    }

    [Fact]
    public async Task BulkLoad_LoadsAll()
    {
        var ds = await _fixture.NewDatastore();

        async IAsyncEnumerable<Relationship> Source()
        {
            yield return Rel("document", "doc1", "viewer", "user", "alice");
            yield return Rel("document", "doc2", "viewer", "user", "bob");
            await Task.CompletedTask;
        }

        ulong loaded = 0;
        var rev = await ds.ReadWriteTx(async tx => loaded = await tx.BulkLoad(Source()));

        Assert.Equal(2ul, loaded);
        var results = await Collect(ds.SnapshotReader(rev).QueryRelationships(new RelationshipsFilter()));
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task HeadRevisionAdvancesAfterWrite()
    {
        var ds = await _fixture.NewDatastore();
        var before = (await ds.HeadRevision()).Revision;

        var committed = await ds.ReadWriteTx(async tx =>
            await tx.WriteRelationships([new RelationshipUpdate(Rel("document", "doc1", "viewer", "user", "alice"), UpdateOperation.Create)]));

        var after = (await ds.HeadRevision()).Revision;
        Assert.True(after.CompareTo(before) > 0);
        // Head must be at least as fresh as the committed revision.
        Assert.True(after.CompareTo(committed) >= 0);
    }

    [Fact]
    public async Task GcWindow_OldRevisionRejected()
    {
        var ds = await _fixture.NewDatastore(gcWindow: TimeSpan.Zero);

        var rev1 = await ds.ReadWriteTx(async tx =>
            await tx.WriteRelationships([new RelationshipUpdate(Rel("document", "doc1", "viewer", "user", "alice"), UpdateOperation.Create)]));

        await Task.Delay(50);
        await ds.ReadWriteTx(async tx =>
            await tx.WriteRelationships([new RelationshipUpdate(Rel("document", "doc2", "viewer", "user", "bob"), UpdateOperation.Create)]));

        Assert.False(await ds.CheckRevision(rev1));
        Assert.Throws<RevisionNotFoundException>(() => ds.SnapshotReader(rev1));
    }

    [Fact]
    public async Task ZedToken_RoundTrips()
    {
        var ds = await _fixture.NewDatastore();
        var rev = await ds.ReadWriteTx(async tx =>
            await tx.WriteRelationships([new RelationshipUpdate(Rel("document", "doc1", "viewer", "user", "alice"), UpdateOperation.Create)]));

        var uniqueId = await ds.GetUniqueId();
        var parser = await ds.GetRevisionParser();
        var token = ZedTokens.FromRevision(rev, datastoreUniqueId: uniqueId);
        var decoded = ZedTokens.DecodeRevision(token, parser);

        Assert.Equal(ZedTokenStatus.Valid, decoded.Status);
        Assert.Equal(0, decoded.Revision.CompareTo(rev));
    }

    [Fact]
    public async Task Watch_EmitsChangeCommittedAfterCursor()
    {
        var ds = await _fixture.NewDatastore();
        var head = await ds.HeadRevision();
        var rel = Rel("document", "doc1", "viewer", "user", "alice");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var collected = new List<RevisionChange>();
        var watchTask = Task.Run(async () =>
        {
            await foreach (var change in ds.Watch(head.Revision, new WatchOptions(WatchContent.All), cts.Token))
            {
                collected.Add(change);
                break;
            }
        });

        await Task.Delay(100);
        await ds.ReadWriteTx(async tx =>
            await tx.WriteRelationships([new RelationshipUpdate(rel, UpdateOperation.Create)]));

        await watchTask.WaitAsync(TimeSpan.FromSeconds(20));
        cts.Cancel();

        var change = Assert.Single(collected);
        var update = Assert.Single(change.RelationshipChanges);
        Assert.Equal(UpdateOperation.Touch, update.Operation);
        Assert.Equal(rel, update.Relationship);
    }

    [Fact]
    public async Task Watch_FromOldCursorReplaysCommittedWrite()
    {
        var ds = await _fixture.NewDatastore();
        var head = await ds.HeadRevision();
        var rel = Rel("document", "doc2", "viewer", "user", "bob");

        await ds.ReadWriteTx(async tx =>
            await tx.WriteRelationships([new RelationshipUpdate(rel, UpdateOperation.Create)]));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        RevisionChange? first = null;
        await foreach (var change in ds.Watch(head.Revision, new WatchOptions(), cts.Token))
        {
            first = change;
            break;
        }

        Assert.NotNull(first);
        var update = Assert.Single(first!.RelationshipChanges);
        Assert.Equal("doc2", update.Relationship.Resource.ObjectId);
    }

    [Fact]
    public async Task ConcurrentCreate_SameKey_OneFailsCreateConflict()
    {
        // Postgres enforces the unique living-row index pessimistically: two concurrent CREATEs of the
        // same relationship serialize on the index. The first to commit wins; the other hits the unique
        // living-row violation and surfaces as a CreateRelationshipExistsException (AlreadyExists at the
        // gRPC boundary), a permanent CREATE-conflict rather than a transient serialization failure.
        var ds = await _fixture.NewDatastore();
        var rel = Rel("document", "doc1", "viewer", "user", "alice");
        var firstInserted = new TaskCompletionSource();
        var firstMayCommit = new TaskCompletionSource();

        var first = ds.ReadWriteTx(async tx =>
        {
            await tx.WriteRelationships([new RelationshipUpdate(rel, UpdateOperation.Create)]);
            firstInserted.SetResult();
            await firstMayCommit.Task;
        });

        await firstInserted.Task;

        // Start the second concurrently; its INSERT blocks on the index until the first resolves.
        var second = ds.ReadWriteTx(async tx =>
            await tx.WriteRelationships([new RelationshipUpdate(rel, UpdateOperation.Create)]));

        await Task.Delay(200); // let the second reach (and block on) its INSERT
        firstMayCommit.SetResult();

        await first; // first commits successfully
        await Assert.ThrowsAsync<CreateRelationshipExistsException>(async () => await second);

        // The winning relationship is present exactly once.
        var head = await ds.HeadRevision();
        var results = await Collect(ds.SnapshotReader(head.Revision).QueryRelationships(new RelationshipsFilter()));
        Assert.Single(results);
    }

    [Fact]
    public async Task ConcurrentWrites_DistinctKeys_BothCommit()
    {
        var ds = await _fixture.NewDatastore();
        var gate = new TaskCompletionSource();

        var first = ds.ReadWriteTx(async tx =>
        {
            await tx.WriteRelationships([new RelationshipUpdate(Rel("document", "doc1", "viewer", "user", "alice"), UpdateOperation.Create)]);
            await gate.Task;
        });

        await Task.Delay(100);
        var second = await ds.ReadWriteTx(async tx =>
            await tx.WriteRelationships([new RelationshipUpdate(Rel("document", "doc2", "viewer", "user", "bob"), UpdateOperation.Create)]));
        Assert.NotNull(second);

        gate.SetResult();
        await first;

        var head = await ds.HeadRevision();
        var results = await Collect(ds.SnapshotReader(head.Revision).QueryRelationships(new RelationshipsFilter()));
        Assert.Equal(2, results.Count);
    }
}
