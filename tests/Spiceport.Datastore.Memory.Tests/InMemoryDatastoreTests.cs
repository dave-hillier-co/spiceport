using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Datastore.Memory;

namespace Spiceport.Datastore.Memory.Tests;

public class InMemoryDatastoreTests
{
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
    public async Task Watch_emits_change_committed_after_cursor()
    {
        var ds = new InMemoryDatastore();
        var head = await ds.HeadRevision();
        var rel = Rel("document", "doc1", "viewer", "user", "alice");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var collected = new List<RevisionChange>();
        var watchTask = Task.Run(async () =>
        {
            await foreach (var change in ds.Watch(head.Revision, new WatchOptions(WatchContent.All), cts.Token))
            {
                collected.Add(change);
                break;
            }
        });

        var rev = await ds.ReadWriteTx(async tx =>
            await tx.WriteRelationships([new RelationshipUpdate(rel, UpdateOperation.Create)]));

        await watchTask.WaitAsync(TimeSpan.FromSeconds(10));
        cts.Cancel();

        var change = Assert.Single(collected);
        Assert.Equal(rev, change.Revision);
        var update = Assert.Single(change.RelationshipChanges);
        Assert.Equal(UpdateOperation.Touch, update.Operation);
        Assert.Equal(rel, update.Relationship);
    }

    [Fact]
    public async Task Watch_from_old_cursor_replays_committed_write()
    {
        var ds = new InMemoryDatastore();
        var head = await ds.HeadRevision();
        var rel = Rel("document", "doc2", "viewer", "user", "bob");

        await ds.ReadWriteTx(async tx =>
            await tx.WriteRelationships([new RelationshipUpdate(rel, UpdateOperation.Create)]));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
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
    public async Task WriteThenReadBack_ReturnsRelationship()
    {
        var ds = new InMemoryDatastore();
        var rel = Rel("document", "doc1", "viewer", "user", "alice");

        var rev = await ds.ReadWriteTx(async tx =>
            await tx.WriteRelationships([new RelationshipUpdate(rel, UpdateOperation.Create)]));

        var reader = ds.SnapshotReader(rev);
        var results = await Collect(reader.QueryRelationships(new RelationshipsFilter { OptionalResourceType = "document" }));

        Assert.Single(results);
        Assert.Equal(rel, results[0]);
    }

    [Fact]
    public async Task SnapshotIsolation_OldRevisionDoesNotSeeNewWrite()
    {
        var ds = new InMemoryDatastore();
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
        var ds = new InMemoryDatastore();
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
    public async Task CreateExisting_ThrowsSerializationException()
    {
        var ds = new InMemoryDatastore();
        var rel = Rel("document", "doc1", "viewer", "user", "alice");

        await ds.ReadWriteTx(async tx =>
            await tx.WriteRelationships([new RelationshipUpdate(rel, UpdateOperation.Create)]));

        await Assert.ThrowsAsync<SerializationException>(async () =>
            await ds.ReadWriteTx(async tx =>
                await tx.WriteRelationships([new RelationshipUpdate(rel, UpdateOperation.Create)])));
    }

    [Fact]
    public async Task Touch_UpsertsWithoutThrowing()
    {
        var ds = new InMemoryDatastore();
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
        var ds = new InMemoryDatastore();
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
        var ds = new InMemoryDatastore();
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
        var ds = new InMemoryDatastore();
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
    public async Task FilterByCaveat_Matches()
    {
        var ds = new InMemoryDatastore();
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
    public async Task ExpiredRelationship_IsExcluded()
    {
        var ds = new InMemoryDatastore();
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
        var ds = new InMemoryDatastore();
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
        var ds = new InMemoryDatastore();
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
        var ds = new InMemoryDatastore();
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
        var ds = new InMemoryDatastore();
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
        var ds = new InMemoryDatastore();

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
    public async Task HeadRevisionIncreasesAfterWrite()
    {
        var ds = new InMemoryDatastore();
        var before = (await ds.HeadRevision()).Revision;

        var committed = await ds.ReadWriteTx(async tx =>
            await tx.WriteRelationships([new RelationshipUpdate(Rel("document", "doc1", "viewer", "user", "alice"), UpdateOperation.Create)]));

        var after = (await ds.HeadRevision()).Revision;
        Assert.True(after.CompareTo(before) > 0);
        Assert.Equal(0, after.CompareTo(committed));
    }

    [Fact]
    public async Task GcWindow_OldRevisionRejected()
    {
        var ds = new InMemoryDatastore(gcWindow: TimeSpan.Zero);

        var rev1 = await ds.ReadWriteTx(async tx =>
            await tx.WriteRelationships([new RelationshipUpdate(Rel("document", "doc1", "viewer", "user", "alice"), UpdateOperation.Create)]));

        // A second commit advances the head; rev1 now falls outside the zero-width GC window.
        await ds.ReadWriteTx(async tx =>
            await tx.WriteRelationships([new RelationshipUpdate(Rel("document", "doc2", "viewer", "user", "bob"), UpdateOperation.Create)]));

        Assert.False(await ds.CheckRevision(rev1));
        Assert.Throws<RevisionNotFoundException>(() => ds.SnapshotReader(rev1));
    }

    [Fact]
    public async Task ConcurrentWrites_SecondCommitFailsSerialization()
    {
        var ds = new InMemoryDatastore();
        var gate = new TaskCompletionSource();

        // First transaction begins and pauses inside its body, holding the base state.
        var first = ds.ReadWriteTx(async tx =>
        {
            await tx.WriteRelationships([new RelationshipUpdate(Rel("document", "doc1", "viewer", "user", "alice"), UpdateOperation.Create)]);
            await gate.Task;
        });

        // Second transaction starts and commits against the same base while the first is paused.
        var second = await ds.ReadWriteTx(async tx =>
            await tx.WriteRelationships([new RelationshipUpdate(Rel("document", "doc2", "viewer", "user", "bob"), UpdateOperation.Create)]));
        Assert.NotNull(second);

        // Release the first; it should now fail to commit because the base changed.
        gate.SetResult();
        await Assert.ThrowsAsync<SerializationException>(async () => await first);
    }
}
