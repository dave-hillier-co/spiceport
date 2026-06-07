using Spiceport.Core;
using Spiceport.Datastore;

namespace Spiceport.Datastore.Memory.Tests;

/// <summary>
/// Datastore-level conformance for the relationship-counter primitive on the in-memory backend:
/// register / read-filter / count / overwrite-conflict / delete, MVCC snapshot isolation, and that the
/// count tracks live matches across writes and deletes.
/// </summary>
public class InMemoryCounterTests
{
    private static Relationship Rel(string resType, string resId, string relation, string subType, string subId, string subRel = CoreConstants.Ellipsis) =>
        Relationship.Create(
            new ObjectAndRelation(resType, resId, relation),
            new ObjectAndRelation(subType, subId, subRel));

    private static RelationshipsFilter DocViewerFilter() =>
        new() { OptionalResourceType = "document", OptionalResourceRelation = "viewer" };

    [Fact]
    public async Task WriteCounter_ThenReadFilter_RoundTrips()
    {
        var ds = new InMemoryDatastore();
        var filter = new RelationshipsFilter
        {
            OptionalResourceType = "document",
            OptionalResourceIds = ["doc1"],
            OptionalResourceRelation = "viewer",
            OptionalSubjectsSelectors = [new SubjectsSelector(OptionalSubjectType: "user")],
        };

        var rev = await ds.ReadWriteTx(tx => tx.WriteCounter("c", filter));

        var read = await ds.SnapshotReader(rev).ReadCounterFilter("c");
        Assert.NotNull(read);
        Assert.Equal("document", read!.OptionalResourceType);
        Assert.Equal("doc1", read.OptionalResourceIds!.Single());
        Assert.Equal("viewer", read.OptionalResourceRelation);
        Assert.Equal("user", read.OptionalSubjectsSelectors!.Single().OptionalSubjectType);
    }

    [Fact]
    public async Task ReadCounterFilter_UnknownName_ReturnsNull()
    {
        var ds = new InMemoryDatastore();
        var head = await ds.HeadRevision();
        Assert.Null(await ds.SnapshotReader(head.Revision).ReadCounterFilter("nope"));
    }

    [Fact]
    public async Task CountRelationships_MatchesFilter()
    {
        var ds = new InMemoryDatastore();
        await ds.ReadWriteTx(tx => tx.WriteRelationships(
        [
            new RelationshipUpdate(Rel("document", "doc1", "viewer", "user", "alice"), UpdateOperation.Create),
            new RelationshipUpdate(Rel("document", "doc2", "viewer", "user", "bob"), UpdateOperation.Create),
            new RelationshipUpdate(Rel("document", "doc3", "editor", "user", "carol"), UpdateOperation.Create),
        ]));
        var rev = await ds.ReadWriteTx(tx => tx.WriteCounter("viewers", DocViewerFilter()));

        Assert.Equal(2UL, await ds.SnapshotReader(rev).CountRelationships("viewers"));
    }

    [Fact]
    public async Task CountRelationships_UnknownName_Throws()
    {
        var ds = new InMemoryDatastore();
        var head = await ds.HeadRevision();
        await Assert.ThrowsAsync<CounterNotRegisteredException>(
            () => ds.SnapshotReader(head.Revision).CountRelationships("nope"));
    }

    [Fact]
    public async Task WriteCounter_Twice_Throws()
    {
        var ds = new InMemoryDatastore();
        await ds.ReadWriteTx(tx => tx.WriteCounter("c", DocViewerFilter()));

        var ex = await Assert.ThrowsAsync<CounterAlreadyRegisteredException>(
            () => ds.ReadWriteTx(tx => tx.WriteCounter("c", DocViewerFilter())));
        Assert.Equal("c", ex.CounterName);
    }

    [Fact]
    public async Task WriteCounter_TwiceWithinSameTx_Throws()
    {
        var ds = new InMemoryDatastore();
        await Assert.ThrowsAsync<CounterAlreadyRegisteredException>(() => ds.ReadWriteTx(async tx =>
        {
            await tx.WriteCounter("c", DocViewerFilter());
            await tx.WriteCounter("c", DocViewerFilter());
        }));
    }

    [Fact]
    public async Task DeleteCounter_ThenCount_Throws()
    {
        var ds = new InMemoryDatastore();
        await ds.ReadWriteTx(tx => tx.WriteCounter("c", DocViewerFilter()));
        var rev = await ds.ReadWriteTx(tx => tx.DeleteCounter("c"));

        await Assert.ThrowsAsync<CounterNotRegisteredException>(
            () => ds.SnapshotReader(rev).CountRelationships("c"));
        Assert.Null(await ds.SnapshotReader(rev).ReadCounterFilter("c"));
    }

    [Fact]
    public async Task DeleteCounter_UnknownName_Throws()
    {
        var ds = new InMemoryDatastore();
        var ex = await Assert.ThrowsAsync<CounterNotRegisteredException>(
            () => ds.ReadWriteTx(tx => tx.DeleteCounter("nope")));
        Assert.Equal("nope", ex.CounterName);
    }

    [Fact]
    public async Task DeleteThenReRegister_Succeeds()
    {
        var ds = new InMemoryDatastore();
        await ds.ReadWriteTx(tx => tx.WriteCounter("c", DocViewerFilter()));
        await ds.ReadWriteTx(tx => tx.DeleteCounter("c"));
        var rev = await ds.ReadWriteTx(tx => tx.WriteCounter("c", DocViewerFilter()));

        Assert.NotNull(await ds.SnapshotReader(rev).ReadCounterFilter("c"));
    }

    [Fact]
    public async Task Count_IsSnapshotIsolated_AcrossWritesAndDeletes()
    {
        var ds = new InMemoryDatastore();

        await ds.ReadWriteTx(tx => tx.WriteRelationships(
            [new RelationshipUpdate(Rel("document", "doc1", "viewer", "user", "alice"), UpdateOperation.Create)]));
        var revRegistered = await ds.ReadWriteTx(tx => tx.WriteCounter("viewers", DocViewerFilter()));

        // A matching write increases the count at the newer snapshot only.
        var revAfterMatch = await ds.ReadWriteTx(tx => tx.WriteRelationships(
            [new RelationshipUpdate(Rel("document", "doc2", "viewer", "user", "bob"), UpdateOperation.Create)]));

        // A non-matching write does not change the count.
        var revAfterNonMatch = await ds.ReadWriteTx(tx => tx.WriteRelationships(
            [new RelationshipUpdate(Rel("document", "doc3", "editor", "user", "carol"), UpdateOperation.Create)]));

        // A matching delete decreases the count at the newest snapshot.
        var revAfterDelete = await ds.ReadWriteTx(tx => tx.WriteRelationships(
            [new RelationshipUpdate(Rel("document", "doc1", "viewer", "user", "alice"), UpdateOperation.Delete)]));

        Assert.Equal(1UL, await ds.SnapshotReader(revRegistered).CountRelationships("viewers"));
        Assert.Equal(2UL, await ds.SnapshotReader(revAfterMatch).CountRelationships("viewers"));
        Assert.Equal(2UL, await ds.SnapshotReader(revAfterNonMatch).CountRelationships("viewers"));
        Assert.Equal(1UL, await ds.SnapshotReader(revAfterDelete).CountRelationships("viewers"));

        // The original snapshot is unchanged by all later writes.
        Assert.Equal(1UL, await ds.SnapshotReader(revRegistered).CountRelationships("viewers"));
    }

    [Fact]
    public async Task Counter_VisiblePerSnapshot_TombstonedAfterUnregister()
    {
        var ds = new InMemoryDatastore();
        var revBefore = (await ds.HeadRevision()).Revision;
        var revRegistered = await ds.ReadWriteTx(tx => tx.WriteCounter("c", DocViewerFilter()));
        var revUnregistered = await ds.ReadWriteTx(tx => tx.DeleteCounter("c"));

        // Not visible before registration.
        Assert.Null(await ds.SnapshotReader(revBefore).ReadCounterFilter("c"));
        // Visible at and after registration, before unregister.
        Assert.NotNull(await ds.SnapshotReader(revRegistered).ReadCounterFilter("c"));
        // Tombstoned after unregister.
        Assert.Null(await ds.SnapshotReader(revUnregistered).ReadCounterFilter("c"));
    }

    [Fact]
    public async Task LookupCounters_ReturnsLiveCounters()
    {
        var ds = new InMemoryDatastore();
        await ds.ReadWriteTx(async tx =>
        {
            await tx.WriteCounter("a", DocViewerFilter());
            await tx.WriteCounter("b", DocViewerFilter());
        });
        var rev = await ds.ReadWriteTx(tx => tx.DeleteCounter("a"));

        var names = new List<string>();
        await foreach (var c in ds.SnapshotReader(rev).LookupCounters())
            names.Add(c.Name);

        Assert.Equal(["b"], names);
    }
}
