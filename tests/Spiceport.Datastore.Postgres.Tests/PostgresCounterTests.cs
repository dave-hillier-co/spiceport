using Spiceport.Core;
using Spiceport.Datastore;

namespace Spiceport.Datastore.Postgres.Tests;

/// <summary>
/// Datastore-level conformance for the relationship-counter primitive on the Postgres backend, ported
/// from the in-memory reference suite: register / read-filter / count / overwrite-conflict / delete,
/// MVCC snapshot isolation, and that the count (SELECT COUNT with visibility + filter) tracks live
/// matches across writes and deletes. Each test runs on a fresh database in a disposable container.
/// </summary>
[Collection(PostgresCollection.Name)]
public class PostgresCounterTests
{
    private readonly PostgresContainerFixture _fixture;

    public PostgresCounterTests(PostgresContainerFixture fixture) => _fixture = fixture;

    private static Relationship Rel(string resType, string resId, string relation, string subType, string subId, string subRel = CoreConstants.Ellipsis) =>
        Relationship.Create(
            new ObjectAndRelation(resType, resId, relation),
            new ObjectAndRelation(subType, subId, subRel));

    private static RelationshipsFilter DocViewerFilter() =>
        new() { OptionalResourceType = "document", OptionalResourceRelation = "viewer" };

    [Fact]
    public async Task WriteCounter_ThenReadFilter_RoundTrips()
    {
        var ds = await _fixture.NewDatastore();
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
        var ds = await _fixture.NewDatastore();
        var head = await ds.HeadRevision();
        Assert.Null(await ds.SnapshotReader(head.Revision).ReadCounterFilter("nope"));
    }

    [Fact]
    public async Task CountRelationships_MatchesFilter()
    {
        var ds = await _fixture.NewDatastore();
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
        var ds = await _fixture.NewDatastore();
        var head = await ds.HeadRevision();
        await Assert.ThrowsAsync<CounterNotRegisteredException>(
            () => ds.SnapshotReader(head.Revision).CountRelationships("nope"));
    }

    [Fact]
    public async Task WriteCounter_Twice_Throws()
    {
        var ds = await _fixture.NewDatastore();
        await ds.ReadWriteTx(tx => tx.WriteCounter("c", DocViewerFilter()));

        var ex = await Assert.ThrowsAsync<CounterAlreadyRegisteredException>(
            () => ds.ReadWriteTx(tx => tx.WriteCounter("c", DocViewerFilter())));
        Assert.Equal("c", ex.CounterName);
    }

    [Fact]
    public async Task DeleteCounter_ThenCount_Throws()
    {
        var ds = await _fixture.NewDatastore();
        await ds.ReadWriteTx(tx => tx.WriteCounter("c", DocViewerFilter()));
        var rev = await ds.ReadWriteTx(tx => tx.DeleteCounter("c"));

        await Assert.ThrowsAsync<CounterNotRegisteredException>(
            () => ds.SnapshotReader(rev).CountRelationships("c"));
        Assert.Null(await ds.SnapshotReader(rev).ReadCounterFilter("c"));
    }

    [Fact]
    public async Task DeleteCounter_UnknownName_Throws()
    {
        var ds = await _fixture.NewDatastore();
        var ex = await Assert.ThrowsAsync<CounterNotRegisteredException>(
            () => ds.ReadWriteTx(tx => tx.DeleteCounter("nope")));
        Assert.Equal("nope", ex.CounterName);
    }

    [Fact]
    public async Task DeleteThenReRegister_Succeeds()
    {
        var ds = await _fixture.NewDatastore();
        await ds.ReadWriteTx(tx => tx.WriteCounter("c", DocViewerFilter()));
        await ds.ReadWriteTx(tx => tx.DeleteCounter("c"));
        var rev = await ds.ReadWriteTx(tx => tx.WriteCounter("c", DocViewerFilter()));

        Assert.NotNull(await ds.SnapshotReader(rev).ReadCounterFilter("c"));
    }

    [Fact]
    public async Task Count_IsSnapshotIsolated_AcrossWritesAndDeletes()
    {
        var ds = await _fixture.NewDatastore();

        await ds.ReadWriteTx(tx => tx.WriteRelationships(
            [new RelationshipUpdate(Rel("document", "doc1", "viewer", "user", "alice"), UpdateOperation.Create)]));
        var revRegistered = await ds.ReadWriteTx(tx => tx.WriteCounter("viewers", DocViewerFilter()));

        var revAfterMatch = await ds.ReadWriteTx(tx => tx.WriteRelationships(
            [new RelationshipUpdate(Rel("document", "doc2", "viewer", "user", "bob"), UpdateOperation.Create)]));

        var revAfterNonMatch = await ds.ReadWriteTx(tx => tx.WriteRelationships(
            [new RelationshipUpdate(Rel("document", "doc3", "editor", "user", "carol"), UpdateOperation.Create)]));

        var revAfterDelete = await ds.ReadWriteTx(tx => tx.WriteRelationships(
            [new RelationshipUpdate(Rel("document", "doc1", "viewer", "user", "alice"), UpdateOperation.Delete)]));

        Assert.Equal(1UL, await ds.SnapshotReader(revRegistered).CountRelationships("viewers"));
        Assert.Equal(2UL, await ds.SnapshotReader(revAfterMatch).CountRelationships("viewers"));
        Assert.Equal(2UL, await ds.SnapshotReader(revAfterNonMatch).CountRelationships("viewers"));
        Assert.Equal(1UL, await ds.SnapshotReader(revAfterDelete).CountRelationships("viewers"));

        Assert.Equal(1UL, await ds.SnapshotReader(revRegistered).CountRelationships("viewers"));
    }

    [Fact]
    public async Task Count_WithResidualSubjectFilter_StreamsAndCounts()
    {
        var ds = await _fixture.NewDatastore();
        await ds.ReadWriteTx(tx => tx.WriteRelationships(
        [
            new RelationshipUpdate(Rel("document", "doc1", "viewer", "user", "alice"), UpdateOperation.Create),
            new RelationshipUpdate(Rel("document", "doc2", "viewer", "group", "eng", "member"), UpdateOperation.Create),
        ]));
        // A subject-relation residual forces the streaming fallback path.
        var filter = new RelationshipsFilter
        {
            OptionalResourceType = "document",
            OptionalResourceRelation = "viewer",
            OptionalSubjectsSelectors =
            [
                new SubjectsSelector(
                    OptionalSubjectType: "group",
                    RelationFilter: new SubjectRelationFilter(NonEllipsisRelation: "member")),
            ],
        };
        var rev = await ds.ReadWriteTx(tx => tx.WriteCounter("members", filter));

        Assert.Equal(1UL, await ds.SnapshotReader(rev).CountRelationships("members"));
    }

    [Fact]
    public async Task Counter_VisiblePerSnapshot_TombstonedAfterUnregister()
    {
        var ds = await _fixture.NewDatastore();
        var revBefore = (await ds.HeadRevision()).Revision;
        var revRegistered = await ds.ReadWriteTx(tx => tx.WriteCounter("c", DocViewerFilter()));
        var revUnregistered = await ds.ReadWriteTx(tx => tx.DeleteCounter("c"));

        Assert.Null(await ds.SnapshotReader(revBefore).ReadCounterFilter("c"));
        Assert.NotNull(await ds.SnapshotReader(revRegistered).ReadCounterFilter("c"));
        Assert.Null(await ds.SnapshotReader(revUnregistered).ReadCounterFilter("c"));
    }

    [Fact]
    public async Task LookupCounters_ReturnsLiveCounters()
    {
        var ds = await _fixture.NewDatastore();
        await ds.ReadWriteTx(async tx =>
        {
            await tx.WriteCounter("a", DocViewerFilter());
            await tx.WriteCounter("b", DocViewerFilter());
        });
        var rev = await ds.ReadWriteTx(tx => tx.DeleteCounter("a"));

        var names = new List<string>();
        await foreach (var c in ds.SnapshotReader(rev).LookupCounters())
            names.Add(c.Name);

        Assert.Equal(["b"], names.OrderBy(x => x).ToArray());
    }
}
