using Spiceport.Api;
using Spiceport.Datastore;

namespace Spiceport.Grains.Tests;

/// <summary>
/// <see cref="SeedData.SeedAsync"/> must be idempotent: it seeds an empty datastore but leaves a populated
/// one untouched, so a host restart over durable storage resumes cleanly instead of re-stamping the
/// fixture relationship (which would churn MVCC history every boot). Plain <see cref="ReferenceDatastore"/>
/// (no cluster) — the seed only depends on the <see cref="IDatastore"/> contract.
/// </summary>
public sealed class SeedDataTests
{
    [Fact]
    public async Task SeedAsync_OnEmptyDatastore_WritesTheFixtureOnce()
    {
        var datastore = new ReferenceDatastore();

        var seeded = await SeedData.SeedAsync(datastore);

        Assert.True(seeded);
        Assert.Equal(1, await CountRelationships(datastore));
    }

    [Fact]
    public async Task SeedAsync_OnPopulatedDatastore_SkipsAndDoesNotChurn()
    {
        var datastore = new ReferenceDatastore();
        Assert.True(await SeedData.SeedAsync(datastore));
        var headAfterFirst = (await datastore.HeadRevision()).Revision;

        var seededAgain = await SeedData.SeedAsync(datastore);

        Assert.False(seededAgain);
        Assert.Equal(1, await CountRelationships(datastore));
        // No write happened on the second call, so head did not advance.
        Assert.Equal(headAfterFirst, (await datastore.HeadRevision()).Revision);
    }

    private static async Task<int> CountRelationships(IDatastore datastore)
    {
        var head = await datastore.HeadRevision();
        var reader = datastore.SnapshotReader(head.Revision);
        var count = 0;
        await foreach (var _ in reader.QueryRelationships(new RelationshipsFilter()))
            count++;
        return count;
    }
}
