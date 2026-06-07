using Npgsql;

namespace Spiceport.Datastore.Postgres.Tests;

/// <summary>
/// Smoke coverage for the bootstrap/migration path and basic connectivity: after
/// <see cref="PostgresDatastore.Create"/> the expected tables and indexes exist,
/// <see cref="PostgresDatastore.Migrate"/> is idempotent (safe to run repeatedly), and a raw connection
/// round-trips. Runs on a fresh database in the shared, disposable container.
/// </summary>
[Collection(PostgresCollection.Name)]
public class PostgresBootstrapSmokeTests
{
    private readonly PostgresContainerFixture _fixture;

    public PostgresBootstrapSmokeTests(PostgresContainerFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task Bootstrap_CreatesSchemaTables()
    {
        var (_, connectionString) = await _fixture.NewDatastoreWithConnectionString();

        var tables = await QueryNames(connectionString,
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename");

        Assert.Contains("relation_tuple_transaction", tables);
        Assert.Contains("relation_tuple", tables);
        Assert.Contains("stored_schema", tables);
        Assert.Contains("datastore_metadata", tables);
    }

    [Fact]
    public async Task Bootstrap_CreatesExpectedIndexes()
    {
        var (_, connectionString) = await _fixture.NewDatastoreWithConnectionString();

        var indexes = await QueryNames(connectionString,
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'public' ORDER BY indexname");

        Assert.Contains("uq_relation_tuple_living", indexes);
        Assert.Contains("ix_relation_tuple_by_resource", indexes);
        Assert.Contains("ix_relation_tuple_by_subject", indexes);
    }

    [Fact]
    public async Task Migrate_IsIdempotent_RunTwiceSucceeds()
    {
        var (ds, connectionString) = await _fixture.NewDatastoreWithConnectionString();

        // Create() already migrated once; running again must not throw.
        await ds.Migrate();
        await ds.Migrate();

        var tables = await QueryNames(connectionString,
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'");
        Assert.Contains("relation_tuple", tables);
    }

    [Fact]
    public async Task Connection_RoundTrips()
    {
        var (_, connectionString) = await _fixture.NewDatastoreWithConnectionString();

        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT 42";
        var value = (int)(await cmd.ExecuteScalarAsync())!;

        Assert.Equal(42, value);
    }

    [Fact]
    public async Task Bootstrap_SeedsUniqueId()
    {
        var ds = await _fixture.NewDatastore();

        var uniqueId = await ds.GetUniqueId();

        Assert.False(string.IsNullOrWhiteSpace(uniqueId));
    }

    private static async Task<List<string>> QueryNames(string connectionString, string sql)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var names = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            names.Add(reader.GetString(0));
        return names;
    }
}
