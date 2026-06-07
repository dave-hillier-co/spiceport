using Testcontainers.PostgreSql;

namespace Spiceport.Datastore.Postgres.Tests;

/// <summary>
/// Spins up a single disposable Postgres container for the test class, and hands out a fresh,
/// migrated <see cref="PostgresDatastore"/> per test (each on its own randomly-named database so
/// tests are isolated). The container and all created data sources are disposed on teardown, leaving
/// no orphaned containers or volumes.
/// </summary>
public sealed class PostgresContainerFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:17-alpine")
        // Many tests run in parallel, each on its own database with its own connection pool. Raise the
        // server's connection ceiling so the suite has headroom (defaults to 100). The argument is appended
        // to the image's default postgres entrypoint command.
        .WithCommand("-c", "max_connections=500")
        .WithCleanUp(true)
        .Build();

    private readonly List<PostgresDatastore> _datastores = new();
    private int _dbCounter;

    public async Task InitializeAsync() => await _container.StartAsync();

    public async Task DisposeAsync()
    {
        foreach (var ds in _datastores)
        {
            try { await ds.DisposeAsync(); } catch { /* best-effort cleanup */ }
        }
        await _container.DisposeAsync();
    }

    /// <summary>Creates a fresh, migrated datastore on a brand-new database within the container.</summary>
    public async Task<PostgresDatastore> NewDatastore(TimeSpan? quantization = null, TimeSpan? gcWindow = null) =>
        (await NewDatastoreWithConnectionString(quantization, gcWindow)).Datastore;

    /// <summary>
    /// Creates a fresh, migrated datastore on a brand-new database and also returns that database's
    /// connection string (so tests can introspect the migrated schema directly without racing on shared state).
    /// </summary>
    public async Task<(PostgresDatastore Datastore, string ConnectionString)> NewDatastoreWithConnectionString(
        TimeSpan? quantization = null, TimeSpan? gcWindow = null)
    {
        var dbName = $"test_{Interlocked.Increment(ref _dbCounter)}_{Guid.NewGuid():N}";
        await using (var admin = new Npgsql.NpgsqlConnection(_container.GetConnectionString()))
        {
            await admin.OpenAsync();
            await using var cmd = admin.CreateCommand();
            cmd.CommandText = $"CREATE DATABASE \"{dbName}\"";
            await cmd.ExecuteNonQueryAsync();
        }

        var builder = new Npgsql.NpgsqlConnectionStringBuilder(_container.GetConnectionString())
        {
            Database = dbName,
            // Bound each datastore's pool and let idle connections drain quickly so many parallel test
            // databases don't collectively exhaust the server.
            MaxPoolSize = 4,
            MinPoolSize = 0,
            ConnectionIdleLifetime = 2,
            ConnectionPruningInterval = 1,
        };
        var connectionString = builder.ConnectionString;
        var ds = await PostgresDatastore.Create(connectionString, quantization, gcWindow);
        lock (_datastores)
            _datastores.Add(ds);
        return (ds, connectionString);
    }
}

[CollectionDefinition(Name)]
public sealed class PostgresCollection : ICollectionFixture<PostgresContainerFixture>
{
    public const string Name = "postgres";
}
