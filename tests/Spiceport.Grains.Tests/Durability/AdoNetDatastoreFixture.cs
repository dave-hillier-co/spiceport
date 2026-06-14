using Npgsql;
using Testcontainers.PostgreSql;

namespace Spiceport.Grains.Tests.Durability;

/// <summary>
/// A disposable Postgres container with the Orleans AdoNet persistence schema applied, for proving the
/// singleton <c>DatastoreGrain</c>'s state is DURABLE across a true reactivation. One database, one
/// connection string, shared by the whole test class (the durability proof needs the SAME store before
/// and after the grain reactivates). Requires Docker (Testcontainers), like the rest of the Postgres
/// tests in this repo. The Orleans 10.1.0 AdoNet nupkg ships NO SQL, so the two persistence scripts are
/// vendored under <c>Durability/OrleansSql</c> and applied here (Main first, then Persistence).
/// </summary>
public sealed class AdoNetDatastoreFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container =
        new PostgreSqlBuilder("postgres:17-alpine").WithCleanUp(true).Build();

    /// <summary>True when the container started and the Orleans schema was applied; false if Docker is absent.</summary>
    public bool Available { get; private set; }

    /// <summary>The reason the fixture is unavailable (e.g. Docker not running), for the test skip message.</summary>
    public string? SkipReason { get; private set; }

    /// <summary>The connection string to the single Postgres database with the Orleans schema applied.</summary>
    public string ConnectionString { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        try
        {
            await _container.StartAsync();
            ConnectionString = _container.GetConnectionString();
            await ApplyOrleansSchema(ConnectionString);
            Available = true;
        }
        catch (Exception ex)
        {
            // Docker not available (or container failed): mark unavailable so the test SKIPS rather than
            // fails the fast suite. The rest of the Postgres-backed suite has the same Docker requirement.
            Available = false;
            SkipReason = $"Docker/Testcontainers unavailable: {ex.GetType().Name}: {ex.Message}";
        }
    }

    public async Task DisposeAsync() => await _container.DisposeAsync();

    private static async Task ApplyOrleansSchema(string connectionString)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        // Apply Main first (creates OrleansQuery + the versioning machinery), then Persistence (creates
        // OrleansStorage + the read/write/clear query rows). Each script is a single batch runnable as one
        // command in Npgsql's simple-query mode.
        foreach (var file in new[] { "PostgreSQL-Main.sql", "PostgreSQL-Persistence.sql" })
        {
            var sql = await File.ReadAllTextAsync(
                Path.Combine(AppContext.BaseDirectory, "Durability", "OrleansSql", file));
            await using var cmd = new NpgsqlCommand(sql, conn);
            await cmd.ExecuteNonQueryAsync();
        }
    }
}

[CollectionDefinition(Name)]
public sealed class AdoNetDurabilityCollection : ICollectionFixture<AdoNetDatastoreFixture>
{
    public const string Name = "adonet-durability";
}
