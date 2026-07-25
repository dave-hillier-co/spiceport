using DotNet.Testcontainers.Builders;
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
    private PostgreSqlContainer? _container;

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
            // Both BUILDING and STARTING the container happen inside this try: Testcontainers' Docker
            // endpoint resolution can throw as early as `.Build()` (not only `.StartAsync()`), and a field
            // initializer runs before this method (i.e. outside any try/catch here) and would let that
            // exception escape the fixture's constructor entirely -- xUnit would then FAIL every test in
            // the collection instead of skipping them. Keeping construction in here is what makes the
            // documented "skips when Docker is unavailable" behavior actually true.
            var container = new PostgreSqlBuilder("postgres:17-alpine").WithCleanUp(true).Build();
            await container.StartAsync();
            _container = container;
        }
        catch (DockerUnavailableException ex)
        {
            // ONLY Docker-unavailable is skip-worthy: DockerUnavailableException is Testcontainers'
            // no-reachable-Docker-endpoint signal ("Docker is either not running or misconfigured"),
            // raised by Build()/StartAsync() when endpoint resolution fails — the one situation where
            // the durability gates genuinely cannot run (CLAUDE.md's skip-without-Docker contract).
            Available = false;
            SkipReason = $"Docker/Testcontainers unavailable: {ex.GetType().Name}: {ex.Message}";
            return;
        }
        // Any OTHER build/start exception (image pull failure, container startup crash, resource
        // exhaustion, ...) means Docker IS present but the infrastructure broke: deliberately NOT
        // caught, so it escapes InitializeAsync and xUnit FAILS every test in the collection — a
        // Docker-enabled machine must never silently skip the durability gates. Schema apply below is
        // outside the try for the same reason.

        ConnectionString = _container.GetConnectionString();
        await ApplyOrleansSchema(ConnectionString);
        Available = true;
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
            await _container.DisposeAsync();
    }

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
