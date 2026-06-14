using System.Data.Common;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Orleans.Hosting;
using Orleans.Serialization;
using Orleans.Storage;

namespace Spiceport.Server.Hosting;

/// <summary>
/// Config-gated registration of the "datastore" grain-storage provider (the durability seam for the
/// cluster-singleton <c>DatastoreGrain</c>). When a Postgres connection string is configured, the
/// singleton's state is persisted DURABLY via Orleans AdoNet grain storage and survives silo restart /
/// activation migration; otherwise it falls back to non-durable in-memory storage so the default
/// localhost dev host and all in-memory tests run with no Postgres dependency.
/// </summary>
public static class DatastoreStorageConfig
{
    /// <summary>The grain-storage provider name. MUST match <c>[PersistentState("state","datastore")]</c>.</summary>
    public const string ProviderName = "datastore";

    /// <summary>The AdoNet invariant for the Npgsql ADO.NET provider.</summary>
    public const string NpgsqlInvariant = "Npgsql";

    /// <summary>
    /// Primary configuration key for the Orleans grain-storage Postgres connection string. Set this
    /// (appsettings, user-secrets, or env <c>ConnectionStrings__OrleansStorage</c>) to enable durable
    /// Postgres storage. When unset/empty, storage falls back to non-durable in-memory.
    /// </summary>
    public const string ConnectionStringKey = "ConnectionStrings:OrleansStorage";

    /// <summary>Fallback configuration key checked when <see cref="ConnectionStringKey"/> is unset.</summary>
    public const string FallbackConnectionStringKey = "Storage:ConnectionString";

    /// <summary>
    /// Ensures the Npgsql <see cref="DbProviderFactory"/> is registered for the AdoNet invariant. AdoNet
    /// grain storage resolves its driver via <see cref="DbProviderFactories"/> by invariant string, and
    /// .NET does not auto-discover ADO factories. Idempotent and process-global (guarded by
    /// <see cref="DbProviderFactories.TryGetFactory(string, out DbProviderFactory)"/>) so it is safe when
    /// Silo+Api configurators run in one process and when a test harness rebuilds clusters.
    /// </summary>
    public static void EnsureNpgsqlFactoryRegistered()
    {
        if (!DbProviderFactories.TryGetFactory(NpgsqlInvariant, out _))
            DbProviderFactories.RegisterFactory(NpgsqlInvariant, NpgsqlFactory.Instance);
    }

    /// <summary>
    /// Registers the "datastore" provider: durable AdoNet Postgres when a connection string is configured,
    /// otherwise non-durable in-memory. The AdoNet path FORCES the BINARY
    /// <see cref="OrleansGrainStorageSerializer"/>, because boxed <see cref="System.Text.Json.JsonElement"/>
    /// caveat-context values only round-trip via <c>JsonElementSurrogate</c> under the binary serializer;
    /// the AdoNet default in this Orleans version is the JSON serializer, which would silently emit
    /// <c>{}</c> for boxed JsonElement and lose the caveat context.
    /// </summary>
    public static ISiloBuilder AddDatastoreGrainStorage(this ISiloBuilder silo, IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(silo);
        ArgumentNullException.ThrowIfNull(configuration);

        var connectionString =
            configuration[ConnectionStringKey] ?? configuration[FallbackConnectionStringKey];

        if (string.IsNullOrWhiteSpace(connectionString))
            return silo.AddMemoryGrainStorage(ProviderName);

        EnsureNpgsqlFactoryRegistered();
        // Bind the connection/invariant first, then resolve the BINARY serializer from DI (it needs the
        // silo's Serializer, available only via the OptionsBuilder/DI overload) and assign it explicitly.
        return silo.AddAdoNetGrainStorage(ProviderName, optionsBuilder =>
        {
            optionsBuilder.Configure(options =>
            {
                options.Invariant = NpgsqlInvariant;
                options.ConnectionString = connectionString;
            });
            optionsBuilder.Configure<Serializer>((options, serializer) =>
                options.GrainStorageSerializer = new OrleansGrainStorageSerializer(serializer));
        });
    }
}
