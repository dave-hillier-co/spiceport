using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
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

        var connectionString = ResolveConnectionString(configuration);

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            // The in-memory fallback must force the SAME binary serializer as the AdoNet path below. The
            // memory provider's default (JsonGrainStorageSerializer) silently loses boxed JsonElement
            // caveat context: a round-tripped element comes back as ValueKind.Undefined, so the first
            // read of a flushed shard row carrying caveat context would serve corrupted context (and
            // deep-copying it throws InvalidOperationException from JsonElement.Clone). Under the
            // thin-sequencer layout the grain reads its own flushed rows back in STEADY STATE (a clean
            // key's serve path), not just on reactivation — so this is load-bearing, not cosmetic.
            return silo.AddMemoryGrainStorage(ProviderName, optionsBuilder =>
                optionsBuilder.Configure<Serializer>((options, serializer) =>
                    options.GrainStorageSerializer = new OrleansGrainStorageSerializer(serializer)));
        }

        // Orleans AdoNet resolves the Npgsql driver by loading the assembly and reflecting on a built-in
        // invariant->factory map; it does NOT consult System.Data.Common.DbProviderFactories, so no factory
        // registration is needed here — only the Npgsql package reference (so the assembly is loadable).
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

    /// <summary>
    /// Resolves the connection string as the first configured key that is present AND non-blank —
    /// unlike <c>??</c>, an empty (but non-null) primary value does NOT short-circuit the fallback.
    /// If a key was configured (present in <see cref="IConfiguration"/>) but every configured key
    /// resolved blank, this refuses to start rather than silently degrading a production silo to
    /// non-durable in-memory storage over what is almost always an unset env var / Helm default /
    /// typo. A silo where NEITHER key is present at all (the ordinary local-dev/test shape) is left
    /// alone and falls back to in-memory as before.
    /// </summary>
    internal static string? ResolveConnectionString(IConfiguration configuration)
    {
        var primary = configuration[ConnectionStringKey];
        var fallback = configuration[FallbackConnectionStringKey];

        if (!string.IsNullOrWhiteSpace(primary))
        {
            return primary;
        }

        if (!string.IsNullOrWhiteSpace(fallback))
        {
            return fallback;
        }

        if (primary is not null || fallback is not null)
        {
            throw new InvalidOperationException(
                $"Datastore connection string configuration is present but blank " +
                $"(checked '{ConnectionStringKey}' and '{FallbackConnectionStringKey}'). " +
                "Refusing to silently start a non-durable in-memory datastore in place of the " +
                "durable Postgres storage that was evidently intended. Either supply a valid " +
                "connection string, or remove the key(s) entirely to opt into in-memory storage.");
        }

        return null;
    }
}
