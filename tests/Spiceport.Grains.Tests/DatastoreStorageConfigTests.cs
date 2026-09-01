using Microsoft.Extensions.Configuration;
using Spiceport.Server.Hosting;
using Xunit;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Pins <see cref="DatastoreStorageConfig.ResolveConnectionString"/> against the "empty env var masks
/// the fallback" regression: <c>??</c> short-circuits on non-null, not non-blank, so a primary key
/// present-but-empty (a common shape for an unset Kubernetes secret / Helm default / <c>--env FOO=</c>)
/// used to swallow a perfectly valid fallback connection string and silently degrade the silo to the
/// non-durable in-memory provider.
/// </summary>
public sealed class DatastoreStorageConfigTests
{
    private static IConfiguration BuildConfig(string? primary, string? fallback)
    {
        var data = new Dictionary<string, string?>();
        if (primary is not null)
        {
            data[DatastoreStorageConfig.ConnectionStringKey] = primary;
        }

        if (fallback is not null)
        {
            data[DatastoreStorageConfig.FallbackConnectionStringKey] = fallback;
        }

        return new ConfigurationBuilder().AddInMemoryCollection(data).Build();
    }

    [Fact]
    public void EmptyPrimary_FallsBackToValidFallback_InsteadOfMaskingIt()
    {
        var config = BuildConfig(primary: "", fallback: "Host=db;Database=spiceport");

        var resolved = DatastoreStorageConfig.ResolveConnectionString(config);

        Assert.Equal("Host=db;Database=spiceport", resolved);
    }

    [Fact]
    public void WhitespacePrimary_FallsBackToValidFallback()
    {
        var config = BuildConfig(primary: "   ", fallback: "Host=db;Database=spiceport");

        var resolved = DatastoreStorageConfig.ResolveConnectionString(config);

        Assert.Equal("Host=db;Database=spiceport", resolved);
    }

    [Fact]
    public void ValidPrimary_IsUsed_FallbackIgnored()
    {
        var config = BuildConfig(primary: "Host=primary", fallback: "Host=fallback");

        var resolved = DatastoreStorageConfig.ResolveConnectionString(config);

        Assert.Equal("Host=primary", resolved);
    }

    [Fact]
    public void NeitherKeyPresent_ResolvesNull_OrdinaryInMemoryDevShape()
    {
        var config = BuildConfig(primary: null, fallback: null);

        var resolved = DatastoreStorageConfig.ResolveConnectionString(config);

        Assert.Null(resolved);
    }

    [Fact]
    public void EmptyPrimary_NoFallbackConfigured_RefusesToStartInsteadOfSilentlyDegrading()
    {
        var config = BuildConfig(primary: "", fallback: null);

        Assert.Throws<InvalidOperationException>(() => DatastoreStorageConfig.ResolveConnectionString(config));
    }

    [Fact]
    public void BothKeysBlank_RefusesToStart()
    {
        var config = BuildConfig(primary: "", fallback: "   ");

        Assert.Throws<InvalidOperationException>(() => DatastoreStorageConfig.ResolveConnectionString(config));
    }
}
