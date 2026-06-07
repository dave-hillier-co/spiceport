using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.TestingHost;
using Spiceport.Datastore;
using Spiceport.Datastore.Memory;
using Spiceport.Grains;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Stands up an Orleans <see cref="TestCluster"/> configured EXACTLY like the production silo
/// (<c>src/Spiceport.Silo/Program.cs</c>): a single in-memory <see cref="IDatastore"/> singleton,
/// the grain DI mesh (<see cref="ServiceCollectionExtensions.AddSpiceportGrainServices(IServiceCollection, string, int)"/>
/// — schema, schema hash, quantizer, dispatch cache, the Caching-over-Orleans root dispatcher and the
/// top-level <see cref="IPermissionChecker"/>), so checks run THROUGH the real grain mesh.
/// </summary>
/// <remarks>
/// One cluster is built per conformance schema because each file declares its own schema, and the silo
/// compiles the schema once at startup. The datastore is a process-wide singleton seeded through the
/// silo's own <see cref="IDatastore"/> so grains read the same data the checker pins a revision against.
/// </remarks>
public sealed class MeshTestCluster : IAsyncDisposable
{
    private readonly TestCluster _cluster;

    private MeshTestCluster(TestCluster cluster) => _cluster = cluster;

    /// <summary>The silo-side service provider (the primary silo's container).</summary>
    public IServiceProvider Services =>
        ((InProcessSiloHandle)_cluster.Primary!).SiloHost.Services;

    /// <summary>The in-memory datastore singleton shared by every grain in the silo.</summary>
    public IDatastore Datastore => Services.GetRequiredService<IDatastore>();

    /// <summary>The top-level permission checker (root dispatcher over the grain mesh).</summary>
    public IPermissionChecker Checker => Services.GetRequiredService<IPermissionChecker>();

    /// <summary>The cluster grain factory, for resolving grains (e.g. the reverse-ops worker) in tests.</summary>
    public IGrainFactory GrainFactory => _cluster.GrainFactory;

    /// <summary>Builds and starts a cluster for the given schema DSL text.</summary>
    public static async Task<MeshTestCluster> CreateAsync(string schemaText)
    {
        SchemaHolder.SchemaText = schemaText;

        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        return new MeshTestCluster(cluster);
    }

    public async ValueTask DisposeAsync()
    {
        await _cluster.DisposeAsync();
    }

    /// <summary>
    /// Carries the schema text to the silo configurator. The Orleans <see cref="TestCluster"/> spins
    /// the silo up in-process, so a static handoff is the simplest faithful way to parameterize the
    /// silo's compiled schema per test file. CreateAsync is invoked serially per file.
    /// </summary>
    private static class SchemaHolder
    {
        public static string SchemaText = string.Empty;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            // Exactly the silo's DI: grain mesh services + a single host-owned in-memory datastore.
            siloBuilder.ConfigureServices(services =>
            {
                services.AddSpiceportGrainServices(SchemaHolder.SchemaText);
                services.AddSingleton<IDatastore>(new InMemoryDatastore());
            });
        }
    }
}
