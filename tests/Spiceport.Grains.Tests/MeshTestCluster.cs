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

    /// <summary>The number of silos in this cluster.</summary>
    public int SiloCount => _cluster.Silos.Count;

    /// <summary>The silo-side service provider (the primary silo's container).</summary>
    public IServiceProvider Services =>
        ((InProcessSiloHandle)_cluster.Primary!).SiloHost.Services;

    /// <summary>The service providers of every silo in the cluster (primary first).</summary>
    public IReadOnlyList<IServiceProvider> AllSiloServices =>
        _cluster.Silos.Cast<InProcessSiloHandle>().Select(h => h.SiloHost.Services).ToArray();

    /// <summary>The in-memory datastore singleton shared by every grain in the silo.</summary>
    public IDatastore Datastore => Services.GetRequiredService<IDatastore>();

    /// <summary>The top-level permission checker (root dispatcher over the grain mesh).</summary>
    public IPermissionChecker Checker => Services.GetRequiredService<IPermissionChecker>();

    /// <summary>Resets the hop/cache counters on EVERY silo (to bracket one workload).</summary>
    public void ResetMetrics()
    {
        foreach (var sp in AllSiloServices)
            sp.GetRequiredService<IDispatchMetrics>().Reset();
    }

    /// <summary>The cluster-wide sum of every silo's dispatch counters.</summary>
    public DispatchMetricsSnapshot MetricsSnapshot()
    {
        var total = default(DispatchMetricsSnapshot);
        foreach (var sp in AllSiloServices)
            total += sp.GetRequiredService<IDispatchMetrics>().Snapshot();
        return total;
    }

    /// <summary>The cluster grain factory, for resolving grains (e.g. the reverse-ops worker) in tests.</summary>
    public IGrainFactory GrainFactory => _cluster.GrainFactory;

    /// <summary>The live, mutable schema provider (for asserting the current snapshot after a swap).</summary>
    public ISchemaProvider SchemaProvider => Services.GetRequiredService<ISchemaProvider>();

    /// <summary>The data-plane grain (schema + relationship writes), resolved by its constant key.</summary>
    public Abstractions.IRelationshipsGrain Relationships =>
        GrainFactory.GetGrain<Abstractions.IRelationshipsGrain>(Abstractions.IRelationshipsGrain.Key);

    /// <summary>Compiles and installs a new schema on the running cluster, exercising the dynamic path.</summary>
    public Task<Abstractions.WriteSchemaReply> WriteSchema(string schemaText) =>
        Relationships.WriteSchema(new Abstractions.WriteSchemaArgs(schemaText));

    /// <summary>Builds and starts a cluster for the given schema DSL text.</summary>
    /// <param name="schemaText">The schema DSL to compile into the silo.</param>
    /// <param name="batchConcurrency">
    /// The bounded fan-out width for <see cref="IPermissionChecker.BatchCheck"/>; pass 1 to serialize the
    /// fan-out so the shared-branch cache behaviour is deterministic (no concurrent-miss races).
    /// </param>
    public static async Task<MeshTestCluster> CreateAsync(
        string schemaText, int batchConcurrency = PermissionChecker.DefaultBatchConcurrency)
    {
        SchemaHolder.SchemaText = schemaText;
        SchemaHolder.BatchConcurrency = batchConcurrency;
        SchemaHolder.LocalRecurseEnabled = true;
        SchemaHolder.SharedDatastore = null;

        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        return new MeshTestCluster(cluster);
    }

    /// <summary>
    /// Builds and starts a MULTI-SILO cluster (<paramref name="siloCount"/> silos) for the given schema,
    /// with <see cref="ConsistentHashPlacement"/> wired so <c>CheckGrain</c> activations are placed by
    /// the deterministic hash ring over the live membership view. All silos share ONE in-memory
    /// datastore (a process-static handoff) so a grain on any silo reads the same data the checker pins a
    /// revision against — mirroring the single shared datastore a real cluster has.
    /// </summary>
    /// <param name="schemaText">The schema DSL to compile into every silo.</param>
    /// <param name="siloCount">The number of silos to deploy (must be >= 1).</param>
    /// <param name="batchConcurrency">Bounded fan-out width for batch checks.</param>
    public static async Task<MeshTestCluster> CreateMultiSiloAsync(
        string schemaText,
        int siloCount = 3,
        int batchConcurrency = PermissionChecker.DefaultBatchConcurrency,
        bool localRecurseEnabled = true)
    {
        if (siloCount < 1)
            throw new ArgumentOutOfRangeException(nameof(siloCount), "Need at least one silo.");

        SchemaHolder.SchemaText = schemaText;
        SchemaHolder.BatchConcurrency = batchConcurrency;
        SchemaHolder.LocalRecurseEnabled = localRecurseEnabled;
        // One datastore instance shared by every silo in this cluster.
        SchemaHolder.SharedDatastore = new InMemoryDatastore();

        var builder = new TestClusterBuilder(initialSilosCount: (short)siloCount);
        builder.AddSiloBuilderConfigurator<MultiSiloConfigurator>();
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
        public static int BatchConcurrency = PermissionChecker.DefaultBatchConcurrency;
        public static InMemoryDatastore? SharedDatastore;
        public static bool LocalRecurseEnabled = true;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            // Exactly the silo's DI: grain mesh services + a single host-owned in-memory datastore.
            // CheckGrain is marked [ConsistentHashPlacement], so its director must be registered on
            // every cluster (with one silo the ring trivially resolves every key to that silo).
            siloBuilder.AddConsistentHashPlacement();
            siloBuilder.ConfigureServices(services =>
            {
                services.AddSpiceportGrainServices(
                    SchemaHolder.SchemaText, batchConcurrency: SchemaHolder.BatchConcurrency);
                services.AddSingleton<IDatastore>(new InMemoryDatastore());
                services.AddSingleton(new OrleansDispatcherOptions
                {
                    LocalRecurseEnabled = SchemaHolder.LocalRecurseEnabled,
                });
            });
        }
    }

    private sealed class MultiSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            // Exactly the production silo's DI plus consistent-hash placement; every silo shares the
            // ONE datastore captured for this cluster so they all see identical data.
            siloBuilder.AddConsistentHashPlacement();
            siloBuilder.ConfigureServices(services =>
            {
                services.AddSpiceportGrainServices(
                    SchemaHolder.SchemaText, batchConcurrency: SchemaHolder.BatchConcurrency);
                services.AddSingleton<IDatastore>(SchemaHolder.SharedDatastore!);
                // Hybrid toggle for this cluster (last AddSingleton wins in the silo container), so a
                // benchmark can deploy OFF (always grain-hop) vs ON (local-recurse shortcut) clusters.
                services.AddSingleton(new OrleansDispatcherOptions
                {
                    LocalRecurseEnabled = SchemaHolder.LocalRecurseEnabled,
                });
            });
        }
    }
}
