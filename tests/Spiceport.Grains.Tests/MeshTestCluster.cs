using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.TestingHost;
using Spiceport.Datastore;
using Spiceport.Grains;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Stands up an Orleans <see cref="TestCluster"/> configured EXACTLY like the production silo
/// (<c>src/Spiceport.Silo/Program.cs</c>): a <see cref="GrainBackedDatastore"/> delegating to the
/// cluster-singleton datastore grain, the grain DI mesh (<see cref="ServiceCollectionExtensions.AddSpiceportGrainServices(IServiceCollection, string, int)"/>
/// — schema, schema hash, the Orleans root dispatcher and the top-level <see cref="IPermissionChecker"/>),
/// so checks run THROUGH the real grain mesh (every sub-problem is a grain call — there is no in-process
/// local-recurse shortcut and no caller-side branch cache; the one cache is each CheckGrain activation's
/// own reply memo).
/// </summary>
/// <remarks>
/// One cluster is built per conformance schema because each file declares its own schema, and the silo
/// compiles the schema once at startup. The datastore is the cluster-singleton datastore grain; every
/// silo's <see cref="IDatastore"/> delegates to it, so all silos read the same data the checker pins a
/// revision against (with zero replica lag, since there is no per-silo state cache).
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

    /// <summary>The grain-backed datastore (delegating to the cluster-singleton datastore grain).</summary>
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
        string schemaText,
        int batchConcurrency = PermissionChecker.DefaultBatchConcurrency,
        bool useMembershipIndex = true,
        bool useActivationMemo = true,
        bool useSubjectFrontierMemo = true,
        int? subjectFrontierMaxMemoSubjects = null)
    {
        SchemaHolder.SchemaText = schemaText;
        SchemaHolder.BatchConcurrency = batchConcurrency;
        SchemaHolder.UseMembershipIndex = useMembershipIndex;
        SchemaHolder.UseActivationMemo = useActivationMemo;
        SchemaHolder.UseSubjectFrontierMemo = useSubjectFrontierMemo;
        SchemaHolder.SubjectFrontierMaxMemoSubjects =
            subjectFrontierMaxMemoSubjects ?? new SubjectFrontierMemoOptions().MaxMemoSubjects;
        SchemaHolder.UseRandomPlacement = false;

        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        return new MeshTestCluster(cluster);
    }

    /// <summary>
    /// Builds and starts a MULTI-SILO cluster (<paramref name="siloCount"/> silos) for the given schema.
    /// <c>CheckGrain</c> activates under Orleans' default placement; every sub-problem is still a real
    /// grain call (no in-process local-recurse shortcut), so recursion genuinely crosses silo boundaries
    /// as the grain directory places activations. All silos delegate to the ONE cluster-singleton
    /// datastore grain (single activation), so a grain on any silo reads the same data the checker pins a
    /// revision against — the architectural payoff: correct multi-silo reads with zero replica lag,
    /// without any process-static shared instance.
    /// </summary>
    /// <param name="schemaText">The schema DSL to compile into every silo.</param>
    /// <param name="siloCount">The number of silos to deploy (must be >= 1).</param>
    /// <param name="batchConcurrency">Bounded fan-out width for batch checks.</param>
    /// <param name="useRandomPlacement">
    /// Overrides the cluster's DEFAULT placement strategy with <see cref="Orleans.Runtime.RandomPlacement"/>.
    /// Orleans 10's out-of-the-box default is <c>ResourceOptimizedPlacement</c>, a load-statistics heuristic
    /// with a local-silo preference margin that may legitimately place a whole workload on the calling silo
    /// when the silos' load scores are close — it makes NO spread guarantee. A test whose ASSERTION is that
    /// activations land on multiple silos (the anti-hollow spread proof) must opt into random placement,
    /// which does guarantee spread statistically; leave false everywhere else to keep exactly the
    /// production default.
    /// </param>
    public static async Task<MeshTestCluster> CreateMultiSiloAsync(
        string schemaText,
        int siloCount = 3,
        int batchConcurrency = PermissionChecker.DefaultBatchConcurrency,
        bool useMembershipIndex = true,
        bool useActivationMemo = true,
        bool useSubjectFrontierMemo = true,
        bool useRandomPlacement = false)
    {
        if (siloCount < 1)
            throw new ArgumentOutOfRangeException(nameof(siloCount), "Need at least one silo.");

        SchemaHolder.SchemaText = schemaText;
        SchemaHolder.BatchConcurrency = batchConcurrency;
        SchemaHolder.UseMembershipIndex = useMembershipIndex;
        SchemaHolder.UseActivationMemo = useActivationMemo;
        SchemaHolder.UseSubjectFrontierMemo = useSubjectFrontierMemo;
        SchemaHolder.UseRandomPlacement = useRandomPlacement;

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
        public static bool UseMembershipIndex;
        public static bool UseActivationMemo = true;
        public static bool UseSubjectFrontierMemo = true;
        public static int SubjectFrontierMaxMemoSubjects = new SubjectFrontierMemoOptions().MaxMemoSubjects;
        public static bool UseRandomPlacement;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            // Exactly the silo's DI: grain mesh services + the grain-backed datastore over the
            // cluster-singleton datastore grain. CheckGrain uses Orleans' default placement (no custom
            // placement director), so there is nothing to register beyond the memo collection age.
            siloBuilder.AddActivationMemoCollectionAge();
            siloBuilder.AddMemoryGrainStorage("datastore");
            siloBuilder.AddCustomStorageBasedLogConsistencyProvider("CustomStorage");
            // Exactly the production silo-lifecycle wiring: the shared per-silo projection/hub, bootstrapped
            // before the silo accepts traffic (see docs/future-work.md §1.8).
            siloBuilder.AddDatastoreProjectionService();
            siloBuilder.ConfigureServices(services =>
            {
                services.AddSpiceportGrainServices(
                    SchemaHolder.SchemaText, batchConcurrency: SchemaHolder.BatchConcurrency);
                services.AddSingleton<IDatastore>(sp =>
                    new GrainBackedDatastore(
                        sp.GetRequiredService<IGrainFactory>(), sp.GetRequiredService<IDatastoreProjectionHost>()));
                services.AddSingleton(new MembershipIndexOptions { Enabled = SchemaHolder.UseMembershipIndex });
                services.AddSingleton(new ActivationMemoOptions { Enabled = SchemaHolder.UseActivationMemo });
                services.AddSingleton(new SubjectFrontierMemoOptions
                {
                    Enabled = SchemaHolder.UseSubjectFrontierMemo,
                    MaxMemoSubjects = SchemaHolder.SubjectFrontierMaxMemoSubjects,
                });
            });
        }
    }

    private sealed class MultiSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            // Exactly the production silo's DI; every silo delegates to the ONE cluster-singleton
            // datastore grain so they all see identical data. The grain's persistent state lives on
            // whichever silo holds its single activation; all silos route to it via the grain directory,
            // so it is shared by construction (no process-static instance). CheckGrain uses Orleans'
            // default placement — no custom placement director to register.
            siloBuilder.AddActivationMemoCollectionAge();
            siloBuilder.AddMemoryGrainStorage("datastore");
            siloBuilder.AddCustomStorageBasedLogConsistencyProvider("CustomStorage");
            // Exactly the production silo-lifecycle wiring: the shared per-silo projection/hub, bootstrapped
            // before the silo accepts traffic (see docs/future-work.md §1.8).
            siloBuilder.AddDatastoreProjectionService();
            siloBuilder.ConfigureServices(services =>
            {
                services.AddSpiceportGrainServices(
                    SchemaHolder.SchemaText, batchConcurrency: SchemaHolder.BatchConcurrency);
                services.AddSingleton<IDatastore>(sp =>
                    new GrainBackedDatastore(
                        sp.GetRequiredService<IGrainFactory>(), sp.GetRequiredService<IDatastoreProjectionHost>()));
                services.AddSingleton(new MembershipIndexOptions { Enabled = SchemaHolder.UseMembershipIndex });
                services.AddSingleton(new ActivationMemoOptions { Enabled = SchemaHolder.UseActivationMemo });
                services.AddSingleton(new SubjectFrontierMemoOptions
                {
                    Enabled = SchemaHolder.UseSubjectFrontierMemo,
                    MaxMemoSubjects = SchemaHolder.SubjectFrontierMaxMemoSubjects,
                });

                // See CreateMultiSiloAsync's useRandomPlacement doc: an opt-in override of the cluster's
                // DEFAULT placement (Orleans 10's ResourceOptimizedPlacement makes no spread guarantee) for
                // tests that ASSERT activation spread. Registered last, so GetService<PlacementStrategy>
                // (how Orleans resolves the default strategy) returns it.
                if (SchemaHolder.UseRandomPlacement)
                    services.AddSingleton<Orleans.Runtime.PlacementStrategy, Orleans.Runtime.RandomPlacement>();
            });
        }
    }
}
