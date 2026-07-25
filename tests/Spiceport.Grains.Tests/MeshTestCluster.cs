using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.TestingHost;
using Spiceport.Datastore;
using Spiceport.Grains;
using Spiceport.Server.Hosting;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Stands up an Orleans <see cref="TestCluster"/> configured EXACTLY like the production silo
/// (<c>src/Spiceport.Silo/Program.cs</c>): a <see cref="GrainBackedDatastore"/> delegating to the
/// cluster-singleton datastore grain, the grain DI mesh (<see cref="ServiceCollectionExtensions.AddSpiceportGrainServices(IServiceCollection, string, int)"/>
/// — schema, schema hash, the Orleans root dispatcher and the top-level <see cref="IPermissionChecker"/>),
/// so checks run THROUGH the real grain mesh (every sub-problem is a grain call — there is no in-process
/// local-recurse shortcut and no caller-side branch cache; the one cache is each CheckGrain activation's
/// own reply memo). Engine graph reads are always served by the <c>IGraphShardGrain</c> mesh (the only
/// read path — the per-silo whole-graph projection is gone).
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

    /// <summary>The cluster grain factory, for resolving grains (e.g. the membership-walk mesh) in tests.</summary>
    public IGrainFactory GrainFactory => _cluster.GrainFactory;

    /// <summary>
    /// The reverse-ops in-process read helper (LookupSubjects/LookupResources/ExpandPermissionTree),
    /// resolved from the primary silo's container — the same instance the silo's gRPC services would
    /// resolve, so tests exercise it exactly as production wiring does (still dispatching onward to
    /// SubjectFrontierGrain/MembershipWalkGrain/the check mesh across silos).
    /// </summary>
    public ReverseOps ReverseOps => Services.GetRequiredService<ReverseOps>();

    /// <summary>
    /// The relationship-read in-process helper (ReadRelationships/BulkExportRelationships), resolved from
    /// the primary silo's container.
    /// </summary>
    public RelationshipReads RelationshipReads => Services.GetRequiredService<RelationshipReads>();

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
    /// <param name="gcWindow">
    /// When set, overrides <see cref="DatastoreGcOptions.Window"/> for the cluster (with the GC
    /// reminder disabled, so <c>RunGc</c> only ever runs when a test invokes it directly). A test
    /// that needs a GC floor near head (e.g. <see cref="TimeSpan.Zero"/>, where the floor becomes
    /// <c>min(head, now) == head</c>) opts in here; null keeps the production default (24h window).
    /// </param>
    public static async Task<MeshTestCluster> CreateAsync(
        string schemaText,
        int batchConcurrency = PermissionChecker.DefaultBatchConcurrency,
        bool useMembershipWalk = true,
        bool useActivationMemo = true,
        bool useSubjectFrontierMemo = true,
        int? subjectFrontierMaxMemoSubjects = null,
        TimeSpan? gcWindow = null,
        bool coLocateWithShards = false)
    {
        SchemaHolder.SchemaText = schemaText;
        SchemaHolder.BatchConcurrency = batchConcurrency;
        SchemaHolder.UseMembershipWalk = useMembershipWalk;
        SchemaHolder.UseActivationMemo = useActivationMemo;
        SchemaHolder.UseSubjectFrontierMemo = useSubjectFrontierMemo;
        SchemaHolder.SubjectFrontierMaxMemoSubjects =
            subjectFrontierMaxMemoSubjects ?? new SubjectFrontierMemoOptions().MaxMemoSubjects;
        SchemaHolder.UseRandomPlacement = false;
        SchemaHolder.CoLocateWithShards = coLocateWithShards;
        SchemaHolder.GcWindow = gcWindow;

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
    /// production default. NOTE: this overrides the cluster's DEFAULT strategy only — the four graph
    /// grain families (CheckGrain, GraphShardGrain, MembershipWalkGrain, SubjectFrontierGrain) carry a
    /// class-specific <see cref="GraphLocalityPlacement"/> strategy whose director, with
    /// <paramref name="coLocateWithShards"/> false, IS a uniform random pick — so those grains spread
    /// under random placement in both settings, and this flag keeps governing every other grain class.
    /// </param>
    /// <param name="coLocateWithShards">
    /// Overrides <see cref="GraphPlacementOptions.CoLocateWithShards"/> for the cluster: when true, the
    /// graph grain families' FIRST activations are steered onto the silo of their object's shard (a pure
    /// locality hint; identity/dedup stay with the grain directory). Default false, the production
    /// default — the placement director then mirrors random placement.
    /// </param>
    public static async Task<MeshTestCluster> CreateMultiSiloAsync(
        string schemaText,
        int siloCount = 3,
        int batchConcurrency = PermissionChecker.DefaultBatchConcurrency,
        bool useMembershipWalk = true,
        bool useActivationMemo = true,
        bool useSubjectFrontierMemo = true,
        bool useRandomPlacement = false,
        bool coLocateWithShards = false)
    {
        if (siloCount < 1)
            throw new ArgumentOutOfRangeException(nameof(siloCount), "Need at least one silo.");

        SchemaHolder.SchemaText = schemaText;
        SchemaHolder.BatchConcurrency = batchConcurrency;
        SchemaHolder.UseMembershipWalk = useMembershipWalk;
        SchemaHolder.UseActivationMemo = useActivationMemo;
        SchemaHolder.UseSubjectFrontierMemo = useSubjectFrontierMemo;
        SchemaHolder.UseRandomPlacement = useRandomPlacement;
        SchemaHolder.CoLocateWithShards = coLocateWithShards;
        SchemaHolder.GcWindow = null; // statics persist across tests; multi-silo always uses the default window

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
        public static bool UseMembershipWalk;
        public static bool UseActivationMemo = true;
        public static bool UseSubjectFrontierMemo = true;
        public static int SubjectFrontierMaxMemoSubjects = new SubjectFrontierMemoOptions().MaxMemoSubjects;
        public static bool UseRandomPlacement;
        public static bool CoLocateWithShards;
        public static TimeSpan? GcWindow;
    }

    /// <summary>
    /// Applies the optional <see cref="DatastoreGcOptions"/> override (the last-registration-wins
    /// options-override pattern the other toggles use): the datastore grain resolves
    /// <c>IOptions&lt;DatastoreGcOptions&gt;</c> from silo DI, so registering here reconfigures its GC
    /// window. The reminder is always disabled under an override — GC must run only when the test
    /// invokes <c>RunGc</c> itself.
    /// </summary>
    private static void OverrideGcOptions(IServiceCollection services)
    {
        if (SchemaHolder.GcWindow is { } window)
        {
            services.AddSingleton<IOptions<DatastoreGcOptions>>(Options.Create(
                new DatastoreGcOptions { Window = window, ReminderEnabled = false }));
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            // Exactly the silo's DI: grain mesh services + the grain-backed datastore over the
            // cluster-singleton datastore grain. Placement goes through the PUBLIC deployment opt-in
            // (AddGraphLocalityPlacement), deliberately BEFORE AddSpiceportGrainServices runs below:
            // that ordering pins the TryAdd contract that keeps an early opt-in from being silently
            // reverted by the mesh registration.
            siloBuilder.AddActivationMemoCollectionAge();
            // The PRODUCTION storage registration with no connection string configured: the in-memory
            // "datastore" provider with the binary grain-storage serializer forced (the provider's JSON
            // default silently corrupts boxed-JsonElement caveat context on the flushed-shard-row read
            // path — see AddDatastoreGrainStorage).
            siloBuilder.AddDatastoreGrainStorage(new ConfigurationBuilder().Build());
            siloBuilder.AddCustomStorageBasedLogConsistencyProvider("CustomStorage");
            siloBuilder.AddGraphLocalityPlacement(new GraphPlacementOptions
            {
                CoLocateWithShards = SchemaHolder.CoLocateWithShards,
            });
            siloBuilder.ConfigureServices(services =>
            {
                services.AddSpiceportGrainServices(
                    SchemaHolder.SchemaText, batchConcurrency: SchemaHolder.BatchConcurrency);
                // Exactly the production wiring: the Watch hub is the DI singleton
                // AddSpiceportGrainServices registered; the container disposes it on silo teardown.
                services.AddSingleton<IDatastore>(sp =>
                    new GrainBackedDatastore(
                        sp.GetRequiredService<IGrainFactory>(), sp.GetRequiredService<LogWatchHub>()));
                services.AddSingleton(new MembershipWalkOptions { Enabled = SchemaHolder.UseMembershipWalk });
                services.AddSingleton(new ActivationMemoOptions { Enabled = SchemaHolder.UseActivationMemo });
                services.AddSingleton(new SubjectFrontierMemoOptions
                {
                    Enabled = SchemaHolder.UseSubjectFrontierMemo,
                    MaxMemoSubjects = SchemaHolder.SubjectFrontierMaxMemoSubjects,
                });
                OverrideGcOptions(services);
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
            // so it is shared by construction (no process-static instance). Placement goes through the
            // PUBLIC deployment opt-in (AddGraphLocalityPlacement) before AddSpiceportGrainServices,
            // pinning the TryAdd ordering contract (see SiloConfigurator).
            siloBuilder.AddActivationMemoCollectionAge();
            // See SiloConfigurator: the production in-memory registration, binary serializer forced.
            siloBuilder.AddDatastoreGrainStorage(new ConfigurationBuilder().Build());
            siloBuilder.AddCustomStorageBasedLogConsistencyProvider("CustomStorage");
            siloBuilder.AddGraphLocalityPlacement(new GraphPlacementOptions
            {
                CoLocateWithShards = SchemaHolder.CoLocateWithShards,
            });
            siloBuilder.ConfigureServices(services =>
            {
                services.AddSpiceportGrainServices(
                    SchemaHolder.SchemaText, batchConcurrency: SchemaHolder.BatchConcurrency);
                // Exactly the production wiring: the Watch hub is the DI singleton
                // AddSpiceportGrainServices registered; the container disposes it on silo teardown.
                services.AddSingleton<IDatastore>(sp =>
                    new GrainBackedDatastore(
                        sp.GetRequiredService<IGrainFactory>(), sp.GetRequiredService<LogWatchHub>()));
                services.AddSingleton(new MembershipWalkOptions { Enabled = SchemaHolder.UseMembershipWalk });
                services.AddSingleton(new ActivationMemoOptions { Enabled = SchemaHolder.UseActivationMemo });
                services.AddSingleton(new SubjectFrontierMemoOptions
                {
                    Enabled = SchemaHolder.UseSubjectFrontierMemo,
                    MaxMemoSubjects = SchemaHolder.SubjectFrontierMaxMemoSubjects,
                });
                // See CreateMultiSiloAsync's useRandomPlacement doc: an opt-in override of the cluster's
                // DEFAULT placement (Orleans 10's ResourceOptimizedPlacement makes no spread guarantee) for
                // tests that ASSERT activation spread. Registered last, so GetService<PlacementStrategy>
                // (how Orleans resolves the default strategy) returns it. The four graph grain families are
                // unaffected either way: their class-specific GraphLocalityPlacement director already makes
                // a uniform random pick whenever CoLocateWithShards is off.
                if (SchemaHolder.UseRandomPlacement)
                    services.AddSingleton<Orleans.Runtime.PlacementStrategy, Orleans.Runtime.RandomPlacement>();
            });
        }
    }
}
