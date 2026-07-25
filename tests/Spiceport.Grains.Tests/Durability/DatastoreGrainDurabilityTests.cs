using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Storage;
using Orleans.TestingHost;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains;
using Spiceport.Grains.Abstractions;
using Spiceport.Server.Hosting;

namespace Spiceport.Grains.Tests.Durability;

/// <summary>
/// The load-bearing durability gate: proves the singleton <c>DatastoreGrain</c>'s state is DURABLE over a
/// GENUINE reactivation when backed by Orleans AdoNet/Postgres grain storage.
/// </summary>
/// <remarks>
/// <para>
/// TRUE reactivation method: write through TestCluster A, fully dispose it (kills the silo, the grain
/// directory, and the single in-memory activation), then build a BRAND-NEW TestCluster B over the SAME
/// Postgres connection string and read back. Cluster B shares no memory whatsoever with A, so the only
/// path for the data to reappear is a real read from Postgres. This is strictly stronger than
/// ForceActivationCollection (same-process reactivation): a fresh cluster cannot be fooled by a warm
/// activation or a leftover in-process value. Clustering is UseLocalhostClustering (NOT AdoNet
/// clustering), so B is fully independent of A — there is no shared membership table.
/// </para>
/// <para>
/// Negative control: the grain re-seeds <c>Empty(NowNanos)</c> in <c>ReadStateFromStorage</c> ONLY when no
/// durable <c>head</c> entry exists. If durable state were lost, cluster B's activation would re-seed a
/// fresh, larger head with zero relationships — so the head-equality and relationship-count assertions
/// below are exactly what makes this test FAIL on data loss rather than silently pass.
/// </para>
/// <para>
/// Binary-serializer proof: caveat context is boxed <see cref="JsonElement"/>. Under a JSON storage
/// serializer those serialize as <c>{}</c> (silent loss); under the binary serializer they survive via
/// <c>JsonElementSurrogate</c>. The per-key <c>GetRawText()</c> assertions are the binary-serializer gate.
/// </para>
/// </remarks>
[Collection(AdoNetDurabilityCollection.Name)]
public sealed class DatastoreGrainDurabilityTests
{
    private const string SchemaText = """
        definition user {}

        caveat is_active(level int) {
          level > 0
        }

        definition doc {
          relation viewer: user | user with is_active
        }
        """;

    private readonly AdoNetDatastoreFixture _fixture;

    public DatastoreGrainDurabilityTests(AdoNetDatastoreFixture fixture) => _fixture = fixture;

    /// <summary>Carries the AdoNet connection string to the silo configurator (instantiated by type).</summary>
    private static class ConnHolder
    {
        public static string ConnectionString = string.Empty;
    }

    private sealed class AdoNetSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            // Durable AdoNet Postgres storage under the "datastore" provider (matches the grain's
            // [PersistentState("state","datastore")]), wired through the production helper so the test
            // exercises the SAME serializer choice (forced binary, not the AdoNet JSON default).
            var config = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    [DatastoreStorageConfig.ConnectionStringKey] = ConnHolder.ConnectionString,
                })
                .Build();
            siloBuilder.AddDatastoreGrainStorage(config);
            siloBuilder.AddCustomStorageBasedLogConsistencyProvider("CustomStorage");
            siloBuilder.ConfigureServices(services =>
            {
                services.AddSpiceportGrainServices(SchemaText);
                // The Watch hub is the DI singleton AddSpiceportGrainServices registered (the container
                // disposes it on cluster teardown) — exactly the production wiring.
                services.AddSingleton<IDatastore>(sp =>
                    new GrainBackedDatastore(
                        sp.GetRequiredService<IGrainFactory>(), sp.GetRequiredService<LogWatchHub>()));
            });
        }
    }

    // A FIXED ServiceId so cluster A and cluster B share the same Orleans grain-storage key namespace (the
    // AdoNet OrleansStorage row key is derived from ServiceId + GrainId; TestClusterBuilder randomizes it per
    // build by default, which would stop B finding A's row). It must be UNIQUE PER TEST: this collection is
    // serialized and shares one Postgres database, so two tests using the same ServiceId would collide on the
    // singleton grain's keys (Key=0). Each test passes its own stable id.
    private static async Task<TestCluster> BuildAdoNetClusterAsync(string connectionString, string serviceId)
    {
        ConnHolder.ConnectionString = connectionString;
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.Options.ServiceId = serviceId;
        builder.AddSiloBuilderConfigurator<AdoNetSiloConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        return cluster;
    }

    private static IDatastore Datastore(TestCluster cluster) =>
        ((InProcessSiloHandle)cluster.Primary!).SiloHost.Services.GetRequiredService<IDatastore>();

    private static IDatastoreGrain GcGrain(TestCluster cluster) =>
        cluster.GrainFactory.GetGrain<IDatastoreGrain>(IDatastoreGrain.Key);

    /// <summary>Carries the AdoNet connection string to the GC-enabled silo configurator.</summary>
    private static class GcConnHolder
    {
        public static string ConnectionString = string.Empty;
    }

    /// <summary>
    /// Same as <see cref="AdoNetSiloConfigurator"/> but with an aggressive (<see cref="TimeSpan.Zero"/>)
    /// GC window, so a single <see cref="IDatastoreGrain.RunGc"/> call deterministically collects
    /// everything dead as of the current head (see <c>DatastoreGcMeshTests</c> for the same pattern
    /// against in-memory storage).
    /// </summary>
    private sealed class GcAdoNetSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            var config = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    [DatastoreStorageConfig.ConnectionStringKey] = GcConnHolder.ConnectionString,
                })
                .Build();
            siloBuilder.AddDatastoreGrainStorage(config);
            siloBuilder.AddCustomStorageBasedLogConsistencyProvider("CustomStorage");
            siloBuilder.ConfigureServices(services =>
            {
                services.AddSpiceportGrainServices(SchemaText);
                // No reminder service is registered on this cluster (RunGc is invoked directly in the
                // test, never via the reminder), which also doubles as proof that a durable AdoNet-backed
                // host with no reminder service still activates (the try/catch gate).
                services.AddSingleton<IOptions<DatastoreGcOptions>>(
                    Options.Create(new DatastoreGcOptions { Window = TimeSpan.Zero, ReminderEnabled = false }));
                // Mirrors production wiring: GrainBackedDatastore's own nominal GC window must track the
                // SAME DatastoreGcOptions the grain is configured with (see GrainBackedDatastore's ctor doc).
                services.AddSingleton<IDatastore>(sp =>
                    new GrainBackedDatastore(
                        sp.GetRequiredService<IGrainFactory>(),
                        sp.GetRequiredService<LogWatchHub>(),
                        gcOptions: sp.GetRequiredService<IOptions<DatastoreGcOptions>>()));
            });
        }
    }

    private static async Task<TestCluster> BuildGcAdoNetClusterAsync(string connectionString, string serviceId)
    {
        GcConnHolder.ConnectionString = connectionString;
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.Options.ServiceId = serviceId;
        builder.AddSiloBuilderConfigurator<GcAdoNetSiloConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        return cluster;
    }

    private static readonly DateTimeOffset Expiry = new(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [SkippableFact]
    public async Task GrainState_Survives_TrueReactivation_FromPostgres()
    {
        Skip.IfNot(_fixture.Available, _fixture.SkipReason ?? "Postgres fixture unavailable");

        long writtenHead;

        // --- Phase 1: write through cluster A, then fully dispose it. ---
        var clusterA = await BuildAdoNetClusterAsync(_fixture.ConnectionString, "spiceport-durability-single");
        try
        {
            // Assert the "datastore" provider really uses the BINARY grain-storage serializer (not JSON).
            // The AdoNet provider reads options.GrainStorageSerializer (the named-options source of truth),
            // so assert against THAT, not a global keyed service. The AdoNet default in this Orleans version
            // is JsonGrainStorageSerializer, so this proves our helper's explicit binary override took effect.
            var sp = ((InProcessSiloHandle)clusterA.Primary!).SiloHost.Services;
            var options = sp.GetRequiredService<IOptionsMonitor<AdoNetGrainStorageOptions>>()
                .Get(DatastoreStorageConfig.ProviderName);
            Assert.IsType<OrleansGrainStorageSerializer>(options.GrainStorageSerializer);

            var dsA = Datastore(clusterA);

            // Boxed JsonElement caveat context, exactly as production parses it.
            var caveatContext =
                JsonSerializer.Deserialize<Dictionary<string, object?>>("""{"region":"eu","level":7}""")!;

            var schemaBytes = Encoding.UTF8.GetBytes(SchemaText);

            var counterFilter = new RelationshipsFilter
            {
                OptionalResourceType = "doc",
                OptionalResourceRelation = "viewer",
            };

            var revision = await dsA.ReadWriteTx(async tx =>
            {
                await tx.WriteStoredSchema(schemaBytes);

                // (a) a plain relationship.
                var plain = Relationship.Create(
                    new ObjectAndRelation("doc", "plain", "viewer"),
                    new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis));

                // (b) a caveated relationship with a populated context of boxed JsonElement.
                var caveated = Relationship.Create(
                    new ObjectAndRelation("doc", "caveated", "viewer"),
                    new ObjectAndRelation("user", "bob", CoreConstants.Ellipsis),
                    caveat: new ContextualizedCaveat("is_active", caveatContext));

                // (c) a relationship with an expiration.
                var expiring = Relationship.Create(
                    new ObjectAndRelation("doc", "expiring", "viewer"),
                    new ObjectAndRelation("user", "carol", CoreConstants.Ellipsis),
                    expiration: Expiry);

                await tx.WriteRelationships(new[]
                {
                    new RelationshipUpdate(plain, UpdateOperation.Create),
                    new RelationshipUpdate(caveated, UpdateOperation.Create),
                    new RelationshipUpdate(expiring, UpdateOperation.Create),
                });

                await tx.WriteCounter("doc_viewers", counterFilter);
            });

            writtenHead = ((TimestampRevision)revision).TimestampNanosSinceEpoch;
        }
        finally
        {
            await clusterA.DisposeAsync();
        }

        // --- Phase 2: read through a BRAND-NEW cluster B over the same Postgres (TRUE reactivation). ---
        var clusterB = await BuildAdoNetClusterAsync(_fixture.ConnectionString, "spiceport-durability-single");
        try
        {
            var dsB = Datastore(clusterB);

            // Negative control: head must equal the written head. If state were lost the grain re-seeds
            // Empty(NowNanos) => a DIFFERENT, larger head and zero relationships. This is the lost-state tripwire.
            var head = await dsB.HeadRevision();
            var headNanos = ((TimestampRevision)head.Revision).TimestampNanosSinceEpoch;
            Assert.Equal(writtenHead, headNanos);

            var reader = dsB.SnapshotReader(head.Revision);

            // All three relationships present (count is the second lost-state tripwire).
            var rels = new List<Relationship>();
            await foreach (var rel in reader.QueryRelationships(new RelationshipsFilter { OptionalResourceType = "doc" }))
                rels.Add(rel);
            Assert.Equal(3, rels.Count);

            var plainRead = rels.Single(r => r.Resource.ObjectId == "plain");
            Assert.Equal("viewer", plainRead.Resource.Relation);
            Assert.Equal("user", plainRead.Subject.ObjectType);
            Assert.Equal("alice", plainRead.Subject.ObjectId);
            Assert.Null(plainRead.OptionalCaveat);
            Assert.Null(plainRead.OptionalExpiration);

            // Caveat context survived per-key as raw JsonElement text (the BINARY-serializer gate).
            var caveatedRead = rels.Single(r => r.Resource.ObjectId == "caveated");
            Assert.NotNull(caveatedRead.OptionalCaveat);
            Assert.Equal("is_active", caveatedRead.OptionalCaveat!.CaveatName);
            var ctx = caveatedRead.OptionalCaveat.Context!;
            Assert.NotNull(ctx);
            Assert.Equal("\"eu\"", ((JsonElement)ctx["region"]!).GetRawText());
            Assert.Equal("7", ((JsonElement)ctx["level"]!).GetRawText());

            // Expiration survived exactly.
            var expiringRead = rels.Single(r => r.Resource.ObjectId == "expiring");
            Assert.Equal(Expiry, expiringRead.OptionalExpiration);

            // Schema bytes survived.
            var schema = await reader.ReadStoredSchema();
            Assert.NotNull(schema);
            Assert.True(Encoding.UTF8.GetBytes(SchemaText).SequenceEqual(schema!));
            // Schema hash at head is non-null (a schema was written).
            Assert.NotNull(head.SchemaHash);

            // Counter filter survived.
            var counterFilterRead = await reader.ReadCounterFilter("doc_viewers");
            Assert.NotNull(counterFilterRead);
            Assert.Equal("doc", counterFilterRead!.OptionalResourceType);
            Assert.Equal("viewer", counterFilterRead.OptionalResourceRelation);
            Assert.Equal(3ul, await reader.CountRelationships("doc_viewers"));
        }
        finally
        {
            await clusterB.DisposeAsync();
        }
    }

    /// <summary>
    /// Crosses the snapshot/compaction interval (&gt; 64 commits) so reactivation must rebuild from a
    /// COMPACTED snapshot + a post-snapshot log tail (not the version-0 seed). Proves snapshot serialization
    /// of a non-empty state (incl. boxed-JsonElement caveat context written BEFORE the snapshot boundary, so
    /// it can only survive through the snapshot), the compaction loop, and replay-from-compacted-snapshot.
    /// </summary>
    [SkippableFact]
    public async Task GrainState_Survives_Reactivation_AcrossSnapshotCompaction()
    {
        Skip.IfNot(_fixture.Available, _fixture.SkipReason ?? "Postgres fixture unavailable");

        const int commits = 70; // > SnapshotInterval (64): forces at least one snapshot + compaction.
        long writtenHead;

        var clusterA = await BuildAdoNetClusterAsync(_fixture.ConnectionString, "spiceport-durability-snapshot");
        try
        {
            var dsA = Datastore(clusterA);

            // Commit 0: a caveated relationship, written FIRST so it is subsumed into the compacted snapshot.
            var caveatContext =
                JsonSerializer.Deserialize<Dictionary<string, object?>>("""{"region":"eu","level":7}""")!;
            await dsA.ReadWriteTx(tx => tx.WriteRelationships(new[]
            {
                new RelationshipUpdate(
                    Relationship.Create(
                        new ObjectAndRelation("doc", "early", "viewer"),
                        new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis),
                        caveat: new ContextualizedCaveat("is_active", caveatContext)),
                    UpdateOperation.Create),
            }));

            // Commits 1..68: one relationship each, crossing the snapshot boundary.
            long last = 0;
            for (var i = 1; i < commits; i++)
            {
                var rev = await dsA.ReadWriteTx(tx => tx.WriteRelationships(new[]
                {
                    new RelationshipUpdate(
                        Relationship.Create(
                            new ObjectAndRelation("doc", $"r{i}", "viewer"),
                            new ObjectAndRelation("user", $"u{i}", CoreConstants.Ellipsis)),
                        UpdateOperation.Create),
                }));
                last = ((TimestampRevision)rev).TimestampNanosSinceEpoch;
            }
            writtenHead = last;
        }
        finally
        {
            await clusterA.DisposeAsync();
        }

        // Reactivate via a brand-new cluster: state must rebuild from the compacted snapshot + tail.
        var clusterB = await BuildAdoNetClusterAsync(_fixture.ConnectionString, "spiceport-durability-snapshot");
        try
        {
            var dsB = Datastore(clusterB);

            var head = await dsB.HeadRevision();
            Assert.Equal(writtenHead, ((TimestampRevision)head.Revision).TimestampNanosSinceEpoch);

            var reader = dsB.SnapshotReader(head.Revision);
            var rels = new List<Relationship>();
            await foreach (var rel in reader.QueryRelationships(new RelationshipsFilter { OptionalResourceType = "doc" }))
                rels.Add(rel);
            Assert.Equal(commits, rels.Count); // all relationships survived snapshot+compaction

            // The pre-boundary caveat row survived through the SNAPSHOT with its context intact.
            var early = rels.Single(r => r.Resource.ObjectId == "early");
            Assert.Equal("is_active", early.OptionalCaveat!.CaveatName);
            Assert.Equal("\"eu\"", ((JsonElement)early.OptionalCaveat.Context!["region"]!).GetRawText());
            Assert.Equal("7", ((JsonElement)early.OptionalCaveat.Context!["level"]!).GetRawText());
        }
        finally
        {
            await clusterB.DisposeAsync();
        }
    }

    /// <summary>
    /// The GC-specific durability gate: commits rows, deletes some, runs <see cref="IDatastoreGrain.RunGc"/>
    /// (collecting the dead rows and stamping a GC floor), then proves BOTH survive a TRUE reactivation
    /// (brand-new cluster over the same Postgres) — the collected relationship set AND the floor itself,
    /// which is only durable if it round-trips through the snapshot/log exactly like any other event.
    /// </summary>
    /// <remarks>
    /// Negative control (mirrors the other tests in this file): if durable state were lost, cluster B's
    /// activation would re-seed <c>Empty(NowNanos)</c> — a DIFFERENT, larger head, GcFloor back at 0, and
    /// zero relationships — so the head-equality, GcFloor-equality, and live-row assertions below are
    /// exactly what makes this test FAIL LOUDLY on real data loss rather than silently pass.
    /// </remarks>
    [SkippableFact]
    public async Task GcFloor_And_CollectedState_Survive_TrueReactivation_FromPostgres()
    {
        Skip.IfNot(_fixture.Available, _fixture.SkipReason ?? "Postgres fixture unavailable");

        long writtenHead;
        long writtenFloor;

        var clusterA = await BuildGcAdoNetClusterAsync(_fixture.ConnectionString, "spiceport-durability-gc");
        try
        {
            var dsA = Datastore(clusterA);
            var grainA = GcGrain(clusterA);

            await dsA.ReadWriteTx(tx => tx.WriteRelationships(new[]
            {
                new RelationshipUpdate(
                    Relationship.Create(
                        new ObjectAndRelation("doc", "dead", "viewer"),
                        new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis)),
                    UpdateOperation.Create),
                new RelationshipUpdate(
                    Relationship.Create(
                        new ObjectAndRelation("doc", "alive", "viewer"),
                        new ObjectAndRelation("user", "bob", CoreConstants.Ellipsis)),
                    UpdateOperation.Create),
            }));

            await dsA.ReadWriteTx(tx => tx.WriteRelationships(new[]
            {
                new RelationshipUpdate(
                    Relationship.Create(
                        new ObjectAndRelation("doc", "dead", "viewer"),
                        new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis)),
                    UpdateOperation.Delete),
            }));

            var floor = await grainA.RunGc();
            Assert.NotNull(floor);
            writtenFloor = floor!.Value;

            var head = await dsA.HeadRevision();
            writtenHead = ((TimestampRevision)head.Revision).TimestampNanosSinceEpoch;

            // Collected in cluster A itself, before any reactivation.
            var stateA = await grainA.ReadState();
            Assert.Equal(writtenFloor, stateA.GcFloor);
            Assert.DoesNotContain(stateA.Relationships, r => r.Relationship.ResourceId == "dead");
            Assert.Contains(stateA.Relationships, r => r.Relationship.ResourceId == "alive");
        }
        finally
        {
            await clusterA.DisposeAsync();
        }

        // --- TRUE reactivation: a brand-new cluster over the same Postgres. ---
        var clusterB = await BuildGcAdoNetClusterAsync(_fixture.ConnectionString, "spiceport-durability-gc");
        try
        {
            var dsB = Datastore(clusterB);
            var grainB = GcGrain(clusterB);

            var head = await dsB.HeadRevision();
            Assert.Equal(writtenHead, ((TimestampRevision)head.Revision).TimestampNanosSinceEpoch);

            var stateB = await grainB.ReadState();
            Assert.Equal(writtenFloor, stateB.GcFloor); // the floor itself is durable

            var reader = dsB.SnapshotReader(head.Revision);
            var rels = new List<Relationship>();
            await foreach (var rel in reader.QueryRelationships(new RelationshipsFilter { OptionalResourceType = "doc" }))
                rels.Add(rel);

            Assert.Single(rels); // "dead" stayed collected across reactivation; only "alive" survives
            Assert.Equal("alive", rels[0].Resource.ObjectId);

            // A read pinned below the (durable) floor is rejected exactly as it would be pre-reactivation.
            await Assert.ThrowsAsync<RevisionNotFoundException>(async () =>
            {
                var stale = dsB.SnapshotReader(new TimestampRevision(writtenFloor - 1));
                await foreach (var _ in stale.QueryRelationships(new RelationshipsFilter()))
                {
                }
            });
        }
        finally
        {
            await clusterB.DisposeAsync();
        }
    }
}
