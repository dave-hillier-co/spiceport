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
/// Negative control: <c>DatastoreGrain.OnActivateAsync</c> re-seeds <c>Empty(NowNanos)</c> ONLY when the
/// loaded HeadRevision == 0. If durable state were lost, cluster B's activation would re-seed a fresh,
/// larger head with zero relationships — so the head-equality and relationship-count assertions below are
/// exactly what makes this test FAIL on data loss rather than silently pass.
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
            siloBuilder.AddConsistentHashPlacement();
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
            siloBuilder.ConfigureServices(services =>
            {
                services.AddSpiceportGrainServices(SchemaText);
                services.AddSingleton<IDatastore>(sp =>
                    new GrainBackedDatastore(sp.GetRequiredService<IGrainFactory>()));
            });
        }
    }

    /// <summary>
    /// A FIXED ServiceId so cluster A and cluster B share the same Orleans grain-storage key namespace
    /// (the AdoNet OrleansStorage row key is derived from ServiceId + GrainId). TestClusterBuilder
    /// randomizes ServiceId per build by default, which would make B unable to find A's persisted row —
    /// pinning it is what makes the cross-cluster reactivation read A's durable state.
    /// </summary>
    private const string SharedServiceId = "spiceport-durability-test";

    private static async Task<TestCluster> BuildAdoNetClusterAsync(string connectionString)
    {
        ConnHolder.ConnectionString = connectionString;
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.Options.ServiceId = SharedServiceId;
        builder.AddSiloBuilderConfigurator<AdoNetSiloConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        return cluster;
    }

    private static IDatastore Datastore(TestCluster cluster) =>
        ((InProcessSiloHandle)cluster.Primary!).SiloHost.Services.GetRequiredService<IDatastore>();

    private static readonly DateTimeOffset Expiry = new(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);

    [SkippableFact]
    public async Task GrainState_Survives_TrueReactivation_FromPostgres()
    {
        Skip.IfNot(_fixture.Available, _fixture.SkipReason ?? "Postgres fixture unavailable");

        long writtenHead;

        // --- Phase 1: write through cluster A, then fully dispose it. ---
        var clusterA = await BuildAdoNetClusterAsync(_fixture.ConnectionString);
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
        var clusterB = await BuildAdoNetClusterAsync(_fixture.ConnectionString);
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
}
