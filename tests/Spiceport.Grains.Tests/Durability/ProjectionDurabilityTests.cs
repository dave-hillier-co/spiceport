using System.Collections.Immutable;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.TestingHost;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains;
using Spiceport.Grains.Abstractions;
using Spiceport.Server.Hosting;

namespace Spiceport.Grains.Tests.Durability;

/// <summary>
/// Durability gate for the journaled sequencer and its shard projection against REAL Postgres (Orleans
/// AdoNet grain storage, same Testcontainers fixture and skip-without-Docker discipline as
/// <see cref="DatastoreGrainDurabilityTests"/>): a mixed load committed past a projection flush through
/// cluster A must be served with full fidelity by a BRAND-NEW cluster B over the same database — reads,
/// schema, counters, the GC floor's below-floor rejection, and a Watch cursor replay — with probes
/// pinning that cluster B's shard grains and shard index recovered the projection from storage.
/// </summary>
[Collection(AdoNetDurabilityCollection.Name)]
public sealed class ProjectionRestartDurabilityTests
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

    private static readonly DateTimeOffset Expiry = new(2032, 3, 1, 0, 0, 0, TimeSpan.Zero);

    private readonly AdoNetDatastoreFixture _fixture;

    public ProjectionRestartDurabilityTests(AdoNetDatastoreFixture fixture) => _fixture = fixture;

    /// <summary>Carries the AdoNet connection string to the silo configurator (instantiated by type).</summary>
    private static class ConnHolder
    {
        public static string ConnectionString = string.Empty;
    }

    /// <summary>
    /// The production wiring plus a ONE-HOUR GC window (reminder disabled): <c>RunGc</c> then appends a
    /// REAL GC event whose floor (<c>now - 1h</c>) advances from 0 but stays far below every revision
    /// this test mints — so the test gets a durable non-zero floor to assert below-floor rejection
    /// against without ever invalidating its own Watch cursor.
    /// </summary>
    private sealed class ProjectionRestartSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            var config = new ConfigurationBuilder()
                .AddInMemoryCollection(new Dictionary<string, string?>
                {
                    [DatastoreStorageConfig.ConnectionStringKey] = ConnHolder.ConnectionString,
                })
                .Build();
            siloBuilder.AddDatastoreGrainStorage(config);
            siloBuilder.AddStateStorageBasedLogConsistencyProvider("StateStorage");
            siloBuilder.ConfigureServices(services =>
            {
                services.AddSpiceportGrainServices(SchemaText);
                services.AddSingleton<IOptions<DatastoreGcOptions>>(
                    Options.Create(new DatastoreGcOptions { Window = TimeSpan.FromHours(1), ReminderEnabled = false }));
                services.AddSingleton<IDatastore>(sp =>
                    new GrainBackedDatastore(
                        sp.GetRequiredService<IGrainFactory>(),
                        sp.GetRequiredService<LogWatchHub>(),
                        gcOptions: sp.GetRequiredService<IOptions<DatastoreGcOptions>>()));
            });
        }
    }

    private static async Task<TestCluster> BuildClusterAsync(string connectionString)
    {
        ConnHolder.ConnectionString = connectionString;
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        // Fixed per-test ServiceId: A and B must share the storage key namespace (see the note on
        // DatastoreGrainDurabilityTests.BuildAdoNetClusterAsync), unique within the shared database.
        builder.Options.ServiceId = "spiceport-durability-projection-restart";
        builder.AddSiloBuilderConfigurator<ProjectionRestartSiloConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        return cluster;
    }

    private static IDatastore Datastore(TestCluster cluster) =>
        ((InProcessSiloHandle)cluster.Primary!).SiloHost.Services.GetRequiredService<IDatastore>();

    private static IDatastoreGrain Grain(TestCluster cluster) =>
        cluster.GrainFactory.GetGrain<IDatastoreGrain>(IDatastoreGrain.Key);

    private static Relationship Row(string docId, string userId) => Relationship.Create(
        new ObjectAndRelation("doc", docId, "viewer"),
        new ObjectAndRelation("user", userId, CoreConstants.Ellipsis));

    private static Task<IRevision> Write(IDatastore ds, Relationship rel, UpdateOperation op = UpdateOperation.Create) =>
        ds.ReadWriteTx(tx => tx.WriteRelationships([new RelationshipUpdate(rel, op)]));

    private static async Task<List<string>> Collect(IAsyncEnumerable<Relationship> source)
    {
        var rows = new List<string>();
        await foreach (var rel in source)
            rows.Add($"{rel.Resource.ObjectType}:{rel.Resource.ObjectId}#{rel.Resource.Relation}" +
                     $"@{rel.Subject.ObjectType}:{rel.Subject.ObjectId}");
        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    [SkippableFact]
    public async Task Projection_FullFidelity_Survives_TrueReactivation_PastAFlushBoundary()
    {
        Skip.IfNot(_fixture.Available, _fixture.SkipReason ?? "Postgres fixture unavailable");

        long writtenHead;
        long writtenFloor;
        string? writtenSchemaHash;
        List<string> liveBefore;
        ulong countBefore;
        IRevision cursor;

        // --- Phase 1: mixed load past the flush boundary through cluster A, then dispose it. ---
        var clusterA = await BuildClusterAsync(_fixture.ConnectionString);
        try
        {
            var dsA = Datastore(clusterA);
            var caveatContext =
                JsonSerializer.Deserialize<Dictionary<string, object?>>("""{"region":"eu","level":7}""")!;

            await dsA.ReadWriteTx(async tx =>
            {
                await tx.WriteStoredSchema(Encoding.UTF8.GetBytes(SchemaText));
                await tx.WriteRelationships(
                [
                    new RelationshipUpdate(Row("plain", "alice"), UpdateOperation.Create),
                    new RelationshipUpdate(
                        Relationship.Create(
                            new ObjectAndRelation("doc", "caveated", "viewer"),
                            new ObjectAndRelation("user", "bob", CoreConstants.Ellipsis),
                            caveat: new ContextualizedCaveat("is_active", caveatContext)),
                        UpdateOperation.Create),
                    new RelationshipUpdate(
                        Relationship.Create(
                            new ObjectAndRelation("doc", "expiring", "viewer"),
                            new ObjectAndRelation("user", "carol", CoreConstants.Ellipsis),
                            expiration: Expiry),
                        UpdateOperation.Create),
                ]);
                await tx.WriteCounter("doc_viewers", new RelationshipsFilter
                {
                    OptionalResourceType = "doc",
                    OptionalResourceRelation = "viewer",
                });
            });

            // Cross the 64-event flush boundary, then a delete and a REAL GC event.
            for (var i = 0; i < 66; i++)
                await Write(dsA, Row($"m{i}", $"w{i}"));
            await Write(dsA, Row("m0", "w0"), UpdateOperation.Delete);

            var floor = await Grain(clusterA).RunGc();
            Assert.NotNull(floor);
            writtenFloor = floor!.Value;

            // The Watch cursor cluster B must replay from, then the two events it must replay.
            cursor = (await dsA.HeadRevision()).Revision;
            await Write(dsA, Row("post-1", "zoe"));
            await Write(dsA, Row("post-2", "zed"));

            var head = await dsA.HeadRevision();
            writtenHead = ((TimestampRevision)head.Revision).TimestampNanosSinceEpoch;
            writtenSchemaHash = head.SchemaHash;
            Assert.NotNull(writtenSchemaHash);

            var reader = dsA.SnapshotReader(head.Revision);
            liveBefore = await Collect(reader.QueryRelationships(new RelationshipsFilter { OptionalResourceType = "doc" }));
            countBefore = await reader.CountRelationships("doc_viewers");
        }
        finally
        {
            await clusterA.DisposeAsync();
        }

        // --- Phase 2: a BRAND-NEW cluster over the same Postgres (true reactivation). ---
        var clusterB = await BuildClusterAsync(_fixture.ConnectionString);
        try
        {
            var dsB = Datastore(clusterB);

            // Negative control: lost state would re-seed Empty(NowNanos) — a different, larger head.
            var head = await dsB.HeadRevision();
            Assert.Equal(writtenHead, ((TimestampRevision)head.Revision).TimestampNanosSinceEpoch);
            Assert.Equal(writtenSchemaHash, head.SchemaHash);
            Assert.Equal(writtenFloor, (await Grain(clusterB).GetHead()).GcFloor);

            // Reads: the live set, payload fidelity, schema bytes, counter, all identical.
            var reader = dsB.SnapshotReader(head.Revision);
            Assert.Equal(liveBefore, await Collect(reader.QueryRelationships(new RelationshipsFilter { OptionalResourceType = "doc" })));
            Assert.Equal(countBefore, await reader.CountRelationships("doc_viewers"));
            var counterFilter = await reader.ReadCounterFilter("doc_viewers");
            Assert.Equal("doc", counterFilter!.OptionalResourceType);
            Assert.Equal("viewer", counterFilter.OptionalResourceRelation);
            var schema = await reader.ReadStoredSchema();
            Assert.True(Encoding.UTF8.GetBytes(SchemaText).SequenceEqual(schema!));

            var rels = new List<Relationship>();
            await foreach (var rel in reader.QueryRelationships(new RelationshipsFilter
            {
                OptionalResourceType = "doc",
                OptionalResourceIds = ["caveated", "expiring"],
            }))
                rels.Add(rel);
            var caveated = rels.Single(r => r.Resource.ObjectId == "caveated");
            Assert.Equal("is_active", caveated.OptionalCaveat!.CaveatName);
            Assert.Equal("\"eu\"", ((JsonElement)caveated.OptionalCaveat.Context!["region"]!).GetRawText());
            Assert.Equal("7", ((JsonElement)caveated.OptionalCaveat.Context!["level"]!).GetRawText());
            Assert.Equal(Expiry, rels.Single(r => r.Resource.ObjectId == "expiring").OptionalExpiration);

            // The durable floor still rejects a below-floor pin.
            await Assert.ThrowsAsync<RevisionNotFoundException>(async () =>
            {
                var stale = dsB.SnapshotReader(new TimestampRevision(writtenFloor - 1));
                await foreach (var _ in stale.QueryRelationships(new RelationshipsFilter()))
                {
                }
            });

            // Watch cursor replay: the two post-cursor commits, in order, from the rebuilt log tail.
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var replayed = new List<RevisionChange>();
            await foreach (var change in dsB.Watch(cursor, new WatchOptions(WatchContent.Relationships), cts.Token))
            {
                replayed.Add(change);
                if (replayed.Count == 2)
                    break;
            }
            Assert.Equal("post-1", Assert.Single(replayed[0].RelationshipChanges).Relationship.Resource.ObjectId);
            Assert.Equal("post-2", Assert.Single(replayed[1].RelationshipChanges).Relationship.Resource.ObjectId);

            // What B recovered rows from really is the projection: doc:plain was projected at the
            // flush, so its freshly activated shard grain reads its rows back from storage, and the
            // shard index lists it.
            var plain = await clusterB.GrainFactory
                .GetGrain<IGraphShardGrain>(GraphShardGrainKey.Build(GraphShardKeyWire.ForResource("doc", "plain")))
                .ReadProjected();
            Assert.Equal("alice", Assert.Single(plain.Rows).Relationship.SubjectId);
            var listed = await clusterB.GrainFactory
                .GetGrain<IShardIndexGrain>(ShardIndexKey.Build(GraphShardDirection.Forward, "doc", ShardIndexKey.BucketOf("plain")))
                .ReadIds();
            Assert.Contains("plain", listed);
        }
        finally
        {
            await clusterB.DisposeAsync();
        }
    }
}
