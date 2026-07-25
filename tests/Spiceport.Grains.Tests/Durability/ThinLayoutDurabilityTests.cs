using System.Collections.Immutable;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.TestingHost;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains;
using Spiceport.Grains.Abstractions;
using Spiceport.Server.Hosting;

namespace Spiceport.Grains.Tests.Durability;

/// <summary>
/// Durability gates for the THIN-SEQUENCER storage layout against REAL Postgres (Orleans AdoNet grain
/// storage, same Testcontainers fixture and skip-without-Docker discipline as
/// <see cref="DatastoreGrainDurabilityTests"/>): a mixed load committed past a flush boundary through
/// cluster A must be served with full fidelity by a BRAND-NEW cluster B over the same database — reads,
/// schema, counters, the GC floor's below-floor rejection, and a Watch cursor replay — with the
/// storage-level probes pinning that what cluster B recovered from really was the NEW row map
/// (<c>head</c> + <c>meta/{v}</c> + per-key <c>shard/…</c> rows + post-flush log tail, no whole-state
/// snapshot row).
/// </summary>
[Collection(AdoNetDurabilityCollection.Name)]
public sealed class NewLayoutRestartDurabilityTests
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

    public NewLayoutRestartDurabilityTests(AdoNetDatastoreFixture fixture) => _fixture = fixture;

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
    private sealed class ThinRestartSiloConfigurator : ISiloConfigurator
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
            siloBuilder.AddCustomStorageBasedLogConsistencyProvider("CustomStorage");
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
        builder.Options.ServiceId = "spiceport-durability-thin-restart";
        builder.AddSiloBuilderConfigurator<ThinRestartSiloConfigurator>();
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

    private static async Task<GrainState<T>> ReadRow<T>(TestCluster cluster, string stateName)
    {
        var storage = ((InProcessSiloHandle)cluster.Primary!).SiloHost.Services
            .GetRequiredKeyedService<IGrainStorage>(DatastoreStorageConfig.ProviderName);
        var grainId = ((GrainReference)Grain(cluster)).GrainId;
        var entry = new GrainState<T>();
        await storage.ReadStateAsync(stateName, grainId, entry);
        return entry;
    }

    // AdoNet "clear" nulls the payload but KEEPS the row (version bumped), and reading a null-payload
    // row yields RecordExists=true with an EMPTY-constructed instance — so live-vs-cleared must be
    // judged by a payload sentinel that no live row can carry (revisions/log versions are always > 0),
    // not by RecordExists. A row that never existed reads RecordExists=false.
    private static bool LiveMeta(GrainState<DatastoreMetaEntry> e) => e.RecordExists && e.State?.Meta is not null;
    private static bool LiveSnapshot(GrainState<DatastoreGrainState> e) => e.RecordExists && e.State is { HeadRevision: > 0 };
    private static bool LiveLogEvent(GrainState<LogEvent> e) => e.RecordExists && e.State is { Revision: > 0 };
    private static bool LiveShard(GrainState<GraphShardState> e) => e.RecordExists && e.State is { AppliedRevision: > 0 };
    private static bool LiveBucket(GrainState<KeyIndexBucketEntry> e) => e.RecordExists && e.State?.Entries is not null;
    private static bool LiveDelta(GrainState<KeyIndexDeltaEntry> e) => e.RecordExists && e.State?.ForwardEntries is not null;

    [SkippableFact]
    public async Task NewLayout_FullFidelity_Survives_TrueReactivation_PastAFlushBoundary()
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

            // Storage-level: what B recovered from really is the NEW layout — a 64-boundary meta row
            // plus shard rows, log trimmed through the boundary, and NO whole-state snapshot row.
            var headRow = await ReadRow<LogHeadEntry>(clusterB, "head");
            Assert.True(headRow.RecordExists);
            var flushVersion = headRow.State!.SnapshotVersion;
            Assert.True(flushVersion >= 64, $"no flush crossed: SnapshotVersion {flushVersion}");
            Assert.Equal(headRow.State.LogVersion - headRow.State.LogVersion % 64, flushVersion);
            Assert.True(LiveMeta(await ReadRow<DatastoreMetaEntry>(clusterB, $"meta/{flushVersion}")));
            Assert.False(LiveSnapshot(await ReadRow<DatastoreGrainState>(clusterB, $"snapshot/{flushVersion}")));
            Assert.False(LiveLogEvent(await ReadRow<LogEvent>(clusterB, "log/1"))); // trimmed at the flush
            Assert.True(
                LiveLogEvent(await ReadRow<LogEvent>(clusterB, $"log/{flushVersion + 1}")),
                "the first post-flush log entry is missing — the tail was over-trimmed");
            // Shard rows are version-qualified: doc:plain was dirty at the (only) flush boundary, so
            // its row lives under that flush version and is reachable only through the meta index.
            Assert.True(
                LiveShard(await ReadRow<GraphShardState>(clusterB,
                    $"shard/{flushVersion}/" + GraphShardGrainKey.Build(GraphShardKeyWire.ForResource("doc", "plain")))),
                "the flushed forward shard row for doc:plain is missing");

            // Durable layout v2: the meta row is SLIM (no inline key maps — cardinality-independent)
            // and what cluster B recovered the index from really was the chunked rows: the flush's
            // delta row plus the rotated bucket pair (the rotation cursor starts at 0, and exactly one
            // flush happened, so bucket 0 of each direction lives at the flush version).
            var metaRowB = await ReadRow<DatastoreMetaEntry>(clusterB, $"meta/{flushVersion}");
            Assert.True(LiveMeta(metaRowB));
            Assert.NotNull(metaRowB.State!.IndexLayout);
            Assert.Empty(metaRowB.State.Meta.ForwardKeys);
            Assert.Empty(metaRowB.State.Meta.ReverseKeys);
            Assert.Contains(flushVersion, metaRowB.State.IndexLayout!.DeltaVersions);
            var deltaRowB = await ReadRow<KeyIndexDeltaEntry>(clusterB, $"indexd/{flushVersion}");
            Assert.True(LiveDelta(deltaRowB), "the flush's indexd/{v} delta row is missing");
            Assert.Equal(
                flushVersion,
                deltaRowB.State!.ForwardEntries[GraphShardGrainKey.Build(GraphShardKeyWire.ForResource("doc", "plain"))]);
            Assert.True(
                LiveBucket(await ReadRow<KeyIndexBucketEntry>(clusterB, $"indexb/{flushVersion}/f/0")),
                "the rotated forward bucket row is missing");
            Assert.True(
                LiveBucket(await ReadRow<KeyIndexBucketEntry>(clusterB, $"indexb/{flushVersion}/r/0")),
                "the rotated reverse bucket row is missing");
        }
        finally
        {
            await clusterB.DisposeAsync();
        }
    }
}

/// <summary>
/// The MIGRATION gate: a database seeded in the RETIRED whole-state layout (one <c>snapshot/{v}</c> row
/// holding the full <see cref="DatastoreGrainState"/> plus the <c>head</c> row naming it — written
/// directly through the same keyed storage provider the grain uses, exactly as the pre-rework grain laid
/// it out) must be migrated IN PLACE by the reworked grain's first activation: reads serve the migrated
/// data exactly (including MVCC time travel over a dead row and boxed-JsonElement caveat context),
/// per-key shard rows and the <c>meta/{v}</c> row now exist, the legacy snapshot row is cleared, and
/// subsequent commits flush normally through the new protocol.
/// </summary>
[Collection(AdoNetDurabilityCollection.Name)]
public sealed class LegacyMigrationDurabilityTests
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

    public LegacyMigrationDurabilityTests(AdoNetDatastoreFixture fixture) => _fixture = fixture;

    private static class ConnHolder
    {
        public static string ConnectionString = string.Empty;
    }

    private sealed class MigrationSiloConfigurator : ISiloConfigurator
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
            siloBuilder.AddCustomStorageBasedLogConsistencyProvider("CustomStorage");
            siloBuilder.ConfigureServices(services =>
            {
                services.AddSpiceportGrainServices(SchemaText);
                services.AddSingleton<IDatastore>(sp =>
                    new GrainBackedDatastore(
                        sp.GetRequiredService<IGrainFactory>(), sp.GetRequiredService<LogWatchHub>()));
            });
        }
    }

    private static async Task<TestCluster> BuildClusterAsync(string connectionString)
    {
        ConnHolder.ConnectionString = connectionString;
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.Options.ServiceId = "spiceport-durability-thin-migration";
        builder.AddSiloBuilderConfigurator<MigrationSiloConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        return cluster;
    }

    private static IDatastore Datastore(TestCluster cluster) =>
        ((InProcessSiloHandle)cluster.Primary!).SiloHost.Services.GetRequiredService<IDatastore>();

    private static IDatastoreGrain Grain(TestCluster cluster) =>
        cluster.GrainFactory.GetGrain<IDatastoreGrain>(IDatastoreGrain.Key);

    private static IGrainStorage Storage(TestCluster cluster) =>
        ((InProcessSiloHandle)cluster.Primary!).SiloHost.Services
            .GetRequiredKeyedService<IGrainStorage>(DatastoreStorageConfig.ProviderName);

    private static GrainId DatastoreGrainId(TestCluster cluster) =>
        ((GrainReference)Grain(cluster)).GrainId;

    private static async Task WriteRow<T>(IGrainStorage storage, GrainId grainId, string stateName, T value)
    {
        // Read-then-write so the wrapper carries the current ETag if the row already exists (a fresh
        // database yields a null ETag, a valid insert) — the same discipline the grain itself uses.
        var entry = new GrainState<T>();
        await storage.ReadStateAsync(stateName, grainId, entry);
        entry.State = value;
        await storage.WriteStateAsync(stateName, grainId, entry);
    }

    private static async Task<GrainState<T>> ReadRow<T>(TestCluster cluster, string stateName)
    {
        var entry = new GrainState<T>();
        await Storage(cluster).ReadStateAsync(stateName, DatastoreGrainId(cluster), entry);
        return entry;
    }

    // See NewLayoutRestartDurabilityTests: AdoNet clears keep the row (null payload, which reads back
    // as an EMPTY-constructed instance), so live-vs-cleared is judged by a payload sentinel no live
    // row can carry, never by RecordExists alone.
    private static bool LiveMeta(GrainState<DatastoreMetaEntry> e) => e.RecordExists && e.State?.Meta is not null;
    private static bool LiveSnapshot(GrainState<DatastoreGrainState> e) => e.RecordExists && e.State is { HeadRevision: > 0 };
    private static bool LiveShard(GrainState<GraphShardState> e) => e.RecordExists && e.State is { AppliedRevision: > 0 };
    private static bool LiveBucket(GrainState<KeyIndexBucketEntry> e) => e.RecordExists && e.State?.Entries is not null;
    private static bool LiveDelta(GrainState<KeyIndexDeltaEntry> e) => e.RecordExists && e.State?.ForwardEntries is not null;

    // Version-qualified row name: the migration writes every split row under its own meta version.
    private static string ShardRowKey(int rowVersion, GraphShardKeyWire key) =>
        $"shard/{rowVersion}/" + GraphShardGrainKey.Build(key);

    [SkippableFact]
    public async Task LegacyWholeStateSnapshot_IsMigratedInPlace_AndCommitsFlushNormally()
    {
        Skip.IfNot(_fixture.Available, _fixture.SkipReason ?? "Postgres fixture unavailable");

        // The legacy state, stamped with plausible recent timestamp revisions (nanos since epoch):
        // two live rows (one caveated, with boxed-JsonElement context), one DEAD row (created then
        // deleted — MVCC history the migration must preserve for time travel), a schema version whose
        // hash is computed exactly as the write path computes it, and one registered counter.
        var head = (DateTimeOffset.UtcNow - DateTimeOffset.UnixEpoch).Ticks * 100L;
        var createdAt = head - 2_000_000L;  // 2ms before head
        var deletedAt = head - 1_000_000L;  // 1ms before head
        var schemaBytes = Encoding.UTF8.GetBytes(SchemaText);
        var schemaHash = Convert.ToHexStringLower(SHA256.HashData(schemaBytes));
        var caveatContext =
            JsonSerializer.Deserialize<Dictionary<string, object?>>("""{"region":"eu","level":7}""")!;

        StoredRelationshipWire Stored(Relationship rel, long created, long? deleted = null) =>
            new(WireConvert.ToWire(rel), created, deleted);

        var plain = Relationship.Create(
            new ObjectAndRelation("doc", "legacy-plain", "viewer"),
            new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis));
        var caveated = Relationship.Create(
            new ObjectAndRelation("doc", "legacy-caveated", "viewer"),
            new ObjectAndRelation("user", "bob", CoreConstants.Ellipsis),
            caveat: new ContextualizedCaveat("is_active", caveatContext));
        var dead = Relationship.Create(
            new ObjectAndRelation("doc", "legacy-dead", "viewer"),
            new ObjectAndRelation("user", "eve", CoreConstants.Ellipsis));

        var legacy = new DatastoreGrainState
        {
            HeadRevision = head,
            Relationships =
            [
                Stored(plain, createdAt),
                Stored(caveated, createdAt),
                Stored(dead, createdAt, deletedAt),
            ],
            Schemas = [new SchemaVersionWire(createdAt, schemaBytes, schemaHash)],
            Counters =
            [
                new CounterVersionWire(createdAt, "doc_viewers", WireConvert.ToFullFilter(new RelationshipsFilter
                {
                    OptionalResourceType = "doc",
                    OptionalResourceRelation = "viewer",
                })),
            ],
            GcFloor = 0,
        };
        const int legacyVersion = 3; // the pre-rework layout: head names snapshot/{v} with LogVersion == v

        // --- Phase 1: seed the database in the OLD layout through the storage provider directly,
        // WITHOUT ever calling the datastore grain (getting a grain reference does not activate it). ---
        var seedCluster = await BuildClusterAsync(_fixture.ConnectionString);
        try
        {
            var storage = Storage(seedCluster);
            var grainId = DatastoreGrainId(seedCluster);
            await WriteRow(storage, grainId, $"snapshot/{legacyVersion}", legacy);
            await WriteRow(storage, grainId, "head", new LogHeadEntry(legacyVersion, head, legacyVersion));
        }
        finally
        {
            await seedCluster.DisposeAsync();
        }

        // --- Phase 2: activate the reworked grain against the legacy store (a brand-new cluster). ---
        var cluster = await BuildClusterAsync(_fixture.ConnectionString);
        try
        {
            var ds = Datastore(cluster);

            // The migrated head/schema-hash are exactly the legacy ones (not a re-seeded empty state).
            var headRead = await ds.HeadRevision();
            Assert.Equal(head, ((TimestampRevision)headRead.Revision).TimestampNanosSinceEpoch);
            Assert.Equal(schemaHash, headRead.SchemaHash);

            // Reads serve the migrated data exactly: live rows at head, the dead row invisible …
            var reader = ds.SnapshotReader(headRead.Revision);
            var ids = new List<string>();
            await foreach (var rel in reader.QueryRelationships(new RelationshipsFilter { OptionalResourceType = "doc" }))
                ids.Add(rel.Resource.ObjectId);
            ids.Sort(StringComparer.Ordinal);
            Assert.Equal(["legacy-caveated", "legacy-plain"], ids);

            // … including payload fidelity (boxed-JsonElement caveat context through migration) …
            var caveatedRead = new List<Relationship>();
            await foreach (var rel in reader.QueryRelationships(new RelationshipsFilter
            {
                OptionalResourceType = "doc",
                OptionalResourceIds = ["legacy-caveated"],
            }))
                caveatedRead.Add(rel);
            var cav = Assert.Single(caveatedRead).OptionalCaveat!;
            Assert.Equal("is_active", cav.CaveatName);
            Assert.Equal("\"eu\"", ((JsonElement)cav.Context!["region"]!).GetRawText());
            Assert.Equal("7", ((JsonElement)cav.Context!["level"]!).GetRawText());

            // … and MVCC time travel: pinned between the dead row's create and delete, it is visible.
            var timeTravel = ds.SnapshotReader(new TimestampRevision(head - 1_500_000L));
            var atPast = new List<string>();
            await foreach (var rel in timeTravel.QueryRelationships(new RelationshipsFilter { OptionalResourceType = "doc" }))
                atPast.Add(rel.Resource.ObjectId);
            Assert.Contains("legacy-dead", atPast);

            // Schema bytes and the counter migrated whole.
            var migratedSchema = await reader.ReadStoredSchema();
            Assert.True(schemaBytes.SequenceEqual(migratedSchema!));
            var counterFilter = await reader.ReadCounterFilter("doc_viewers");
            Assert.Equal("doc", counterFilter!.OptionalResourceType);
            Assert.Equal(2ul, await reader.CountRelationships("doc_viewers"));

            // The store is now the NEW layout: meta + per-key shard rows (both directions) exist and
            // the legacy snapshot row is cleared.
            var metaRow = await ReadRow<DatastoreMetaEntry>(cluster, $"meta/{legacyVersion}");
            Assert.True(LiveMeta(metaRow), "migration did not write the meta/{v} row");
            Assert.Equal(legacyVersion, metaRow.State!.FlushedThroughLogVersion);

            // The v0 migration lands DIRECTLY on durable layout v2: the meta row is slim (no inline
            // key maps), carries the chunked-index layout with FULL bucket coverage at the migration
            // version (DeltaFloorVersion starts there; no pending deltas), and the bucket holding
            // doc:legacy-plain's forward key maps it to the migration version. Full coverage means
            // even a bucket holding NO migrated key has an (empty) row.
            Assert.NotNull(metaRow.State.IndexLayout);
            Assert.Empty(metaRow.State.Meta.ForwardKeys);
            Assert.Empty(metaRow.State.Meta.ReverseKeys);
            var layout = metaRow.State.IndexLayout!;
            Assert.Equal(legacyVersion, layout.DeltaFloorVersion);
            Assert.Empty(layout.DeltaVersions);
            Assert.All(layout.ForwardBucketVersions, v => Assert.Equal(legacyVersion, v));
            var plainForwardKey = GraphShardGrainKey.Build(GraphShardKeyWire.ForResource("doc", "legacy-plain"));
            var plainBucket = KeyIndexLayout.BucketOf(plainForwardKey, layout.BucketCount);
            var plainBucketRow = await ReadRow<KeyIndexBucketEntry>(
                cluster, $"indexb/{legacyVersion}/f/{plainBucket}");
            Assert.True(LiveBucket(plainBucketRow), "migration did not write the forward bucket row");
            Assert.Equal(legacyVersion, plainBucketRow.State!.Entries[plainForwardKey]);
            var occupiedForwardBuckets = new[] { "legacy-plain", "legacy-caveated", "legacy-dead" }
                .Select(id => KeyIndexLayout.BucketOf(
                    GraphShardGrainKey.Build(GraphShardKeyWire.ForResource("doc", id)), layout.BucketCount))
                .ToHashSet();
            var emptyBucket = Enumerable.Range(0, layout.BucketCount).First(b => !occupiedForwardBuckets.Contains(b));
            var emptyBucketRow = await ReadRow<KeyIndexBucketEntry>(
                cluster, $"indexb/{legacyVersion}/f/{emptyBucket}");
            Assert.True(LiveBucket(emptyBucketRow), "migration did not write full bucket coverage");
            Assert.Empty(emptyBucketRow.State!.Entries);
            Assert.True(
                LiveShard(await ReadRow<GraphShardState>(cluster, ShardRowKey(legacyVersion, GraphShardKeyWire.ForResource("doc", "legacy-plain")))),
                "migration did not split out the forward shard row");

            // REVERSE ROW CONTENT, not just liveness: the reverse split of user:alice must carry
            // exactly the stored rows derivable from the FORWARD data — the legacy snapshot's rows
            // whose subject is user:alice, with identical payloads and MVCC stamps. Liveness alone
            // would pass a migration that wrote the right key with wrong or duplicated content.
            static List<string> RowMultiset(IEnumerable<StoredRelationshipWire> rows) => rows
                .Select(r => $"{r.Relationship.ResourceType}:{r.Relationship.ResourceId}#{r.Relationship.ResourceRelation}"
                    + $"@{r.Relationship.SubjectType}:{r.Relationship.SubjectId}#{r.Relationship.SubjectRelation}"
                    + $"|created={r.CreatedRevision}|deleted={r.DeletedRevision?.ToString() ?? "live"}")
                .OrderBy(s => s, StringComparer.Ordinal)
                .ToList();
            var reverseAlice = await ReadRow<GraphShardState>(
                cluster, ShardRowKey(legacyVersion, GraphShardKeyWire.ForSubject("user", "alice")));
            Assert.True(LiveShard(reverseAlice), "migration did not split out the reverse shard row");
            Assert.Equal(
                RowMultiset(legacy.Relationships.Where(r =>
                    r.Relationship.SubjectType == "user" && r.Relationship.SubjectId == "alice")),
                RowMultiset(reverseAlice.State!.Rows));

            // And the DEAD row's reverse split: the created+deleted stamps must migrate intact too.
            var reverseEve = await ReadRow<GraphShardState>(
                cluster, ShardRowKey(legacyVersion, GraphShardKeyWire.ForSubject("user", "eve")));
            Assert.True(LiveShard(reverseEve), "migration did not split out the dead row's reverse shard row");
            Assert.Equal(
                RowMultiset(legacy.Relationships.Where(r =>
                    r.Relationship.SubjectType == "user" && r.Relationship.SubjectId == "eve")),
                RowMultiset(reverseEve.State!.Rows));
            Assert.False(
                LiveSnapshot(await ReadRow<DatastoreGrainState>(cluster, $"snapshot/{legacyVersion}")),
                "the legacy whole-state snapshot row was not cleared");

            // Subsequent commits flush normally: enough commits to cross the next 64-boundary, then the
            // head row must name a NEW meta version, the old meta is cleared, and reads stay exact.
            for (var i = 0; i < 64; i++)
            {
                await ds.ReadWriteTx(tx => tx.WriteRelationships(
                [
                    new RelationshipUpdate(
                        Relationship.Create(
                            new ObjectAndRelation("doc", $"post-{i}", "viewer"),
                            new ObjectAndRelation("user", $"u{i}", CoreConstants.Ellipsis)),
                        UpdateOperation.Create),
                ]));
            }

            var headRow = await ReadRow<LogHeadEntry>(cluster, "head");
            Assert.True(headRow.RecordExists);
            Assert.True(
                headRow.State!.SnapshotVersion > legacyVersion,
                $"no post-migration flush happened: SnapshotVersion still {headRow.State.SnapshotVersion}");
            Assert.Equal(headRow.State.LogVersion - headRow.State.LogVersion % 64, headRow.State.SnapshotVersion);
            Assert.True(LiveMeta(await ReadRow<DatastoreMetaEntry>(cluster, $"meta/{headRow.State.SnapshotVersion}")));
            Assert.False(
                LiveMeta(await ReadRow<DatastoreMetaEntry>(cluster, $"meta/{legacyVersion}")),
                "the superseded migration meta row was not cleared by the flush");

            // Post-migration flushes speak the v2 protocol: the flush wrote its delta row and rotated
            // bucket 0 in both directions (the migration resets the rotation cursor to 0).
            Assert.True(
                LiveDelta(await ReadRow<KeyIndexDeltaEntry>(cluster, $"indexd/{headRow.State.SnapshotVersion}")),
                "the post-migration flush wrote no indexd/{v} delta row");
            Assert.True(
                LiveBucket(await ReadRow<KeyIndexBucketEntry>(cluster, $"indexb/{headRow.State.SnapshotVersion}/f/0")),
                "the post-migration flush wrote no rotated forward bucket row");
            Assert.True(
                LiveBucket(await ReadRow<KeyIndexBucketEntry>(cluster, $"indexb/{headRow.State.SnapshotVersion}/r/0")),
                "the post-migration flush wrote no rotated reverse bucket row");

            var newHead = await ds.HeadRevision();
            var finalReader = ds.SnapshotReader(newHead.Revision);
            var finalCount = 0;
            await foreach (var _ in finalReader.QueryRelationships(new RelationshipsFilter { OptionalResourceType = "doc" }))
                finalCount++;
            Assert.Equal(2 + 64, finalCount); // the two live legacy rows plus every post-migration row

            // The caveated legacy row's context survived the post-migration flush cycle too.
            var caveatedAfter = new List<Relationship>();
            await foreach (var rel in finalReader.QueryRelationships(new RelationshipsFilter
            {
                OptionalResourceType = "doc",
                OptionalResourceIds = ["legacy-caveated"],
            }))
                caveatedAfter.Add(rel);
            Assert.Equal(
                "\"eu\"",
                ((JsonElement)Assert.Single(caveatedAfter).OptionalCaveat!.Context!["region"]!).GetRawText());
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }
}

/// <summary>
/// The v1-to-v2 MIGRATION gate: a database seeded in the RETIRED v1 thin-sequencer layout — per-key
/// shard rows plus a <c>meta/{v}</c> row carrying the key-index maps INLINE (the old-shape
/// <see cref="DatastoreMetaEntry"/> with no <see cref="DatastoreMetaEntry.IndexLayout"/>), written
/// directly through the same keyed storage provider the grain uses — must be migrated in place by the
/// reworked grain's first activation: reads serve the migrated data exactly (including MVCC time travel
/// over a dead row), the meta row is rewritten SLIM with full bucket-row coverage at the migration
/// version, shard rows are untouched (row-level content equality), and subsequent commits flush through
/// the v2 protocol (delta + rotated bucket rows).
/// </summary>
[Collection(AdoNetDurabilityCollection.Name)]
public sealed class InlineIndexMigrationDurabilityTests
{
    private const string SchemaText = """
        definition user {}

        definition doc {
          relation viewer: user
        }
        """;

    private readonly AdoNetDatastoreFixture _fixture;

    public InlineIndexMigrationDurabilityTests(AdoNetDatastoreFixture fixture) => _fixture = fixture;

    private static class ConnHolder
    {
        public static string ConnectionString = string.Empty;
    }

    private sealed class InlineMigrationSiloConfigurator : ISiloConfigurator
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
            siloBuilder.AddCustomStorageBasedLogConsistencyProvider("CustomStorage");
            siloBuilder.ConfigureServices(services =>
            {
                services.AddSpiceportGrainServices(SchemaText);
                services.AddSingleton<IDatastore>(sp =>
                    new GrainBackedDatastore(
                        sp.GetRequiredService<IGrainFactory>(), sp.GetRequiredService<LogWatchHub>()));
            });
        }
    }

    private static async Task<TestCluster> BuildClusterAsync(string connectionString)
    {
        ConnHolder.ConnectionString = connectionString;
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.Options.ServiceId = "spiceport-durability-thin-v1-migration";
        builder.AddSiloBuilderConfigurator<InlineMigrationSiloConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        return cluster;
    }

    private static IDatastore Datastore(TestCluster cluster) =>
        ((InProcessSiloHandle)cluster.Primary!).SiloHost.Services.GetRequiredService<IDatastore>();

    private static IDatastoreGrain Grain(TestCluster cluster) =>
        cluster.GrainFactory.GetGrain<IDatastoreGrain>(IDatastoreGrain.Key);

    private static IGrainStorage Storage(TestCluster cluster) =>
        ((InProcessSiloHandle)cluster.Primary!).SiloHost.Services
            .GetRequiredKeyedService<IGrainStorage>(DatastoreStorageConfig.ProviderName);

    private static GrainId DatastoreGrainId(TestCluster cluster) =>
        ((GrainReference)Grain(cluster)).GrainId;

    private static async Task WriteRow<T>(IGrainStorage storage, GrainId grainId, string stateName, T value)
    {
        var entry = new GrainState<T>();
        await storage.ReadStateAsync(stateName, grainId, entry);
        entry.State = value;
        await storage.WriteStateAsync(stateName, grainId, entry);
    }

    private static async Task<GrainState<T>> ReadRow<T>(TestCluster cluster, string stateName)
    {
        var entry = new GrainState<T>();
        await Storage(cluster).ReadStateAsync(stateName, DatastoreGrainId(cluster), entry);
        return entry;
    }

    // See NewLayoutRestartDurabilityTests for the AdoNet null-payload sentinel rationale.
    private static bool LiveMeta(GrainState<DatastoreMetaEntry> e) => e.RecordExists && e.State?.Meta is not null;
    private static bool LiveBucket(GrainState<KeyIndexBucketEntry> e) => e.RecordExists && e.State?.Entries is not null;
    private static bool LiveDelta(GrainState<KeyIndexDeltaEntry> e) => e.RecordExists && e.State?.ForwardEntries is not null;

    private static string ShardRowKey(int rowVersion, GraphShardKeyWire key) =>
        $"shard/{rowVersion}/" + GraphShardGrainKey.Build(key);

    private static List<string> RowMultiset(IEnumerable<StoredRelationshipWire> rows) => rows
        .Select(r => $"{r.Relationship.ResourceType}:{r.Relationship.ResourceId}#{r.Relationship.ResourceRelation}"
            + $"@{r.Relationship.SubjectType}:{r.Relationship.SubjectId}#{r.Relationship.SubjectRelation}"
            + $"|created={r.CreatedRevision}|deleted={r.DeletedRevision?.ToString() ?? "live"}")
        .OrderBy(s => s, StringComparer.Ordinal)
        .ToList();

    [SkippableFact]
    public async Task V1InlineIndexMeta_IsChunkedInPlace_AndCommitsFlushThroughV2()
    {
        Skip.IfNot(_fixture.Available, _fixture.SkipReason ?? "Postgres fixture unavailable");

        // The v1 state: one live row, one DEAD row (created then deleted — MVCC history the migration
        // must preserve), a schema version hashed exactly as the write path hashes it, and the inline
        // key-index maps pointing every key at the shard rows' version.
        var head = (DateTimeOffset.UtcNow - DateTimeOffset.UnixEpoch).Ticks * 100L;
        var createdAt = head - 2_000_000L;
        var deletedAt = head - 1_000_000L;
        var schemaBytes = Encoding.UTF8.GetBytes(SchemaText);
        var schemaHash = Convert.ToHexStringLower(SHA256.HashData(schemaBytes));

        var plain = Relationship.Create(
            new ObjectAndRelation("doc", "v1-plain", "viewer"),
            new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis));
        var dead = Relationship.Create(
            new ObjectAndRelation("doc", "v1-dead", "viewer"),
            new ObjectAndRelation("user", "eve", CoreConstants.Ellipsis));
        var plainStored = new StoredRelationshipWire(WireConvert.ToWire(plain), createdAt, null);
        var deadStored = new StoredRelationshipWire(WireConvert.ToWire(dead), createdAt, deletedAt);

        const int v1Version = 5; // v1 layout: head names meta/{v} with LogVersion == v, maps inline

        var shardRows = new Dictionary<GraphShardKeyWire, StoredRelationshipWire>
        {
            [GraphShardKeyWire.ForResource("doc", "v1-plain")] = plainStored,
            [GraphShardKeyWire.ForResource("doc", "v1-dead")] = deadStored,
            [GraphShardKeyWire.ForSubject("user", "alice")] = plainStored,
            [GraphShardKeyWire.ForSubject("user", "eve")] = deadStored,
        };
        var forward = ImmutableDictionary.CreateBuilder<string, int>(StringComparer.Ordinal);
        var reverse = ImmutableDictionary.CreateBuilder<string, int>(StringComparer.Ordinal);
        foreach (var key in shardRows.Keys)
        {
            var keyString = GraphShardGrainKey.Build(key);
            if (keyString.StartsWith("f/", StringComparison.Ordinal))
                forward[keyString] = v1Version;
            else
                reverse[keyString] = v1Version;
        }
        var v1Meta = new DatastoreMetaState
        {
            HeadRevision = head,
            Schemas = [new SchemaVersionWire(createdAt, schemaBytes, schemaHash)],
            Counters = [],
            GcFloor = 0,
            ForwardKeys = forward.ToImmutable(),
            ReverseKeys = reverse.ToImmutable(),
        };

        // --- Phase 1: seed the database in the v1 layout through the storage provider directly (the
        // old-shape meta entry: inline maps, no IndexLayout), WITHOUT activating the grain. ---
        var seedCluster = await BuildClusterAsync(_fixture.ConnectionString);
        try
        {
            var storage = Storage(seedCluster);
            var grainId = DatastoreGrainId(seedCluster);
            foreach (var (key, row) in shardRows)
            {
                await WriteRow(storage, grainId, ShardRowKey(v1Version, key),
                    new GraphShardState(head, 0, ImmutableList.Create(row)));
            }
            await WriteRow(storage, grainId, $"meta/{v1Version}", new DatastoreMetaEntry(v1Meta, v1Version));
            await WriteRow(storage, grainId, "head", new LogHeadEntry(v1Version, head, v1Version));
        }
        finally
        {
            await seedCluster.DisposeAsync();
        }

        // --- Phase 2: activate the grain against the v1 store (a brand-new cluster). ---
        var cluster = await BuildClusterAsync(_fixture.ConnectionString);
        try
        {
            var ds = Datastore(cluster);

            // Migrated head/schema-hash exactly the seeded ones (not a re-seeded empty state).
            var headRead = await ds.HeadRevision();
            Assert.Equal(head, ((TimestampRevision)headRead.Revision).TimestampNanosSinceEpoch);
            Assert.Equal(schemaHash, headRead.SchemaHash);

            // Reads serve the migrated data exactly: the live row at head, the dead row only via
            // MVCC time travel between its create and delete.
            var reader = ds.SnapshotReader(headRead.Revision);
            var ids = new List<string>();
            await foreach (var rel in reader.QueryRelationships(new RelationshipsFilter { OptionalResourceType = "doc" }))
                ids.Add(rel.Resource.ObjectId);
            Assert.Equal(["v1-plain"], ids);
            var timeTravel = ds.SnapshotReader(new TimestampRevision(head - 1_500_000L));
            var atPast = new List<string>();
            await foreach (var rel in timeTravel.QueryRelationships(new RelationshipsFilter { OptionalResourceType = "doc" }))
                atPast.Add(rel.Resource.ObjectId);
            atPast.Sort(StringComparer.Ordinal);
            Assert.Equal(["v1-dead", "v1-plain"], atPast);

            // The meta row was rewritten SLIM in place: no inline maps, a layout with full bucket
            // coverage at the migration version (DeltaFloorVersion there, no pending deltas), and the
            // bucket holding the live row's forward key maps it to the shard rows' UNCHANGED version.
            var metaRow = await ReadRow<DatastoreMetaEntry>(cluster, $"meta/{v1Version}");
            Assert.True(LiveMeta(metaRow), "the migrated meta/{v} row is missing");
            Assert.Equal(v1Version, metaRow.State!.FlushedThroughLogVersion);
            Assert.NotNull(metaRow.State.IndexLayout);
            Assert.Empty(metaRow.State.Meta.ForwardKeys);
            Assert.Empty(metaRow.State.Meta.ReverseKeys);
            var layout = metaRow.State.IndexLayout!;
            Assert.Equal(v1Version, layout.DeltaFloorVersion);
            Assert.Empty(layout.DeltaVersions);
            Assert.All(layout.ForwardBucketVersions, v => Assert.Equal(v1Version, v));
            Assert.All(layout.ReverseBucketVersions, v => Assert.Equal(v1Version, v));
            var plainForwardKey = GraphShardGrainKey.Build(GraphShardKeyWire.ForResource("doc", "v1-plain"));
            var plainBucketRow = await ReadRow<KeyIndexBucketEntry>(
                cluster, $"indexb/{v1Version}/f/{KeyIndexLayout.BucketOf(plainForwardKey, layout.BucketCount)}");
            Assert.True(LiveBucket(plainBucketRow), "the migration wrote no forward bucket row");
            Assert.Equal(v1Version, plainBucketRow.State!.Entries[plainForwardKey]);

            // Shard rows are UNTOUCHED by the index migration: row-level content equality against the
            // seeded rows, forward and reverse, dead-row stamps included.
            foreach (var (key, row) in shardRows)
            {
                var shardRow = await ReadRow<GraphShardState>(cluster, ShardRowKey(v1Version, key));
                Assert.True(shardRow.RecordExists, $"seeded shard row for {GraphShardGrainKey.Build(key)} vanished");
                Assert.Equal(RowMultiset([row]), RowMultiset(shardRow.State!.Rows));
            }

            // Subsequent commits flush through the v2 protocol: cross the next 64-boundary, then the
            // head names a new slim meta, the migration meta is cleared, and the flush wrote its delta
            // row plus the rotated bucket pair (cursor reset to 0 by the migration).
            for (var i = 0; i < 64; i++)
            {
                await ds.ReadWriteTx(tx => tx.WriteRelationships(
                [
                    new RelationshipUpdate(
                        Relationship.Create(
                            new ObjectAndRelation("doc", $"post-{i}", "viewer"),
                            new ObjectAndRelation("user", $"u{i}", CoreConstants.Ellipsis)),
                        UpdateOperation.Create),
                ]));
            }

            var headRow = await ReadRow<LogHeadEntry>(cluster, "head");
            Assert.True(headRow.RecordExists);
            var flushVersion = headRow.State!.SnapshotVersion;
            Assert.True(flushVersion > v1Version, $"no post-migration flush happened: SnapshotVersion {flushVersion}");
            var flushedMeta = await ReadRow<DatastoreMetaEntry>(cluster, $"meta/{flushVersion}");
            Assert.True(LiveMeta(flushedMeta));
            Assert.NotNull(flushedMeta.State!.IndexLayout);
            Assert.Empty(flushedMeta.State.Meta.ForwardKeys);
            Assert.False(
                LiveMeta(await ReadRow<DatastoreMetaEntry>(cluster, $"meta/{v1Version}")),
                "the superseded migration meta row was not cleared by the flush");
            Assert.True(
                LiveDelta(await ReadRow<KeyIndexDeltaEntry>(cluster, $"indexd/{flushVersion}")),
                "the post-migration flush wrote no indexd/{v} delta row");
            Assert.True(
                LiveBucket(await ReadRow<KeyIndexBucketEntry>(cluster, $"indexb/{flushVersion}/f/0")),
                "the post-migration flush wrote no rotated forward bucket row");
            Assert.True(
                LiveBucket(await ReadRow<KeyIndexBucketEntry>(cluster, $"indexb/{flushVersion}/r/0")),
                "the post-migration flush wrote no rotated reverse bucket row");

            // Reads stay exact across the whole cycle: the live migrated row plus every post row.
            var finalReader = ds.SnapshotReader((await ds.HeadRevision()).Revision);
            var finalCount = 0;
            await foreach (var _ in finalReader.QueryRelationships(new RelationshipsFilter { OptionalResourceType = "doc" }))
                finalCount++;
            Assert.Equal(1 + 64, finalCount);
        }
        finally
        {
            await cluster.DisposeAsync();
        }
    }
}
