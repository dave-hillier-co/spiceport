using Microsoft.Extensions.Options;
using Spiceport.Api;
using Spiceport.Datastore;
using Spiceport.Grains;
using Spiceport.Server.Hosting;

var builder = WebApplication.CreateBuilder(args);

// Co-host the Orleans silo in this process. With an in-process silo, Orleans auto-provides
// IGrainFactory / IClusterClient to DI, so the gRPC service can resolve grains directly.
builder.Host.UseOrleans(silo =>
{
    silo.UseLocalhostClustering();
    // CheckGrain's per-activation reply memo (default ON) needs a matching idle-collection age so a
    // warm activation survives long enough between calls for the memo to pay off.
    silo.AddActivationMemoCollectionAge();
    // Storage for the singleton datastore grain (the single source of truth). Durable Postgres when
    // ConnectionStrings:OrleansStorage is configured; otherwise in-memory (default localhost dev = no Postgres).
    silo.AddDatastoreGrainStorage(builder.Configuration);
    // The event-sourced datastore grain owns its persistence via ICustomStorageInterface over the
    // "datastore" grain-storage provider above.
    silo.AddCustomStorageBasedLogConsistencyProvider("CustomStorage");
    // Backs the datastore grain's periodic MVCC-GC reminder ("mvcc-gc"). In-memory is fine even for a
    // durable deployment: losing a reminder registration is safe because the singleton grain re-registers
    // it on every activation (see DatastoreGrain.OnActivateAsync) — a production cluster that wants the
    // reminder to survive a full cluster restart with no activation in between can swap this for a
    // persistent reminder table (e.g. AddReminders / AdoNet reminder storage) without any grain changes.
    silo.UseInMemoryReminderService();
});

// Schema + check-engine singletons (compiled once from the embedded seed schema).
builder.Services.AddSpiceportGrainServices(SeedData.SchemaText);

// The datastore delegates to the cluster-singleton datastore grain. Reads serve from the per-silo
// materialized projection (folded incrementally from the event log) instead of a per-Check full fetch.
// Pass the SAME DatastoreGcOptions the datastore grain is configured with (if any is registered), so this
// datastore's nominal GC window never drifts from the grain's real GcFloor policy.
builder.Services.AddSingleton<IDatastore>(sp =>
    new GrainBackedDatastore(
        sp.GetRequiredService<IGrainFactory>(),
        gcOptions: sp.GetService<IOptions<DatastoreGcOptions>>()));

builder.Services.AddGrpc();

var app = builder.Build();

app.MapGrpcService<PermissionsGrpcService>();
app.MapGrpcService<WatchGrpcService>();
app.MapGrpcService<BulkGrpcService>();

// authzed.api.v1 service surface, served from the SAME grain mesh as the internal services above. The
// two proto families coexist because they live in distinct C# namespaces.
app.MapGrpcService<AuthzedPermissionsV1Service>();
app.MapGrpcService<AuthzedSchemaV1Service>();
app.MapGrpcService<AuthzedWatchV1Service>();
app.MapGrpcService<AuthzedExperimentalV1Service>();

app.MapGet("/", () => "Spiceport API up.");

// The datastore now lives behind the Orleans grain, so the cluster must be running before seeding.
// Start the host, seed once the singleton grain is reachable, then block until shutdown.
await app.StartAsync();

// Seed relationships once at startup so CheckPermission returns a real answer.
await SeedData.SeedAsync(app.Services.GetRequiredService<IDatastore>());

await app.WaitForShutdownAsync();
