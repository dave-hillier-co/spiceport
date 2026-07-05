using Microsoft.Extensions.Options;
using Spiceport.Datastore;
using Spiceport.Grains;
using Spiceport.Server.Hosting;
using Spiceport.Silo;

var builder = Host.CreateApplicationBuilder(args);

// Run an Orleans silo in this host so the keyed check grains activate here.
builder.UseOrleans(silo =>
{
    silo.UseLocalhostClustering();
    // Place CheckGrain activations by consistent hash of the sub-problem key.
    silo.AddConsistentHashPlacement();
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

// Schema + dispatch mesh (Caching over Orleans) + check-engine singletons.
builder.Services.AddSpiceportGrainServices(SiloSchema.SchemaText);

// The datastore delegates to the cluster-singleton datastore grain. Reads serve from the per-silo
// materialized projection (folded incrementally from the event log) instead of a per-Check full fetch.
// Pass the SAME DatastoreGcOptions the datastore grain is configured with (if any is registered), so this
// datastore's nominal GC window never drifts from the grain's real GcFloor policy.
builder.Services.AddSingleton<IDatastore>(sp =>
    new GrainBackedDatastore(
        sp.GetRequiredService<IGrainFactory>(),
        gcOptions: sp.GetService<IOptions<DatastoreGcOptions>>()));

var host = builder.Build();
host.Run();
