using Spiceport.Api;
using Spiceport.Datastore;
using Spiceport.Grains;

var builder = WebApplication.CreateBuilder(args);

// Co-host the Orleans silo in this process. With an in-process silo, Orleans auto-provides
// IGrainFactory / IClusterClient to DI, so the gRPC service can resolve grains directly.
builder.Host.UseOrleans(silo =>
{
    silo.UseLocalhostClustering();
    // Persistent storage for the singleton datastore grain (the single source of truth).
    silo.AddMemoryGrainStorage("datastore");
});

// Schema + check-engine singletons (compiled once from the embedded seed schema).
builder.Services.AddSpiceportGrainServices(SeedData.SchemaText);

// The datastore delegates to the cluster-singleton datastore grain.
builder.Services.AddSingleton<IDatastore>(sp =>
    new GrainBackedDatastore(sp.GetRequiredService<IGrainFactory>()));

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
