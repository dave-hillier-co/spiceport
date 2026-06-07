using Spiceport.Api;
using Spiceport.Datastore;
using Spiceport.Datastore.Memory;
using Spiceport.Grains;

var builder = WebApplication.CreateBuilder(args);

// Co-host the Orleans silo in this process. With an in-process silo, Orleans auto-provides
// IGrainFactory / IClusterClient to DI, so the gRPC service can resolve grains directly.
builder.Host.UseOrleans(silo => silo.UseLocalhostClustering());

// Schema + check-engine singletons (compiled once from the embedded seed schema).
builder.Services.AddSpiceportGrainServices(SeedData.SchemaText);

// The datastore is host-owned (must persist writes across calls) and is therefore registered
// here rather than by the grain DI extension.
builder.Services.AddSingleton<IDatastore>(new InMemoryDatastore());

builder.Services.AddGrpc();

var app = builder.Build();

// Seed relationships once at startup so CheckPermission returns a real answer.
await SeedData.SeedAsync(app.Services.GetRequiredService<IDatastore>());

app.MapGrpcService<PermissionsGrpcService>();
app.MapGrpcService<WatchGrpcService>();
app.MapGrpcService<BulkGrpcService>();

// authzed.api.v1 service surface, served from the SAME grain mesh as the internal services above. The
// two proto families coexist because they live in distinct C# namespaces.
app.MapGrpcService<AuthzedPermissionsV1Service>();
app.MapGrpcService<AuthzedSchemaV1Service>();
app.MapGrpcService<AuthzedWatchV1Service>();

app.MapGet("/", () => "Spiceport API up.");

app.Run();
