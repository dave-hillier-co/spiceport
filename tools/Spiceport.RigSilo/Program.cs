using System.Net;
using System.Text.Json;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Options;
using Spiceport.Api;
using Spiceport.Datastore;
using Spiceport.Grains;
using Spiceport.RigSilo;
using Spiceport.Server.Hosting;

// The real-network measurement rig's silo process (docs/scalability-program.md section 2): an
// attended, orchestrator-launched multi-process cluster over real sockets/gRPC, standing opposite the
// in-process TestCluster the rest of the bench harness drives. Never started from tests/ (house rule) —
// shutdown is SIGTERM-driven (WaitForShutdownAsync) with the orchestrator killing TERM then KILL.

RigArgs args_;
try
{
    args_ = RigArgs.Parse(args);
}
catch (RigUsageException ex)
{
    Console.Error.WriteLine(ex.Message);
    Console.Error.WriteLine(
        "Usage: dotnet run --project tools/Spiceport.RigSilo -- " +
        "--silo-port=N --gateway-port=N --primary-silo-port=N --grpc-port=N --http-port=N " +
        "[--co-locate=on|off] [--cluster-id=spiceport-rig]");
    return 2;
}

var siloPort = args_.GetRequiredInt("silo-port");
var gatewayPort = args_.GetRequiredInt("gateway-port");
var primarySiloPort = args_.GetRequiredInt("primary-silo-port");
var grpcPort = args_.GetRequiredInt("grpc-port");
var httpPort = args_.GetRequiredInt("http-port");
var clusterId = args_.GetString("cluster-id", "spiceport-rig");
var coLocate = args_.GetString("co-locate", "on") switch
{
    "on" => true,
    "off" => false,
    var other => throw new RigUsageException($"--co-locate expects on|off, got '{other}'."),
};

var builder = WebApplication.CreateBuilder([]);

// Two explicit Kestrel listeners: HTTP/2 (h2c, no TLS — a local attended rig) for the authzed.api.v1
// gRPC surface, HTTP/1.1 for the plain-JSON rig endpoints below. gRPC requires an HTTP/2-only endpoint;
// mixing protocols on one port is the documented Kestrel gotcha this sidesteps.
builder.WebHost.ConfigureKestrel(kestrel =>
{
    kestrel.ListenLocalhost(grpcPort, listen => listen.Protocols = HttpProtocols.Http2);
    kestrel.ListenLocalhost(httpPort, listen => listen.Protocols = HttpProtocols.Http1);
});

// Co-host the Orleans silo in this process — SAME production wiring as src/Spiceport.Api/Program.cs,
// except UseLocalhostClustering is parameterized with the rig's real per-process ports (production's
// single-process Api host takes the library defaults) and, when requested, the co-placement director.
builder.Host.UseOrleans(silo =>
{
    silo.UseLocalhostClustering(
        siloPort: siloPort,
        gatewayPort: gatewayPort,
        primarySiloEndpoint: new IPEndPoint(IPAddress.Loopback, primarySiloPort),
        clusterId: clusterId);
    silo.AddActivationMemoCollectionAge();
    silo.AddDatastoreGrainStorage(builder.Configuration);
    silo.AddCustomStorageBasedLogConsistencyProvider("CustomStorage");
    silo.UseInMemoryReminderService();

    // Registered unconditionally so BOTH A/B arms are explicit rather than one riding the library
    // default (co-location ON). Must run BEFORE AddSpiceportGrainServices below: that call registers
    // GraphPlacementOptions with TryAddSingleton, so an override registered first wins; registered
    // after, it would be silently dropped (see MeshTestCluster's SiloConfigurator for the same
    // ordering contract).
    silo.AddGraphLocalityPlacement(new GraphPlacementOptions { CoLocateWithShards = coLocate });
});

builder.Services.AddSpiceportGrainServices(SeedData.SchemaText);

// The datastore facade delegates to the cluster-singleton datastore grain; the Watch hub is the DI
// singleton AddSpiceportGrainServices registered.
builder.Services.AddSingleton<IDatastore>(sp =>
    new GrainBackedDatastore(
        sp.GetRequiredService<IGrainFactory>(),
        sp.GetRequiredService<LogWatchHub>(),
        gcOptions: sp.GetService<IOptions<DatastoreGcOptions>>()));

builder.Services.AddGrpc();

var app = builder.Build();

app.MapGrpcService<PermissionsGrpcService>();
app.MapGrpcService<WatchGrpcService>();
app.MapGrpcService<BulkGrpcService>();
app.MapGrpcService<AuthzedPermissionsV1Service>();
app.MapGrpcService<AuthzedSchemaV1Service>();
app.MapGrpcService<AuthzedWatchV1Service>();
app.MapGrpcService<AuthzedExperimentalV1Service>();

var metricsJson = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

app.MapGet("/healthz", () => Results.Text("ok"));

// The driver reads this once at the warmup boundary (reset) and once at run end (snapshot), summing
// across every silo in the cluster — only the silo hosting the singleton DatastoreGrain activation
// reports nonzero sequencer counters, which is expected, not a bug.
app.MapGet("/rig/metrics", (IDispatchMetrics dispatch, ISequencerMetrics sequencer) =>
    Results.Json(
        new { dispatch = dispatch.Snapshot(), sequencer = sequencer.Snapshot() },
        metricsJson));

app.MapPost("/rig/reset", (IDispatchMetrics dispatch, ISequencerMetrics sequencer) =>
{
    dispatch.Reset();
    sequencer.Reset();
    return Results.Ok();
});

// Deliberately NO SeedData.SeedAsync: the driver writes schema + world over gRPC (WriteSchema +
// WriteRelationships), so a second, in-process seed would race the wire-driven world load.
await app.StartAsync();
await app.WaitForShutdownAsync();
return 0;
