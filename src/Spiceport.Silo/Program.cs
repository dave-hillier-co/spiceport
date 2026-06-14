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
});

// Schema + dispatch mesh (Caching over Orleans) + check-engine singletons.
builder.Services.AddSpiceportGrainServices(SiloSchema.SchemaText);

// The datastore delegates to the cluster-singleton datastore grain.
builder.Services.AddSingleton<IDatastore>(sp =>
    new GrainBackedDatastore(sp.GetRequiredService<IGrainFactory>()));

var host = builder.Build();
host.Run();
