using Spiceport.Datastore;
using Spiceport.Datastore.Memory;
using Spiceport.Grains;
using Spiceport.Silo;

var builder = Host.CreateApplicationBuilder(args);

// Run an Orleans silo in this host so the keyed check grains activate here.
builder.UseOrleans(silo => silo.UseLocalhostClustering());

// Schema + dispatch mesh (Caching over Orleans) + check-engine singletons.
builder.Services.AddSpiceportGrainServices(SiloSchema.SchemaText);

// The datastore is host-owned (it must persist writes across calls).
builder.Services.AddSingleton<IDatastore>(new InMemoryDatastore());

var host = builder.Build();
host.Run();
