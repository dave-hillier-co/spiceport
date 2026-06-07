namespace Spiceport.Grains.Tests;

/// <summary>
/// Serializes every test class that stands up a <see cref="MeshTestCluster"/>. The cluster passes its
/// schema to the in-process silo through a process-wide static handoff
/// (<c>MeshTestCluster.CreateAsync</c> is documented as invoked serially per schema), so cluster-using
/// classes must not run in parallel or they would race on that static and a silo could compile the
/// wrong schema. Membership of this collection makes xUnit run them one at a time.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class MeshClusterCollection
{
    /// <summary>The collection name shared by all cluster-using test classes.</summary>
    public const string Name = "MeshCluster";
}
