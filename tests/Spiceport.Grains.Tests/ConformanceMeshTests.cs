using System.Collections.Immutable;
using Spiceport.Conformance.Tests;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Datastore.Memory;
using Spiceport.Engine;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Proves the dispatch mesh: a representative subset of the SpiceDB conformance corpus is run through
/// the REAL Orleans grain mesh (the silo-wide Caching-over-Orleans root dispatcher and the keyed
/// <see cref="ICheckGrain"/> activations) rather than the in-process <see cref="CheckEngine"/>, and every
/// assertion's verdict must match its expected Member / NotMember / Caveated outcome.
/// </summary>
/// <remarks>
/// This is the distributed == local proof: the exact same fixtures the in-process conformance suite
/// asserts are replayed, but each Check recurses across grain boundaries.
/// </remarks>
[Collection(MeshClusterCollection.Name)]
public class ConformanceMeshTests
{
    /// <summary>
    /// The representative subset run through the mesh: a set-ops file, an arrow file, a wildcard file,
    /// an indirect/nested-group file, a recursive file, and three caveat files.
    /// </summary>
    public static IEnumerable<object[]> MeshFiles() =>
    [
        ["multipleops.yaml"],          // set-ops (union / intersection / exclusion)
        ["teamwitharrow.yaml"],        // arrow (tuple-to-userset)
        ["simplewildcard.yaml"],       // wildcard
        ["indirectnestedgroups.yaml"], // indirect / nested group
        ["simplerecursive.yaml"],      // recursive
        ["basiccaveat.yaml"],          // caveat
        ["caveatlr.yaml"],             // caveat (left/right ordering)
        ["caveatip.yaml"],             // caveat (ip / typed params)
    ];

    [Theory]
    [MemberData(nameof(MeshFiles))]
    public async Task Conformance_Through_Grain_Mesh(string fileName)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "TestData", fileName);
        Assert.True(File.Exists(path), $"Linked corpus file missing from output: {path}");

        var file = ValidationFileLoader.LoadFromFile(path);

        await using var cluster = await MeshTestCluster.CreateAsync(file.SchemaText);

        await SeedRelationships(cluster.Datastore, file.Relationships);

        var failures = new List<string>();
        foreach (var assertion in file.Assertions)
        {
            var result = await cluster.Checker.Check(
                assertion.Resource.ObjectType,
                assertion.Resource.ObjectId,
                assertion.Resource.Relation,
                assertion.Subject,
                assertion.CaveatContext);

            var expected = assertion.ExpectedMembership;
            if (result.Verdict != expected)
            {
                failures.Add($"  {assertion.SourceText} => expected {expected}, got {result.Verdict}");
            }
        }

        Assert.True(
            failures.Count == 0,
            $"{fileName} (through grain mesh): {failures.Count}/{file.Assertions.Count} assertion(s) failed:\n{string.Join('\n', failures)}");
    }

    private static async Task SeedRelationships(
        IDatastore datastore,
        ImmutableList<Relationship> relationships)
    {
        if (relationships.Count == 0)
        {
            return;
        }

        var updates = relationships
            .Select(r => new RelationshipUpdate(r, UpdateOperation.Create))
            .ToList();

        await datastore.ReadWriteTx(tx => tx.WriteRelationships(updates));
    }
}
