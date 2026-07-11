using System.Collections.Immutable;
using Spiceport.Conformance.Tests;
using Spiceport.Core;
using Spiceport.Datastore;

namespace Spiceport.Grains.Tests;

/// <summary>
/// The MANDATORY exact-visited-set-correctness gate: the ENTIRE SpiceDB conformance corpus (every YAML
/// file, including the recursive schemas) is replayed through the REAL multi-silo Orleans grain mesh with
/// the exact visited set as the cycle guard. Every expected Member / NotMember / Caveated verdict must
/// match exactly — proving the exact set never false-cuts the corpus: a spurious CycleCut that flipped a
/// verdict or starved the memo would surface as a corpus mismatch.
/// </summary>
/// <remarks>
/// This is stronger than <see cref="ConformanceMeshTests"/> (which runs a representative 8-file subset on
/// a single silo): here the full corpus runs across a 3-silo cluster, so sub-problems genuinely cross
/// grain boundaries and the visited set genuinely travels on the wire. If any file regresses, there is a
/// real bug in the loop-bypass or memo wiring; it fails loudly rather than silently shrinking corpus
/// coverage.
/// </remarks>
[Collection(MeshClusterCollection.Name)]
public class FullCorpusMeshVisitedSetTests
{
    public static IEnumerable<object[]> AllCorpusFiles()
    {
        var dir = Path.Combine(AppContext.BaseDirectory, "TestData");
        foreach (var path in Directory.EnumerateFiles(dir, "*.yaml").OrderBy(p => p, StringComparer.Ordinal))
            yield return [Path.GetFileName(path)];
    }

    [Theory]
    [MemberData(nameof(AllCorpusFiles))]
    public async Task Full_corpus_through_multisilo_mesh_with_visited_set(string fileName)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "TestData", fileName);
        Assert.True(File.Exists(path), $"Corpus file missing from output: {path}");

        var file = ValidationFileLoader.LoadFromFile(path);

        // 3 silos + consistent-hash placement + the exact visited set: a check recurses across grains, and
        // the cycle guard crosses every grain boundary as the visited set.
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(file.SchemaText, siloCount: 3);

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
                failures.Add($"  {assertion.SourceText} => expected {expected}, got {result.Verdict}");
        }

        Assert.True(
            failures.Count == 0,
            $"{fileName} (full corpus, 3-silo mesh, visited set): {failures.Count}/{file.Assertions.Count} assertion(s) failed:\n{string.Join('\n', failures)}");
    }

    private static async Task SeedRelationships(IDatastore datastore, ImmutableList<Relationship> relationships)
    {
        if (relationships.Count == 0)
            return;
        var updates = relationships.Select(r => new RelationshipUpdate(r, UpdateOperation.Create)).ToList();
        await datastore.ReadWriteTx(tx => tx.WriteRelationships(updates));
    }
}
