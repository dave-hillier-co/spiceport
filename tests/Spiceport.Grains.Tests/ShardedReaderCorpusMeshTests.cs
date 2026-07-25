using System.Collections.Immutable;
using Orleans.Runtime;
using Spiceport.Conformance.Tests;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Verdict-level gates that are ADDITIVE to <see cref="ConformanceMeshTests"/> now that sharded reads
/// are the only engine path: with the projection-vs-shards flag gone, <see cref="ConformanceMeshTests"/>
/// itself already replays its representative corpus subset over the sharded read path, so the duplicate
/// per-file corpus loop this class used to run (the flag-ON shadow of that suite) is deliberately gone.
/// What remains is what only this class covered: the <c>relexpiration.yaml</c> file (its expired row is
/// live in MVCC storage and must be sheared at query time by the sharded reader's caller-side expiry
/// filter), the <c>GraphShardGrain</c> activation POSITIVE CONTROL (verdicts must actually have been
/// served by the shard mesh), the multi-silo composition case (shard grain calls crossing silo
/// boundaries while check dispatch does the same), and the reverse-ops two-cluster agreement gate.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class ShardedReaderCorpusMeshTests
{
    /// <summary>
    /// The expiration file, which the <see cref="ConformanceMeshTests.MeshFiles"/> subset does not carry:
    /// its already-expired row is stored live in MVCC and must be skipped at query time by the sharded
    /// reader for the verdicts to hold. Includes the activation positive control.
    /// </summary>
    [Fact]
    public async Task Relexpiration_Conformance_Through_Grain_Mesh()
    {
        const string fileName = "relexpiration.yaml";
        var file = LoadCorpusFile(fileName);
        Assert.True(file.Assertions.Count > 0, $"{fileName}: the gate needs assertions to drive reads");

        await using var cluster = await MeshTestCluster.CreateAsync(file.SchemaText);

        await SeedRelationships(cluster.Datastore, file.Relationships);
        await AssertAllVerdictsHold(cluster, file, fileName);
        await AssertShardGrainActivated(cluster);
    }

    /// <summary>
    /// Cross-silo composition: three silos over the indirect/nested-group file — check dispatch AND
    /// shard reads both genuinely cross silo boundaries as the grain directory places activations, and
    /// every verdict must still hold.
    /// </summary>
    [Fact]
    public async Task Conformance_Across_Silos_With_Sharded_Reads()
    {
        const string fileName = "indirectnestedgroups.yaml";
        var file = LoadCorpusFile(fileName);

        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(file.SchemaText, siloCount: 3);

        await SeedRelationships(cluster.Datastore, file.Relationships);
        await AssertAllVerdictsHold(cluster, file, $"{fileName} (3 silos)");
        await AssertShardGrainActivated(cluster);
    }

    /// <summary>
    /// Reverse-ops agreement between TWO independent clusters over the same data: LookupResources +
    /// LookupSubjects + ExpandPermissionTree driven through <see cref="MeshTestCluster.ReverseOps"/> over
    /// the arrow-bearing corpus file must produce identical results on both — pinning that the sharded
    /// reverse-ops results are a deterministic function of (schema, data), not of incidental cluster
    /// state such as placement or shard hydration order. The lookups are unordered APIs, so results
    /// compare as sorted id sets (with permissionship); the expand tree compares by an exact canonical
    /// rendering (structure and child order are deterministic; only subject lists within a leaf are
    /// unordered and are sorted in the rendering). The clusters run sequentially, never concurrently.
    /// </summary>
    [Fact]
    public async Task ReverseOps_Agree_Between_Two_Independent_Clusters()
    {
        var file = LoadCorpusFile("teamwitharrow.yaml");

        var first = await RunReverseOps(file);
        var second = await RunReverseOps(file);

        Assert.Equal(first.Resources, second.Resources);
        Assert.Equal(first.Subjects, second.Subjects);
        Assert.Equal(first.ExpandTree, second.ExpandTree);

        // Guard against a vacuous pass: the arrow path must actually surface results.
        Assert.Contains("authzed_go|member", first.Resources);
        Assert.Contains("ian|member", first.Subjects);
    }

    private sealed record ReverseOpsResults(
        List<string> Resources,
        List<string> Subjects,
        string ExpandTree);

    private static async Task<ReverseOpsResults> RunReverseOps(ValidationFile file)
    {
        await using var cluster = await MeshTestCluster.CreateAsync(file.SchemaText);
        await SeedRelationships(cluster.Datastore, file.Relationships);

        // ian reaches test/repository:authzed_go#read only via the team#member userset plus the
        // organization arrow schema — the shapes the sharded reader must serve for reverse ops.
        var resources = new List<string>();
        await foreach (var item in cluster.ReverseOps.StreamLookupResources(new LookupResourcesArgs(
            "test/repository", "read", "test/user", "ian", CoreConstants.Ellipsis,
            Context: null, Limit: null, Cursor: null)))
        {
            resources.Add($"{item.ResourceId}|{(item.Permissionship.IsCaveated ? "caveated" : "member")}");
        }
        resources.Sort(StringComparer.Ordinal);

        var subjects = new List<string>();
        await foreach (var item in cluster.ReverseOps.StreamLookupSubjects(new LookupSubjectsArgs(
            "test/repository", "authzed_go", "read", "test/user", CoreConstants.Ellipsis,
            Context: null, Limit: null, Cursor: null)))
        {
            subjects.Add($"{item.Subject.SubjectId}|{(item.Subject.Permissionship.IsCaveated ? "caveated" : "member")}");
        }
        subjects.Sort(StringComparer.Ordinal);

        var expand = await cluster.ReverseOps.ExpandPermissionTree(
            new ExpandTreeArgs("test/repository", "authzed_go", "read", ExpandModeWire.Shallow));

        return new ReverseOpsResults(resources, subjects, RenderTree(expand.Root));
    }

    // Canonical rendering: node structure and child order verbatim (deterministic from the schema
    // expression), leaf subject lists sorted (the one unordered part of the wire shape).
    private static string RenderTree(ExpandTreeNodeWire node)
    {
        var subjects = node.Subjects
            .Select(s => $"{s.SubjectType}:{s.SubjectId}#{s.SubjectRelation}" +
                (s.CaveatMissingFields.Count > 0 ? $"?[{string.Join(",", s.CaveatMissingFields)}]" : ""))
            .OrderBy(s => s, StringComparer.Ordinal);
        var children = node.Children.Select(RenderTree);
        return $"{node.ExpandedType}:{node.ExpandedId}#{node.ExpandedRelation}" +
            $"({(node.IsLeaf ? "leaf" : node.Operation.ToString())}" +
            $"|subjects:[{string.Join(",", subjects)}]|children:[{string.Join(",", children)}])";
    }

    /// <summary>
    /// Positive control: the verdicts must have been served by the shard mesh, so at least one
    /// <c>GraphShardGrain</c> activation must exist. Management statistics only enumerate existing
    /// activations — they never create one — so this cannot self-satisfy.
    /// </summary>
    private static async Task AssertShardGrainActivated(MeshTestCluster cluster)
    {
        var management = cluster.GrainFactory.GetGrain<IManagementGrain>(0);
        var stats = await management.GetDetailedGrainStatistics();
        Assert.Contains(stats, s =>
            s.GrainType.ToString()!.Contains("graphshardgrain", StringComparison.OrdinalIgnoreCase));
    }

    private static ValidationFile LoadCorpusFile(string fileName)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "TestData", fileName);
        Assert.True(File.Exists(path), $"Linked corpus file missing from output: {path}");
        return ValidationFileLoader.LoadFromFile(path);
    }

    private static async Task AssertAllVerdictsHold(MeshTestCluster cluster, ValidationFile file, string label)
    {
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
            $"{label} (through grain mesh, sharded reads): {failures.Count}/{file.Assertions.Count} assertion(s) failed:\n{string.Join('\n', failures)}");
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
