using System.Collections.Immutable;
using Orleans.Runtime;
using Spiceport.Conformance.Tests;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// The verdict-level gate for migration step 3: <see cref="ConformanceMeshTests"/>' representative
/// corpus subset replayed through the real grain mesh with <c>useShardedGraphReader: true</c> — every
/// engine graph read is served by an <c>IGraphShardGrain</c> instead of the per-silo projection, and
/// every Check verdict must still match its expected Member / NotMember / Caveated outcome.
/// </summary>
/// <remarks>
/// The fold-equivalence gate (<see cref="ShardedReaderEquivalenceTests"/>) proves the readers agree
/// row-for-row; this gate proves the flag's actual production wiring — <c>IGraphReaderSource</c>
/// resolving the shard-mesh source inside every engine — leaves verdicts untouched end to end. The
/// multi-silo case adds the composition the single-silo run cannot see: shard grain calls crossing
/// silo boundaries while the check dispatch mesh does the same.
/// </remarks>
[Collection(MeshClusterCollection.Name)]
public class ShardedReaderCorpusMeshTests
{
    /// <summary>
    /// The same representative subset <see cref="ConformanceMeshTests.MeshFiles"/> runs (reused so the
    /// two lists cannot drift), plus <c>relexpiration.yaml</c>: its expired row is live in MVCC storage
    /// and must be sheared at query time by the sharded reader's caller-side expiry filter for the
    /// verdicts to hold.
    /// </summary>
    public static IEnumerable<object[]> MeshFiles() =>
        ConformanceMeshTests.MeshFiles().Append<object[]>(["relexpiration.yaml"]);

    [Theory]
    [MemberData(nameof(MeshFiles))]
    public async Task Conformance_Through_Grain_Mesh_With_Sharded_Reads(string fileName)
    {
        var file = LoadCorpusFile(fileName);

        await using var cluster = await MeshTestCluster.CreateAsync(
            file.SchemaText, useShardedGraphReader: true);

        await SeedRelationships(cluster.Datastore, file.Relationships);
        await AssertAllVerdictsHold(cluster, file, fileName);

        // Positive control only when the file actually drove Checks: a file with an empty assertions
        // block (simplerecursive.yaml) performs no reads, so no shard grain can have activated.
        if (file.Assertions.Count > 0)
            await AssertShardGrainActivated(cluster);
    }

    /// <summary>
    /// Cross-silo composition: three silos, sharded reads ON, over the indirect/nested-group file —
    /// check dispatch AND shard reads both genuinely cross silo boundaries as the grain directory
    /// places activations, and every verdict must still hold.
    /// </summary>
    [Fact]
    public async Task Conformance_Across_Silos_With_Sharded_Reads()
    {
        const string fileName = "indirectnestedgroups.yaml";
        var file = LoadCorpusFile(fileName);

        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(
            file.SchemaText, siloCount: 3, useShardedGraphReader: true);

        await SeedRelationships(cluster.Datastore, file.Relationships);
        await AssertAllVerdictsHold(cluster, file, $"{fileName} (3 silos)");
        await AssertShardGrainActivated(cluster);
    }

    /// <summary>
    /// Flag-on reverse-ops coverage: LookupResources + LookupSubjects + ExpandPermissionTree driven
    /// through <see cref="MeshTestCluster.ReverseOps"/> over the arrow-bearing corpus file with
    /// <c>useShardedGraphReader: true</c> must produce the same results as the identical calls on a
    /// flag-OFF cluster over the same data. The lookups are unordered APIs, so results compare as
    /// sorted id sets (with permissionship); the expand tree compares by an exact canonical rendering
    /// (structure and child order are deterministic; only subject lists within a leaf are unordered
    /// and are sorted in the rendering). The clusters run sequentially, never concurrently.
    /// </summary>
    [Fact]
    public async Task ReverseOps_With_Sharded_Reads_Agree_With_Flag_Off()
    {
        var file = LoadCorpusFile("teamwitharrow.yaml");

        var flagOff = await RunReverseOps(file, useShardedGraphReader: false);
        var flagOn = await RunReverseOps(file, useShardedGraphReader: true);

        Assert.Equal(flagOff.Resources, flagOn.Resources);
        Assert.Equal(flagOff.Subjects, flagOn.Subjects);
        Assert.Equal(flagOff.ExpandTree, flagOn.ExpandTree);

        // Guard against a vacuous pass: the arrow path must actually surface results.
        Assert.Contains("authzed_go|member", flagOff.Resources);
        Assert.Contains("ian|member", flagOff.Subjects);
    }

    private sealed record ReverseOpsResults(
        List<string> Resources,
        List<string> Subjects,
        string ExpandTree);

    private static async Task<ReverseOpsResults> RunReverseOps(ValidationFile file, bool useShardedGraphReader)
    {
        await using var cluster = await MeshTestCluster.CreateAsync(
            file.SchemaText, useShardedGraphReader: useShardedGraphReader);
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
    /// Positive control for every flag-on gate in this class: the verdicts must have been served by
    /// the shard mesh, so at least one <c>GraphShardGrain</c> activation must exist. Management
    /// statistics only enumerate existing activations — they never create one — so this cannot
    /// self-satisfy.
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
