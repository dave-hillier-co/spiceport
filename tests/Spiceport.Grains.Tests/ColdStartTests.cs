using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;
using Spiceport.Conformance.Tests;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Closing gates for the removal of the per-silo whole-graph projection. A freshly booted cluster must
/// serve a check with NO pre-traffic warmup of any kind — there is no per-silo replica left to
/// bootstrap before the first request, so the first check on a cold silo exercises exactly the
/// production cold path (shard grains hydrating on demand against the sequencer log). And the shard
/// mesh's activation footprint must stay bounded by the slices traffic actually TOUCHED — the hot-set
/// property the projection removal exists to buy: memory follows the working set, not the graph size.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class ColdStartTests
{
    private const string Schema = """
        definition user {}

        definition group {
          relation member: user
        }

        definition doc {
          relation viewer: user | group#member
          permission view = viewer
        }
        """;

    /// <summary>
    /// Boots a fresh THREE-silo cluster and, without any warmup step, loads data through the production
    /// write wire (the data-plane grain) and immediately checks from a NON-primary silo's own checker —
    /// proving no pre-traffic projection bootstrap exists or is needed anywhere in the read path. The
    /// check recurses through the group userset, so it genuinely reads the graph (cold shard hydration),
    /// not just a single direct row. Ends with the reflection tripwire: the retired projection component
    /// types must not exist in the Server assembly, so the replica can never quietly return.
    /// </summary>
    [Fact]
    public async Task Fresh_Cluster_Serves_A_Check_From_A_Non_Primary_Silo_With_No_Warmup()
    {
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(Schema, siloCount: 3);

        // Load data through the production write wire; the schema was compiled into every silo at boot.
        // (A List, not a collection-expression array: the args cross the grain boundary and Orleans has
        // no copier for the compiler-synthesized read-only array type.)
        await cluster.Relationships.WriteRelationships(new WriteRelationshipsArgs(new List<RelationshipUpdateWire>
        {
            Touch("group", "eng", "member", "user", "alice", CoreConstants.Ellipsis),
            Touch("doc", "spec", "viewer", "group", "eng", "member"),
        }));

        // Immediately check from a NON-primary silo's checker — the same IPermissionChecker its gRPC
        // front door would resolve — with no warmup call of any kind in between.
        Assert.True(cluster.AllSiloServices.Count >= 2, "gate needs a non-primary silo to check from");
        var checker = cluster.AllSiloServices[1].GetRequiredService<IPermissionChecker>();

        var member = await checker.Check("doc", "spec", "view", User("alice"), caveatContext: null);
        Assert.Equal(Membership.Member, member.Verdict);

        var stranger = await checker.Check("doc", "spec", "view", User("mallory"), caveatContext: null);
        Assert.Equal(Membership.NotMember, stranger.Verdict);

        // Tripwire: no retired projection component type may exist in the Server assembly. The probe
        // anchors on a type that IS in that assembly, so it cannot silently scan the wrong one.
        var serverAssembly = typeof(GrainBackedDatastore).Assembly;
        string[] retired =
        [
            "SiloProjection",
            "DatastoreProjectionHost",
            "IDatastoreProjectionHost",
            "DatastoreProjectionService",
            "IDatastoreProjectionGrainService",
            "ProjectionGraphReaderSource",
            "GraphReaderOptions",
        ];
        var survivors = serverAssembly.GetTypes()
            .Select(t => t.Name)
            .Where(retired.Contains)
            .ToList();
        Assert.True(
            survivors.Count == 0,
            $"retired projection type(s) resurfaced in {serverAssembly.GetName().Name}: {string.Join(", ", survivors)}");
    }

    /// <summary>
    /// Memory-shape tripwire: after a corpus load and its checks, the number of <c>GraphShardGrain</c>
    /// activations must not exceed the (generously computed) number of distinct
    /// <c>(direction, object)</c> slices the workload could have touched. Every shard key names one
    /// object per direction, and every slice a check can reach names an object appearing somewhere in
    /// the loaded relationships or the check requests (sub-problems only ever come from rows read or
    /// the original request) — so the bound is 2 x distinct objects. The point is not the constant:
    /// it is that the activation count is proportional to the TOUCHED keys and nothing else (not
    /// revisions, not checks issued, not total graph size).
    /// </summary>
    [Fact]
    public async Task Shard_Activations_Are_Bounded_By_The_Touched_Slices()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "TestData", "indirectnestedgroups.yaml");
        Assert.True(File.Exists(path), $"Linked corpus file missing from output: {path}");
        var file = ValidationFileLoader.LoadFromFile(path);
        Assert.True(file.Relationships.Count > 0 && file.Assertions.Count > 0,
            "gate needs a corpus file with relationships and assertions");

        await using var cluster = await MeshTestCluster.CreateAsync(file.SchemaText);
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(
            file.Relationships.Select(r => new RelationshipUpdate(r, UpdateOperation.Create)).ToList()));

        foreach (var assertion in file.Assertions)
        {
            var result = await cluster.Checker.Check(
                assertion.Resource.ObjectType,
                assertion.Resource.ObjectId,
                assertion.Resource.Relation,
                assertion.Subject,
                assertion.CaveatContext);
            Assert.Equal(assertion.ExpectedMembership, result.Verdict);
        }

        var distinctObjects = file.Relationships
            .SelectMany(r => new[]
            {
                (r.Resource.ObjectType, r.Resource.ObjectId),
                (r.Subject.ObjectType, r.Subject.ObjectId),
            })
            .Concat(file.Assertions.SelectMany(a => new[]
            {
                (a.Resource.ObjectType, a.Resource.ObjectId),
                (a.Subject.ObjectType, a.Subject.ObjectId),
            }))
            .Distinct()
            .Count();
        var bound = 2 * distinctObjects;

        var stats = await cluster.GrainFactory.GetGrain<IManagementGrain>(0).GetDetailedGrainStatistics();
        var shardActivations = stats.Count(s =>
            s.GrainType.ToString()!.Contains("graphshardgrain", StringComparison.OrdinalIgnoreCase));

        // Positive control first: the verdicts above must actually have been served by the shard mesh.
        Assert.True(shardActivations >= 1, "positive control failed: no GraphShardGrain activation exists");
        Assert.True(
            shardActivations <= bound,
            $"hot-set property violated: {shardActivations} GraphShardGrain activation(s) exceed the " +
            $"touched-slice bound of {bound} (2 x {distinctObjects} distinct objects)");
    }

    private static RelationshipUpdateWire Touch(
        string resourceType, string resourceId, string relation,
        string subjectType, string subjectId, string subjectRelation) =>
        new(RelationshipUpdateOpWire.Touch, new RelationshipWire(
            resourceType, resourceId, relation, subjectType, subjectId, subjectRelation,
            CaveatName: null, CaveatContext: null, Expiration: null));

    private static ObjectAndRelation User(string id) => new("user", id, CoreConstants.Ellipsis);
}
