using Microsoft.Extensions.DependencyInjection;
using Spiceport.Conformance.Tests;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Exercises the three reverse / tree ops (ExpandPermissionTree, LookupSubjects, LookupResources)
/// THROUGH the <see cref="MeshTestCluster"/>'s <see cref="ReverseOps"/> in-process read helper (the same
/// instance a silo's gRPC services resolve) against representative SpiceDB corpus schemas (a nested-group
/// / exclusion file and a caveat file), NOT against the in-process engine directly. The helper still
/// dispatches onward to the SubjectFrontierGrain / MembershipWalkGrain / Check mesh, so this remains a
/// distributed exercise, not a purely local one.
/// </summary>
/// <remarks>
/// Each test asserts two things:
/// <list type="number">
/// <item>Agreement with the engine: the mesh grain's result set equals the engine's own op
/// (<see cref="LookupSubjectsEngine"/> / <see cref="LookupResourcesEngine"/>) run directly over the
/// same datastore snapshot. This is the distributed == local proof for the reverse ops.</item>
/// <item>The consistency invariant with Check: every subject / resource the mesh returns is confirmed
/// Member-or-Caveated by the mesh's own <see cref="IPermissionChecker.Check"/> (a returned item is
/// never a definite non-member), and conversely the corpus's assertTrue subjects all appear.</item>
/// </list>
/// </remarks>
[Collection(MeshClusterCollection.Name)]
public class ReverseOpsCorpusMeshTests
{
    private static ValidationFile LoadCorpus(string fileName)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "TestData", fileName);
        Assert.True(File.Exists(path), $"Linked corpus file missing from output: {path}");
        return ValidationFileLoader.LoadFromFile(path);
    }

    private static async Task SeedAsync(IDatastore datastore, ValidationFile file)
    {
        if (file.Relationships.Count == 0)
        {
            return;
        }

        var updates = file.Relationships
            .Select(r => new RelationshipUpdate(r, UpdateOperation.Create))
            .ToList();
        await datastore.ReadWriteTx(tx => tx.WriteRelationships(updates));
    }

    // The reverse LOOKUP ops stream from the in-process ReverseOps helper; collect the whole stream so the
    // corpus assertions read the same shape as before.
    private static async Task<List<FoundSubjectWire>> LookupSubjectsViaMesh(
        MeshTestCluster cluster, LookupSubjectsArgs args)
    {
        var list = new List<FoundSubjectWire>();
        await foreach (var item in cluster.ReverseOps.StreamLookupSubjects(args))
            list.Add(item.Subject);
        return list;
    }

    private static async Task<List<FoundResourceWire>> LookupResourcesViaMesh(
        MeshTestCluster cluster, LookupResourcesArgs args)
    {
        var list = new List<FoundResourceWire>();
        await foreach (var item in cluster.ReverseOps.StreamLookupResources(args))
            list.Add(item);
        return list;
    }

    /// <summary>
    /// Builds the engine-side LookupSubjects op over the SAME pinned snapshot the grain would use,
    /// so the mesh result can be compared against the engine's own answer.
    /// </summary>
    private static async Task<HashSet<string>> EngineLookupSubjects(
        MeshTestCluster cluster, string resourceType, string resourceId, string permission, string subjectType)
    {
        var schema = cluster.Services.GetRequiredService<ISchemaProvider>().Current;
        var datastore = cluster.Datastore;
        var rev = await datastore.OptimizedRevision();
        var reader = datastore.SnapshotReader(rev.Revision);
        var engine = new LookupSubjectsEngine(schema.Namespaces);

        var found = new HashSet<string>();
        await foreach (var s in engine.LookupSubjects(
            reader, new ObjectAndRelation(resourceType, resourceId, permission), subjectType))
        {
            found.Add(s.SubjectId);
        }

        return found;
    }

    private static async Task<HashSet<string>> EngineLookupResources(
        MeshTestCluster cluster, string resourceType, string permission, string subjectType, string subjectId)
    {
        var schema = cluster.Services.GetRequiredService<ISchemaProvider>().Current;
        var datastore = cluster.Datastore;
        var rev = await datastore.OptimizedRevision();
        var reader = datastore.SnapshotReader(rev.Revision);
        var engine = new LookupResourcesEngine(schema.Namespaces, schema.Caveats);

        var found = new HashSet<string>();
        await foreach (var r in engine.LookupResources(
            reader, subjectType, subjectId, CoreConstants.Ellipsis,
            resourceType, permission, caveatContext: null, evaluationTime: null, cursor: null, limit: null))
        {
            found.Add(r.ResourceId);
        }

        return found;
    }

    // ---- Nested-group / exclusion corpus: indirectnestedgroups.yaml ------------------------------
    //
    // document:firstdoc#view = viewer; viewer: user | group#non_intern_member, where
    // non_intern_member = direct_member - (intern - allowed). Members (assertTrue): tom, sarah, fred,
    // tim. Non-members (assertFalse): james, jim, frank.

    [Fact]
    public async Task NestedGroups_LookupSubjects_Through_Mesh_Agrees_With_Engine_And_Check()
    {
        var file = LoadCorpus("indirectnestedgroups.yaml");
        await using var cluster = await MeshTestCluster.CreateAsync(file.SchemaText);
        await SeedAsync(cluster.Datastore, file);

        var subjects = await LookupSubjectsViaMesh(cluster, new LookupSubjectsArgs(
            "document", "firstdoc", "view", "user", CoreConstants.Ellipsis,
            Context: null, Limit: null, Cursor: null));

        var meshIds = subjects.Select(s => s.SubjectId).ToHashSet();

        // (1) Mesh grain agrees with the engine's own LookupSubjects over the same snapshot.
        var engineIds = await EngineLookupSubjects(cluster, "document", "firstdoc", "view", "user");
        Assert.Equal(engineIds, meshIds);

        // The corpus's known members are all enumerated; known non-members are absent.
        Assert.Contains("tom", meshIds);
        Assert.Contains("sarah", meshIds);
        Assert.Contains("fred", meshIds);
        Assert.Contains("tim", meshIds);
        Assert.DoesNotContain("james", meshIds);
        Assert.DoesNotContain("jim", meshIds);
        Assert.DoesNotContain("frank", meshIds);

        // (2) Consistency invariant: every returned subject is Member-or-Caveated under mesh Check.
        foreach (var s in subjects)
        {
            var check = await cluster.Checker.Check(
                "document", "firstdoc", "view",
                new ObjectAndRelation("user", s.SubjectId, CoreConstants.Ellipsis),
                caveatContext: null);
            Assert.True(
                check.Verdict is Membership.Member or Membership.Caveated,
                $"LookupSubjects returned user:{s.SubjectId} but Check says {check.Verdict}");
        }
    }

    [Fact]
    public async Task NestedGroups_LookupResources_Through_Mesh_Agrees_With_Engine_And_Check()
    {
        var file = LoadCorpus("indirectnestedgroups.yaml");
        await using var cluster = await MeshTestCluster.CreateAsync(file.SchemaText);
        await SeedAsync(cluster.Datastore, file);

        // tom is a direct viewer AND a non-intern member of engineering, so reachable on firstdoc.
        var resources = await LookupResourcesViaMesh(cluster, new LookupResourcesArgs(
            "document", "view", "user", "tom", CoreConstants.Ellipsis,
            Context: null, Limit: null, Cursor: null));

        var meshIds = resources.Select(r => r.ResourceId).ToHashSet();

        // (1) Mesh grain agrees with the engine's own LookupResources over the same snapshot.
        var engineIds = await EngineLookupResources(cluster, "document", "view", "user", "tom");
        Assert.Equal(engineIds, meshIds);
        Assert.Contains("firstdoc", meshIds);

        // (2) Consistency invariant: every returned resource is Member-or-Caveated under mesh Check.
        foreach (var r in resources)
        {
            var check = await cluster.Checker.Check(
                "document", r.ResourceId, "view",
                new ObjectAndRelation("user", "tom", CoreConstants.Ellipsis),
                caveatContext: null);
            Assert.True(
                check.Verdict is Membership.Member or Membership.Caveated,
                $"LookupResources returned document:{r.ResourceId} but Check says {check.Verdict}");
        }
    }

    [Fact]
    public async Task NestedGroups_ExpandPermissionTree_Through_Mesh_Yields_Viewer_Structure()
    {
        var file = LoadCorpus("indirectnestedgroups.yaml");
        await using var cluster = await MeshTestCluster.CreateAsync(file.SchemaText);
        await SeedAsync(cluster.Datastore, file);

        var reply = await cluster.ReverseOps.ExpandPermissionTree(
            new ExpandTreeArgs("document", "firstdoc", "view", ExpandModeWire.Shallow));

        // view = viewer, so the tree root expands the "view" permission over the document.
        Assert.Equal("document", reply.Root.ExpandedType);
        Assert.Equal("firstdoc", reply.Root.ExpandedId);
        Assert.Equal("view", reply.Root.ExpandedRelation);

        // The expanded direct subjects under the tree are a subset of the actual members (no phantom
        // subjects), confirmed via mesh Check.
        var directUsers = CollectDirectUserSubjects(reply.Root);
        foreach (var id in directUsers)
        {
            var check = await cluster.Checker.Check(
                "document", "firstdoc", "view",
                new ObjectAndRelation("user", id, CoreConstants.Ellipsis),
                caveatContext: null);
            Assert.True(
                check.Verdict is Membership.Member or Membership.Caveated,
                $"Expand surfaced user:{id} as a direct subject but Check says {check.Verdict}");
        }
    }

    // ---- Caveat corpus: basiccaveat.yaml --------------------------------------------------------
    //
    // viewer: user with some_caveat(somecondition int) | user. Seeded: tom[somecondition:42] (true),
    // fred[somecondition:41] (false), sarah[some_caveat] (caveated, needs context), tracy (unconditional).

    [Fact]
    public async Task Caveat_LookupSubjects_Through_Mesh_Agrees_With_Engine_And_Surfaces_Caveated()
    {
        var file = LoadCorpus("basiccaveat.yaml");
        await using var cluster = await MeshTestCluster.CreateAsync(file.SchemaText);
        await SeedAsync(cluster.Datastore, file);

        // No request context: tracy is unconditional, sarah is caveated (missing somecondition),
        // tom's written context (42) satisfies the caveat, fred's written context (41) fails it.
        var subjects = await LookupSubjectsViaMesh(cluster, new LookupSubjectsArgs(
            "document", "firstdoc", "view", "user", CoreConstants.Ellipsis,
            Context: null, Limit: null, Cursor: null));

        var byId = subjects.ToDictionary(s => s.SubjectId, s => s.Permissionship);

        // (1) Mesh grain agrees with the engine's own pre-collapse subject set over the same snapshot.
        // The engine yields the structural members (tracy, tom, sarah, fred carry caveats verbatim);
        // the grain collapses against an empty context, shearing the definitely-false fred.
        var engineIds = await EngineLookupSubjects(cluster, "document", "firstdoc", "view", "user");
        // Every collapsed (mesh) subject came from the engine's pre-collapse set: meshKeys ⊆ engineIds.
        // Assert.Superset(expectedSuperset, actual) checks actual ⊇ expectedSuperset.
        Assert.Superset(byId.Keys.ToHashSet(), engineIds);

        // tracy: unconditional member.
        Assert.True(byId.ContainsKey("tracy"));
        Assert.False(byId["tracy"].IsCaveated);

        // sarah: caveated, surfaces the missing parameter name.
        Assert.True(byId.ContainsKey("sarah"));
        Assert.True(byId["sarah"].IsCaveated);
        Assert.Contains("somecondition", byId["sarah"].MissingContextParams);

        // fred: written context fails the caveat → definitely false → sheared off entirely.
        Assert.False(byId.ContainsKey("fred"));

        // (2) Consistency invariant: every returned subject is Member-or-Caveated under mesh Check
        // (using the same null request context).
        foreach (var s in subjects)
        {
            var check = await cluster.Checker.Check(
                "document", "firstdoc", "view",
                new ObjectAndRelation("user", s.SubjectId, CoreConstants.Ellipsis),
                caveatContext: null);
            Assert.True(
                check.Verdict is Membership.Member or Membership.Caveated,
                $"Caveat LookupSubjects returned user:{s.SubjectId} but Check says {check.Verdict}");
            // The grain's collapsed permissionship matches Check's verdict.
            Assert.Equal(check.Verdict == Membership.Caveated, s.Permissionship.IsCaveated);
        }
    }

    [Fact]
    public async Task Caveat_LookupResources_Through_Mesh_Agrees_With_Engine_And_Check()
    {
        var file = LoadCorpus("basiccaveat.yaml");
        await using var cluster = await MeshTestCluster.CreateAsync(file.SchemaText);
        await SeedAsync(cluster.Datastore, file);

        // Provide somecondition=42 so the caveated subjects resolve to definite members.
        var context = new Dictionary<string, object?> { ["somecondition"] = 42L };

        var resources = await LookupResourcesViaMesh(cluster, new LookupResourcesArgs(
            "document", "view", "user", "sarah", CoreConstants.Ellipsis,
            Context: context, Limit: null, Cursor: null));

        var meshIds = resources.Select(r => r.ResourceId).ToHashSet();

        // (1) Mesh grain (with satisfying context) reaches firstdoc for sarah; engine (no caveat shear,
        // candidate set) also includes firstdoc.
        var engineIds = await EngineLookupResources(cluster, "document", "view", "user", "sarah");
        Assert.Contains("firstdoc", meshIds);
        // Mesh result is within the engine's reachable candidate set: meshIds ⊆ engineIds.
        // Assert.Subset(expectedSubset, actual) checks actual ⊆ expectedSubset.
        Assert.Subset(engineIds, meshIds);

        // (2) Consistency invariant: every returned resource is Member-or-Caveated under mesh Check
        // with the same context.
        foreach (var r in resources)
        {
            var check = await cluster.Checker.Check(
                "document", r.ResourceId, "view",
                new ObjectAndRelation("user", "sarah", CoreConstants.Ellipsis),
                caveatContext: context);
            Assert.True(
                check.Verdict is Membership.Member or Membership.Caveated,
                $"Caveat LookupResources returned document:{r.ResourceId} but Check says {check.Verdict}");
        }
    }

    private static List<string> CollectDirectUserSubjects(ExpandTreeNodeWire node)
    {
        var ids = new List<string>();
        Walk(node);
        return ids;

        void Walk(ExpandTreeNodeWire n)
        {
            foreach (var s in n.Subjects)
            {
                if (s.SubjectType == "user" && !s.IsWildcard)
                {
                    ids.Add(s.SubjectId);
                }
            }

            foreach (var child in n.Children)
            {
                Walk(child);
            }
        }
    }
}
