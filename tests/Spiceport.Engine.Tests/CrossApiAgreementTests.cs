using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Schema;

namespace Spiceport.Engine.Tests;

/// <summary>
/// Gate 3 -- cross-API agreement. <see cref="CheckEngine"/>, <see cref="LookupResourcesEngine"/> and
/// <see cref="LookupSubjectsEngine"/> are three different traversal entry points over the same
/// schema/relationship semantics, so for any world they must agree with each other BY DEFINITION of
/// Zanzibar semantics -- no external truth is needed, which makes this gate independent of (and
/// complementary to) the SpiceDB conformance corpus.
/// <list type="bullet">
/// <item>
/// (a) Check &lt;-&gt; LookupResources: for every (subject, resourceType, permission), a resource is in
/// <c>LookupResources(subject, ...)</c> iff <c>Check(resource, permission, subject)</c> is Member.
/// </item>
/// <item>
/// (b) Check &lt;-&gt; LookupSubjects: for every (resource, permission), a concrete user is effectively a
/// member of <c>LookupSubjects(resource, permission, "user")</c> iff Check is Member. Wildcards are
/// minded explicitly, not ignored: when a wildcard <see cref="FoundSubject"/> is yielded, every user in
/// the (closed) alphabet is covered by it EXCEPT those named in its
/// <see cref="FoundSubject.ExcludedSubjects"/>, so the concrete-id set is expanded against the alphabet
/// before the iff is checked. This is exact (not an approximation) precisely because the alphabet the
/// generator draws relationships from is the same closed alphabet Check evaluates against.
/// </item>
/// </list>
/// </summary>
/// <remarks>
/// STATED LIMITS (see <see cref="RandomAuthzWorlds"/>): no caveats/expiration, so every Membership in
/// these worlds is exactly Member or NotMember -- no Caveated collapse ambiguity to reconcile.
/// </remarks>
public class CrossApiAgreementTests
{
    public static IEnumerable<object[]> Seeds => RandomAuthzWorlds.Seeds;

    private static readonly (string ResourceType, string Permission)[] Targets =
    [
        ("document", "view"),
        ("document", "view_mono"),
        ("folder", "view"),
        ("group", "member"),
    ];

    [Theory]
    [MemberData(nameof(Seeds))]
    public async Task CheckAgreesWithLookupResources(int seed)
    {
        var world = RandomAuthzWorlds.Build(seed);
        var compiled = SchemaCompiler.CompileSchema(world.SchemaText);
        var check = new CheckEngine(compiled.Namespaces, compiled.Caveats);
        var lookupResources = new LookupResourcesEngine(compiled.Namespaces, compiled.Caveats);

        var store = new ReferenceDatastore();
        var rev = await store.ReadWriteTx(tx => tx.WriteRelationships(
            world.Relationships.Select(r => new RelationshipUpdate(r, UpdateOperation.Touch)).ToList()));
        var reader = store.SnapshotReader(rev);

        var resourceIdsByType = ResourceIdsByType(world);

        foreach (var (resourceType, permission) in Targets)
        {
            foreach (var userId in world.Users)
            {
                var found = await Collect(lookupResources.LookupResources(
                    reader, "user", userId, CoreConstants.Ellipsis, resourceType, permission));
                var foundIds = found.Select(f => f.ResourceId).ToHashSet();

                foreach (var resourceId in resourceIdsByType[resourceType])
                {
                    var verdict = (await check.Check(reader, resourceType, resourceId, permission, Onr("user", userId))).Verdict;
                    Assert.True((verdict == Membership.Member) == foundIds.Contains(resourceId),
                        $"seed={seed}: Check/LookupResources disagree on {resourceType}:{resourceId}/{permission} " +
                        $"for user:{userId} (Check={verdict}, inLookupResources={foundIds.Contains(resourceId)})");
                }
            }
        }
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public async Task CheckAgreesWithLookupSubjects(int seed)
    {
        var world = RandomAuthzWorlds.Build(seed);
        var compiled = SchemaCompiler.CompileSchema(world.SchemaText);
        var check = new CheckEngine(compiled.Namespaces, compiled.Caveats);
        var lookupSubjects = new LookupSubjectsEngine(compiled.Namespaces);

        var store = new ReferenceDatastore();
        var rev = await store.ReadWriteTx(tx => tx.WriteRelationships(
            world.Relationships.Select(r => new RelationshipUpdate(r, UpdateOperation.Touch)).ToList()));
        var reader = store.SnapshotReader(rev);

        var resourceIdsByType = ResourceIdsByType(world);

        foreach (var (resourceType, permission) in Targets)
        {
            foreach (var resourceId in resourceIdsByType[resourceType])
            {
                var found = await Collect(lookupSubjects.LookupSubjects(
                    reader, new ObjectAndRelation(resourceType, resourceId, permission), "user"));

                var concrete = new HashSet<string>();
                var sawWildcard = false;
                var excluded = new HashSet<string>();
                foreach (var f in found)
                {
                    if (!f.IsWildcard)
                    {
                        concrete.Add(f.SubjectId);
                        continue;
                    }
                    sawWildcard = true;
                    foreach (var ex in f.ExcludedSubjects ?? [])
                        excluded.Add(ex.SubjectId);
                }

                foreach (var userId in world.Users)
                {
                    var inLookup = concrete.Contains(userId) || (sawWildcard && !excluded.Contains(userId));
                    var verdict = (await check.Check(reader, resourceType, resourceId, permission, Onr("user", userId))).Verdict;
                    Assert.True((verdict == Membership.Member) == inLookup,
                        $"seed={seed}: Check/LookupSubjects disagree on {resourceType}:{resourceId}/{permission} " +
                        $"for user:{userId} (Check={verdict}, inLookupSubjects={inLookup}, sawWildcard={sawWildcard})");
                }
            }
        }
    }

    private static Dictionary<string, IReadOnlyList<string>> ResourceIdsByType(RandomAuthzWorlds.World world) => new()
    {
        ["document"] = world.Documents,
        ["folder"] = world.Folders,
        ["group"] = world.Groups,
    };

    private static ObjectAndRelation Onr(string type, string id, string relation = CoreConstants.Ellipsis) =>
        new(type, id, relation);

    private static async Task<List<FoundResource>> Collect(IAsyncEnumerable<FoundResource> e)
    {
        var list = new List<FoundResource>();
        await foreach (var f in e)
            list.Add(f);
        return list;
    }

    private static async Task<List<FoundSubject>> Collect(IAsyncEnumerable<FoundSubject> e)
    {
        var list = new List<FoundSubject>();
        await foreach (var f in e)
            list.Add(f);
        return list;
    }
}
