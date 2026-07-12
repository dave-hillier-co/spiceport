using Spiceport.Conformance.Tests;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Schema;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Stage-4 NON-NEGOTIABLE conformance gate: across the ENTIRE SpiceDB conformance corpus, the Leopard
/// membership-walk accelerator (<see cref="MembershipCoverage"/> + <see cref="MembershipWalk.LocalClosure"/>)
/// must not change a single LookupResources verdict. For every file, every assertion's
/// (subject, resource-type, permission) is run through <see cref="LookupResourcesEngine"/> twice — once with
/// the walked candidate set, once without — and the (resource id, membership) result sets must be identical.
/// Run at the engine level (no Orleans) so the whole corpus sweeps in seconds. This is the walk-based
/// replacement for the retired flattened-index equivalence gate; VERDICT-level comparison is unchanged
/// (candidate-set comparison would be a weakening — deliberately not done here).
/// </summary>
public class Stage4CorpusEquivalenceTests
{
    public static IEnumerable<object[]> CorpusFiles()
    {
        var dir = Path.Combine(AppContext.BaseDirectory, "TestData");
        foreach (var path in Directory.EnumerateFiles(dir, "*.yaml").OrderBy(p => p, StringComparer.Ordinal))
            yield return [Path.GetFileName(path)];
    }

    [Theory]
    [MemberData(nameof(CorpusFiles))]
    public async Task WalkOn_EqualsWalkOff_ForEveryAssertionShape(string fileName)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "TestData", fileName);
        var file = ValidationFileLoader.LoadFromFile(path);

        var compiled = SchemaCompiler.CompileSchema(file.SchemaText);
        var engine = new LookupResourcesEngine(compiled.Namespaces, compiled.Caveats);
        var coverage = MembershipCoverage.Build(compiled.Namespaces);

        var store = new ReferenceDatastore();
        var rev = await store.ReadWriteTx(tx => tx.WriteRelationships(
            file.Relationships.Select(r => new RelationshipUpdate(r, UpdateOperation.Create)).ToList()));
        var reader = store.SnapshotReader(rev);

        // Distinct (subject, resourceType, permission) shapes drawn from the file's assertions.
        var shapes = file.Assertions
            .Select(a => (a.Subject, a.Resource.ObjectType, a.Resource.Relation, a.CaveatContext))
            .Distinct()
            .ToList();

        var mismatches = new List<string>();
        foreach (var (subject, resourceType, permission, context) in shapes)
        {
            if (subject.ObjectId == CoreConstants.PublicWildcard)
                continue; // a wildcard subject is not a concrete LookupResources query

            var live = await Collect(engine.LookupResources(
                reader, subject.ObjectType, subject.ObjectId, subject.Relation, resourceType, permission,
                coveredCandidateIds: null, context));

            IReadOnlyList<string>? candidates = null;
            if (coverage.TryGetYields(resourceType, permission, out var yields))
            {
                var nodes = await MembershipWalk.LocalClosure(
                    reader, coverage, new MembershipWalk.SubjectKey(subject.ObjectType, subject.ObjectId, subject.Relation));
                candidates = MembershipWalk.ToCoveredCandidates(nodes, yields, resourceType, subject.ObjectType, subject.ObjectId);
            }

            var walked = await Collect(engine.LookupResources(
                reader, subject.ObjectType, subject.ObjectId, subject.Relation, resourceType, permission,
                candidates, context));

            // Compare the per-resource collapsed verdict. The live engine has NO global dedup (it may emit a
            // resource once per entrypoint); the walked path Checks each id once. A duplicate is not a
            // verdict change, so collapse both by id (Member dominates Caveated) before comparing.
            if (live.Count != walked.Count || live.Any(kv => !walked.TryGetValue(kv.Key, out var m) || m != kv.Value))
                mismatches.Add(
                    $"  {subject.ObjectType}:{subject.ObjectId}#{subject.Relation} -> {resourceType}#{permission}: " +
                    $"live=[{Render(live)}] walked=[{Render(walked)}]");
        }

        Assert.True(mismatches.Count == 0,
            $"{fileName}: the walk-based accelerator changed {mismatches.Count} LookupResources verdict(s):\n{string.Join('\n', mismatches)}");
    }

    private static string Render(Dictionary<string, Membership> d) =>
        string.Join(",", d.OrderBy(kv => kv.Key, StringComparer.Ordinal).Select(kv => $"{kv.Key}:{kv.Value}"));

    // Collapse a result stream to one verdict per resource id (Member dominates Caveated).
    private static async Task<Dictionary<string, Membership>> Collect(IAsyncEnumerable<FoundResource> e)
    {
        var map = new Dictionary<string, Membership>();
        await foreach (var f in e)
        {
            if (!map.TryGetValue(f.ResourceId, out var existing) || (existing == Membership.Caveated && f.Membership == Membership.Member))
                map[f.ResourceId] = f.Membership;
        }
        return map;
    }
}
