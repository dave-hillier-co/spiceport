using Spiceport.Conformance.Tests;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Datastore.Memory;
using Spiceport.Engine;
using Spiceport.Schema;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Stage-4 NON-NEGOTIABLE oracle gate: across the ENTIRE SpiceDB conformance corpus, the Leopard
/// <see cref="MembershipIndex"/> must not change a single LookupResources verdict. For every file, every
/// assertion's (subject, resource-type, permission) is run through <see cref="LookupResourcesEngine"/> twice —
/// once with the index, once without — and the (resource id, membership) result sets must be identical. Run at
/// the engine level (no Orleans) so the whole corpus sweeps in seconds.
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
    public async Task IndexOn_EqualsIndexOff_ForEveryAssertionShape(string fileName)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "TestData", fileName);
        var file = ValidationFileLoader.LoadFromFile(path);

        var compiled = SchemaCompiler.CompileSchema(file.SchemaText);
        var engine = new LookupResourcesEngine(compiled.Namespaces, compiled.Caveats);

        var store = new InMemoryDatastore();
        var rev = await store.ReadWriteTx(tx => tx.WriteRelationships(
            file.Relationships.Select(r => new RelationshipUpdate(r, UpdateOperation.Create)).ToList()));
        var reader = store.SnapshotReader(rev);

        var index = await MembershipIndex.Build(compiled.Namespaces, reader, "corpus-hash");

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
                index: null, context));
            var indexed = await Collect(engine.LookupResources(
                reader, subject.ObjectType, subject.ObjectId, subject.Relation, resourceType, permission,
                index, context));

            // Compare the per-resource collapsed verdict. The live engine has NO global dedup (it may emit a
            // resource once per entrypoint); the index Checks each id once. A duplicate is not a verdict change,
            // so collapse both by id (Member dominates Caveated) before comparing.
            if (live.Count != indexed.Count || live.Any(kv => !indexed.TryGetValue(kv.Key, out var m) || m != kv.Value))
                mismatches.Add(
                    $"  {subject.ObjectType}:{subject.ObjectId}#{subject.Relation} -> {resourceType}#{permission}: " +
                    $"live=[{Render(live)}] indexed=[{Render(indexed)}]");
        }

        Assert.True(mismatches.Count == 0,
            $"{fileName}: index changed {mismatches.Count} LookupResources verdict(s):\n{string.Join('\n', mismatches)}");
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
