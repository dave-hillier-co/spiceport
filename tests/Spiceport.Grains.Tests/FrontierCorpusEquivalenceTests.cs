using Spiceport.Conformance.Tests;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;
using Spiceport.Schema;

namespace Spiceport.Grains.Tests;

/// <summary>
/// NON-NEGOTIABLE oracle gate for the <see cref="SubjectFrontierGrain"/> memo: across the entire SpiceDB
/// conformance corpus, every LookupSubjects-shaped assertion (resource, permission, subject type) must
/// yield the IDENTICAL <see cref="StreamLookupSubjects"/> result set whether the memo is consulted or not.
/// </summary>
/// <remarks>
/// Mirrors <c>Stage4CorpusEquivalenceTests</c>' on==off gate shape, adapted to a grain-hosted memo: that
/// test avoids Orleans entirely because its accelerator (the Leopard index) lives at the engine level, so
/// both sides can run in-process over a <see cref="ReferenceDatastore"/>. This memo instead lives on
/// <see cref="SubjectFrontierGrain"/>, so the "on" side genuinely needs ONE real mesh
/// (<see cref="MeshTestCluster"/>, memo enabled — the production default) driving
/// <see cref="IReverseOpsStreamGrain.StreamLookupSubjects"/>. The "off" side does not need a SECOND
/// Orleans stack: disabling the memo makes <c>ReverseOpsStreamGrain</c> fall back to running
/// <see cref="LookupSubjectsEngine"/> directly and collapsing with <see cref="CaveatEvaluator"/> via
/// <c>ReverseOpsSupport.TryCollapse</c> — exactly what this test computes in-process over a
/// <see cref="ReferenceDatastore"/> seeded with the same relationships, so it is a faithful "off" oracle
/// without paying for a second cluster per file.
/// </remarks>
[Collection(MeshClusterCollection.Name)]
public class FrontierCorpusEquivalenceTests
{
    public static IEnumerable<object[]> CorpusFiles()
    {
        var dir = Path.Combine(AppContext.BaseDirectory, "TestData");
        foreach (var path in Directory.EnumerateFiles(dir, "*.yaml").OrderBy(p => p, StringComparer.Ordinal))
            yield return [Path.GetFileName(path)];
    }

    private static async Task SeedAsync(IDatastore datastore, ValidationFile file)
    {
        if (file.Relationships.Count == 0)
            return;

        var updates = file.Relationships
            .Select(r => new RelationshipUpdate(r, UpdateOperation.Create))
            .ToList();
        await datastore.ReadWriteTx(tx => tx.WriteRelationships(updates));
    }

    private readonly record struct Found(string SubjectId, bool IsWildcard, bool IsCaveated);

    private static async Task<HashSet<Found>> StreamViaMesh(
        MeshTestCluster cluster, string resourceType, string resourceId, string permission, string subjectType)
    {
        var args = new LookupSubjectsArgs(
            resourceType, resourceId, permission, subjectType, CoreConstants.Ellipsis,
            Context: null, Limit: null, Cursor: null);

        var result = new HashSet<Found>();
        await foreach (var item in cluster.GrainFactory
            .GetGrain<IReverseOpsStreamGrain>(Guid.NewGuid()).StreamLookupSubjects(args))
        {
            result.Add(new Found(item.Subject.SubjectId, item.Subject.IsWildcard, item.Subject.Permissionship.IsCaveated));
        }
        return result;
    }

    /// <summary>
    /// The memo-OFF oracle: the identical computation <c>ReverseOpsStreamGrain.StreamLookupSubjects</c>
    /// runs when <see cref="SubjectFrontierMemoOptions.Enabled"/> is false — a direct
    /// <see cref="LookupSubjectsEngine"/> walk collapsed against a null request context via
    /// <see cref="ReverseOpsSupport.TryCollapse"/> — computed in-process over a
    /// <see cref="ReferenceDatastore"/>.
    /// </summary>
    private static async Task<HashSet<Found>> StreamViaEngineDirectly(
        CompiledSchema schema, ReferenceDatastore datastore,
        string resourceType, string resourceId, string permission, string subjectType)
    {
        var head = await datastore.HeadRevision();
        var reader = datastore.SnapshotReader(head.Revision);
        var engine = new LookupSubjectsEngine(schema.Namespaces);
        var evaluator = new CaveatEvaluator(schema.Caveats);
        var resource = new ObjectAndRelation(resourceType, resourceId, permission);

        var result = new HashSet<Found>();
        await foreach (var found in engine.LookupSubjects(reader, resource, subjectType))
        {
            if (!ReverseOpsSupport.TryCollapse(found.Caveat, context: null, evaluator, out var permissionship))
                continue;
            result.Add(new Found(found.SubjectId, found.IsWildcard, permissionship.IsCaveated));
        }
        return result;
    }

    [Theory]
    [MemberData(nameof(CorpusFiles))]
    public async Task MemoOn_EqualsMemoOff_ForEveryLookupSubjectsShape(string fileName)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "TestData", fileName);
        var file = ValidationFileLoader.LoadFromFile(path);

        // Distinct (resourceType, resourceId, permission, subjectType) shapes drawn from the file's
        // assertions — every assertion names a concrete subject, so its (type) is a LookupSubjects shape
        // worth sweeping (the exact subjectId does not matter: LookupSubjects enumerates ALL holders of
        // that type, so many assertions collapse onto the same shape).
        var shapes = file.Assertions
            .Select(a => (a.Resource.ObjectType, a.Resource.ObjectId, a.Resource.Relation, a.Subject.ObjectType))
            .Distinct()
            .ToList();

        if (shapes.Count == 0)
            return;

        await using var cluster = await MeshTestCluster.CreateAsync(file.SchemaText, useSubjectFrontierMemo: true);
        await SeedAsync(cluster.Datastore, file);

        var compiled = SchemaCompiler.CompileSchema(file.SchemaText);
        var reference = new ReferenceDatastore();
        await SeedAsync(reference, file);

        var mismatches = new List<string>();
        foreach (var (resourceType, resourceId, permission, subjectType) in shapes)
        {
            var withMemo = await StreamViaMesh(cluster, resourceType, resourceId, permission, subjectType);
            var withoutMemo = await StreamViaEngineDirectly(
                compiled, reference, resourceType, resourceId, permission, subjectType);

            if (!withMemo.SetEquals(withoutMemo))
            {
                mismatches.Add(
                    $"  {resourceType}:{resourceId}#{permission} -> {subjectType}: " +
                    $"on=[{Render(withMemo)}] off=[{Render(withoutMemo)}]");
            }
        }

        Assert.True(mismatches.Count == 0,
            $"{fileName}: frontier memo changed {mismatches.Count} LookupSubjects result set(s):\n{string.Join('\n', mismatches)}");
    }

    private static string Render(HashSet<Found> set) =>
        string.Join(",", set.OrderBy(t => t.SubjectId, StringComparer.Ordinal)
            .Select(t => $"{t.SubjectId}(w={t.IsWildcard},c={t.IsCaveated})"));
}
