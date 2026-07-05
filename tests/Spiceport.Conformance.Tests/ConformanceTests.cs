using System.Collections.Immutable;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Schema;

namespace Spiceport.Conformance.Tests;

/// <summary>
/// SpiceDB consistency/validation conformance harness. For every YAML test config it
/// compiles the schema (yielding namespace AND caveat definitions), loads the
/// relationships (with caveat context + expiration) into a <see cref="ReferenceDatastore"/>
/// (the conformance oracle), and
/// runs every assertion through the <see cref="CheckEngine"/>, comparing the engine's
/// membership verdict against the file's expected outcome
/// (assertTrue → Member, assertFalse → NotMember, assertCaveated → Caveated).
/// </summary>
public class ConformanceTests
{
    /// <summary>
    /// Files that cannot be run faithfully and the precise reason. A file is only listed
    /// here when its expected outcome depends on a specific evaluation "now" that we
    /// cannot derive from the file. The expiration files in this suite all use far-past
    /// (≤2024) versus far-future (≥2200) timestamps, so the real wall clock falls
    /// unambiguously between them and is a faithful "now"; they are therefore NOT skipped.
    /// </summary>
    private static readonly IReadOnlyDictionary<string, string> SkipReasons =
        new Dictionary<string, string>(StringComparer.Ordinal);

    public static IEnumerable<object[]> AllYamlFiles()
    {
        var dir = Path.Combine(AppContext.BaseDirectory, "TestData");
        foreach (var path in Directory.EnumerateFiles(dir, "*.yaml").OrderBy(p => p, StringComparer.Ordinal))
        {
            yield return [Path.GetFileName(path)];
        }
    }

    [SkippableTheory]
    [MemberData(nameof(AllYamlFiles))]
    public async Task Conformance(string fileName)
    {
        Skip.If(
            SkipReasons.TryGetValue(fileName, out var reason),
            $"{fileName}: {reason}");

        var path = Path.Combine(AppContext.BaseDirectory, "TestData", fileName);
        var file = ValidationFileLoader.LoadFromFile(path);

        var compiled = SchemaCompiler.CompileSchema(file.SchemaText);
        var engine = new CheckEngine(compiled.Namespaces, compiled.Caveats);

        var datastore = new ReferenceDatastore();
        var revision = await LoadRelationships(datastore, file.Relationships);
        var reader = datastore.SnapshotReader(revision);

        var failures = new List<string>();
        foreach (var assertion in file.Assertions)
        {
            var result = await engine.Check(
                reader,
                assertion.Resource.ObjectType,
                assertion.Resource.ObjectId,
                assertion.Resource.Relation,
                assertion.Subject,
                caveatContext: assertion.CaveatContext);

            var expected = assertion.ExpectedMembership;
            if (result.Verdict != expected)
            {
                var missing = result.MissingExprFields.Count > 0
                    ? $" [missing: {string.Join(", ", result.MissingExprFields)}]"
                    : string.Empty;
                failures.Add(
                    $"  {assertion.SourceText} => expected {expected}, got {result.Verdict}{missing}");
            }
        }

        Assert.True(
            failures.Count == 0,
            $"{fileName}: {failures.Count}/{file.Assertions.Count} assertion(s) failed:\n{string.Join('\n', failures)}");
    }

    private static async Task<IRevision> LoadRelationships(
        ReferenceDatastore datastore,
        ImmutableList<Relationship> relationships)
    {
        if (relationships.Count == 0)
        {
            var head = await datastore.HeadRevision();
            return head.Revision;
        }

        var updates = relationships
            .Select(r => new RelationshipUpdate(r, UpdateOperation.Create))
            .ToList();

        return await datastore.ReadWriteTx(tx => tx.WriteRelationships(updates));
    }
}
