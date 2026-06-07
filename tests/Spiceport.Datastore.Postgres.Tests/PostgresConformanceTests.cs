using System.Collections.Immutable;
using Spiceport.Conformance.Tests;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Schema;

namespace Spiceport.Datastore.Postgres.Tests;

/// <summary>
/// The decisive correctness proof: runs the EXISTING SpiceDB conformance corpus (the same validation
/// YAML loader + <see cref="CheckEngine"/> as the in-memory suite) against the Postgres datastore and
/// asserts identical verdicts. For every YAML file the schema is compiled, relationships are loaded into
/// a fresh Postgres database, and each assertion is evaluated through the engine reading from a Postgres
/// snapshot reader. Any divergence from the spec's expected outcome fails the test.
/// </summary>
[Collection(PostgresCollection.Name)]
public class PostgresConformanceTests
{
    private readonly PostgresContainerFixture _fixture;

    public PostgresConformanceTests(PostgresContainerFixture fixture) => _fixture = fixture;

    public static IEnumerable<object[]> AllYamlFiles()
    {
        var dir = Path.Combine(AppContext.BaseDirectory, "TestData");
        foreach (var path in Directory.EnumerateFiles(dir, "*.yaml").OrderBy(p => p, StringComparer.Ordinal))
            yield return [Path.GetFileName(path)];
    }

    [SkippableTheory]
    [MemberData(nameof(AllYamlFiles))]
    public async Task Conformance(string fileName)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "TestData", fileName);
        var file = ValidationFileLoader.LoadFromFile(path);

        var compiled = SchemaCompiler.CompileSchema(file.SchemaText);
        var engine = new CheckEngine(compiled.Namespaces, compiled.Caveats);

        var datastore = await _fixture.NewDatastore();
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
                failures.Add($"  {assertion.SourceText} => expected {expected}, got {result.Verdict}{missing}");
            }
        }

        Assert.True(
            failures.Count == 0,
            $"{fileName}: {failures.Count}/{file.Assertions.Count} assertion(s) failed:\n{string.Join('\n', failures)}");
    }

    private static async Task<IRevision> LoadRelationships(
        PostgresDatastore datastore,
        ImmutableList<Relationship> relationships)
    {
        if (relationships.Count == 0)
            return (await datastore.HeadRevision()).Revision;

        var updates = relationships
            .Select(r => new RelationshipUpdate(r, UpdateOperation.Create))
            .ToList();

        return await datastore.ReadWriteTx(tx => tx.WriteRelationships(updates));
    }
}
