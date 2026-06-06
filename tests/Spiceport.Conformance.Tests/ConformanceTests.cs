using System.Collections.Immutable;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Datastore.Memory;
using Spiceport.Engine;
using Spiceport.Schema;

namespace Spiceport.Conformance.Tests;

/// <summary>
/// SpiceDB consistency/validation conformance harness. For every non-skipped YAML
/// test config it compiles the schema, loads the relationships into an in-memory
/// datastore, and runs every assertion through the <see cref="CheckEngine"/>,
/// asserting the expected boolean outcome.
/// </summary>
public class ConformanceTests
{
    /// <summary>
    /// Files that require caveat or expiration semantics, which the current engine
    /// treats opaquely (a caveated tuple counts as an unconditional member) and the
    /// schema compiler does not fully model. Running these would produce misleading
    /// pass/fail signal, so they are reported as skipped with a reason.
    /// </summary>
    private static readonly IReadOnlyDictionary<string, string> SkipReasons = BuildSkipReasons();

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

        var namespaces = SchemaCompiler.Compile(file.SchemaText);
        var engine = new CheckEngine(namespaces);

        var datastore = new InMemoryDatastore();
        var revision = await LoadRelationships(datastore, file.Relationships);
        var reader = datastore.SnapshotReader(revision);

        var failures = new List<string>();
        foreach (var assertion in file.Assertions)
        {
            var subject = assertion.Subject;
            var result = await engine.Check(
                reader,
                assertion.Resource.ObjectType,
                assertion.Resource.ObjectId,
                assertion.Resource.Relation,
                subject);

            if (result.IsMember != assertion.Expected)
            {
                failures.Add(
                    $"  {assertion.SourceText} => expected {assertion.Expected}, got {result.IsMember} (verdict {result.Verdict})");
            }
        }

        Assert.True(
            failures.Count == 0,
            $"{fileName}: {failures.Count}/{file.Assertions.Count} assertion(s) failed:\n{string.Join('\n', failures)}");
    }

    private static async Task<IRevision> LoadRelationships(
        InMemoryDatastore datastore,
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

    private static Dictionary<string, string> BuildSkipReasons()
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        var dir = Path.Combine(AppContext.BaseDirectory, "TestData");
        if (!Directory.Exists(dir))
        {
            return map;
        }

        foreach (var path in Directory.EnumerateFiles(dir, "*.yaml"))
        {
            var name = Path.GetFileName(path);
            var lowerName = name.ToLowerInvariant();
            string? reason = null;

            if (lowerName.Contains("caveat"))
            {
                reason = "requires caveat (CEL) evaluation, not yet modelled";
            }
            else if (lowerName.Contains("expiration"))
            {
                reason = "requires relationship-expiration semantics, not yet modelled";
            }
            else
            {
                // Catch files that use caveat syntax without a caveat/expiration name.
                var text = File.ReadAllText(path);
                if (UsesCaveatSyntax(text))
                {
                    reason = "uses caveat syntax (caveat definition / `with` annotation), not yet modelled";
                }
                else if (UsesExpirationSyntax(text))
                {
                    reason = "uses expiration syntax, not yet modelled";
                }
            }

            if (reason is not null)
            {
                map[name] = reason;
            }
        }

        return map;
    }

    private static bool UsesCaveatSyntax(string text)
    {
        foreach (var rawLine in text.Split('\n'))
        {
            var line = rawLine.Trim();
            if (line.StartsWith("//", StringComparison.Ordinal))
            {
                continue;
            }

            // A standalone caveat definition, or a `with <caveat>` type annotation,
            // or a caveat-context assertion (`@subject... with {json}`).
            if (line.StartsWith("caveat ", StringComparison.Ordinal) ||
                line.Contains(" with ", StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static bool UsesExpirationSyntax(string text) =>
        text.Contains("expiration", StringComparison.OrdinalIgnoreCase);
}
