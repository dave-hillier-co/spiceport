using System.Collections.Immutable;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Schema;

namespace Spiceport.Conformance.Tests;

/// <summary>
/// Cross-checks every <c>validation:</c> block in the main conformance corpus (Quarantine
/// excluded) against <see cref="LookupSubjectsEngine"/> over the <see cref="ReferenceDatastore"/>
/// oracle. For each <c>resource#permission</c> key the parsed expected-subjects set (concrete
/// subjects, subject-with-relation, wildcard and caveated markers, per SpiceDB's
/// <c>pkg/validationfile/blocks</c> grammar — see <see cref="ValidationFileLoader"/>) must equal
/// the actual subject set the engine reports, following the same LookupSubjects usage pattern as
/// <see cref="ReverseConsistencyCrossCheckTests"/>.
/// </summary>
/// <remarks>
/// As of the SpiceDB v1.44.2 vendoring, none of the 73 pre-existing corpus files (nor the
/// upstream files considered for addition) carry a populated <c>validation:</c> block — the
/// upstream integration-testing corpus at this tag exclusively uses assertTrue/assertFalse/
/// assertCaveated. This suite is therefore a no-op today (0 files exercised) but exists so that
/// a future re-vendor that reintroduces validation blocks is asserted automatically rather than
/// silently ignored.
/// </remarks>
public class ValidationBlockTests
{
    /// <summary>
    /// A single test method (rather than a [Theory]/MemberData enumeration) deliberately:
    /// an empty MemberData set is itself an xUnit failure ("No data found"), which would make
    /// this suite brittle to the (expected, documented) zero-files case at the current vendor
    /// point. Iterating internally lets the assertion coverage — currently zero files, zero
    /// entries — show up as a clean pass instead.
    /// </summary>
    [Fact]
    public async Task ValidationBlock_subjects_match_LookupSubjects()
    {
        var dir = Path.Combine(AppContext.BaseDirectory, "TestData");
        var failures = new List<string>();
        var filesChecked = 0;
        var entriesChecked = 0;

        foreach (var path in Directory.EnumerateFiles(dir, "*.yaml").OrderBy(p => p, StringComparer.Ordinal))
        {
            var fileName = Path.GetFileName(path);
            var file = ValidationFileLoader.LoadResolved(path);
            if (file.Validations.Count == 0)
            {
                continue;
            }

            filesChecked++;
            var compiled = SchemaCompiler.CompileSchema(file.SchemaText);
            var lookupSubjects = new LookupSubjectsEngine(compiled.Namespaces);

            var datastore = new ReferenceDatastore();
            var revision = await LoadRelationships(datastore, file.Relationships);
            var reader = datastore.SnapshotReader(revision);

            foreach (var entry in file.Validations)
            {
                entriesChecked++;

                // Group expected subjects by (type, relation): LookupSubjects is queried per
                // subject type/relation pair, mirroring how each is independently reported.
                var byTypeAndRelation = entry.ExpectedSubjects
                    .GroupBy(s => (s.Subject.ObjectType, s.Subject.Relation));

                foreach (var group in byTypeAndRelation)
                {
                    var (subjectType, subjectRelation) = group.Key;
                    var found = await Collect(lookupSubjects.LookupSubjects(
                        reader, entry.ObjectAndRelation, subjectType, subjectRelation));

                    var actual = ToComparableSet(found);
                    var expected = ToComparableSet(group);

                    if (!actual.SetEquals(expected))
                    {
                        var key = $"{entry.ObjectAndRelation.ObjectType}:{entry.ObjectAndRelation.ObjectId}#{entry.ObjectAndRelation.Relation}";
                        failures.Add(
                            $"  {fileName}: {key} ({subjectType}#{subjectRelation}): expected [{string.Join(", ", expected)}], " +
                            $"got [{string.Join(", ", actual)}]");
                    }
                }
            }
        }

        Assert.True(
            failures.Count == 0,
            $"{filesChecked} file(s) / {entriesChecked} entries checked; {failures.Count} mismatch(es):\n{string.Join('\n', failures)}");
    }

    /// <summary>A subject/caveated pair normalised for set comparison; <c>"*"</c> denotes the wildcard.</summary>
    private readonly record struct ComparableSubject(string ObjectId, bool IsCaveated);

    private static ISet<ComparableSubject> ToComparableSet(IEnumerable<FoundSubject> found) =>
        found.Select(f => new ComparableSubject(f.SubjectId, f.Caveat is not null)).ToHashSet();

    private static ISet<ComparableSubject> ToComparableSet(IEnumerable<ExpectedSubject> expected)
    {
        var set = new HashSet<ComparableSubject>();
        foreach (var subject in expected)
        {
            set.Add(new ComparableSubject(subject.Subject.ObjectId, subject.IsCaveated));
            foreach (var exception in subject.Exceptions)
            {
                set.Add(new ComparableSubject(exception.Subject.ObjectId, exception.IsCaveated));
            }
        }

        return set;
    }

    private static async Task<List<FoundSubject>> Collect(IAsyncEnumerable<FoundSubject> source)
    {
        var list = new List<FoundSubject>();
        await foreach (var item in source)
        {
            list.Add(item);
        }

        return list;
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
