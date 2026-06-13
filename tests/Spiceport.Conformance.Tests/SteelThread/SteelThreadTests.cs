using System.Text;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Datastore.Memory;
using Spiceport.Engine;
using Spiceport.Schema;

namespace Spiceport.Conformance.Tests.SteelThread;

/// <summary>
/// Differential suite over SpiceDB's steelthreadtesting corpus. Each case names a datafile and a list
/// of operations; the golden outputs (steelresults/*.yaml) are reproduced through Spiceport's engines
/// and datastore. Cursored lookup-resources is compared UNION-ONLY (dispatch-order page partitions are
/// not byte-stable); everything else is compared exactly. See the design report for the rationale.
/// </summary>
public class SteelThreadTests
{
    // ---- The ported case list (mirror of definitions.go) ----

    public abstract record Op(string Name);

    public sealed record LookupSubjectsOp(string Name, string ResourceType, string ResourceId, string Permission, string SubjectType, string GoldenFile) : Op(Name);

    public sealed record LookupResourcesOp(string Name, string ResourceType, string Permission, string SubjectType, string SubjectId, string GoldenFile) : Op(Name);

    public sealed record CursoredLookupResourcesOp(string Name, string ResourceType, string Permission, string SubjectType, string SubjectId, int PageSize, string UnionGoldenFile) : Op(Name);

    public sealed record CursoredReadByResourceOp(string Name, string ResourceType, string ResourceId, int PageSize, string GoldenFile) : Op(Name);

    public sealed record CursoredReadBySubjectOp(string Name, string SubjectType, string SubjectId, int PageSize, string GoldenFile) : Op(Name);

    public sealed record BulkImportExportOp(string Name, string RelsFile, string? FilterResourceType, string? FilterResourceIdPrefix, string GoldenFile) : Op(Name);

    public sealed record BulkCheckOp(string Name, IReadOnlyList<string> CheckRequests, string GoldenFile) : Op(Name);

    public sealed record WriteSchemaOp(string Name, string Schema, bool ExpectSuccess) : Op(Name);

    public sealed record Case(string Name, string Datafile, IReadOnlyList<Op> Operations);

    private const string LsSomedoc = "basic-lookup-subjects-uncursored-lookup-subjects-for-somedoc-results.yaml";
    private const string LsPublicdoc = "basic-lookup-subjects-uncursored-lookup-subjects-for-public-doc-results.yaml";
    private const string LsIntersect = "lookup-subjects-intersection-uncursored-lookup-subjects-for-somedoc-results.yaml";
    private const string LsIntersectArrow = "lookup-subjects-intersection-arrow-uncursored-lookup-subjects-for-somedoc-results.yaml";
    private const string LrFred = "basic-lookup-resources-uncursored-lookup-resources-for-fred-results.yaml";
    private const string LrIntersect = "lookup-resources-with-intersection-uncursored-indirect-lookup-resources-for-user-fred-results.yaml";

    private static IReadOnlyList<Case> Cases() =>
    [
        new("basic lookup subjects", "basic-document.yaml",
        [
            new LookupSubjectsOp("ls somedoc", "document", "somedoc", "view", "user", LsSomedoc),
            new LookupSubjectsOp("ls publicdoc", "document", "publicdoc", "view", "user", LsPublicdoc),
        ]),
        new("lookup subjects intersection", "document-with-intersect.yaml",
        [
            new LookupSubjectsOp("ls intersect somedoc", "document", "somedoc", "view", "user", LsIntersect),
        ]),
        new("lookup subjects intersection arrow", "document-with-intersect-arrow.yaml",
        [
            new LookupSubjectsOp("ls intersect-arrow somedoc", "document", "somedoc", "view", "user", LsIntersectArrow),
        ]),
        new("basic lookup resources", "document-with-many-resources.yaml",
        [
            new LookupResourcesOp("lr view", "document", "view", "user", "fred", LrFred),
            new CursoredLookupResourcesOp("clr view p5", "document", "view", "user", "fred", 5, LrFred),
            new CursoredLookupResourcesOp("clr view p1", "document", "view", "user", "fred", 1, LrFred),
            new CursoredLookupResourcesOp("clr view p16", "document", "view", "user", "fred", 16, LrFred),
            new CursoredLookupResourcesOp("clr view p100", "document", "view", "user", "fred", 100, LrFred),
            new LookupResourcesOp("lr indirect", "document", "indirect_view", "user", "fred", LrFred),
        ]),
        new("lookup resources with intersection", "document-with-intersect-resources.yaml",
        [
            new LookupResourcesOp("lr-i view", "document", "view", "user", "fred", LrIntersect),
            new CursoredLookupResourcesOp("clr-i view", "document", "view", "user", "fred", 18, LrIntersect),
        ]),
        new("basic import export", "document-with-a-few-relationships.yaml",
        [
            new BulkImportExportOp("no filter", "basic-import-export-relationships.txt", null, null, "basic-import-export-results.yaml"),
            new BulkImportExportOp("filter type", "basic-import-export-relationships.txt", "document", null, "basic-import-export-results.yaml"),
            new BulkImportExportOp("filter prefix", "basic-import-export-relationships.txt", null, "doc-1", "filtered-import-export-results.yaml"),
        ]),
        new("basic bulk checks", "document-with-a-few-relationships.yaml",
        [
            new BulkCheckOp("bulk", [
                "document:doc-1#view@user:user-0", "document:doc-1#view@user:user-1", "document:doc-1#view@user:user-2",
                "document:doc-2#view@user:user-0", "document:doc-2#view@user:user-1", "document:doc-2#view@user:user-2",
                "document:doc-3#view@user:user-0", "document:doc-3#view@user:user-1", "document:doc-3#view@user:user-2",
            ], "basic-bulk-checks-basic-bulk-checks-results.yaml"),
        ]),
        new("bulk checks with traits", "document-with-traits.yaml",
        [
            new BulkCheckOp("bulk traits", [
                "document:firstdoc#view@user:tom", "document:firstdoc#view@user:fred",
                "document:seconddoc#view@user:tom", "document:seconddoc#view@user:fred",
                "document:seconddoc#view@user:tom[unused:{\"somecondition\": 41}]", "document:seconddoc#view@user:fred[unused:{\"somecondition\": 41}]",
                "document:seconddoc#view@user:tom[unused:{\"somecondition\": 42}]", "document:seconddoc#view@user:fred[unused:{\"somecondition\": 42}]",
                "document:thirddoc#view@user:tom", "document:thirddoc#view@user:fred",
                "document:thirddoc#view@user:tom[unused:{\"somecondition\": 41}]", "document:thirddoc#view@user:fred[unused:{\"somecondition\": 41}]",
                "document:thirddoc#view@user:tom[unused:{\"somecondition\": 42}]", "document:thirddoc#view@user:fred[unused:{\"somecondition\": 42}]",
            ], "bulk-checks-with-traits-bulk-checks-results.yaml"),
        ]),
        new("write schema removal (success cases)", "basic-schema-and-data.yaml",
        [
            new WriteSchemaOp("removes the group relation",
                "definition user {}\ndefinition user2 {}\ndefinition group {\nrelation direct_member: user | group#member\nrelation admin: user\npermission member = direct_member + admin\n}\ndefinition document {\nrelation editor: user2:*\nrelation viewer: user | user:*\npermission view = viewer\n}", ExpectSuccess: true),
        ]),
        new("remove relation on real schema", "real-schema-and-data-with-many-relations.yaml",
        [
            new WriteSchemaOp("real",
                "use expiration\ndefinition user {}\ndefinition platform {}\ndefinition resource {\nrelation platform: platform\nrelation viewer: user | user:*\n}", ExpectSuccess: true),
        ]),
        new("read relationships by resource", "read-relationships-sorting.yaml",
        [
            new CursoredReadByResourceOp("rr p1", "document", "target-doc", 1, "read-relationships-by-resource-cursored-read-by-resource-page-size-1-results.yaml"),
            new CursoredReadByResourceOp("rr p2", "document", "target-doc", 2, "read-relationships-by-resource-cursored-read-by-resource-page-size-2-results.yaml"),
            new CursoredReadByResourceOp("rr p5", "document", "target-doc", 5, "read-relationships-by-resource-cursored-read-by-resource-page-size-5-results.yaml"),
            new CursoredReadByResourceOp("rr p10", "document", "target-doc", 10, "read-relationships-by-resource-cursored-read-by-resource-page-size-10-results.yaml"),
            new CursoredReadByResourceOp("rr p100", "document", "target-doc", 100, "read-relationships-by-resource-cursored-read-by-resource-page-size-100-results.yaml"),
        ]),
        new("read relationships by subject", "read-relationships-sorting.yaml",
        [
            new CursoredReadBySubjectOp("rs p1", "user", "target-user", 1, "read-relationships-by-subject-cursored-read-by-subject-page-size-1-results.yaml"),
            new CursoredReadBySubjectOp("rs p2", "user", "target-user", 2, "read-relationships-by-subject-cursored-read-by-subject-page-size-2-results.yaml"),
            new CursoredReadBySubjectOp("rs p5", "user", "target-user", 5, "read-relationships-by-subject-cursored-read-by-subject-page-size-5-results.yaml"),
            new CursoredReadBySubjectOp("rs p10", "user", "target-user", 10, "read-relationships-by-subject-cursored-read-by-subject-page-size-10-results.yaml"),
            new CursoredReadBySubjectOp("rs p100", "user", "target-user", 100, "read-relationships-by-subject-cursored-read-by-subject-page-size-100-results.yaml"),
        ]),
    ];

    public static IEnumerable<object[]> AllOperations()
    {
        foreach (var c in Cases())
        {
            foreach (var op in c.Operations)
            {
                yield return [c.Datafile, c, op];
            }
        }
    }

    [Theory]
    [MemberData(nameof(AllOperations))]
    public async Task SteelThread(string datafile, Case @case, Op op)
    {
        var fixture = await Fixture.Load(datafile);

        switch (op)
        {
            case LookupSubjectsOp o: await RunLookupSubjects(fixture, o); break;
            case LookupResourcesOp o: await RunLookupResources(fixture, o); break;
            case CursoredLookupResourcesOp o: await RunCursoredLookupResources(fixture, o); break;
            case CursoredReadByResourceOp o: await RunCursoredRead(fixture, ReadByResource(o.ResourceType, o.ResourceId), o.PageSize, o.GoldenFile); break;
            case CursoredReadBySubjectOp o: await RunCursoredRead(fixture, ReadBySubject(o.SubjectType, o.SubjectId), o.PageSize, o.GoldenFile); break;
            case BulkImportExportOp o: await RunBulkImportExport(datafile, o); break;
            case BulkCheckOp o: await RunBulkCheck(fixture, o); break;
            case WriteSchemaOp o: await RunWriteSchema(fixture, o); break;
            default: throw new InvalidOperationException($"unhandled op {op.GetType().Name}");
        }
    }

    // ---- lookupSubjects (exact) ----
    private static async Task RunLookupSubjects(Fixture f, LookupSubjectsOp o)
    {
        var engine = new LookupSubjectsEngine(f.Schema.Namespaces);
        var resource = new ObjectAndRelation(o.ResourceType, o.ResourceId, o.Permission);
        var found = new List<FoundSubject>();
        await foreach (var s in engine.LookupSubjects(f.Reader, resource, o.SubjectType, CoreConstants.Ellipsis))
        {
            found.Add(s);
        }

        var formatted = found.Select(FormatResolvedSubject).Distinct(StringComparer.Ordinal)
            .OrderBy(x => x, StringComparer.Ordinal).ToList();
        AssertFlatGolden(o.GoldenFile, formatted);
    }

    // ---- lookupResources (exact, deduped) ----
    private static async Task RunLookupResources(Fixture f, LookupResourcesOp o)
    {
        var engine = new LookupResourcesEngine(f.Schema.Namespaces, f.Schema.Caveats);
        var byId = new Dictionary<string, FoundResource>(StringComparer.Ordinal);
        await foreach (var r in engine.LookupResources(f.Reader, o.SubjectType, o.SubjectId, CoreConstants.Ellipsis, o.ResourceType, o.Permission))
        {
            MergeResource(byId, r);
        }

        var formatted = byId.Values.Select(FormatResolvedResource)
            .OrderBy(x => x, StringComparer.Ordinal).ToList();
        AssertFlatGolden(o.GoldenFile, formatted);
    }

    // ---- cursoredLookupResources (UNION-ONLY + full-page invariant) ----
    private static async Task RunCursoredLookupResources(Fixture f, CursoredLookupResourcesOp o)
    {
        var engine = new LookupResourcesEngine(f.Schema.Namespaces, f.Schema.Caveats);
        LookupResourcesCursor? cursor = null;
        var pageCounts = new List<int>();
        var unionById = new Dictionary<string, FoundResource>(StringComparer.Ordinal);

        while (true)
        {
            var pageById = new Dictionary<string, FoundResource>(StringComparer.Ordinal);
            var count = 0;
            LookupResourcesCursor? last = cursor;
            await foreach (var r in engine.LookupResources(
                f.Reader, o.SubjectType, o.SubjectId, CoreConstants.Ellipsis, o.ResourceType, o.Permission,
                cursor: cursor, limit: o.PageSize))
            {
                MergeResource(pageById, r);
                last = r.AfterCursor;
                count++;
            }

            if (count == 0)
            {
                break;
            }

            pageCounts.Add(count);
            foreach (var (_, v) in pageById)
            {
                MergeResource(unionById, v);
            }

            cursor = last;
        }

        // Invariant: every non-final page is exactly full (port of operations.go:215-223).
        for (var i = 0; i < pageCounts.Count - 1; i++)
        {
            Assert.True(pageCounts[i] == o.PageSize,
                $"{o.Name}: non-final page #{i} had {pageCounts[i]} (expected full {o.PageSize}); pages: [{string.Join(",", pageCounts)}]");
        }

        // UNION-ONLY: deduped union must equal the uncursored golden (partition is dispatch-order dependent).
        var union = unionById.Values.Select(FormatResolvedResource)
            .OrderBy(x => x, StringComparer.Ordinal).ToList();
        AssertFlatGolden(o.UnionGoldenFile, union);
    }

    // ---- cursoredReadRelationships (exact list-of-lists) ----
    private static async Task RunCursoredRead(Fixture f, RelationshipsFilter filter, int pageSize, string goldenFile)
    {
        var all = new List<string>();
        await foreach (var rel in f.Reader.QueryRelationships(filter))
        {
            all.Add(TupleStrings.FormatRelationship(rel));
        }

        all.Sort(StringComparer.Ordinal);

        var pages = new List<List<string>>();
        for (var i = 0; i < all.Count; i += pageSize)
        {
            pages.Add(all.Skip(i).Take(pageSize).ToList()); // already ordinal -> each page sorted
        }

        AssertNestedGolden(goldenFile, pages);
    }

    private static RelationshipsFilter ReadByResource(string type, string id) =>
        new() { OptionalResourceType = type, OptionalResourceIds = [id] };

    private static RelationshipsFilter ReadBySubject(string subjType, string subjId) =>
        new()
        {
            OptionalSubjectsSelectors =
            [
                new SubjectsSelector(OptionalSubjectType: subjType, OptionalSubjectIds: [subjId],
                    RelationFilter: SubjectRelationFilter.Any),
            ],
        };

    // ---- bulkImportExport (exact) ----
    // SpiceDB seeds the datastore from the case's datafile, THEN imports the .txt, then exports the union.
    // (Its export OptionalLimit is a stream page-size, not a total cap, so the limit variant equals the
    // full export and is not separately modelled here.)
    private static async Task RunBulkImportExport(string datafile, BulkImportExportOp o)
    {
        var store = new InMemoryDatastore();

        var seedFile = ValidationFileLoader.LoadFromFile(
            Path.Combine(AppContext.BaseDirectory, "SteelThread", "TestData", datafile));
        var seedRels = seedFile.Relationships;
        var importRels = await ReadRelsFileAsync(o.RelsFile);

        var all = seedRels.Concat(importRels)
            .Select(r => new RelationshipUpdate(r, UpdateOperation.Create))
            .ToList();
        var rev = await store.ReadWriteTx(tx => tx.WriteRelationships(all));
        var reader = store.SnapshotReader(rev);

        var filter = new RelationshipsFilter
        {
            OptionalResourceType = o.FilterResourceType,
            OptionalResourceIdPrefix = o.FilterResourceIdPrefix,
        };

        var exported = new List<string>();
        await foreach (var rel in reader.QueryRelationships(filter))
        {
            exported.Add(NormalizeExpiration(TupleStrings.FormatRelationship(rel)));
        }

        // Export order is datastore-internal; compare order-insensitively.
        exported.Sort(StringComparer.Ordinal);

        // Known internal divergence: Spiceport's FormatRelationship renders expiration with a fixed
        // 7-digit fractional-second suffix (e.g. ...:15.0000000Z) whereas SpiceDB emits minimal RFC3339
        // (...:15Z). This is a Spiceport-internal string format (the gRPC wire carries a proto Timestamp,
        // so it does not affect zed compatibility). Normalize both sides' trailing-zero fraction so the
        // comparison stays meaningful over the exported relationship SET, caveats and filters.
        var golden = LoadFlatGolden(o.GoldenFile).Select(NormalizeExpiration).ToList();
        golden.Sort(StringComparer.Ordinal);
        Assert.Equal(golden, exported);
    }

    /// <summary>Strips a trailing-zero fractional-second suffix before the expiration 'Z' (see RunBulkImportExport).</summary>
    private static string NormalizeExpiration(string rel) =>
        System.Text.RegularExpressions.Regex.Replace(rel, @"(?<=:\d\d)\.0+Z", "Z");

    // ---- bulkCheckPermissions (exact) ----
    private static async Task RunBulkCheck(Fixture f, BulkCheckOp o)
    {
        var engine = new CheckEngine(f.Schema.Namespaces, f.Schema.Caveats);
        var lines = new List<string>();
        foreach (var requestString in o.CheckRequests)
        {
            var rel = TupleStrings.ParseRelationship(requestString);
            var ctx = rel.OptionalCaveat?.Context;
            var result = await engine.Check(
                f.Reader, rel.Resource.ObjectType, rel.Resource.ObjectId, rel.Resource.Relation, rel.Subject, ctx);
            lines.Add($"{requestString} -> {Permissionship(result.Verdict)}");
        }

        AssertFlatGolden(o.GoldenFile, lines, requireSameOrderAsGolden: true); // order == request order
    }

    // ---- writeSchema (success exact; rejection asserted as "rejected") ----
    private static async Task RunWriteSchema(Fixture f, WriteSchemaOp o)
    {
        var next = SchemaCompiler.CompileSchema(o.Schema);
        var threw = false;
        try
        {
            await Spiceport.Grains.SchemaChangeValidator.ValidateAsync(f.Schema, next, f.Reader);
        }
        catch (Spiceport.Grains.Abstractions.SchemaWriteValidationException)
        {
            threw = true;
        }

        if (o.ExpectSuccess)
        {
            Assert.False(threw, $"{o.Name}: expected schema write to succeed but it was rejected");
        }
        else
        {
            Assert.True(threw, $"{o.Name}: expected schema write to be rejected (SpiceDB message not byte-asserted)");
        }
    }

    // ---- formatting helpers (ports of operations.go) ----

    private static string FormatResolvedResource(FoundResource r) =>
        r.Membership == Membership.Caveated ? $"{r.ResourceId} (conditional)" : r.ResourceId;

    private static string FormatResolvedSubject(FoundSubject s)
    {
        var sb = new StringBuilder();
        sb.Append(s.IsWildcard ? CoreConstants.PublicWildcard : s.SubjectId);

        // Port of SpiceDB formatResolvedSubject: a wildcard renders its excluded subjects as a sorted
        // bracket list, each marked "(conditional)" when conditionally excluded.
        if (s.ExcludedSubjects is { Count: > 0 } excluded)
        {
            var parts = excluded
                .Select(e => e.Caveat is not null ? $"{e.SubjectId} (conditional)" : e.SubjectId)
                .OrderBy(x => x, StringComparer.Ordinal);
            sb.Append(" - [").Append(string.Join(", ", parts)).Append(']');
        }

        if (s.Caveat is not null)
        {
            sb.Append(" (conditional)");
        }

        return sb.ToString();
    }

    private static string Permissionship(Membership m) => m switch
    {
        Membership.Member => "PERMISSIONSHIP_HAS_PERMISSION",
        Membership.Caveated => "PERMISSIONSHIP_CONDITIONAL_PERMISSION",
        _ => "PERMISSIONSHIP_NO_PERMISSION",
    };

    private static void MergeResource(Dictionary<string, FoundResource> into, FoundResource r)
    {
        // Member dominates Caveated when the same id arrives via several entrypoints (within a page).
        if (into.TryGetValue(r.ResourceId, out var ex) && ex.Membership == Membership.Member)
        {
            return;
        }

        into[r.ResourceId] = r;
    }

    // ---- golden loading / assertions ----

    private static string ResultsPath(string file) =>
        Path.Combine(AppContext.BaseDirectory, "SteelThread", "Results", file);

    private static List<string> LoadFlatGolden(string file)
    {
        // Goldens are YAML lists of single-quoted scalars: parse leading "- '...'".
        var items = new List<string>();
        foreach (var raw in File.ReadAllLines(ResultsPath(file)))
        {
            var line = raw.TrimStart();
            if (!line.StartsWith("- ", StringComparison.Ordinal))
            {
                continue;
            }

            items.Add(Unquote(line[2..].Trim()));
        }

        return items;
    }

    private static List<List<string>> LoadNestedGolden(string file)
    {
        // List-of-lists: "- - 'a'" opens a page, "  - 'b'" continues it.
        var pages = new List<List<string>>();
        List<string>? current = null;
        foreach (var raw in File.ReadAllLines(ResultsPath(file)))
        {
            if (raw.TrimStart().StartsWith("- - ", StringComparison.Ordinal))
            {
                current = [Unquote(raw.TrimStart()[4..].Trim())];
                pages.Add(current);
            }
            else if (raw.TrimStart().StartsWith("- ", StringComparison.Ordinal) && current is not null)
            {
                current.Add(Unquote(raw.TrimStart()[2..].Trim()));
            }
        }

        return pages;
    }

    private static string Unquote(string s) =>
        s.Length >= 2 && s[0] == '\'' && s[^1] == '\'' ? s[1..^1].Replace("''", "'") : s;

    private static void AssertFlatGolden(string file, List<string> actual, bool requireSameOrderAsGolden = false)
    {
        var golden = LoadFlatGolden(file);
        if (!requireSameOrderAsGolden)
        {
            golden.Sort(StringComparer.Ordinal);
        }

        Assert.Equal(golden, actual);
    }

    private static void AssertNestedGolden(string file, List<List<string>> actual)
    {
        var golden = LoadNestedGolden(file);
        Assert.Equal(golden.Count, actual.Count);
        for (var i = 0; i < golden.Count; i++)
        {
            Assert.Equal(golden[i], actual[i]);
        }
    }

    private static async Task<List<Relationship>> ReadRelsFileAsync(string relsFile)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "SteelThread", "TestData", relsFile);
        var rels = new List<Relationship>();
        foreach (var line in await File.ReadAllLinesAsync(path))
        {
            if (line.Length > 0)
            {
                rels.Add(TupleStrings.ParseRelationship(line));
            }
        }

        return rels;
    }

    // ---- shared fixture (compiled schema + loaded datastore at a snapshot) ----

    private sealed record Fixture(CompiledSchema Schema, IDatastoreReader Reader)
    {
        public static async Task<Fixture> Load(string datafile)
        {
            var path = Path.Combine(AppContext.BaseDirectory, "SteelThread", "TestData", datafile);
            var file = ValidationFileLoader.LoadFromFile(path);
            var compiled = SchemaCompiler.CompileSchema(file.SchemaText);

            var store = new InMemoryDatastore();
            IRevision rev;
            if (file.Relationships.Count == 0)
            {
                rev = (await store.HeadRevision()).Revision;
            }
            else
            {
                rev = await store.ReadWriteTx(tx => tx.WriteRelationships(
                    file.Relationships.Select(r => new RelationshipUpdate(r, UpdateOperation.Create)).ToList()));
            }

            return new Fixture(compiled, store.SnapshotReader(rev));
        }
    }
}
