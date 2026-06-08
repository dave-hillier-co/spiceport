using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Datastore.Memory;
using Spiceport.Schema;

namespace Spiceport.Engine.Tests;

public class LookupResourcesEngineTests
{
    private const string Schema = """
        definition user {}

        definition group {
            relation member: user | group#member
        }

        definition document {
            relation viewer: user | group#member
            relation editor: user
            relation banned: user
            relation parent: document

            permission view = viewer + editor
            permission edit_only = editor & viewer
            permission allowed_view = viewer - banned
            permission inherited_view = viewer + parent->view
        }
        """;

    private const string WildcardSchema = """
        definition user {}

        definition document {
            relation viewer: user | user:*
            permission view = viewer
        }
        """;

    private const string CaveatSchema = """
        caveat over_age(age int, min_age int) {
          age >= min_age
        }

        definition user {}

        definition document {
          relation viewer: user with over_age
          permission view = viewer
        }
        """;

    private static LookupResourcesEngine BuildEngine(string schemaText)
    {
        var compiled = SchemaCompiler.CompileSchema(schemaText);
        return new LookupResourcesEngine(compiled.Namespaces, compiled.Caveats);
    }

    private static CheckEngine BuildCheckEngine(string schemaText)
    {
        var compiled = SchemaCompiler.CompileSchema(schemaText);
        return new CheckEngine(compiled.Namespaces, compiled.Caveats);
    }

    private static async Task<(InMemoryDatastore Store, IRevision Rev)> Seed(params Relationship[] rels)
    {
        var store = new InMemoryDatastore();
        var rev = await store.ReadWriteTx(async tx =>
        {
            var updates = rels.Select(r => new RelationshipUpdate(r, UpdateOperation.Create)).ToList();
            await tx.WriteRelationships(updates);
        });
        return (store, rev);
    }

    private static ObjectAndRelation Onr(string type, string id, string relation = CoreConstants.Ellipsis) =>
        new(type, id, relation);

    private static Relationship Tuple(string resType, string resId, string resRel, ObjectAndRelation subject) =>
        Relationship.Create(new ObjectAndRelation(resType, resId, resRel), subject);

    private static Relationship Caveated(
        string resType, string resId, string resRel, ObjectAndRelation subject,
        string caveatName, IReadOnlyDictionary<string, object?>? ctx = null) =>
        Relationship.Create(
            new ObjectAndRelation(resType, resId, resRel),
            subject,
            new ContextualizedCaveat(caveatName, ctx));

    private static async Task<List<FoundResource>> Collect(IAsyncEnumerable<FoundResource> e)
    {
        var list = new List<FoundResource>();
        await foreach (var f in e)
            list.Add(f);
        return list;
    }

    [Fact]
    public async Task Direct_FindsResourcesWhereSubjectIsViewer()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "viewer", Onr("user", "alice")),
            Tuple("document", "doc2", "viewer", Onr("user", "alice")),
            Tuple("document", "doc3", "viewer", Onr("user", "bob")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        var found = await Collect(engine.LookupResources(reader, "user", "alice", CoreConstants.Ellipsis, "document", "view"));

        Assert.Equal(["doc1", "doc2"], found.Select(f => f.ResourceId).OrderBy(x => x).ToArray());
        Assert.All(found, f => Assert.Equal(Membership.Member, f.Membership));
        Assert.All(found, f => Assert.Contains("alice", f.ForSubjectIds));
    }

    [Fact]
    public async Task DirectSubjectMatch_Portion1()
    {
        // resourceType == subjectType and permission == subjectRelation: the subject is itself a resource.
        var (store, rev) = await Seed();
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        var found = await Collect(engine.LookupResources(reader, "user", "alice", CoreConstants.Ellipsis, "user", CoreConstants.Ellipsis));

        var single = Assert.Single(found);
        Assert.Equal("alice", single.ResourceId);
        Assert.Equal(Membership.Member, single.Membership);
    }

    [Fact]
    public async Task Arrow_FindsResourcesThroughParent()
    {
        var (store, rev) = await Seed(
            Tuple("document", "folder", "viewer", Onr("user", "alice")),
            Tuple("document", "doc1", "parent", Onr("document", "folder")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        // inherited_view = viewer + parent->view ; alice views folder, doc1.parent = folder.
        var found = await Collect(engine.LookupResources(reader, "user", "alice", CoreConstants.Ellipsis, "document", "inherited_view"));

        var ids = found.Select(f => f.ResourceId).OrderBy(x => x).ToArray();
        Assert.Contains("folder", ids); // direct viewer
        Assert.Contains("doc1", ids);   // via parent->view
    }

    [Fact]
    public async Task NestedGroupIndirection_FindsResourcesThroughGroupMembership()
    {
        var (store, rev) = await Seed(
            Tuple("group", "eng", "member", Onr("user", "alice")),
            Tuple("group", "all", "member", Onr("group", "eng", "member")),
            Tuple("document", "doc1", "viewer", Onr("group", "all", "member")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        // alice -> eng#member -> all#member -> doc1 viewer -> view.
        var found = await Collect(engine.LookupResources(reader, "user", "alice", CoreConstants.Ellipsis, "document", "view"));

        Assert.Equal(["doc1"], found.Select(f => f.ResourceId).ToArray());
        Assert.Equal(Membership.Member, Assert.Single(found).Membership);
    }

    [Fact]
    public async Task Wildcard_FindsResourcesForAnySubject()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "viewer", Onr("user", CoreConstants.PublicWildcard)),
            Tuple("document", "doc2", "viewer", Onr("user", "bob")));
        var engine = BuildEngine(WildcardSchema);
        var reader = store.SnapshotReader(rev);

        // alice is not directly a viewer of doc1, but user:* makes everyone a viewer.
        var found = await Collect(engine.LookupResources(reader, "user", "alice", CoreConstants.Ellipsis, "document", "view"));

        Assert.Equal(["doc1"], found.Select(f => f.ResourceId).ToArray());
    }

    [Fact]
    public async Task Exclusion_RemovesBannedResources()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "viewer", Onr("user", "alice")),
            Tuple("document", "doc2", "viewer", Onr("user", "alice")),
            Tuple("document", "doc2", "banned", Onr("user", "alice")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        // allowed_view = viewer - banned ; alice is banned on doc2.
        var found = await Collect(engine.LookupResources(reader, "user", "alice", CoreConstants.Ellipsis, "document", "allowed_view"));

        Assert.Equal(["doc1"], found.Select(f => f.ResourceId).ToArray());
    }

    [Fact]
    public async Task Intersection_KeepsOnlyResourcesWhereSubjectIsBoth()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "editor", Onr("user", "alice")),
            Tuple("document", "doc1", "viewer", Onr("user", "alice")),
            Tuple("document", "doc2", "editor", Onr("user", "alice")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        // edit_only = editor & viewer ; alice is both only on doc1.
        var found = await Collect(engine.LookupResources(reader, "user", "alice", CoreConstants.Ellipsis, "document", "edit_only"));

        Assert.Equal(["doc1"], found.Select(f => f.ResourceId).ToArray());
    }

    [Fact]
    public async Task Caveated_ResultCarriesMembershipAndMissingParams()
    {
        var (store, rev) = await Seed(
            Caveated("document", "doc1", "viewer", Onr("user", "alice"), "over_age",
                new Dictionary<string, object?> { ["min_age"] = 18 }));
        var engine = BuildEngine(CaveatSchema);
        var check = BuildCheckEngine(CaveatSchema);
        var reader = store.SnapshotReader(rev);

        // No context for `age`: the result is Caveated with `age` missing.
        var found = await Collect(engine.LookupResources(reader, "user", "alice", CoreConstants.Ellipsis, "document", "view"));
        var single = Assert.Single(found);
        Assert.Equal("doc1", single.ResourceId);
        Assert.Equal(Membership.Caveated, single.Membership);
        Assert.Contains("age", single.MissingContextParams);

        // With satisfying context -> the resource is found as a member (matching Check).
        var memberCtx = new Dictionary<string, object?> { ["age"] = 21 };
        var memberFound = await Collect(engine.LookupResources(reader, "user", "alice", CoreConstants.Ellipsis, "document", "view", memberCtx));
        Assert.Equal(Membership.Member, Assert.Single(memberFound).Membership);
        Assert.Equal(Membership.Member,
            (await check.Check(reader, "document", "doc1", "view", Onr("user", "alice"), memberCtx)).Verdict);

        // With failing context -> the resource is not found (matching Check).
        var failCtx = new Dictionary<string, object?> { ["age"] = 16 };
        var failFound = await Collect(engine.LookupResources(reader, "user", "alice", CoreConstants.Ellipsis, "document", "view", failCtx));
        Assert.Empty(failFound);
        Assert.Equal(Membership.NotMember,
            (await check.Check(reader, "document", "doc1", "view", Onr("user", "alice"), failCtx)).Verdict);
    }

    [Fact]
    public async Task NoReachablePath_ReturnsEmpty()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "viewer", Onr("user", "bob")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        var found = await Collect(engine.LookupResources(reader, "user", "alice", CoreConstants.Ellipsis, "document", "view"));

        Assert.Empty(found);
    }

    [Fact]
    public async Task Limit_StopsEnumeration()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "viewer", Onr("user", "alice")),
            Tuple("document", "doc2", "viewer", Onr("user", "alice")),
            Tuple("document", "doc3", "viewer", Onr("user", "alice")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        var found = await Collect(engine.LookupResources(
            reader, "user", "alice", CoreConstants.Ellipsis, "document", "view", limit: 2));

        Assert.Equal(2, found.Count);
    }

    [Fact]
    public async Task Cursor_ResumesAfterToken()
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "viewer", Onr("user", "alice")),
            Tuple("document", "doc2", "viewer", Onr("user", "alice")),
            Tuple("document", "doc3", "viewer", Onr("user", "alice")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        var first = await Collect(engine.LookupResources(
            reader, "user", "alice", CoreConstants.Ellipsis, "document", "view", limit: 1));
        var firstResource = Assert.Single(first);
        Assert.NotNull(firstResource.AfterCursor);

        var rest = await Collect(engine.LookupResources(
            reader, "user", "alice", CoreConstants.Ellipsis, "document", "view", cursor: firstResource.AfterCursor));

        // The first result is not repeated; together they cover the full set.
        Assert.DoesNotContain(rest, r => r.ResourceId == firstResource.ResourceId);
        var all = first.Concat(rest).Select(r => r.ResourceId).OrderBy(x => x).ToArray();
        Assert.Equal(["doc1", "doc2", "doc3"], all);
    }

    [Fact]
    public async Task MultiLevelCursor_PagedResumeEqualsUnpaged()
    {
        // A deep chain alice -> g1#member -> g2#member -> {g3a,g3b}#member -> document#viewer -> view.
        // Two sibling groups at the deepest level mean the viewer reverse-query yields docs in BySubject
        // order (by group, then doc id): doc_a, doc_z (g3a) then doc_b, doc_m (g3b) -- which is NOT sorted
        // by doc id. Resuming by a resource-id comparison would drop doc_b/doc_m (they sort before doc_z
        // already yielded); resuming by the datastore keyset, carried per nesting level, does not. Paging
        // one result at a time must reproduce the unpaged set exactly.
        var (store, rev) = await Seed(
            Tuple("group", "g1", "member", Onr("user", "alice")),
            Tuple("group", "g2", "member", Onr("group", "g1", "member")),
            Tuple("group", "g3a", "member", Onr("group", "g2", "member")),
            Tuple("group", "g3b", "member", Onr("group", "g2", "member")),
            Tuple("document", "doc_a", "viewer", Onr("group", "g3a", "member")),
            Tuple("document", "doc_z", "viewer", Onr("group", "g3a", "member")),
            Tuple("document", "doc_b", "viewer", Onr("group", "g3b", "member")),
            Tuple("document", "doc_m", "viewer", Onr("group", "g3b", "member")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        var unpaged = await Collect(engine.LookupResources(
            reader, "user", "alice", CoreConstants.Ellipsis, "document", "view"));
        var expected = unpaged.Select(r => r.ResourceId).OrderBy(x => x, StringComparer.Ordinal).ToArray();
        Assert.Equal(["doc_a", "doc_b", "doc_m", "doc_z"], expected);

        // The walk is deep enough that at least one result carries a multi-section (multi-level) cursor.
        Assert.Contains(unpaged, r => r.AfterCursor is { Sections.Count: > 1 });

        var paged = new List<string>();
        LookupResourcesCursor? cursor = null;
        while (paged.Count <= unpaged.Count) // bounded so a resume regression cannot loop forever
        {
            var page = await Collect(engine.LookupResources(
                reader, "user", "alice", CoreConstants.Ellipsis, "document", "view",
                cursor: cursor, limit: 1));
            if (page.Count == 0)
                break;
            paged.Add(page[0].ResourceId);
            cursor = page[0].AfterCursor;
        }

        // No drops and no cross-page duplicates: the paged multiset equals the unpaged one.
        Assert.Equal(expected, paged.OrderBy(x => x, StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public async Task Portion1SelfMatch_CarriesResumeCursor()
    {
        // A recursive relation (group.member admits group#member) makes a matched group both a
        // resource AND a subject of further resources. When the subject IS a group#member, Portion #1
        // self-matches the group(s) directly; each such result must carry a resume cursor so a limited
        // page resumes rather than silently truncating.
        var (store, rev) = await Seed(
            Tuple("group", "ga", "member", Onr("group", "subgrp", "member")),
            Tuple("group", "gb", "member", Onr("group", "subgrp", "member")),
            Tuple("group", "gc", "member", Onr("group", "subgrp", "member")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        // subgrp#member is a member of ga, gb, gc (Portion #2) and also self-matches (Portion #1).
        var first = await Collect(engine.LookupResources(
            reader, "group", "subgrp", "member", "group", "member", limit: 1));
        var firstResource = Assert.Single(first);
        Assert.NotNull(firstResource.AfterCursor); // the Portion #1 result is decorated.

        var rest = await Collect(engine.LookupResources(
            reader, "group", "subgrp", "member", "group", "member", cursor: firstResource.AfterCursor));

        Assert.DoesNotContain(rest, r => r.ResourceId == firstResource.ResourceId);
        var all = first.Concat(rest).Select(r => r.ResourceId).Distinct().OrderBy(x => x).ToArray();
        Assert.Equal(["ga", "gb", "gc", "subgrp"], all);
    }

    [Fact]
    public async Task CyclicSchema_Terminates()
    {
        var (store, rev) = await Seed(
            Tuple("group", "a", "member", Onr("group", "b", "member")),
            Tuple("group", "b", "member", Onr("group", "a", "member")));
        var engine = BuildEngine(Schema);
        var reader = store.SnapshotReader(rev);

        // No user reaches anything; must terminate.
        var found = await Collect(engine.LookupResources(reader, "user", "alice", CoreConstants.Ellipsis, "document", "view"));

        Assert.Empty(found);
    }

    [Theory]
    [InlineData("view")]
    [InlineData("edit_only")]
    [InlineData("allowed_view")]
    [InlineData("inherited_view")]
    public async Task LookupResources_AgreesWithCheckOracle(string permission)
    {
        var (store, rev) = await Seed(
            Tuple("document", "doc1", "parent", Onr("document", "folder")),
            Tuple("document", "folder", "viewer", Onr("user", "alice")),
            Tuple("document", "doc1", "viewer", Onr("user", "alice")),
            Tuple("document", "doc1", "viewer", Onr("user", "bob")),
            Tuple("document", "doc1", "editor", Onr("user", "alice")),
            Tuple("document", "doc2", "editor", Onr("user", "alice")),
            Tuple("document", "doc2", "viewer", Onr("user", "alice")),
            Tuple("document", "doc1", "banned", Onr("user", "alice")));
        var engine = BuildEngine(Schema);
        var check = BuildCheckEngine(Schema);
        var reader = store.SnapshotReader(rev);

        var found = await Collect(engine.LookupResources(reader, "user", "alice", CoreConstants.Ellipsis, "document", permission));
        var foundIds = found.Select(f => f.ResourceId).ToHashSet();

        // Soundness + completeness against the Check oracle across the resource universe.
        string[] universe = ["doc1", "doc2", "folder", "doc3"];
        foreach (var resourceId in universe)
        {
            var verdict = (await check.Check(reader, "document", resourceId, permission, Onr("user", "alice"))).Verdict;
            Assert.Equal(verdict == Membership.Member, foundIds.Contains(resourceId));
        }
    }
}
