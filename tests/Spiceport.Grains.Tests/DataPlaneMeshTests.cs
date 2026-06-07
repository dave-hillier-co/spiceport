using Spiceport.Core;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Exercises the dynamic-schema and relationship-write data plane THROUGH the real grain mesh
/// (in-process Orleans TestingHost). Clusters start seeded from an initial schema; tests then call the
/// data-plane <see cref="IRelationshipsGrain"/> on the running cluster (WriteSchema / WriteRelationships
/// / Read / Delete) and assert that checks reflect the writes — including that a schema swap correctly
/// changes a Check outcome despite the dispatch cache (the schema hash flows into every cache/grain key).
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class DataPlaneMeshTests
{
    private const string EmptySchema = "definition user {}";

    private const string ViewerOnlySchema = """
        definition user {}

        definition document {
            relation viewer: user
            relation editor: user
            permission view = viewer
        }
        """;

    private const string ViewerOrEditorSchema = """
        definition user {}

        definition document {
            relation viewer: user
            relation editor: user
            permission view = viewer + editor
        }
        """;

    private static RelationshipUpdateWire Touch(string res, string rel, string subj) =>
        new(RelationshipUpdateOpWire.Touch,
            new RelationshipWire("document", res, rel, "user", subj, CoreConstants.Ellipsis, null, null, null));

    private static ObjectAndRelation User(string id) => new("user", id, CoreConstants.Ellipsis);

    [Fact]
    public async Task WriteSchema_then_WriteRelationships_then_Check_reflects_write()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);

        var schemaReply = await cluster.WriteSchema(ViewerOrEditorSchema);
        Assert.False(string.IsNullOrEmpty(schemaReply.WrittenAtToken));

        var writeReply = await cluster.Relationships.WriteRelationships(
            new WriteRelationshipsArgs(new List<RelationshipUpdateWire> { Touch("readme", "viewer", "alice") }));
        Assert.False(string.IsNullOrEmpty(writeReply.WrittenAtToken));

        var result = await cluster.Checker.Check("document", "readme", "view", User("alice"), null);
        Assert.Equal(Membership.Member, result.Verdict);

        // The token is an opaque, base64-encoded ZedToken payload.
        var bytes = Convert.FromBase64String(writeReply.WrittenAtToken);
        Assert.NotEmpty(bytes);
    }

    [Fact]
    public async Task ReadSchema_returns_current_source_text()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        await cluster.WriteSchema(ViewerOrEditorSchema);

        var reply = await cluster.Relationships.ReadSchema();
        Assert.Equal(ViewerOrEditorSchema, reply.SchemaText);
    }

    [Fact]
    public async Task ReadRelationships_pages_with_cursor()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerOrEditorSchema);
        await cluster.Relationships.WriteRelationships(new WriteRelationshipsArgs(
            new List<RelationshipUpdateWire>
            {
                Touch("a", "viewer", "alice"),
                Touch("b", "viewer", "alice"),
                Touch("c", "viewer", "alice"),
            }));

        var filter = new RelationshipsFilterWire("document", null, null, "viewer", null, null, null);

        var first = await cluster.Relationships.ReadRelationships(new ReadRelationshipsArgs(filter, 2, null));
        Assert.Equal(2, first.Relationships.Count);
        Assert.False(string.IsNullOrEmpty(first.Cursor));

        var second = await cluster.Relationships.ReadRelationships(new ReadRelationshipsArgs(filter, 2, first.Cursor));
        Assert.Single(second.Relationships);
        Assert.Null(second.Cursor);

        var all = first.Relationships.Concat(second.Relationships).Select(r => r.ResourceId).ToHashSet();
        Assert.Equal(new HashSet<string> { "a", "b", "c" }, all);
    }

    [Fact]
    public async Task DeleteRelationships_by_filter_removes_them()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerOrEditorSchema);
        await cluster.Relationships.WriteRelationships(new WriteRelationshipsArgs(
            new List<RelationshipUpdateWire>
            {
                Touch("readme", "viewer", "alice"),
                Touch("readme", "viewer", "bob"),
            }));

        var readFilter = new RelationshipsFilterWire("document", null, null, "viewer", null, null, null);
        var beforeRead = await cluster.Relationships.ReadRelationships(new ReadRelationshipsArgs(readFilter, null, null));
        Assert.Equal(2, beforeRead.Relationships.Count);

        var delFilter = new RelationshipsFilterWire(
            "document", null, new List<string> { "readme" }, "viewer", null, null, null);
        var delReply = await cluster.Relationships.DeleteRelationships(new DeleteRelationshipsArgs(delFilter, null));
        Assert.Equal(2UL, delReply.DeletedCount);

        // Read FullyConsistent to observe the delete immediately: a default (minimize-latency) read
        // resolves to the optimized revision, a real-but-cached head held stable for the quantization
        // window (matching SpiceDB's CachedOptimizedRevisions / memdb floored bucket), so a write that
        // lands mid-window is not yet reflected. To see one's own just-committed mutation deterministically
        // a caller asks for full consistency (or reads at the mutation's returned token).
        var afterRead = await cluster.Relationships.ReadRelationships(
            new ReadRelationshipsArgs(readFilter, null, null, ConsistencyWire.FullyConsistent));
        Assert.Empty(afterRead.Relationships);
    }

    [Fact]
    public async Task SchemaChange_flips_Check_outcome_across_cache()
    {
        // Schema A: view = viewer. Bob is editor (NOT viewer) -> NO permission (this caches under hash(A)).
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerOnlySchema);
        await cluster.Relationships.WriteRelationships(
            new WriteRelationshipsArgs(new List<RelationshipUpdateWire> { Touch("readme", "editor", "bob") }));

        var hashA = cluster.SchemaProvider.Current.SchemaHash;
        var before = await cluster.Checker.Check("document", "readme", "view", User("bob"), null);
        Assert.Equal(Membership.NotMember, before.Verdict);

        // Schema B: view = viewer + editor. Swap the live schema -> new hash.
        await cluster.WriteSchema(ViewerOrEditorSchema);
        var hashB = cluster.SchemaProvider.Current.SchemaHash;
        Assert.NotEqual(hashA, hashB);

        // Same check must now be HAS_PERMISSION: the stale hash(A) NO entry can never be returned
        // because every new cache/grain key is prefixed with the current hash(B).
        var after = await cluster.Checker.Check("document", "readme", "view", User("bob"), null);
        Assert.Equal(Membership.Member, after.Verdict);
    }

    [Fact]
    public async Task SchemaChange_reverse_direction_flips_Member_to_NotMember()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerOrEditorSchema);
        await cluster.Relationships.WriteRelationships(
            new WriteRelationshipsArgs(new List<RelationshipUpdateWire> { Touch("readme", "editor", "bob") }));

        var before = await cluster.Checker.Check("document", "readme", "view", User("bob"), null);
        Assert.Equal(Membership.Member, before.Verdict);

        await cluster.WriteSchema(ViewerOnlySchema);

        var after = await cluster.Checker.Check("document", "readme", "view", User("bob"), null);
        Assert.Equal(Membership.NotMember, after.Verdict);
    }

    [Fact]
    public async Task WriteSchema_invalid_throws_and_leaves_current_unchanged()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerOrEditorSchema);
        var hashBefore = cluster.SchemaProvider.Current.SchemaHash;

        // The grain surfaces a compile failure as a serializable ArgumentException across the boundary.
        await Assert.ThrowsAsync<ArgumentException>(
            () => cluster.WriteSchema("definition document { relation"));

        Assert.Equal(hashBefore, cluster.SchemaProvider.Current.SchemaHash);
    }

    // ---- WriteSchema change validation, exercised directly over the grain mesh ----
    //
    // These complement WriteSafetyGrpcServiceTests (which drive the same paths through the gRPC
    // service) by asserting, at the grain boundary, that a rejected schema change surfaces the
    // serializable SchemaWriteValidationException AND leaves the LIVE schema snapshot intact (the
    // datastore-persist and the in-memory swap are both abandoned).

    private const string SubjectRefSchema = """
        definition user {}

        definition group {
            relation member: user
        }

        definition document {
            relation viewer: user | group#member
            relation editor: user
            permission view = viewer + editor
        }
        """;

    private static RelationshipUpdateWire Member(string group, string user) =>
        new(RelationshipUpdateOpWire.Touch,
            new RelationshipWire("group", group, "member", "user", user, CoreConstants.Ellipsis, null, null, null));

    private static RelationshipUpdateWire ViewerGroup(string doc, string group) =>
        new(RelationshipUpdateOpWire.Touch,
            new RelationshipWire("document", doc, "viewer", "group", group, "member", null, null, null));

    [Fact]
    public async Task WriteSchema_removing_referenced_definition_is_rejected_and_live_schema_unchanged()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerOrEditorSchema);
        await cluster.Relationships.WriteRelationships(
            new WriteRelationshipsArgs(new List<RelationshipUpdateWire> { Touch("readme", "viewer", "alice") }));

        var hashBefore = cluster.SchemaProvider.Current.SchemaHash;

        // Drop `document` entirely while a relationship is written under it.
        var ex = await Assert.ThrowsAsync<SchemaWriteValidationException>(
            () => cluster.WriteSchema(EmptySchema));
        Assert.Contains("document", ex.Message);

        // Live schema snapshot untouched, and the check still resolves against the old schema.
        Assert.Equal(hashBefore, cluster.SchemaProvider.Current.SchemaHash);
        var check = await cluster.Checker.Check("document", "readme", "view", User("alice"), null);
        Assert.Equal(Membership.Member, check.Verdict);
    }

    [Fact]
    public async Task WriteSchema_removing_referenced_relation_is_rejected_and_live_schema_unchanged()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerOrEditorSchema);
        await cluster.Relationships.WriteRelationships(
            new WriteRelationshipsArgs(new List<RelationshipUpdateWire> { Touch("readme", "viewer", "alice") }));

        var hashBefore = cluster.SchemaProvider.Current.SchemaHash;

        // Drop the `viewer` relation while it is still written.
        const string withoutViewer = """
            definition user {}

            definition document {
                relation editor: user
                permission view = editor
            }
            """;

        var ex = await Assert.ThrowsAsync<SchemaWriteValidationException>(
            () => cluster.WriteSchema(withoutViewer));
        Assert.Contains("viewer", ex.Message);

        Assert.Equal(hashBefore, cluster.SchemaProvider.Current.SchemaHash);
    }

    [Fact]
    public async Task WriteSchema_removing_relation_referenced_as_subject_is_rejected_and_live_schema_unchanged()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(SubjectRefSchema);
        await cluster.Relationships.WriteRelationships(new WriteRelationshipsArgs(
            new List<RelationshipUpdateWire> { ViewerGroup("readme", "eng") }));

        var hashBefore = cluster.SchemaProvider.Current.SchemaHash;

        // Remove group#member: it is referenced on the SUBJECT side of document#viewer.
        const string withoutGroupMember = """
            definition user {}

            definition group {}

            definition document {
                relation viewer: user | group#member
                relation editor: user
                permission view = viewer + editor
            }
            """;

        var ex = await Assert.ThrowsAsync<SchemaWriteValidationException>(
            () => cluster.WriteSchema(withoutGroupMember));
        Assert.Contains("member", ex.Message);

        Assert.Equal(hashBefore, cluster.SchemaProvider.Current.SchemaHash);
    }

    [Fact]
    public async Task WriteSchema_removing_referenced_allowed_subject_type_is_rejected_and_live_schema_unchanged()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(SubjectRefSchema);
        await cluster.Relationships.WriteRelationships(
            new WriteRelationshipsArgs(new List<RelationshipUpdateWire> { Touch("readme", "viewer", "alice") }));

        var hashBefore = cluster.SchemaProvider.Current.SchemaHash;

        // Keep `viewer` but drop `user` from its allowed subject types while a user subject is written.
        const string viewerWithoutUser = """
            definition user {}

            definition group {
                relation member: user
            }

            definition document {
                relation viewer: group#member
                relation editor: user
                permission view = viewer + editor
            }
            """;

        await Assert.ThrowsAsync<SchemaWriteValidationException>(
            () => cluster.WriteSchema(viewerWithoutUser));

        Assert.Equal(hashBefore, cluster.SchemaProvider.Current.SchemaHash);
    }

    [Fact]
    public async Task WriteSchema_removing_unreferenced_definition_is_allowed()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(SubjectRefSchema);

        // Only document relationships exist; `group` carries none, so dropping it is safe.
        await cluster.Relationships.WriteRelationships(
            new WriteRelationshipsArgs(new List<RelationshipUpdateWire> { Touch("readme", "viewer", "alice") }));

        const string withoutGroup = """
            definition user {}

            definition document {
                relation viewer: user
                relation editor: user
                permission view = viewer + editor
            }
            """;

        var reply = await cluster.WriteSchema(withoutGroup);
        Assert.False(string.IsNullOrEmpty(reply.WrittenAtToken));
        Assert.Equal(withoutGroup, cluster.SchemaProvider.Current.SourceText);
    }

    [Fact]
    public async Task WriteSchema_removing_unreferenced_relation_is_allowed()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerOrEditorSchema);

        // Only viewer is written; editor carries no relationships, so dropping editor is safe.
        await cluster.Relationships.WriteRelationships(
            new WriteRelationshipsArgs(new List<RelationshipUpdateWire> { Touch("readme", "viewer", "alice") }));

        const string withoutEditor = """
            definition user {}

            definition document {
                relation viewer: user
                permission view = viewer
            }
            """;

        var reply = await cluster.WriteSchema(withoutEditor);
        Assert.False(string.IsNullOrEmpty(reply.WrittenAtToken));
        Assert.Equal(withoutEditor, cluster.SchemaProvider.Current.SourceText);
    }

    [Fact]
    public async Task WriteSchema_permission_only_change_is_always_allowed()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerOnlySchema);
        await cluster.Relationships.WriteRelationships(
            new WriteRelationshipsArgs(new List<RelationshipUpdateWire> { Touch("readme", "viewer", "alice") }));

        // Relations untouched; only the permission expression changes -> always safe.
        var reply = await cluster.WriteSchema(ViewerOrEditorSchema);
        Assert.False(string.IsNullOrEmpty(reply.WrittenAtToken));
        Assert.Equal(ViewerOrEditorSchema, cluster.SchemaProvider.Current.SourceText);
    }
}
