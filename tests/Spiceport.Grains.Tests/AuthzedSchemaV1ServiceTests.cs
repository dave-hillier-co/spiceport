using Grpc.Core;
using Spiceport.Api;
using V1 = Authzed.Api.V1;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Drives the <c>authzed.api.v1</c> <see cref="AuthzedSchemaV1Service"/> IN-PROCESS over the in-process
/// cluster's grain mesh. Verifies: write-then-read round-trips the schema text; invalid schema text maps to
/// INVALID_ARGUMENT; an orphaning change maps to FAILED_PRECONDITION; reading a never-written schema maps
/// to NOT_FOUND.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class AuthzedSchemaV1ServiceTests
{
    private const string Schema = """
        definition user {}
        definition document {
            relation viewer: user
            permission view = viewer
        }
        """;

    // A cluster needs a compiled schema to start, so the "no schema" case starts from an empty schema text.
    private const string EmptySchema = "";

    private static AuthzedSchemaV1Service Service(MeshTestCluster cluster) =>
        new(cluster.GrainFactory, cluster.SchemaProvider);

    [Fact]
    public async Task WriteSchema_then_ReadSchema_round_trips_text()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);

        await service.WriteSchema(new V1::WriteSchemaRequest { Schema = Schema }, FakeServerCallContext.Default);

        var read = await service.ReadSchema(new V1::ReadSchemaRequest(), FakeServerCallContext.Default);
        Assert.Contains("definition document", read.SchemaText);
        Assert.Contains("permission view", read.SchemaText);
    }

    [Fact]
    public async Task WriteSchema_invalid_text_is_invalid_argument()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);

        var ex = await Assert.ThrowsAsync<RpcException>(() => service.WriteSchema(
            new V1::WriteSchemaRequest { Schema = "this is not a valid schema {{{" },
            FakeServerCallContext.Default));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
    }

    [Fact]
    public async Task WriteSchema_orphaning_change_is_failed_precondition()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);

        await service.WriteSchema(new V1::WriteSchemaRequest { Schema = Schema }, FakeServerCallContext.Default);

        // Write a viewer relationship, then attempt to drop the viewer relation that backs it.
        await cluster.Relationships.WriteRelationships(new Abstractions.WriteRelationshipsArgs(
            new List<Abstractions.RelationshipUpdateWire>
            {
                new(Abstractions.RelationshipUpdateOpWire.Touch,
                    new Abstractions.RelationshipWire(
                        "document", "readme", "viewer", "user", "alice", "...", null, null, null)),
            }));

        var orphaning = """
            definition user {}
            definition document {
                permission view = nil
            }
            """;

        var ex = await Assert.ThrowsAsync<RpcException>(() => service.WriteSchema(
            new V1::WriteSchemaRequest { Schema = orphaning }, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
    }

    [Fact]
    public async Task WriteSchema_with_type_error_is_failed_precondition()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);

        // A permission referencing an undefined relation: rejected at write time as a type error.
        var bad = """
            definition user {}
            definition document {
                permission view = nonexistent
            }
            """;

        var ex = await Assert.ThrowsAsync<RpcException>(() => service.WriteSchema(
            new V1::WriteSchemaRequest { Schema = bad }, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
    }

    [Fact]
    public async Task WriteSchema_removing_caveat_parameter_is_failed_precondition()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);

        var withTwo = """
            definition user {}
            definition document {
                relation viewer: user with c
            }
            caveat c(a int, b int) { a > 0 && b > 0 }
            """;
        await service.WriteSchema(new V1::WriteSchemaRequest { Schema = withTwo }, FakeServerCallContext.Default);

        // Removing parameter `b` is rejected unconditionally (existing relationships may be typed by it).
        var withOne = """
            definition user {}
            definition document {
                relation viewer: user with c
            }
            caveat c(a int) { a > 0 }
            """;
        var ex = await Assert.ThrowsAsync<RpcException>(() => service.WriteSchema(
            new V1::WriteSchemaRequest { Schema = withOne }, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
    }

    [Fact]
    public async Task WriteSchema_removing_expiration_trait_with_expiring_relationship_is_failed_precondition()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);

        // `user with expiration` requires the expiration trait. The trait is part of the allowed-type
        // identity, so dropping it is a RelationSubjectTypeRemoved delta that the orphan check inspects.
        var withExpiration = """
            use expiration

            definition user {}
            definition document {
                relation viewer: user with expiration
            }
            """;
        await service.WriteSchema(
            new V1::WriteSchemaRequest { Schema = withExpiration }, FakeServerCallContext.Default);

        // A relationship that actually carries an expiration: it references `user with expiration`.
        await cluster.Relationships.WriteRelationships(new Abstractions.WriteRelationshipsArgs(
            new List<Abstractions.RelationshipUpdateWire>
            {
                new(Abstractions.RelationshipUpdateOpWire.Touch,
                    new Abstractions.RelationshipWire(
                        "document", "readme", "viewer", "user", "alice", "...", null, null,
                        DateTimeOffset.UtcNow.AddDays(7))),
            }));

        // Dropping the expiration trait orphans that relationship, so the change is rejected.
        var withoutExpiration = """
            definition user {}
            definition document {
                relation viewer: user
            }
            """;
        var ex = await Assert.ThrowsAsync<RpcException>(() => service.WriteSchema(
            new V1::WriteSchemaRequest { Schema = withoutExpiration }, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
    }

    [Fact]
    public async Task WriteSchema_removing_expiration_trait_with_non_expiring_relationship_is_allowed()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);

        // The relation allows BOTH an expiring and a non-expiring `user` subject, so a relationship
        // without an expiration references the plain `user` allowed type, not `user with expiration`.
        var withBoth = """
            use expiration

            definition user {}
            definition document {
                relation viewer: user | user with expiration
            }
            """;
        await service.WriteSchema(
            new V1::WriteSchemaRequest { Schema = withBoth }, FakeServerCallContext.Default);

        await cluster.Relationships.WriteRelationships(new Abstractions.WriteRelationshipsArgs(
            new List<Abstractions.RelationshipUpdateWire>
            {
                new(Abstractions.RelationshipUpdateOpWire.Touch,
                    new Abstractions.RelationshipWire(
                        "document", "readme", "viewer", "user", "alice", "...", null, null, null)),
            }));

        // Dropping only the `with expiration` allowed type: the non-expiring relationship is filtered
        // out by the orphan check's expiration filter, so the change is accepted.
        var withoutExpiration = """
            definition user {}
            definition document {
                relation viewer: user
            }
            """;
        await service.WriteSchema(
            new V1::WriteSchemaRequest { Schema = withoutExpiration }, FakeServerCallContext.Default);

        var read = await service.ReadSchema(new V1::ReadSchemaRequest(), FakeServerCallContext.Default);
        Assert.Contains("relation viewer: user", read.SchemaText);
    }

    [Fact]
    public async Task ReadSchema_on_fresh_cluster_is_not_found()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            service.ReadSchema(new V1::ReadSchemaRequest(), FakeServerCallContext.Default));
        Assert.Equal(StatusCode.NotFound, ex.StatusCode);
    }

    private const string ReflectSchema = """
        definition user {}
        caveat ip_match(allowed string, user_ip string) {
            user_ip == allowed
        }
        definition document {
            relation viewer: user
            relation editor: user
            permission view = viewer + editor
        }
        """;

    [Fact]
    public async Task ReflectSchema_returns_definitions_relations_permissions_and_caveats()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);
        await service.WriteSchema(new V1::WriteSchemaRequest { Schema = ReflectSchema }, FakeServerCallContext.Default);

        var resp = await service.ReflectSchema(new V1::ReflectSchemaRequest(), FakeServerCallContext.Default);

        Assert.Equal(2, resp.Definitions.Count);
        Assert.Contains(resp.Definitions, d => d.Name == "user");
        var doc = Assert.Single(resp.Definitions, d => d.Name == "document");

        Assert.Equal(2, doc.Relations.Count);
        var viewer = Assert.Single(doc.Relations, r => r.Name == "viewer");
        var subject = Assert.Single(viewer.SubjectTypes);
        Assert.Equal("user", subject.SubjectDefinitionName);
        Assert.True(subject.IsTerminalSubject);

        var view = Assert.Single(doc.Permissions);
        Assert.Equal("view", view.Name);

        var caveat = Assert.Single(resp.Caveats);
        Assert.Equal("ip_match", caveat.Name);
        Assert.Equal(2, caveat.Parameters.Count);
        Assert.Contains(caveat.Parameters, p => p.Name == "allowed" && p.Type == "string");
        Assert.Contains("user_ip == allowed", caveat.Expression);
    }

    [Fact]
    public async Task ReflectSchema_definition_name_filter_returns_only_matching_definitions()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);
        await service.WriteSchema(new V1::WriteSchemaRequest { Schema = ReflectSchema }, FakeServerCallContext.Default);

        var req = new V1::ReflectSchemaRequest();
        req.OptionalFilters.Add(new V1::ReflectionSchemaFilter { OptionalDefinitionNameFilter = "doc" });

        var resp = await service.ReflectSchema(req, FakeServerCallContext.Default);

        var def = Assert.Single(resp.Definitions);
        Assert.Equal("document", def.Name);
        Assert.Empty(resp.Caveats);
    }

    [Fact]
    public async Task ReflectSchema_mutually_exclusive_filter_is_invalid_argument()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);
        await service.WriteSchema(new V1::WriteSchemaRequest { Schema = ReflectSchema }, FakeServerCallContext.Default);

        var req = new V1::ReflectSchemaRequest();
        req.OptionalFilters.Add(new V1::ReflectionSchemaFilter
        {
            OptionalDefinitionNameFilter = "document",
            OptionalCaveatNameFilter = "ip",
        });

        var ex = await Assert.ThrowsAsync<RpcException>(() => service.ReflectSchema(req, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
    }

    [Fact]
    public async Task DiffSchema_reports_expected_oneof_cases()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);

        var baseSchema = """
            definition user {}
            definition group {}
            definition document {
                relation viewer: user
                relation editor: user
                permission view = viewer
                permission manage = editor
            }
            """;
        await service.WriteSchema(new V1::WriteSchemaRequest { Schema = baseSchema }, FakeServerCallContext.Default);

        var comparison = """
            definition user {}
            definition group {}
            definition folder {}
            definition document {
                relation viewer: user | group
                relation editor: user
                relation owner: user
                permission view = viewer + editor
            }
            """;

        var resp = await service.DiffSchema(
            new V1::DiffSchemaRequest { ComparisonSchema = comparison }, FakeServerCallContext.Default);

        Assert.Contains(resp.Diffs, d => d.DiffCase == V1::ReflectionSchemaDiff.DiffOneofCase.DefinitionAdded
            && d.DefinitionAdded.Name == "folder");
        Assert.Contains(resp.Diffs, d => d.DiffCase == V1::ReflectionSchemaDiff.DiffOneofCase.RelationAdded
            && d.RelationAdded.Name == "owner");
        Assert.Contains(resp.Diffs, d => d.DiffCase == V1::ReflectionSchemaDiff.DiffOneofCase.PermissionRemoved
            && d.PermissionRemoved.Name == "manage");
        Assert.Contains(resp.Diffs, d => d.DiffCase == V1::ReflectionSchemaDiff.DiffOneofCase.RelationSubjectTypeAdded
            && d.RelationSubjectTypeAdded.Relation.Name == "viewer"
            && d.RelationSubjectTypeAdded.ChangedSubjectType.SubjectDefinitionName == "group");
        Assert.Contains(resp.Diffs, d => d.DiffCase == V1::ReflectionSchemaDiff.DiffOneofCase.PermissionExprChanged
            && d.PermissionExprChanged.Name == "view");
    }

    [Fact]
    public async Task DiffSchema_adding_expiration_trait_reports_subject_type_add_and_remove()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);

        // The expiration trait is part of the allowed-type identity, so `user` -> `user with expiration`
        // is a genuine subject-type change: the old `user` is removed and `user with expiration` is added.
        var baseSchema = """
            use expiration

            definition user {}
            definition document {
                relation viewer: user
            }
            """;
        await service.WriteSchema(new V1::WriteSchemaRequest { Schema = baseSchema }, FakeServerCallContext.Default);

        var comparison = """
            use expiration

            definition user {}
            definition document {
                relation viewer: user with expiration
            }
            """;

        var resp = await service.DiffSchema(
            new V1::DiffSchemaRequest { ComparisonSchema = comparison }, FakeServerCallContext.Default);

        Assert.Contains(resp.Diffs, d => d.DiffCase == V1::ReflectionSchemaDiff.DiffOneofCase.RelationSubjectTypeRemoved
            && d.RelationSubjectTypeRemoved.Relation.Name == "viewer"
            && d.RelationSubjectTypeRemoved.ChangedSubjectType.SubjectDefinitionName == "user");
        Assert.Contains(resp.Diffs, d => d.DiffCase == V1::ReflectionSchemaDiff.DiffOneofCase.RelationSubjectTypeAdded
            && d.RelationSubjectTypeAdded.Relation.Name == "viewer"
            && d.RelationSubjectTypeAdded.ChangedSubjectType.SubjectDefinitionName == "user");
    }

    [Fact]
    public async Task DiffSchema_identical_schema_yields_no_diffs()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);
        await service.WriteSchema(new V1::WriteSchemaRequest { Schema = ReflectSchema }, FakeServerCallContext.Default);

        var resp = await service.DiffSchema(
            new V1::DiffSchemaRequest { ComparisonSchema = ReflectSchema }, FakeServerCallContext.Default);

        Assert.Empty(resp.Diffs);
    }

    [Fact]
    public async Task DiffSchema_invalid_comparison_is_invalid_argument()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);
        await service.WriteSchema(new V1::WriteSchemaRequest { Schema = ReflectSchema }, FakeServerCallContext.Default);

        var ex = await Assert.ThrowsAsync<RpcException>(() => service.DiffSchema(
            new V1::DiffSchemaRequest { ComparisonSchema = "definition {{{ bad" }, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
    }

    private const string ComputableSchema = """
        definition user {}
        definition document {
            relation viewer: user
            relation editor: user
            permission view = viewer + editor
            permission manage = editor
        }
        """;

    [Fact]
    public async Task ComputablePermissions_returns_permissions_reachable_from_a_relation()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);
        await service.WriteSchema(new V1::WriteSchemaRequest { Schema = ComputableSchema }, FakeServerCallContext.Default);

        var viewer = await service.ComputablePermissions(
            new V1::ComputablePermissionsRequest { DefinitionName = "document", RelationName = "viewer" },
            FakeServerCallContext.Default);
        Assert.Equal(["view"], viewer.Permissions.Select(p => p.RelationName).ToArray());
        Assert.All(viewer.Permissions, p => Assert.True(p.IsPermission));

        var editor = await service.ComputablePermissions(
            new V1::ComputablePermissionsRequest { DefinitionName = "document", RelationName = "editor" },
            FakeServerCallContext.Default);
        Assert.Equal(["manage", "view"], editor.Permissions.Select(p => p.RelationName).ToArray());
    }

    [Fact]
    public async Task ComputablePermissions_unknown_relation_is_failed_precondition()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);
        await service.WriteSchema(new V1::WriteSchemaRequest { Schema = ComputableSchema }, FakeServerCallContext.Default);

        var ex = await Assert.ThrowsAsync<RpcException>(() => service.ComputablePermissions(
            new V1::ComputablePermissionsRequest { DefinitionName = "document", RelationName = "nope" },
            FakeServerCallContext.Default));
        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
    }

    [Fact]
    public async Task DependentRelations_returns_relations_a_permission_depends_on()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);
        await service.WriteSchema(new V1::WriteSchemaRequest { Schema = ComputableSchema }, FakeServerCallContext.Default);

        var resp = await service.DependentRelations(
            new V1::DependentRelationsRequest { DefinitionName = "document", PermissionName = "view" },
            FakeServerCallContext.Default);

        Assert.Equal(["editor", "viewer"], resp.Relations.Select(r => r.RelationName).OrderBy(x => x).ToArray());
        Assert.All(resp.Relations, r => Assert.False(r.IsPermission));
    }

    [Fact]
    public async Task DependentRelations_through_arrow_includes_tupleset_and_parent_permission()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);

        var arrowSchema = """
            definition user {}
            definition folder {
                relation viewer: user
                permission view = viewer
            }
            definition document {
                relation viewer: user
                relation parent: folder
                permission view = viewer + parent->view
            }
            """;
        await service.WriteSchema(new V1::WriteSchemaRequest { Schema = arrowSchema }, FakeServerCallContext.Default);

        var resp = await service.DependentRelations(
            new V1::DependentRelationsRequest { DefinitionName = "document", PermissionName = "view" },
            FakeServerCallContext.Default);

        Assert.Contains(resp.Relations, r => r.DefinitionName == "document" && r.RelationName == "parent");
        Assert.Contains(resp.Relations, r => r.DefinitionName == "folder" && r.RelationName == "view");
        Assert.Contains(resp.Relations, r => r.DefinitionName == "folder" && r.RelationName == "viewer");
    }

    [Fact]
    public async Task DependentRelations_on_base_relation_is_invalid_argument()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);
        await service.WriteSchema(new V1::WriteSchemaRequest { Schema = ComputableSchema }, FakeServerCallContext.Default);

        var ex = await Assert.ThrowsAsync<RpcException>(() => service.DependentRelations(
            new V1::DependentRelationsRequest { DefinitionName = "document", PermissionName = "viewer" },
            FakeServerCallContext.Default));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
    }

    [Fact]
    public async Task DependentRelations_unknown_definition_is_not_found()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(EmptySchema);
        var service = Service(cluster);
        await service.WriteSchema(new V1::WriteSchemaRequest { Schema = ComputableSchema }, FakeServerCallContext.Default);

        var ex = await Assert.ThrowsAsync<RpcException>(() => service.DependentRelations(
            new V1::DependentRelationsRequest { DefinitionName = "missing", PermissionName = "view" },
            FakeServerCallContext.Default));
        Assert.Equal(StatusCode.NotFound, ex.StatusCode);
    }

    private sealed class FakeServerCallContext(CancellationToken cancellationToken) : ServerCallContext
    {
        public static FakeServerCallContext Default { get; } = new(CancellationToken.None);

        protected override string MethodCore => string.Empty;
        protected override string HostCore => string.Empty;
        protected override string PeerCore => string.Empty;
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override Metadata RequestHeadersCore => [];
        protected override CancellationToken CancellationTokenCore => cancellationToken;
        protected override Metadata ResponseTrailersCore => [];
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => new(string.Empty, new Dictionary<string, List<AuthProperty>>());

        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) =>
            throw new NotSupportedException();

        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => Task.CompletedTask;
    }
}
