using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Spiceport.Api;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains;
using Spiceport.Grains.Abstractions;
using V1 = Authzed.Api.V1;
using CoreRelationship = Spiceport.Core.Relationship;
using CoreRelationshipUpdate = Spiceport.Core.RelationshipUpdate;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Drives the <c>authzed.api.v1</c> <see cref="AuthzedPermissionsV1Service"/> IN-PROCESS (no Kestrel):
/// the service is instantiated with the in-process cluster's <see cref="IPermissionChecker"/> and grain
/// factory; server-streaming RPCs are drained through a fake <see cref="IServerStreamWriter{T}"/>. Verifies
/// the v1 proto &lt;-&gt; grain mapping: permissionship + partial-caveat-info, preconditions, the nested v1
/// relationship filter, internal cursor paging, and the modern + deprecated lookup-subject fields.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class AuthzedPermissionsV1ServiceTests
{
    private const string Schema = """
        definition user {}

        caveat over_limit(limit int, requested int) {
            requested <= limit
        }

        definition document {
            relation viewer: user | user with over_limit
            relation editor: user
            permission view = viewer + editor
        }
        """;

    private static AuthzedPermissionsV1Service Service(MeshTestCluster cluster) =>
        new(cluster.Services.GetRequiredService<IPermissionChecker>(), cluster.GrainFactory,
            cluster.ReverseOps, cluster.RelationshipReads,
            cluster.Services.GetRequiredService<ISchemaProvider>());

    private static async Task SeedAsync(IDatastore datastore, params (string res, string rel, string subj)[] tuples)
    {
        var updates = tuples
            .Select(t => new CoreRelationshipUpdate(
                CoreRelationship.Create(
                    new ObjectAndRelation("document", t.res, t.rel),
                    new ObjectAndRelation("user", t.subj, CoreConstants.Ellipsis)),
                UpdateOperation.Touch))
            .ToList();
        await datastore.ReadWriteTx(tx => tx.WriteRelationships(updates));
    }

    private static async Task SeedCaveatedViewerAsync(IDatastore datastore, string res, string subj)
    {
        var rel = CoreRelationship.Create(
            new ObjectAndRelation("document", res, "viewer"),
            new ObjectAndRelation("user", subj, CoreConstants.Ellipsis),
            new ContextualizedCaveat("over_limit", null));
        await datastore.ReadWriteTx(tx => tx.WriteRelationships(
            new List<CoreRelationshipUpdate> { new(rel, UpdateOperation.Touch) }));
    }

    private static V1::SubjectReference UserSubject(string id) => new()
    {
        Object = new V1::ObjectReference { ObjectType = "user", ObjectId = id },
    };

    [Fact]
    public async Task CheckPermission_member_has_permission()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"));

        var resp = await Service(cluster).CheckPermission(new V1::CheckPermissionRequest
        {
            Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
            Permission = "view",
            Subject = UserSubject("alice"),
        }, FakeServerCallContext.Default);

        Assert.Equal(V1::CheckPermissionResponse.Types.Permissionship.HasPermission, resp.Permissionship);
        Assert.False(string.IsNullOrEmpty(resp.CheckedAt.Token));
        Assert.Null(resp.PartialCaveatInfo);
    }

    [Fact]
    public async Task CheckPermission_non_member_no_permission()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"));

        var resp = await Service(cluster).CheckPermission(new V1::CheckPermissionRequest
        {
            Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
            Permission = "view",
            Subject = UserSubject("bob"),
        }, FakeServerCallContext.Default);

        Assert.Equal(V1::CheckPermissionResponse.Types.Permissionship.NoPermission, resp.Permissionship);
    }

    [Fact]
    public async Task CheckPermission_unknown_definition_is_failed_precondition()
    {
        // An unknown resource definition is a client schema/typo bug. SpiceDB validates up front with
        // CheckNamespaceAndRelations and returns FailedPrecondition, NOT a NO_PERMISSION verdict.
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).CheckPermission(
            new V1::CheckPermissionRequest
            {
                Resource = new V1::ObjectReference { ObjectType = "nonesuch", ObjectId = "readme" },
                Permission = "view",
                Subject = UserSubject("alice"),
            },
            FakeServerCallContext.Default));

        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
    }

    [Fact]
    public async Task CheckPermission_unknown_permission_is_failed_precondition()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).CheckPermission(
            new V1::CheckPermissionRequest
            {
                Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
                Permission = "no_such_permission",
                Subject = UserSubject("alice"),
            },
            FakeServerCallContext.Default));

        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
    }

    [Fact]
    public async Task CheckPermission_unknown_subject_definition_is_failed_precondition()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).CheckPermission(
            new V1::CheckPermissionRequest
            {
                Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
                Permission = "view",
                Subject = new V1::SubjectReference
                {
                    Object = new V1::ObjectReference { ObjectType = "ghost", ObjectId = "alice" },
                },
            },
            FakeServerCallContext.Default));

        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
    }

    [Fact]
    public async Task CheckPermission_wildcard_subject_is_invalid_argument()
    {
        // A "*"-id subject is not a valid thing to check (SpiceDB: checkInternal returns
        // NewWildcardNotAllowedErr -> InvalidArgument). It must be rejected up front rather than
        // silently evaluated (where it could even match a stored wildcard tuple).
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"));

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).CheckPermission(
            new V1::CheckPermissionRequest
            {
                Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
                Permission = "view",
                Subject = UserSubject("*"),
            },
            FakeServerCallContext.Default));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Contains("cannot perform check on wildcard subject", ex.Status.Detail);
    }

    [Fact]
    public async Task CheckBulkPermissions_wildcard_subject_is_per_pair_error()
    {
        // SpiceDB's CheckBulkPermissions validates each pair independently and reports a bad pair via that
        // pair's google.rpc.Status error (CheckBulkPermissionsPair_Error), NOT by failing the whole RPC. A
        // wildcard ("*") check subject in one item must therefore error ONLY that pair; valid items still
        // return their verdict.
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"));

        var req = new V1::CheckBulkPermissionsRequest();
        req.Items.Add(BulkItem("readme", "view", "alice"));
        req.Items.Add(BulkItem("readme", "view", "*"));

        var resp = await Service(cluster).CheckBulkPermissions(req, FakeServerCallContext.Default);

        Assert.Equal(2, resp.Pairs.Count);
        Assert.Equal(V1::CheckBulkPermissionsPair.ResponseOneofCase.Item, resp.Pairs[0].ResponseCase);
        Assert.Equal(V1::CheckPermissionResponse.Types.Permissionship.HasPermission, resp.Pairs[0].Item.Permissionship);

        Assert.Equal(V1::CheckBulkPermissionsPair.ResponseOneofCase.Error, resp.Pairs[1].ResponseCase);
        Assert.Equal((int)StatusCode.InvalidArgument, resp.Pairs[1].Error.Code);
        Assert.Contains("cannot perform check on wildcard subject", resp.Pairs[1].Error.Message);
    }

    [Fact]
    public async Task CheckBulkPermissions_unknown_definition_is_per_pair_error()
    {
        // An unknown resource definition is a client schema/typo bug for that one pair: SpiceDB surfaces it
        // as ERROR_REASON_UNKNOWN_DEFINITION in the pair's error (FailedPrecondition code), not a whole-RPC
        // failure and not a silent NO_PERMISSION verdict. The other (valid) pair still returns its verdict.
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"));

        var req = new V1::CheckBulkPermissionsRequest();
        req.Items.Add(BulkItem("readme", "view", "alice"));
        req.Items.Add(new V1::CheckBulkPermissionsRequestItem
        {
            Resource = new V1::ObjectReference { ObjectType = "missing_type", ObjectId = "x" },
            Permission = "view",
            Subject = UserSubject("alice"),
        });

        var resp = await Service(cluster).CheckBulkPermissions(req, FakeServerCallContext.Default);

        Assert.Equal(2, resp.Pairs.Count);
        Assert.Equal(V1::CheckBulkPermissionsPair.ResponseOneofCase.Item, resp.Pairs[0].ResponseCase);
        Assert.Equal(V1::CheckPermissionResponse.Types.Permissionship.HasPermission, resp.Pairs[0].Item.Permissionship);

        Assert.Equal(V1::CheckBulkPermissionsPair.ResponseOneofCase.Error, resp.Pairs[1].ResponseCase);
        Assert.Equal((int)StatusCode.FailedPrecondition, resp.Pairs[1].Error.Code);
        Assert.Contains("missing_type", resp.Pairs[1].Error.Message);
    }

    [Fact]
    public async Task CheckBulkPermissions_unknown_relation_is_per_pair_error()
    {
        // An unknown relation/permission on a known definition is ERROR_REASON_UNKNOWN_RELATION_OR_PERMISSION
        // per pair (FailedPrecondition code), again leaving sibling valid pairs untouched.
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"));

        var req = new V1::CheckBulkPermissionsRequest();
        req.Items.Add(BulkItem("readme", "nonexistent_perm", "alice"));
        req.Items.Add(BulkItem("readme", "view", "alice"));

        var resp = await Service(cluster).CheckBulkPermissions(req, FakeServerCallContext.Default);

        Assert.Equal(2, resp.Pairs.Count);
        Assert.Equal(V1::CheckBulkPermissionsPair.ResponseOneofCase.Error, resp.Pairs[0].ResponseCase);
        Assert.Equal((int)StatusCode.FailedPrecondition, resp.Pairs[0].Error.Code);
        Assert.Contains("nonexistent_perm", resp.Pairs[0].Error.Message);

        Assert.Equal(V1::CheckBulkPermissionsPair.ResponseOneofCase.Item, resp.Pairs[1].ResponseCase);
        Assert.Equal(V1::CheckPermissionResponse.Types.Permissionship.HasPermission, resp.Pairs[1].Item.Permissionship);
    }

    [Fact]
    public async Task CheckBulkPermissions_all_pairs_invalid_emits_no_token()
    {
        // When every pair fails validation no revision is pinned (nothing is dispatched), so the response
        // carries per-pair errors and no batch CheckedAt token.
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var req = new V1::CheckBulkPermissionsRequest();
        req.Items.Add(BulkItem("readme", "nonexistent_perm", "alice"));

        var resp = await Service(cluster).CheckBulkPermissions(req, FakeServerCallContext.Default);

        var pair = Assert.Single(resp.Pairs);
        Assert.Equal(V1::CheckBulkPermissionsPair.ResponseOneofCase.Error, pair.ResponseCase);
        Assert.Null(resp.CheckedAt);
    }

    [Fact]
    public async Task WriteRelationships_create_existing_is_already_exists()
    {
        // A CREATE on an already-existing relationship is a permanent duplicate-create: AlreadyExists,
        // distinct from a transient write-write serialization conflict (which maps to Aborted).
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        await Service(cluster).WriteRelationships(
            WriteReq(V1::RelationshipUpdate.Types.Operation.Create, "readme", "alice"),
            FakeServerCallContext.Default);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).WriteRelationships(
            WriteReq(V1::RelationshipUpdate.Types.Operation.Create, "readme", "alice"),
            FakeServerCallContext.Default));

        Assert.Equal(StatusCode.AlreadyExists, ex.StatusCode);
    }

    /// <summary>
    /// The schema from issue #56: <c>document#viewer</c> admits only <c>user</c>, so a
    /// <c>group#member</c> subject is outside its allowed direct relations; plus a permission and a
    /// caveat-only relation to exercise the other write-time rejections.
    /// </summary>
    private const string WriteValidationSchema = """
        definition user {}

        caveat only_on_tuesday(day string) {
            day == "tuesday"
        }

        definition group {
            relation member: user
        }

        definition document {
            relation viewer: user
            relation tuesday_viewer: user with only_on_tuesday
            permission view = viewer
        }
        """;

    private static V1::WriteRelationshipsRequest WriteOne(
        V1::RelationshipUpdate.Types.Operation op,
        string resourceType, string resourceId, string relation,
        string subjectType, string subjectId, string? subjectRelation = null, string? caveat = null)
    {
        var rel = new V1::Relationship
        {
            Resource = new V1::ObjectReference { ObjectType = resourceType, ObjectId = resourceId },
            Relation = relation,
            Subject = new V1::SubjectReference
            {
                Object = new V1::ObjectReference { ObjectType = subjectType, ObjectId = subjectId },
                OptionalRelation = subjectRelation ?? string.Empty,
            },
        };
        if (caveat is not null)
            rel.OptionalCaveat = new V1::ContextualizedCaveat { CaveatName = caveat };

        var req = new V1::WriteRelationshipsRequest();
        req.Updates.Add(new V1::RelationshipUpdate { Operation = op, Relationship = rel });
        return req;
    }

    private static async Task<int> CountStoredAsync(MeshTestCluster cluster, string resourceType)
    {
        var writer = new CollectingStreamWriter<V1::ReadRelationshipsResponse>();
        await Service(cluster).ReadRelationships(new V1::ReadRelationshipsRequest
        {
            Consistency = new V1::Consistency { FullyConsistent = true },
            RelationshipFilter = new V1::RelationshipFilter { ResourceType = resourceType },
        }, writer, FakeServerCallContext.Default);
        return writer.Collected.Count;
    }

    [Fact]
    public async Task WriteRelationships_subject_type_not_allowed_on_relation_is_invalid_argument()
    {
        // Issue #56: SpiceDB validates every update against the schema inside the write transaction
        // (relationships.ValidateRelationshipUpdates) and rejects a subject type outside the relation's
        // allowed direct relations with INVALID_ARGUMENT (InvalidSubjectTypeError). Nothing persists.
        await using var cluster = await MeshTestCluster.CreateAsync(WriteValidationSchema);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).WriteRelationships(
            WriteOne(V1::RelationshipUpdate.Types.Operation.Create,
                "document", "doc", "viewer", "group", "eng", "member"),
            FakeServerCallContext.Default));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Contains("subjects of type `group#member` are not allowed on relation `document#viewer`", ex.Status.Detail);
        Assert.Equal(0, await CountStoredAsync(cluster, "document"));
    }

    [Fact]
    public async Task WriteRelationships_unknown_definition_is_failed_precondition()
    {
        // An unknown object definition is SpiceDB's NamespaceNotFound, which the v1 services rewrite to
        // FAILED_PRECONDITION (the same mapping SchemaValidation applies for Check).
        await using var cluster = await MeshTestCluster.CreateAsync(WriteValidationSchema);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).WriteRelationships(
            WriteOne(V1::RelationshipUpdate.Types.Operation.Touch,
                "nonesuch", "doc", "viewer", "user", "alice"),
            FakeServerCallContext.Default));

        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
        Assert.Contains("object definition `nonesuch` not found", ex.Status.Detail);
    }

    [Fact]
    public async Task WriteRelationships_unknown_relation_is_failed_precondition()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(WriteValidationSchema);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).WriteRelationships(
            WriteOne(V1::RelationshipUpdate.Types.Operation.Touch,
                "document", "doc", "nonesuch", "user", "alice"),
            FakeServerCallContext.Default));

        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
        Assert.Contains("relation/permission `nonesuch` not found under definition `document`", ex.Status.Detail);
    }

    [Fact]
    public async Task WriteRelationships_to_a_permission_is_invalid_argument()
    {
        // SpiceDB CannotWriteToPermissionError -> INVALID_ARGUMENT.
        await using var cluster = await MeshTestCluster.CreateAsync(WriteValidationSchema);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).WriteRelationships(
            WriteOne(V1::RelationshipUpdate.Types.Operation.Touch,
                "document", "doc", "view", "user", "alice"),
            FakeServerCallContext.Default));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Contains("cannot write a relationship to permission `view` under definition `document`", ex.Status.Detail);
    }

    [Fact]
    public async Task WriteRelationships_caveat_not_allowed_on_relation_is_invalid_argument()
    {
        // `document#viewer` admits only an uncaveated `user`; a caveated subject is not an allowed type.
        await using var cluster = await MeshTestCluster.CreateAsync(WriteValidationSchema);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).WriteRelationships(
            WriteOne(V1::RelationshipUpdate.Types.Operation.Touch,
                "document", "doc", "viewer", "user", "alice", caveat: "only_on_tuesday"),
            FakeServerCallContext.Default));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Contains("subjects of type `user with only_on_tuesday` are not allowed on relation `document#viewer`", ex.Status.Detail);
    }

    [Fact]
    public async Task WriteRelationships_uncaveated_subject_on_caveat_only_relation_names_the_required_caveat()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(WriteValidationSchema);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).WriteRelationships(
            WriteOne(V1::RelationshipUpdate.Types.Operation.Touch,
                "document", "doc", "tuesday_viewer", "user", "alice"),
            FakeServerCallContext.Default));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Contains(
            "subjects of type `user` are not allowed on relation `document#tuesday_viewer` without one of the following caveats: only_on_tuesday",
            ex.Status.Detail);
    }

    [Fact]
    public async Task WriteRelationships_delete_ignores_an_unspecified_caveat()
    {
        // The DELETE rule (ValidateRelationshipForDeletion) ignores the caveat when none is given, so an
        // uncaveated DELETE of a caveat-only relation is valid; a disallowed subject type still is not.
        await using var cluster = await MeshTestCluster.CreateAsync(WriteValidationSchema);

        await Service(cluster).WriteRelationships(
            WriteOne(V1::RelationshipUpdate.Types.Operation.Delete,
                "document", "doc", "tuesday_viewer", "user", "alice"),
            FakeServerCallContext.Default);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).WriteRelationships(
            WriteOne(V1::RelationshipUpdate.Types.Operation.Delete,
                "document", "doc", "viewer", "group", "eng", "member"),
            FakeServerCallContext.Default));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
    }

    [Fact]
    public async Task WriteRelationships_schema_valid_write_succeeds()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(WriteValidationSchema);

        await Service(cluster).WriteRelationships(
            WriteOne(V1::RelationshipUpdate.Types.Operation.Create,
                "group", "eng", "member", "user", "alice"),
            FakeServerCallContext.Default);

        Assert.Equal(1, await CountStoredAsync(cluster, "group"));
    }

    [Fact]
    public async Task ImportBulkRelationships_disallowed_subject_is_invalid_argument_and_imports_nothing()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(WriteValidationSchema);

        var bad = new V1::ImportBulkRelationshipsRequest();
        bad.Relationships.Add(new V1::Relationship
        {
            Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "doc" },
            Relation = "viewer",
            Subject = new V1::SubjectReference
            {
                Object = new V1::ObjectReference { ObjectType = "group", ObjectId = "eng" },
                OptionalRelation = "member",
            },
        });
        var reader = new FakeAsyncStreamReader<V1::ImportBulkRelationshipsRequest>(
            ImportBatch(("doc1", "alice")), bad);

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            Service(cluster).ImportBulkRelationships(reader, FakeServerCallContext.Default));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal(0, await CountStoredAsync(cluster, "document"));
    }

    [Fact]
    public async Task WriteRelationships_duplicate_relationship_is_invalid_argument()
    {
        // A relationship may appear in an update only once per request (SpiceDB:
        // NewDuplicateRelationshipErr -> InvalidArgument). The key ignores caveat/expiration, so a
        // CREATE + DELETE of the same tuple in one request is a duplicate too.
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var req = new V1::WriteRelationshipsRequest();
        req.Updates.Add(new V1::RelationshipUpdate
        {
            Operation = V1::RelationshipUpdate.Types.Operation.Touch,
            Relationship = new V1::Relationship
            {
                Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
                Relation = "viewer",
                Subject = UserSubject("alice"),
            },
        });
        req.Updates.Add(new V1::RelationshipUpdate
        {
            Operation = V1::RelationshipUpdate.Types.Operation.Delete,
            Relationship = new V1::Relationship
            {
                Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
                Relation = "viewer",
                Subject = UserSubject("alice"),
            },
        });

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            Service(cluster).WriteRelationships(req, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Contains("more than one update with relationship", ex.Status.Detail);
    }

    [Fact]
    public async Task WriteRelationships_two_touch_of_same_tuple_is_invalid_argument()
    {
        // Verified against real SpiceDB (authzed/spicedb v1.49.2): two TOUCH updates for the identical
        // tuple in one request reject with InvalidArgument and this exact message, not silent dedup.
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var req = WriteReq(V1::RelationshipUpdate.Types.Operation.Touch, "readme", "alice");
        req.Updates.Add(new V1::RelationshipUpdate
        {
            Operation = V1::RelationshipUpdate.Types.Operation.Touch,
            Relationship = new V1::Relationship
            {
                Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
                Relation = "viewer",
                Subject = UserSubject("alice"),
            },
        });

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            Service(cluster).WriteRelationships(req, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Contains("more than one update with relationship", ex.Status.Detail);
    }

    [Fact]
    public async Task WriteRelationships_create_then_touch_of_same_tuple_is_invalid_argument()
    {
        // Verified against real SpiceDB: mixing CREATE and TOUCH for the same tuple in one request is
        // ALSO a duplicate -- the key is the bare tuple, independent of operation kind.
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var req = new V1::WriteRelationshipsRequest();
        req.Updates.Add(new V1::RelationshipUpdate
        {
            Operation = V1::RelationshipUpdate.Types.Operation.Create,
            Relationship = new V1::Relationship
            {
                Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
                Relation = "editor",
                Subject = UserSubject("alice"),
            },
        });
        req.Updates.Add(new V1::RelationshipUpdate
        {
            Operation = V1::RelationshipUpdate.Types.Operation.Touch,
            Relationship = new V1::Relationship
            {
                Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
                Relation = "editor",
                Subject = UserSubject("alice"),
            },
        });

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            Service(cluster).WriteRelationships(req, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Contains("more than one update with relationship", ex.Status.Detail);
    }

    [Fact]
    public async Task WriteRelationships_two_touch_differing_only_by_caveat_context_is_invalid_argument()
    {
        // Verified against real SpiceDB: two updates for the same (resource, relation, subject) that
        // differ ONLY in caveat name/context are still a duplicate -- the dedup key is computed WITHOUT
        // the caveat (SpiceDB's V1StringRelationshipWithoutCaveatOrExpiration).
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var req = new V1::WriteRelationshipsRequest();
        req.Updates.Add(new V1::RelationshipUpdate
        {
            Operation = V1::RelationshipUpdate.Types.Operation.Touch,
            Relationship = new V1::Relationship
            {
                Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
                Relation = "viewer",
                Subject = UserSubject("alice"),
                OptionalCaveat = new V1::ContextualizedCaveat { CaveatName = "over_limit" },
            },
        });
        req.Updates.Add(new V1::RelationshipUpdate
        {
            Operation = V1::RelationshipUpdate.Types.Operation.Touch,
            Relationship = new V1::Relationship
            {
                Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
                Relation = "viewer",
                Subject = UserSubject("alice"),
                // No caveat on the second update -- still the same underlying tuple key.
            },
        });

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            Service(cluster).WriteRelationships(req, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Contains("more than one update with relationship", ex.Status.Detail);
    }

    [Fact]
    public async Task WriteRelationships_non_duplicate_updates_are_unaffected()
    {
        // Distinct tuples (different relation, different subject) in the same request are unaffected by
        // the duplicate check and both apply.
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var req = new V1::WriteRelationshipsRequest();
        req.Updates.Add(new V1::RelationshipUpdate
        {
            Operation = V1::RelationshipUpdate.Types.Operation.Touch,
            Relationship = new V1::Relationship
            {
                Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
                Relation = "editor",
                Subject = UserSubject("alice"),
            },
        });
        req.Updates.Add(new V1::RelationshipUpdate
        {
            Operation = V1::RelationshipUpdate.Types.Operation.Touch,
            Relationship = new V1::Relationship
            {
                Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
                Relation = "editor",
                Subject = UserSubject("bob"),
            },
        });

        var resp = await Service(cluster).WriteRelationships(req, FakeServerCallContext.Default);

        Assert.NotEmpty(resp.WrittenAt.Token);
    }

    [Fact]
    public async Task WriteRelationships_too_many_updates_is_invalid_argument()
    {
        // More than MaxUpdatesPerWrite (1000) updates is rejected up front with InvalidArgument
        // (SpiceDB: NewExceedsMaximumUpdatesErr).
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var req = new V1::WriteRelationshipsRequest();
        for (var i = 0; i <= 1000; i++)
        {
            req.Updates.Add(new V1::RelationshipUpdate
            {
                Operation = V1::RelationshipUpdate.Types.Operation.Touch,
                Relationship = new V1::Relationship
                {
                    Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = $"doc{i}" },
                    Relation = "viewer",
                    Subject = UserSubject("alice"),
                },
            });
        }

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            Service(cluster).WriteRelationships(req, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Contains("too many updates", ex.Status.Detail);
    }

    [Fact]
    public async Task WriteRelationships_too_many_preconditions_is_invalid_argument()
    {
        // More than MaxPreconditionsCount (1000) preconditions is rejected with InvalidArgument
        // (SpiceDB: NewExceedsMaximumPreconditionsErr).
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var req = WriteReq(V1::RelationshipUpdate.Types.Operation.Touch, "readme", "alice");
        for (var i = 0; i <= 1000; i++)
        {
            req.OptionalPreconditions.Add(new V1::Precondition
            {
                Operation = V1::Precondition.Types.Operation.MustMatch,
                Filter = new V1::RelationshipFilter { ResourceType = "document", OptionalResourceId = $"doc{i}" },
            });
        }

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            Service(cluster).WriteRelationships(req, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Contains("precondition count", ex.Status.Detail);
    }

    [Fact]
    public async Task WriteRelationships_oversized_relationship_caveat_context_is_invalid_argument()
    {
        // A per-relationship caveat context larger than MaxRelationshipContextSize (25000 bytes) is
        // rejected with InvalidArgument (SpiceDB: NewMaxRelationshipContextError).
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var ctx = new Google.Protobuf.WellKnownTypes.Struct();
        ctx.Fields["blob"] = Google.Protobuf.WellKnownTypes.Value.ForString(new string('x', 30_000));

        var req = new V1::WriteRelationshipsRequest();
        req.Updates.Add(new V1::RelationshipUpdate
        {
            Operation = V1::RelationshipUpdate.Types.Operation.Touch,
            Relationship = new V1::Relationship
            {
                Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
                Relation = "viewer",
                Subject = UserSubject("alice"),
                OptionalCaveat = new V1::ContextualizedCaveat { CaveatName = "over_limit", Context = ctx },
            },
        });

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            Service(cluster).WriteRelationships(req, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Contains("exceeded maximum allowed caveat size", ex.Status.Detail);
    }

    [Fact]
    public async Task CheckPermission_oversized_caveat_context_is_invalid_argument()
    {
        // A request caveat context larger than MaxCaveatContextSize (4096 bytes) is rejected with
        // InvalidArgument (SpiceDB: GetCaveatContext).
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var ctx = new Google.Protobuf.WellKnownTypes.Struct();
        ctx.Fields["blob"] = Google.Protobuf.WellKnownTypes.Value.ForString(new string('x', 5000));

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).CheckPermission(
            new V1::CheckPermissionRequest
            {
                Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
                Permission = "view",
                Subject = UserSubject("alice"),
                Context = ctx,
            },
            FakeServerCallContext.Default));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Contains("request caveat context should have less than", ex.Status.Detail);
    }

    [Fact]
    public async Task CheckBulkPermissions_oversized_caveat_context_is_invalid_argument()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var ctx = new Google.Protobuf.WellKnownTypes.Struct();
        ctx.Fields["blob"] = Google.Protobuf.WellKnownTypes.Value.ForString(new string('x', 5000));

        var req = new V1::CheckBulkPermissionsRequest();
        var item = BulkItem("readme", "view", "alice");
        item.Context = ctx;
        req.Items.Add(item);

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            Service(cluster).CheckBulkPermissions(req, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Contains("request caveat context should have less than", ex.Status.Detail);
    }

    [Fact]
    public async Task CheckPermission_caveated_missing_context_populates_partial_caveat_info()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedCaveatedViewerAsync(cluster.Datastore, "readme", "alice");

        // No context supplied: the caveat cannot be fully evaluated -> conditional + missing fields.
        var resp = await Service(cluster).CheckPermission(new V1::CheckPermissionRequest
        {
            Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
            Permission = "view",
            Subject = UserSubject("alice"),
        }, FakeServerCallContext.Default);

        Assert.Equal(V1::CheckPermissionResponse.Types.Permissionship.ConditionalPermission, resp.Permissionship);
        Assert.NotNull(resp.PartialCaveatInfo);
        Assert.NotEmpty(resp.PartialCaveatInfo.MissingRequiredContext);
    }

    [Fact]
    public async Task CheckPermission_invalid_consistency_token_is_invalid_argument()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).CheckPermission(
            new V1::CheckPermissionRequest
            {
                Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
                Permission = "view",
                Subject = UserSubject("alice"),
                Consistency = new V1::Consistency
                {
                    AtExactSnapshot = new V1::ZedToken { Token = "not-a-real-token" },
                },
            },
            FakeServerCallContext.Default));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
    }

    [Fact]
    public async Task WriteRelationships_writes_and_returns_token()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var resp = await Service(cluster).WriteRelationships(WriteReq(
            V1::RelationshipUpdate.Types.Operation.Touch, "readme", "alice"), FakeServerCallContext.Default);

        Assert.False(string.IsNullOrEmpty(resp.WrittenAt.Token));

        var check = await Service(cluster).CheckPermission(new V1::CheckPermissionRequest
        {
            Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
            Permission = "view",
            Subject = UserSubject("alice"),
        }, FakeServerCallContext.Default);
        Assert.Equal(V1::CheckPermissionResponse.Types.Permissionship.HasPermission, check.Permissionship);
    }

    [Fact]
    public async Task WriteRelationships_unspecified_operation_is_invalid_argument()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).WriteRelationships(
            WriteReq(V1::RelationshipUpdate.Types.Operation.Unspecified, "readme", "alice"),
            FakeServerCallContext.Default));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
    }

    [Fact]
    public async Task WriteRelationships_precondition_must_match_success_and_failure()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"));

        // MUST_MATCH on an existing relationship -> success.
        var ok = WriteReq(V1::RelationshipUpdate.Types.Operation.Touch, "readme", "bob");
        ok.OptionalPreconditions.Add(new V1::Precondition
        {
            Operation = V1::Precondition.Types.Operation.MustMatch,
            Filter = new V1::RelationshipFilter { ResourceType = "document", OptionalResourceId = "readme" },
        });
        var resp = await Service(cluster).WriteRelationships(ok, FakeServerCallContext.Default);
        Assert.False(string.IsNullOrEmpty(resp.WrittenAt.Token));

        // MUST_MATCH on a non-existent resource -> FailedPrecondition.
        var bad = WriteReq(V1::RelationshipUpdate.Types.Operation.Touch, "other", "carol");
        bad.OptionalPreconditions.Add(new V1::Precondition
        {
            Operation = V1::Precondition.Types.Operation.MustMatch,
            Filter = new V1::RelationshipFilter { ResourceType = "document", OptionalResourceId = "missing" },
        });
        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            Service(cluster).WriteRelationships(bad, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
    }

    [Fact]
    public async Task WriteRelationships_precondition_must_not_match_failure()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"));

        var req = WriteReq(V1::RelationshipUpdate.Types.Operation.Touch, "readme", "bob");
        req.OptionalPreconditions.Add(new V1::Precondition
        {
            Operation = V1::Precondition.Types.Operation.MustNotMatch,
            Filter = new V1::RelationshipFilter { ResourceType = "document", OptionalResourceId = "readme" },
        });

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            Service(cluster).WriteRelationships(req, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
    }

    [Fact]
    public async Task ReadRelationships_streams_all_with_read_at_and_nested_subject_filter()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore,
            ("readme", "viewer", "alice"),
            ("readme", "viewer", "bob"),
            ("readme", "editor", "carol"));

        // Resource filter only: all three viewer/editor relationships on readme.
        var allWriter = new CollectingStreamWriter<V1::ReadRelationshipsResponse>();
        await Service(cluster).ReadRelationships(new V1::ReadRelationshipsRequest
        {
            RelationshipFilter = new V1::RelationshipFilter { ResourceType = "document", OptionalResourceId = "readme" },
        }, allWriter, FakeServerCallContext.Default);

        Assert.Equal(3, allWriter.Collected.Count);
        Assert.All(allWriter.Collected, r => Assert.False(string.IsNullOrEmpty(r.ReadAt.Token)));

        // Nested subject filter narrows to a single subject id.
        var narrowWriter = new CollectingStreamWriter<V1::ReadRelationshipsResponse>();
        await Service(cluster).ReadRelationships(new V1::ReadRelationshipsRequest
        {
            RelationshipFilter = new V1::RelationshipFilter
            {
                ResourceType = "document",
                OptionalResourceId = "readme",
                OptionalRelation = "viewer",
                OptionalSubjectFilter = new V1::SubjectFilter
                {
                    SubjectType = "user",
                    OptionalSubjectId = "alice",
                },
            },
        }, narrowWriter, FakeServerCallContext.Default);

        var only = Assert.Single(narrowWriter.Collected);
        Assert.Equal("alice", only.Relationship.Subject.Object.ObjectId);
        Assert.Equal("viewer", only.Relationship.Relation);
    }

    [Fact]
    public async Task WriteRelationships_then_ReadRelationships_round_trips_expiration()
    {
        // An expiring relationship must target a relation whose allowed type carries the expiration
        // trait: the write is validated against the schema like any other.
        await using var cluster = await MeshTestCluster.CreateAsync("""
            use expiration

            definition user {}

            definition document {
                relation viewer: user with expiration
            }
            """);

        var expiresAt = new DateTimeOffset(2099, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var req = new V1::WriteRelationshipsRequest();
        req.Updates.Add(new V1::RelationshipUpdate
        {
            Operation = V1::RelationshipUpdate.Types.Operation.Touch,
            Relationship = new V1::Relationship
            {
                Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
                Relation = "viewer",
                Subject = UserSubject("alice"),
                OptionalExpiresAt = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTimeOffset(expiresAt),
            },
        });
        await Service(cluster).WriteRelationships(req, FakeServerCallContext.Default);

        var writer = new CollectingStreamWriter<V1::ReadRelationshipsResponse>();
        await Service(cluster).ReadRelationships(new V1::ReadRelationshipsRequest
        {
            RelationshipFilter = new V1::RelationshipFilter { ResourceType = "document", OptionalResourceId = "readme" },
        }, writer, FakeServerCallContext.Default);

        var only = Assert.Single(writer.Collected);
        Assert.Equal(Google.Protobuf.WellKnownTypes.Timestamp.FromDateTimeOffset(expiresAt), only.Relationship.OptionalExpiresAt);
    }

    // --- issue #46: limit ceilings and the transaction-metadata cap ---

    private static V1::RelationshipFilter ReadmeFilter() =>
        new() { ResourceType = "document", OptionalResourceId = "readme" };

    private static Google.Protobuf.WellKnownTypes.Struct MetadataOfBytes(int approximateBytes) => new()
    {
        Fields = { ["blob"] = Google.Protobuf.WellKnownTypes.Value.ForString(new string('x', approximateBytes)) },
    };

    [Fact]
    public async Task LookupResources_limit_above_1000_is_invalid_argument()
    {
        // SpiceDB MaxLookupResourcesLimit (default 1000) -> ExceedsMaximumLimitErr, INVALID_ARGUMENT.
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var request = AliceViewLookup();
        request.OptionalLimit = 1001;
        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).LookupResources(
            request, new CollectingStreamWriter<V1::LookupResourcesResponse>(), FakeServerCallContext.Default));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal("provided limit 1001 is greater than maximum allowed of 1000", ex.Status.Detail);
    }

    [Fact]
    public async Task ReadRelationships_limit_above_1000_is_invalid_argument()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).ReadRelationships(
            new V1::ReadRelationshipsRequest { RelationshipFilter = ReadmeFilter(), OptionalLimit = 1001 },
            new CollectingStreamWriter<V1::ReadRelationshipsResponse>(), FakeServerCallContext.Default));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal("provided limit 1001 is greater than maximum allowed of 1000", ex.Status.Detail);
    }

    [Fact]
    public async Task DeleteRelationships_limit_above_1000_is_invalid_argument()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).DeleteRelationships(
            new V1::DeleteRelationshipsRequest { RelationshipFilter = ReadmeFilter(), OptionalLimit = 1001 },
            FakeServerCallContext.Default));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal("provided limit 1001 is greater than maximum allowed of 1000", ex.Status.Detail);
    }

    [Fact]
    public async Task ExportBulkRelationships_limit_above_10000_is_invalid_argument()
    {
        // SpiceDB MaxBulkExportRelationshipsLimit (default 10000).
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).ExportBulkRelationships(
            new V1::ExportBulkRelationshipsRequest { OptionalLimit = 10_001 },
            new CollectingStreamWriter<V1::ExportBulkRelationshipsResponse>(), FakeServerCallContext.Default));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal("provided limit 10001 is greater than maximum allowed of 10000", ex.Status.Detail);
    }

    [Fact]
    public async Task ReadRelationships_honours_optional_limit_and_resumes_from_after_result_cursor()
    {
        // SpiceDB returns at most optional_limit relationships, each with an after_result_cursor that
        // optional_cursor resumes from.
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore,
            ("readme", "viewer", "alice"), ("readme", "viewer", "bob"), ("readme", "viewer", "carol"));

        var first = new CollectingStreamWriter<V1::ReadRelationshipsResponse>();
        await Service(cluster).ReadRelationships(
            new V1::ReadRelationshipsRequest { RelationshipFilter = ReadmeFilter(), OptionalLimit = 2 },
            first, FakeServerCallContext.Default);

        Assert.Equal(2, first.Collected.Count);
        Assert.All(first.Collected, r => Assert.False(string.IsNullOrEmpty(r.AfterResultCursor?.Token)));

        var rest = new CollectingStreamWriter<V1::ReadRelationshipsResponse>();
        await Service(cluster).ReadRelationships(
            new V1::ReadRelationshipsRequest
            {
                RelationshipFilter = ReadmeFilter(),
                OptionalLimit = 2,
                OptionalCursor = first.Collected[^1].AfterResultCursor,
            },
            rest, FakeServerCallContext.Default);

        var subjects = first.Collected.Concat(rest.Collected).Select(r => r.Relationship.Subject.Object.ObjectId);
        Assert.Equal(new[] { "alice", "bob", "carol" }, subjects.Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task ReadRelationships_cursor_that_is_not_base64_is_invalid_argument()
    {
        // SpiceDB: cursor.Decode fails -> InvalidCursorError (INVALID_ARGUMENT), never an empty OK stream.
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"));

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).ReadRelationships(
            new V1::ReadRelationshipsRequest
            {
                RelationshipFilter = ReadmeFilter(),
                OptionalCursor = new V1::Cursor { Token = "zzz" },
            },
            new CollectingStreamWriter<V1::ReadRelationshipsResponse>(), FakeServerCallContext.Default));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.StartsWith("error decoding cursor: ", ex.Status.Detail);
    }

    [Fact]
    public async Task ReadRelationships_cursor_from_a_different_request_is_invalid_argument()
    {
        // SpiceDB binds the cursor to a hash of the request (filter, consistency, limit): a cursor minted
        // for another call is rejected with ErrHashMismatch.
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"), ("readme", "viewer", "bob"));

        var first = new CollectingStreamWriter<V1::ReadRelationshipsResponse>();
        await Service(cluster).ReadRelationships(
            new V1::ReadRelationshipsRequest { RelationshipFilter = ReadmeFilter(), OptionalLimit = 1 },
            first, FakeServerCallContext.Default);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).ReadRelationships(
            new V1::ReadRelationshipsRequest
            {
                RelationshipFilter = new V1::RelationshipFilter { ResourceType = "document" },
                OptionalLimit = 1,
                OptionalCursor = first.Collected[0].AfterResultCursor,
            },
            new CollectingStreamWriter<V1::ReadRelationshipsResponse>(), FakeServerCallContext.Default));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.StartsWith("the cursor provided does not have the same arguments as the original API call", ex.Status.Detail);
    }

    [Fact]
    public async Task ReadRelationships_cursor_without_a_parsable_resume_relationship_is_failed_precondition()
    {
        // SpiceDB: a cursor for this call whose section is not a relationship -> NewInvalidCursorErr
        // ("could not parse resume relationship"), FAILED_PRECONDITION.
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var request = new V1::ReadRelationshipsRequest { RelationshipFilter = ReadmeFilter() };
        request.OptionalCursor = new V1::Cursor
        {
            Token = ReadRelationshipsCursor.Encode(ReadRelationshipsCursor.RequestHash(request), "not a tuple"),
        };

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).ReadRelationships(
            request, new CollectingStreamWriter<V1::ReadRelationshipsResponse>(), FakeServerCallContext.Default));

        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
        Assert.Equal("the cursor provided is not valid: could not parse resume relationship", ex.Status.Detail);
    }

    [Fact]
    public async Task ReadRelationships_after_result_cursor_is_opaque()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"));

        var read = new CollectingStreamWriter<V1::ReadRelationshipsResponse>();
        await Service(cluster).ReadRelationships(
            new V1::ReadRelationshipsRequest { RelationshipFilter = ReadmeFilter() },
            read, FakeServerCallContext.Default);

        var token = Assert.Single(read.Collected).AfterResultCursor.Token;
        Assert.DoesNotContain("alice", token);
        Assert.NotEmpty(Convert.FromBase64String(token));
    }

    [Fact]
    public async Task DeleteRelationships_partial_deletion_honours_optional_limit()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore,
            ("readme", "viewer", "alice"), ("readme", "viewer", "bob"), ("readme", "viewer", "carol"));

        var partial = await Service(cluster).DeleteRelationships(new V1::DeleteRelationshipsRequest
        {
            RelationshipFilter = ReadmeFilter(),
            OptionalLimit = 2,
            OptionalAllowPartialDeletions = true,
        }, FakeServerCallContext.Default);

        Assert.Equal(2UL, partial.RelationshipsDeletedCount);
        Assert.Equal(V1::DeleteRelationshipsResponse.Types.DeletionProgress.Partial, partial.DeletionProgress);
        Assert.Equal(1, await CountStoredAsync(cluster, "document"));

        var complete = await Service(cluster).DeleteRelationships(new V1::DeleteRelationshipsRequest
        {
            RelationshipFilter = ReadmeFilter(),
            OptionalLimit = 2,
            OptionalAllowPartialDeletions = true,
        }, FakeServerCallContext.Default);

        Assert.Equal(1UL, complete.RelationshipsDeletedCount);
        Assert.Equal(V1::DeleteRelationshipsResponse.Types.DeletionProgress.Complete, complete.DeletionProgress);
        Assert.Equal(0, await CountStoredAsync(cluster, "document"));
    }

    [Fact]
    public async Task DeleteRelationships_over_limit_without_partial_deletion_is_invalid_argument_and_deletes_nothing()
    {
        // SpiceDB CouldNotTransactionallyDeleteErr -> INVALID_ARGUMENT, nothing deleted.
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore,
            ("readme", "viewer", "alice"), ("readme", "viewer", "bob"), ("readme", "viewer", "carol"));

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).DeleteRelationships(
            new V1::DeleteRelationshipsRequest { RelationshipFilter = ReadmeFilter(), OptionalLimit = 2 },
            FakeServerCallContext.Default));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal("found more than 2 relationships to be deleted and partial deletion was not requested", ex.Status.Detail);
        Assert.Equal(3, await CountStoredAsync(cluster, "document"));

        // Exactly at the limit, the transactional delete removes everything; like SpiceDB (whose datastores
        // report LimitReached when the deleted count equals the limit) it reports PARTIAL.
        var resp = await Service(cluster).DeleteRelationships(
            new V1::DeleteRelationshipsRequest { RelationshipFilter = ReadmeFilter(), OptionalLimit = 3 },
            FakeServerCallContext.Default);
        Assert.Equal(3UL, resp.RelationshipsDeletedCount);
        Assert.Equal(V1::DeleteRelationshipsResponse.Types.DeletionProgress.Partial, resp.DeletionProgress);
        Assert.Equal(0, await CountStoredAsync(cluster, "document"));

        // Under the limit it reports COMPLETE.
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"));
        var under = await Service(cluster).DeleteRelationships(
            new V1::DeleteRelationshipsRequest { RelationshipFilter = ReadmeFilter(), OptionalLimit = 3 },
            FakeServerCallContext.Default);
        Assert.Equal(1UL, under.RelationshipsDeletedCount);
        Assert.Equal(V1::DeleteRelationshipsResponse.Types.DeletionProgress.Complete, under.DeletionProgress);
    }

    [Fact]
    public async Task DeleteRelationships_failed_precondition_wins_over_the_transactional_limit()
    {
        // SpiceDB evaluates optional_preconditions before the limit+1 count, so a request that fails
        // both is FAILED_PRECONDITION, not CouldNotTransactionallyDelete's INVALID_ARGUMENT.
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore,
            ("readme", "viewer", "alice"), ("readme", "viewer", "bob"), ("readme", "viewer", "carol"));

        var request = new V1::DeleteRelationshipsRequest { RelationshipFilter = ReadmeFilter(), OptionalLimit = 2 };
        request.OptionalPreconditions.Add(new V1::Precondition
        {
            Operation = V1::Precondition.Types.Operation.MustMatch,
            Filter = new V1::RelationshipFilter { ResourceType = "document", OptionalResourceId = "nonesuch" },
        });

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            Service(cluster).DeleteRelationships(request, FakeServerCallContext.Default));

        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
        Assert.Equal(3, await CountStoredAsync(cluster, "document"));
    }

    [Fact]
    public async Task DeleteRelationships_transactional_limit_is_checked_after_a_passing_precondition()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore,
            ("readme", "viewer", "alice"), ("readme", "viewer", "bob"), ("readme", "viewer", "carol"));

        var request = new V1::DeleteRelationshipsRequest { RelationshipFilter = ReadmeFilter(), OptionalLimit = 2 };
        request.OptionalPreconditions.Add(new V1::Precondition
        {
            Operation = V1::Precondition.Types.Operation.MustMatch,
            Filter = ReadmeFilter(),
        });

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            Service(cluster).DeleteRelationships(request, FakeServerCallContext.Default));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal("found more than 2 relationships to be deleted and partial deletion was not requested", ex.Status.Detail);
        Assert.Equal(3, await CountStoredAsync(cluster, "document"));
    }

    [Fact]
    public async Task WriteRelationships_transaction_metadata_over_65000_bytes_is_invalid_argument()
    {
        // SpiceDB MaximumTransactionMetadataSize: TransactionMetadataTooLargeErr -> INVALID_ARGUMENT.
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var request = WriteReq(V1::RelationshipUpdate.Types.Operation.Touch, "readme", "alice");
        request.OptionalTransactionMetadata = MetadataOfBytes(65_000);
        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            Service(cluster).WriteRelationships(request, FakeServerCallContext.Default));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Matches("^metadata size of [0-9]+ is greater than maximum allowed of 65000$", ex.Status.Detail);
        Assert.Equal(0, await CountStoredAsync(cluster, "document"));

        // Metadata under the cap is accepted.
        request.OptionalTransactionMetadata = MetadataOfBytes(100);
        await Service(cluster).WriteRelationships(request, FakeServerCallContext.Default);
    }

    [Fact]
    public async Task DeleteRelationships_transaction_metadata_over_65000_bytes_is_invalid_argument()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var ex = await Assert.ThrowsAsync<RpcException>(() => Service(cluster).DeleteRelationships(
            new V1::DeleteRelationshipsRequest
            {
                RelationshipFilter = ReadmeFilter(),
                OptionalTransactionMetadata = MetadataOfBytes(65_000),
            },
            FakeServerCallContext.Default));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Matches("^metadata size of [0-9]+ is greater than maximum allowed of 65000$", ex.Status.Detail);
    }

    [Fact]
    public async Task DeleteRelationships_deletes_matching_and_returns_token()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"), ("readme", "viewer", "bob"));

        var resp = await Service(cluster).DeleteRelationships(new V1::DeleteRelationshipsRequest
        {
            RelationshipFilter = new V1::RelationshipFilter { ResourceType = "document", OptionalResourceId = "readme" },
        }, FakeServerCallContext.Default);

        Assert.False(string.IsNullOrEmpty(resp.DeletedAt.Token));

        var after = new CollectingStreamWriter<V1::ReadRelationshipsResponse>();
        await Service(cluster).ReadRelationships(new V1::ReadRelationshipsRequest
        {
            RelationshipFilter = new V1::RelationshipFilter { ResourceType = "document", OptionalResourceId = "readme" },
        }, after, FakeServerCallContext.Default);
        Assert.Empty(after.Collected);
    }

    [Fact]
    public async Task DeleteRelationships_precondition_failure_is_failed_precondition()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"));

        var req = new V1::DeleteRelationshipsRequest
        {
            RelationshipFilter = new V1::RelationshipFilter { ResourceType = "document", OptionalResourceId = "readme" },
        };
        req.OptionalPreconditions.Add(new V1::Precondition
        {
            Operation = V1::Precondition.Types.Operation.MustNotMatch,
            Filter = new V1::RelationshipFilter { ResourceType = "document", OptionalResourceId = "readme" },
        });

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            Service(cluster).DeleteRelationships(req, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
    }

    [Fact]
    public async Task LookupResources_streams_accessible_resources()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore,
            ("doc1", "viewer", "alice"),
            ("doc2", "editor", "alice"),
            ("doc3", "viewer", "bob"));

        var writer = new CollectingStreamWriter<V1::LookupResourcesResponse>();
        await Service(cluster).LookupResources(new V1::LookupResourcesRequest
        {
            ResourceObjectType = "document",
            Permission = "view",
            Subject = UserSubject("alice"),
        }, writer, FakeServerCallContext.Default);

        var ids = writer.Collected.Select(r => r.ResourceObjectId).OrderBy(x => x, StringComparer.Ordinal).ToList();
        Assert.Equal(new[] { "doc1", "doc2" }, ids);
        Assert.All(writer.Collected, r =>
            Assert.Equal(V1::LookupPermissionship.HasPermission, r.Permissionship));
        Assert.All(writer.Collected, r => Assert.False(string.IsNullOrEmpty(r.LookedUpAt.Token)));
    }

    private static readonly (string res, string rel, string subj)[] LookupSeed =
    [
        ("doc1", "viewer", "alice"),
        ("doc2", "editor", "alice"),
        ("doc3", "viewer", "bob"),
        ("doc4", "viewer", "alice"),
        ("doc5", "editor", "alice"),
        ("doc6", "viewer", "alice"),
        ("doc6", "editor", "alice"),
        ("doc7", "viewer", "alice"),
    ];

    private static V1::LookupResourcesRequest AliceViewLookup(string? cursor = null)
    {
        var req = new V1::LookupResourcesRequest
        {
            Consistency = new V1::Consistency { FullyConsistent = true },
            ResourceObjectType = "document",
            Permission = "view",
            Subject = UserSubject("alice"),
        };
        if (cursor is not null)
            req.OptionalCursor = new V1::Cursor { Token = cursor };
        return req;
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task LookupResources_without_limit_emits_after_result_cursor_on_every_item(bool useMembershipWalk)
    {
        // Issue #47: SpiceDB populates after_result_cursor on every streamed item whenever cursor sections
        // exist, with no limit condition (internal/services/v1/permissions.go). An unlimited stream must be
        // resumable from any item — on the Leopard path (walk on) as well as the live traversal.
        await using var cluster = await MeshTestCluster.CreateAsync(Schema, useMembershipWalk: useMembershipWalk);
        await SeedAsync(cluster.Datastore, LookupSeed);

        var writer = new CollectingStreamWriter<V1::LookupResourcesResponse>();
        await Service(cluster).LookupResources(AliceViewLookup(), writer, FakeServerCallContext.Default);

        Assert.NotEmpty(writer.Collected);
        Assert.All(writer.Collected, r => Assert.False(string.IsNullOrEmpty(r.AfterResultCursor?.Token)));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task LookupResources_unlimited_resume_from_mid_stream_cursor_yields_the_remainder(bool useMembershipWalk)
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema, useMembershipWalk: useMembershipWalk);
        await SeedAsync(cluster.Datastore, LookupSeed);

        var full = new CollectingStreamWriter<V1::LookupResourcesResponse>();
        await Service(cluster).LookupResources(AliceViewLookup(), full, FakeServerCallContext.Default);
        var fullIds = full.Collected.Select(r => r.ResourceObjectId).ToHashSet(StringComparer.Ordinal);
        Assert.Equal(new[] { "doc1", "doc2", "doc4", "doc5", "doc6", "doc7" }, fullIds.Order(StringComparer.Ordinal));

        for (var k = 1; k < full.Collected.Count; k++)
        {
            var resumed = new CollectingStreamWriter<V1::LookupResourcesResponse>();
            await Service(cluster).LookupResources(
                AliceViewLookup(full.Collected[k - 1].AfterResultCursor.Token), resumed, FakeServerCallContext.Default);

            var union = full.Collected.Take(k).Select(r => r.ResourceObjectId)
                .Concat(resumed.Collected.Select(r => r.ResourceObjectId))
                .ToHashSet(StringComparer.Ordinal);
            Assert.Equal(fullIds.Order(StringComparer.Ordinal), union.Order(StringComparer.Ordinal));

            // Resuming never re-emits an item already delivered before the cursor (beyond the per-entrypoint
            // repeats the live traversal emits within one stream).
            Assert.All(resumed.Collected, r => Assert.True(
                full.Collected.Skip(k).Any(f => f.ResourceObjectId == r.ResourceObjectId)));
        }
    }

    [Fact]
    public async Task LookupResources_candidate_cursor_resumes_when_the_walk_declines_on_resume()
    {
        // A cursor minted on the Leopard path must stay resumable when the accelerator is unavailable on
        // the resuming request (here: a cluster with the walk disabled, over the same data).
        string cursor;
        List<string> firstIds;
        await using (var walkOn = await MeshTestCluster.CreateAsync(Schema, useMembershipWalk: true))
        {
            await SeedAsync(walkOn.Datastore, LookupSeed);
            var full = new CollectingStreamWriter<V1::LookupResourcesResponse>();
            await Service(walkOn).LookupResources(AliceViewLookup(), full, FakeServerCallContext.Default);
            cursor = full.Collected[2].AfterResultCursor.Token;
            firstIds = full.Collected.Take(3).Select(r => r.ResourceObjectId).ToList();
        }

        // The stream came from the accelerator: its cursors are candidate-resume cursors.
        Assert.StartsWith("C:", System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(cursor)), StringComparison.Ordinal);

        await using var walkOff = await MeshTestCluster.CreateAsync(Schema, useMembershipWalk: false);
        await SeedAsync(walkOff.Datastore, LookupSeed);
        var resumed = new CollectingStreamWriter<V1::LookupResourcesResponse>();
        await Service(walkOff).LookupResources(AliceViewLookup(cursor), resumed, FakeServerCallContext.Default);

        var resumedIds = resumed.Collected.Select(r => r.ResourceObjectId).ToList();
        Assert.Empty(resumedIds.Intersect(firstIds));
        Assert.Equal(
            new[] { "doc1", "doc2", "doc4", "doc5", "doc6", "doc7" },
            firstIds.Concat(resumedIds).Order(StringComparer.Ordinal));
        Assert.All(resumed.Collected, r => Assert.False(string.IsNullOrEmpty(r.AfterResultCursor?.Token)));
    }

    [Fact]
    public async Task LookupResources_candidate_cursor_pages_through_the_live_fallback_with_a_limit()
    {
        // The live fallback keeps only the `limit` lowest ids past the cursor: chained limited resumes
        // over it must still return every remaining resource exactly once, in ordinal order.
        string cursor;
        await using (var walkOn = await MeshTestCluster.CreateAsync(Schema, useMembershipWalk: true))
        {
            await SeedAsync(walkOn.Datastore, LookupSeed);
            var full = new CollectingStreamWriter<V1::LookupResourcesResponse>();
            await Service(walkOn).LookupResources(AliceViewLookup(), full, FakeServerCallContext.Default);
            Assert.Equal("doc1", full.Collected[0].ResourceObjectId);
            cursor = full.Collected[0].AfterResultCursor.Token;
        }

        await using var walkOff = await MeshTestCluster.CreateAsync(Schema, useMembershipWalk: false);
        await SeedAsync(walkOff.Datastore, LookupSeed);

        var pages = new List<List<string>>();
        for (var guard = 0; guard < 10; guard++)
        {
            var request = AliceViewLookup(cursor);
            request.OptionalLimit = 2;
            var page = new CollectingStreamWriter<V1::LookupResourcesResponse>();
            await Service(walkOff).LookupResources(request, page, FakeServerCallContext.Default);
            if (page.Collected.Count == 0)
                break;
            pages.Add(page.Collected.Select(r => r.ResourceObjectId).ToList());
            cursor = page.Collected[^1].AfterResultCursor.Token;
        }

        Assert.Equal(
            new[] { new[] { "doc2", "doc4" }, new[] { "doc5", "doc6" }, new[] { "doc7" } },
            pages.Select(p => p.ToArray()));
    }

    [Fact]
    public async Task LookupSubjects_populates_modern_and_deprecated_fields()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"), ("readme", "editor", "bob"));

        var writer = new CollectingStreamWriter<V1::LookupSubjectsResponse>();
        await Service(cluster).LookupSubjects(new V1::LookupSubjectsRequest
        {
            Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
            Permission = "view",
            SubjectObjectType = "user",
        }, writer, FakeServerCallContext.Default);

        var ids = writer.Collected.Select(r => r.Subject.SubjectObjectId).OrderBy(x => x, StringComparer.Ordinal).ToList();
        Assert.Equal(new[] { "alice", "bob" }, ids);

        // Both the modern ResolvedSubject and the deprecated mirror fields are populated.
        Assert.All(writer.Collected, r =>
        {
            Assert.Equal(r.SubjectObjectId, r.Subject.SubjectObjectId);
            Assert.Equal(V1::LookupPermissionship.HasPermission, r.Subject.Permissionship);
            Assert.Equal(V1::LookupPermissionship.HasPermission, r.Permissionship);
        });
    }

    [Fact]
    public async Task ExpandPermissionTree_returns_union_tree_with_leaves()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"), ("readme", "editor", "bob"));

        var resp = await Service(cluster).ExpandPermissionTree(new V1::ExpandPermissionTreeRequest
        {
            Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
            Permission = "view",
        }, FakeServerCallContext.Default);

        Assert.NotNull(resp.TreeRoot);
        Assert.Equal("readme", resp.TreeRoot.ExpandedObject.ObjectId);
        Assert.Equal("view", resp.TreeRoot.ExpandedRelation);
        Assert.Equal(V1::PermissionRelationshipTree.TreeTypeOneofCase.Intermediate, resp.TreeRoot.TreeTypeCase);
        Assert.Equal(V1::AlgebraicSubjectSet.Types.Operation.Union, resp.TreeRoot.Intermediate.Operation);

        // Every concrete subject lands in some leaf's DirectSubjectSet.
        var subjectIds = CollectLeafSubjects(resp.TreeRoot).ToHashSet();
        Assert.Contains("alice", subjectIds);
        Assert.Contains("bob", subjectIds);
    }

    private static IEnumerable<string> CollectLeafSubjects(V1::PermissionRelationshipTree tree)
    {
        if (tree.TreeTypeCase == V1::PermissionRelationshipTree.TreeTypeOneofCase.Leaf)
        {
            foreach (var s in tree.Leaf.Subjects)
                yield return s.Object.ObjectId;
        }
        else if (tree.TreeTypeCase == V1::PermissionRelationshipTree.TreeTypeOneofCase.Intermediate)
        {
            foreach (var child in tree.Intermediate.Children)
                foreach (var id in CollectLeafSubjects(child))
                    yield return id;
        }
    }

    [Fact]
    public async Task CheckBulkPermissions_preserves_item_order_with_one_token()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore, ("readme", "viewer", "alice"));

        var req = new V1::CheckBulkPermissionsRequest();
        req.Items.Add(BulkItem("readme", "view", "alice")); // member
        req.Items.Add(BulkItem("readme", "view", "bob"));   // non-member
        req.Items.Add(BulkItem("other", "view", "alice"));  // non-member (no rel)

        var resp = await Service(cluster).CheckBulkPermissions(req, FakeServerCallContext.Default);

        Assert.False(string.IsNullOrEmpty(resp.CheckedAt.Token));
        Assert.Equal(3, resp.Pairs.Count);

        // Order preserved and each pair echoes its originating request.
        Assert.Equal("alice", resp.Pairs[0].Request.Subject.Object.ObjectId);
        Assert.Equal("bob", resp.Pairs[1].Request.Subject.Object.ObjectId);
        Assert.Equal("other", resp.Pairs[2].Request.Resource.ObjectId);

        Assert.Equal(V1::CheckPermissionResponse.Types.Permissionship.HasPermission, resp.Pairs[0].Item.Permissionship);
        Assert.Equal(V1::CheckPermissionResponse.Types.Permissionship.NoPermission, resp.Pairs[1].Item.Permissionship);
        Assert.Equal(V1::CheckPermissionResponse.Types.Permissionship.NoPermission, resp.Pairs[2].Item.Permissionship);

        // One pinned revision -> all pairs share the single batch token.
        Assert.All(resp.Pairs, p => Assert.Equal(V1::CheckBulkPermissionsPair.ResponseOneofCase.Item, p.ResponseCase));
    }

    [Fact]
    public async Task CheckBulkPermissions_caveated_item_carries_partial_caveat_info()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedCaveatedViewerAsync(cluster.Datastore, "readme", "alice");

        var req = new V1::CheckBulkPermissionsRequest();
        req.Items.Add(BulkItem("readme", "view", "alice"));

        var resp = await Service(cluster).CheckBulkPermissions(req, FakeServerCallContext.Default);

        var pair = Assert.Single(resp.Pairs);
        Assert.Equal(
            V1::CheckPermissionResponse.Types.Permissionship.ConditionalPermission,
            pair.Item.Permissionship);
        Assert.NotNull(pair.Item.PartialCaveatInfo);
        Assert.NotEmpty(pair.Item.PartialCaveatInfo.MissingRequiredContext);
    }

    [Fact]
    public async Task CheckBulkPermissions_invalid_consistency_token_is_invalid_argument()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var req = new V1::CheckBulkPermissionsRequest
        {
            Consistency = new V1::Consistency
            {
                AtExactSnapshot = new V1::ZedToken { Token = "not-a-real-token" },
            },
        };
        req.Items.Add(BulkItem("readme", "view", "alice"));

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            Service(cluster).CheckBulkPermissions(req, FakeServerCallContext.Default));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
    }

    [Fact]
    public async Task ImportBulkRelationships_loads_across_batches_and_returns_count()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var reader = new FakeAsyncStreamReader<V1::ImportBulkRelationshipsRequest>(
            ImportBatch(("doc1", "alice"), ("doc1", "bob")),
            new V1::ImportBulkRelationshipsRequest(), // empty batch -> skipped, no empty tx
            ImportBatch(("doc2", "carol")));

        var resp = await Service(cluster).ImportBulkRelationships(reader, FakeServerCallContext.Default);

        Assert.Equal(3ul, resp.NumLoaded);

        // The imported relationships are visible via a check.
        var check = await Service(cluster).CheckPermission(new V1::CheckPermissionRequest
        {
            Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "doc2" },
            Permission = "view",
            Subject = UserSubject("carol"),
        }, FakeServerCallContext.Default);
        Assert.Equal(V1::CheckPermissionResponse.Types.Permissionship.HasPermission, check.Permissionship);
    }

    [Fact]
    public async Task ImportBulkRelationships_duplicate_in_stream_is_already_exists_and_applies_nothing()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        // The duplicate sits in a LATER batch than a clean row: real SpiceDB rejects the whole stream
        // and applies nothing from it, including earlier batches (observed v1.49.2).
        var reader = new FakeAsyncStreamReader<V1::ImportBulkRelationshipsRequest>(
            ImportBatch(("doc1", "alice"), ("doc2", "bob")),
            ImportBatch(("doc1", "alice")));

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            Service(cluster).ImportBulkRelationships(reader, FakeServerCallContext.Default));

        Assert.Equal(StatusCode.AlreadyExists, ex.StatusCode);
        Assert.Contains("could not CREATE relationship", ex.Status.Detail);
        Assert.Contains("document:doc1#viewer@user:alice", ex.Status.Detail);
        Assert.Contains("already existed", ex.Status.Detail);

        // Whole-stream atomicity: the clean row from the first batch must not be visible.
        var check = await Service(cluster).CheckPermission(new V1::CheckPermissionRequest
        {
            Consistency = new V1::Consistency { FullyConsistent = true },
            Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "doc2" },
            Permission = "view",
            Subject = UserSubject("bob"),
        }, FakeServerCallContext.Default);
        Assert.Equal(V1::CheckPermissionResponse.Types.Permissionship.NoPermission, check.Permissionship);
    }

    [Fact]
    public async Task ImportBulkRelationships_preexisting_row_is_already_exists()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore, ("doc1", "viewer", "alice"));

        var reader = new FakeAsyncStreamReader<V1::ImportBulkRelationshipsRequest>(
            ImportBatch(("doc1", "alice"), ("doc2", "bob")));

        var ex = await Assert.ThrowsAsync<RpcException>(() =>
            Service(cluster).ImportBulkRelationships(reader, FakeServerCallContext.Default));

        Assert.Equal(StatusCode.AlreadyExists, ex.StatusCode);
        Assert.Contains("could not CREATE relationship", ex.Status.Detail);
        Assert.Contains("document:doc1#viewer@user:alice", ex.Status.Detail);

        // Nothing from the failed import applied.
        var check = await Service(cluster).CheckPermission(new V1::CheckPermissionRequest
        {
            Consistency = new V1::Consistency { FullyConsistent = true },
            Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "doc2" },
            Permission = "view",
            Subject = UserSubject("bob"),
        }, FakeServerCallContext.Default);
        Assert.Equal(V1::CheckPermissionResponse.Types.Permissionship.NoPermission, check.Permissionship);
    }

    [Fact]
    public async Task ExportBulkRelationships_pages_all_over_one_snapshot()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore,
            ("doc1", "viewer", "alice"),
            ("doc2", "viewer", "bob"),
            ("doc3", "editor", "carol"),
            ("doc4", "viewer", "dave"),
            ("doc5", "viewer", "erin"));

        var writer = new CollectingStreamWriter<V1::ExportBulkRelationshipsResponse>();
        await Service(cluster).ExportBulkRelationships(new V1::ExportBulkRelationshipsRequest
        {
            OptionalLimit = 2, // force multiple pages
        }, writer, FakeServerCallContext.Default);

        var all = writer.Collected.SelectMany(p => p.Relationships).ToList();
        Assert.Equal(5, all.Count);

        // Every page carries a continuation cursor token.
        Assert.All(writer.Collected, p => Assert.NotNull(p.AfterResultCursor));

        // Multiple pages were produced under the limit.
        Assert.True(writer.Collected.Count >= 2);

        var resourceIds = all.Select(r => r.Resource.ObjectId).OrderBy(x => x, StringComparer.Ordinal).ToList();
        Assert.Equal(new[] { "doc1", "doc2", "doc3", "doc4", "doc5" }, resourceIds);
    }

    [Fact]
    public async Task ExportBulkRelationships_filter_narrows_results()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        await SeedAsync(cluster.Datastore,
            ("doc1", "viewer", "alice"),
            ("doc1", "editor", "bob"),
            ("doc2", "viewer", "carol"));

        var writer = new CollectingStreamWriter<V1::ExportBulkRelationshipsResponse>();
        await Service(cluster).ExportBulkRelationships(new V1::ExportBulkRelationshipsRequest
        {
            OptionalRelationshipFilter = new V1::RelationshipFilter
            {
                ResourceType = "document",
                OptionalResourceId = "doc1",
                OptionalRelation = "viewer",
            },
        }, writer, FakeServerCallContext.Default);

        var all = writer.Collected.SelectMany(p => p.Relationships).ToList();
        var only = Assert.Single(all);
        Assert.Equal("doc1", only.Resource.ObjectId);
        Assert.Equal("viewer", only.Relation);
        Assert.Equal("alice", only.Subject.Object.ObjectId);
    }

    private static V1::CheckBulkPermissionsRequestItem BulkItem(string doc, string perm, string user) => new()
    {
        Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = doc },
        Permission = perm,
        Subject = UserSubject(user),
    };

    private static V1::ImportBulkRelationshipsRequest ImportBatch(params (string doc, string user)[] tuples)
    {
        var batch = new V1::ImportBulkRelationshipsRequest();
        foreach (var (doc, user) in tuples)
        {
            batch.Relationships.Add(new V1::Relationship
            {
                Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = doc },
                Relation = "viewer",
                Subject = UserSubject(user),
            });
        }
        return batch;
    }

    private static V1::WriteRelationshipsRequest WriteReq(
        V1::RelationshipUpdate.Types.Operation op, string doc, string user)
    {
        var req = new V1::WriteRelationshipsRequest();
        req.Updates.Add(new V1::RelationshipUpdate
        {
            Operation = op,
            Relationship = new V1::Relationship
            {
                Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = doc },
                Relation = "viewer",
                Subject = UserSubject(user),
            },
        });
        return req;
    }

    /// <summary>A server stream writer that records every response.</summary>
    private sealed class CollectingStreamWriter<T> : IServerStreamWriter<T>
    {
        public List<T> Collected { get; } = [];
        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(T message)
        {
            Collected.Add(message);
            return Task.CompletedTask;
        }
    }

    // A cross-silo dispatch failure (which OrleansDispatcher re-raises as a [GenerateSerializer]
    // DispatchFailedException carrying its mapped code) must surface at the v1 front door as the
    // corresponding stable gRPC status, NOT as an opaque error. A genuine Orleans transport failure
    // cannot be injected into the in-process TestCluster, so we drive the SAME exception the dispatcher
    // would raise through the service via a fake checker, asserting the front-door mapping; the
    // dispatcher's classification of raw Orleans exceptions is covered by DispatchErrorMapperTests.
    [Theory]
    [InlineData(DispatchErrorCode.Unavailable, StatusCode.Unavailable)]
    [InlineData(DispatchErrorCode.Cancelled, StatusCode.Cancelled)]
    [InlineData(DispatchErrorCode.DeadlineExceeded, StatusCode.DeadlineExceeded)]
    [InlineData(DispatchErrorCode.Internal, StatusCode.Internal)]
    public async Task CheckPermission_maps_a_cross_silo_dispatch_failure_to_its_grpc_code(
        DispatchErrorCode code, StatusCode expected)
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = new AuthzedPermissionsV1Service(
            new ThrowingChecker(new DispatchFailedException(code, "boom")),
            cluster.GrainFactory,
            cluster.ReverseOps,
            cluster.RelationshipReads,
            cluster.Services.GetRequiredService<ISchemaProvider>());

        var ex = await Assert.ThrowsAsync<RpcException>(() => service.CheckPermission(
            new V1::CheckPermissionRequest
            {
                Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
                Permission = "view",
                Subject = UserSubject("alice"),
            },
            FakeServerCallContext.Default));

        Assert.Equal(expected, ex.StatusCode);
    }

    /// <summary>An <see cref="IPermissionChecker"/> that always throws the supplied exception.</summary>
    private sealed class ThrowingChecker(Exception ex) : IPermissionChecker
    {
        public Task<PermissionCheckResult> Check(
            string resourceType, string resourceId, string permission, ObjectAndRelation subject,
            IReadOnlyDictionary<string, object?>? caveatContext, ConsistencyRequirement? consistency = null,
            CancellationToken ct = default) => throw ex;

        public Task<BatchCheckResult> BatchCheck(
            IReadOnlyList<BatchCheckItem> items, ConsistencyRequirement? consistency = null,
            CancellationToken ct = default) => throw ex;
    }

    /// <summary>A client-stream reader that replays a fixed sequence of inbound messages.</summary>
    private sealed class FakeAsyncStreamReader<T>(params T[] messages) : IAsyncStreamReader<T>
    {
        private int _index = -1;

        public T Current => messages[_index];

        public Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _index++;
            return Task.FromResult(_index < messages.Length);
        }
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
