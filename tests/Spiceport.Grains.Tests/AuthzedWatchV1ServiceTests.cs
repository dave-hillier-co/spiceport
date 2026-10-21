using System.Threading.Channels;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Spiceport.Api;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains;
using V1 = Authzed.Api.V1;
using WireRelationship = Spiceport.Grains.Abstractions.RelationshipWire;
using WireUpdate = Spiceport.Grains.Abstractions.RelationshipUpdateWire;
using WireOp = Spiceport.Grains.Abstractions.RelationshipUpdateOpWire;
using WireWriteArgs = Spiceport.Grains.Abstractions.WriteRelationshipsArgs;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Drives the server-streaming <c>authzed.api.v1</c> <see cref="AuthzedWatchV1Service"/> IN-PROCESS (no
/// Kestrel). Verifies: tailing from head sees a subsequent write with its changes_through token; resuming
/// from a pre-write token replays the write exactly once; cancellation terminates the stream cleanly; the
/// optional_object_types filter excludes updates of other resource types; optional_relationship_filters
/// emit only updates matching any filter, and cannot be combined with optional_object_types.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class AuthzedWatchV1ServiceTests
{
    private const string Schema = """
        definition user {}
        definition document {
            relation viewer: user
            permission view = viewer
        }
        definition folder {
            relation viewer: user
            permission view = viewer
        }
        """;

    private static AuthzedWatchV1Service Service(MeshTestCluster cluster) =>
        new(cluster.Datastore, cluster.Services.GetRequiredService<ISchemaProvider>());

    private static Task WriteViewer(MeshTestCluster cluster, string type, string id, string user)
    {
        var update = new WireUpdate(
            WireOp.Touch,
            new WireRelationship(type, id, "viewer", "user", user, "...", null, null, null));
        return cluster.Relationships.WriteRelationships(new WireWriteArgs(new List<WireUpdate> { update }));
    }

    private static Task WriteViewerWithExpiration(
        MeshTestCluster cluster, string type, string id, string user, DateTimeOffset expiration)
    {
        var update = new WireUpdate(
            WireOp.Touch,
            new WireRelationship(type, id, "viewer", "user", user, "...", null, null, expiration));
        return cluster.Relationships.WriteRelationships(new WireWriteArgs(new List<WireUpdate> { update }));
    }

    [Fact]
    public async Task Watch_from_head_sees_subsequent_write_with_token()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new CollectingStreamWriter<V1::WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);

        var watchTask = service.Watch(new V1::WatchRequest(), writer, ctx);

        await WriteViewer(cluster, "document", "doc1", "alice");

        var response = await writer.WaitForNext(TimeSpan.FromSeconds(10), watchTask);
        cts.Cancel();
        await watchTask;

        Assert.False(string.IsNullOrEmpty(response.ChangesThrough.Token));
        var update = Assert.Single(response.Updates);
        Assert.Equal(V1::RelationshipUpdate.Types.Operation.Touch, update.Operation);
        Assert.Equal("doc1", update.Relationship.Resource.ObjectId);
        Assert.Equal("alice", update.Relationship.Subject.Object.ObjectId);
    }

    [Fact]
    public async Task Watch_resumes_from_pre_write_token_and_replays_the_write()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        var preWrite = await cluster.Datastore.HeadRevision(CancellationToken.None);
        var datastoreId = await cluster.Datastore.GetUniqueId(CancellationToken.None);
        var startToken = ZedTokens.FromRevision(preWrite.Revision, preWrite.SchemaHash, datastoreId);

        await WriteViewer(cluster, "document", "doc2", "bob");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new CollectingStreamWriter<V1::WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);

        var request = new V1::WatchRequest { OptionalStartCursor = new V1::ZedToken { Token = startToken.Token } };
        var watchTask = service.Watch(request, writer, ctx);

        var response = await writer.WaitForNext(TimeSpan.FromSeconds(10));
        cts.Cancel();
        await watchTask;

        var update = Assert.Single(response.Updates);
        Assert.Equal("doc2", update.Relationship.Resource.ObjectId);
        Assert.Equal("bob", update.Relationship.Subject.Object.ObjectId);
    }

    [Fact]
    public async Task Watch_includes_relationship_expiration()
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
        var service = Service(cluster);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new CollectingStreamWriter<V1::WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);

        var watchTask = service.Watch(new V1::WatchRequest(), writer, ctx);

        var expiresAt = new DateTimeOffset(2099, 1, 1, 0, 0, 0, TimeSpan.Zero);
        await WriteViewerWithExpiration(cluster, "document", "doc3", "dana", expiresAt);

        var response = await writer.WaitForNext(TimeSpan.FromSeconds(10), watchTask);
        cts.Cancel();
        await watchTask;

        var update = Assert.Single(response.Updates);
        Assert.Equal(
            Google.Protobuf.WellKnownTypes.Timestamp.FromDateTimeOffset(expiresAt),
            update.Relationship.OptionalExpiresAt);
    }

    [Fact]
    public async Task Watch_cancellation_stops_the_stream()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        using var cts = new CancellationTokenSource();
        var writer = new CollectingStreamWriter<V1::WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);

        var watchTask = service.Watch(new V1::WatchRequest(), writer, ctx);

        cts.Cancel();
        await watchTask.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.True(watchTask.IsCompletedSuccessfully);
        Assert.Empty(writer.Collected);
    }

    [Fact]
    public async Task Watch_object_type_filter_excludes_other_types()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new CollectingStreamWriter<V1::WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);

        var request = new V1::WatchRequest();
        request.OptionalObjectTypes.Add("document");
        var watchTask = service.Watch(request, writer, ctx);

        // A folder write must be filtered out; a document write must come through.
        await WriteViewer(cluster, "folder", "f1", "alice");
        await WriteViewer(cluster, "document", "doc3", "carol");

        var response = await writer.WaitForNext(TimeSpan.FromSeconds(10), watchTask);
        cts.Cancel();
        await watchTask;

        // The emitted response carries only the document update, never the folder update.
        var update = Assert.Single(response.Updates);
        Assert.Equal("document", update.Relationship.Resource.ObjectType);
        Assert.Equal("doc3", update.Relationship.Resource.ObjectId);

        Assert.All(writer.Collected,
            r => Assert.All(r.Updates, u => Assert.Equal("document", u.Relationship.Resource.ObjectType)));
    }

    [Fact]
    public async Task Watch_with_relationship_filter_emits_only_matching_updates()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new CollectingStreamWriter<V1::WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);

        var request = new V1::WatchRequest();
        request.OptionalRelationshipFilters.Add(new V1::RelationshipFilter
        {
            ResourceType = "document",
            OptionalRelation = "viewer",
            OptionalSubjectFilter = new V1::SubjectFilter { SubjectType = "user", OptionalSubjectId = "alice" },
        });
        var watchTask = service.Watch(request, writer, ctx);

        // Same resource type and relation, but a different subject: filtered out server-side.
        await WriteViewer(cluster, "document", "doc1", "bob");
        await WriteViewer(cluster, "document", "doc2", "alice");

        var response = await writer.WaitForNext(TimeSpan.FromSeconds(10), watchTask);
        cts.Cancel();
        await watchTask;

        var update = Assert.Single(response.Updates);
        Assert.Equal("doc2", update.Relationship.Resource.ObjectId);
        Assert.Equal("alice", update.Relationship.Subject.Object.ObjectId);
        Assert.All(writer.Collected,
            r => Assert.All(r.Updates, u => Assert.Equal("alice", u.Relationship.Subject.Object.ObjectId)));
    }

    [Fact]
    public async Task Watch_with_multiple_relationship_filters_emits_updates_matching_any()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new CollectingStreamWriter<V1::WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);

        var request = new V1::WatchRequest();
        request.OptionalRelationshipFilters.Add(new V1::RelationshipFilter
        {
            ResourceType = "document",
            OptionalResourceId = "doc1",
        });
        request.OptionalRelationshipFilters.Add(new V1::RelationshipFilter
        {
            ResourceType = "folder",
            OptionalResourceIdPrefix = "f",
        });
        var watchTask = service.Watch(request, writer, ctx);

        // doc2 matches neither filter; f1 matches the second; doc1 matches the first.
        await WriteViewer(cluster, "document", "doc2", "alice");
        await WriteViewer(cluster, "folder", "f1", "alice");
        await WriteViewer(cluster, "document", "doc1", "alice");

        var first = await writer.WaitForNext(TimeSpan.FromSeconds(10), watchTask);
        var second = await writer.WaitForNext(TimeSpan.FromSeconds(10), watchTask);
        cts.Cancel();
        await watchTask;

        Assert.Equal("f1", Assert.Single(first.Updates).Relationship.Resource.ObjectId);
        Assert.Equal("doc1", Assert.Single(second.Updates).Relationship.Resource.ObjectId);
        Assert.All(writer.Collected,
            r => Assert.All(r.Updates, u => Assert.NotEqual("doc2", u.Relationship.Resource.ObjectId)));
    }

    [Fact]
    public async Task Watch_with_both_object_types_and_relationship_filters_is_invalid_argument()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new CollectingStreamWriter<V1::WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);

        var request = new V1::WatchRequest();
        request.OptionalObjectTypes.Add("document");
        request.OptionalRelationshipFilters.Add(new V1::RelationshipFilter { ResourceType = "document" });

        var ex = await Assert.ThrowsAsync<RpcException>(() => service.Watch(request, writer, ctx));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal("cannot specify both object types and relationship filters", ex.Status.Detail);
        Assert.Empty(writer.Collected);
    }

    private const string GroupSchema = """
        definition user {}
        definition group {
            relation member: user
        }
        definition document {
            relation viewer: user | group | group#member
            permission view = viewer
        }
        """;

    private static Task WriteGroupViewer(MeshTestCluster cluster, string id, string group, string subjectRelation)
    {
        var update = new WireUpdate(
            WireOp.Touch,
            new WireRelationship("document", id, "viewer", "group", group, subjectRelation, null, null, null));
        return cluster.Relationships.WriteRelationships(new WireWriteArgs(new List<WireUpdate> { update }));
    }

    [Fact]
    public async Task Watch_with_empty_subject_relation_emits_only_ellipsis_subjects()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(GroupSchema);
        var service = Service(cluster);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new CollectingStreamWriter<V1::WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);

        // A present-but-empty optional_relation selects only ellipsis subjects, as upstream's
        // RelationshipsFilterFromPublicFilter does (WithEllipsisRelation).
        var request = new V1::WatchRequest();
        request.OptionalRelationshipFilters.Add(new V1::RelationshipFilter
        {
            OptionalSubjectFilter = new V1::SubjectFilter
            {
                SubjectType = "group",
                OptionalRelation = new V1::SubjectFilter.Types.RelationFilter { Relation = string.Empty },
            },
        });
        var watchTask = service.Watch(request, writer, ctx);

        await WriteGroupViewer(cluster, "doc1", "eng", "member");
        await WriteGroupViewer(cluster, "doc2", "eng", "...");

        var response = await writer.WaitForNext(TimeSpan.FromSeconds(10), watchTask);
        cts.Cancel();
        await watchTask;

        var update = Assert.Single(response.Updates);
        Assert.Equal("doc2", update.Relationship.Resource.ObjectId);
        Assert.Equal(string.Empty, update.Relationship.Subject.OptionalRelation);
        Assert.All(writer.Collected,
            r => Assert.All(r.Updates, u => Assert.NotEqual("doc1", u.Relationship.Resource.ObjectId)));
    }

    [Fact]
    public async Task Watch_with_named_subject_relation_emits_only_that_relation()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(GroupSchema);
        var service = Service(cluster);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new CollectingStreamWriter<V1::WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);

        var request = new V1::WatchRequest();
        request.OptionalRelationshipFilters.Add(new V1::RelationshipFilter
        {
            OptionalSubjectFilter = new V1::SubjectFilter
            {
                SubjectType = "group",
                OptionalRelation = new V1::SubjectFilter.Types.RelationFilter { Relation = "member" },
            },
        });
        var watchTask = service.Watch(request, writer, ctx);

        await WriteGroupViewer(cluster, "doc1", "eng", "...");
        await WriteGroupViewer(cluster, "doc2", "eng", "member");

        var response = await writer.WaitForNext(TimeSpan.FromSeconds(10), watchTask);
        cts.Cancel();
        await watchTask;

        var update = Assert.Single(response.Updates);
        Assert.Equal("doc2", update.Relationship.Resource.ObjectId);
        Assert.Equal("member", update.Relationship.Subject.OptionalRelation);
    }

    [Fact]
    public async Task Watch_with_absent_subject_relation_emits_any_subject_relation()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(GroupSchema);
        var service = Service(cluster);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new CollectingStreamWriter<V1::WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);

        var request = new V1::WatchRequest();
        request.OptionalRelationshipFilters.Add(new V1::RelationshipFilter
        {
            OptionalSubjectFilter = new V1::SubjectFilter { SubjectType = "group" },
        });
        var watchTask = service.Watch(request, writer, ctx);

        await WriteGroupViewer(cluster, "doc1", "eng", "member");
        await WriteGroupViewer(cluster, "doc2", "eng", "...");

        var first = await writer.WaitForNext(TimeSpan.FromSeconds(10), watchTask);
        var second = await writer.WaitForNext(TimeSpan.FromSeconds(10), watchTask);
        cts.Cancel();
        await watchTask;

        Assert.Equal("doc1", Assert.Single(first.Updates).Relationship.Resource.ObjectId);
        Assert.Equal("doc2", Assert.Single(second.Updates).Relationship.Resource.ObjectId);
    }

    public static TheoryData<V1::RelationshipFilter, StatusCode, string> InvalidFilters() => new()
    {
        {
            new V1::RelationshipFilter(),
            StatusCode.InvalidArgument,
            "the relationship filter provided is not valid: at least one field must be set"
        },
        {
            new V1::RelationshipFilter { ResourceType = "document", OptionalResourceId = "d", OptionalResourceIdPrefix = "d" },
            StatusCode.InvalidArgument,
            "the relationship filter provided is not valid: resource_id and resource_id_prefix cannot be set at the same time"
        },
        {
            new V1::RelationshipFilter { ResourceType = "nope" },
            StatusCode.FailedPrecondition,
            "object definition `nope` not found"
        },
        {
            new V1::RelationshipFilter { ResourceType = "document", OptionalRelation = "nope" },
            StatusCode.FailedPrecondition,
            "relation/permission `nope` not found under definition `document`"
        },
        {
            new V1::RelationshipFilter { OptionalSubjectFilter = new V1::SubjectFilter { SubjectType = "nope" } },
            StatusCode.FailedPrecondition,
            "object definition `nope` not found"
        },
        {
            new V1::RelationshipFilter
            {
                OptionalSubjectFilter = new V1::SubjectFilter
                {
                    SubjectType = "group",
                    OptionalRelation = new V1::SubjectFilter.Types.RelationFilter { Relation = "nope" },
                },
            },
            StatusCode.FailedPrecondition,
            "relation/permission `nope` not found under definition `group`"
        },
    };

    [Theory]
    [MemberData(nameof(InvalidFilters))]
    public async Task Watch_with_invalid_relationship_filter_is_rejected(
        V1::RelationshipFilter filter, StatusCode expectedCode, string expectedDetail)
    {
        await using var cluster = await MeshTestCluster.CreateAsync(GroupSchema);
        var service = Service(cluster);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new CollectingStreamWriter<V1::WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);

        var request = new V1::WatchRequest();
        request.OptionalRelationshipFilters.Add(filter);

        var ex = await Assert.ThrowsAsync<RpcException>(() => service.Watch(request, writer, ctx));

        Assert.Equal(expectedCode, ex.StatusCode);
        Assert.Equal(expectedDetail, ex.Status.Detail);
        Assert.Empty(writer.Collected);
    }

    [Fact]
    public async Task Watch_with_checkpoints_emits_a_checkpoint_response_after_a_change()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new CollectingStreamWriter<V1::WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);

        var request = new V1::WatchRequest();
        request.OptionalUpdateKinds.Add(V1::WatchKind.IncludeRelationshipUpdates);
        request.OptionalUpdateKinds.Add(V1::WatchKind.IncludeCheckpoints);
        var watchTask = service.Watch(request, writer, ctx);

        await WriteViewer(cluster, "document", "doc1", "alice");

        // First the content response, then the checkpoint marking the revision progressed through.
        var content = await writer.WaitForNext(TimeSpan.FromSeconds(10), watchTask);
        var checkpoint = await writer.WaitForNext(TimeSpan.FromSeconds(10), watchTask);
        cts.Cancel();
        await watchTask;

        Assert.False(content.IsCheckpoint);
        Assert.Single(content.Updates);
        Assert.True(checkpoint.IsCheckpoint);
        Assert.Empty(checkpoint.Updates);
        Assert.False(string.IsNullOrEmpty(checkpoint.ChangesThrough.Token));
    }

    [Fact]
    public async Task Watch_with_checkpoints_emits_checkpoint_even_when_filter_excludes_the_change()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new CollectingStreamWriter<V1::WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);

        var request = new V1::WatchRequest();
        request.OptionalObjectTypes.Add("document");
        request.OptionalUpdateKinds.Add(V1::WatchKind.IncludeRelationshipUpdates);
        request.OptionalUpdateKinds.Add(V1::WatchKind.IncludeCheckpoints);
        var watchTask = service.Watch(request, writer, ctx);

        // A folder write is filtered out of content, but the checkpoint must still surface so the
        // consumer sees the revision advanced (liveness for a filtered subset).
        await WriteViewer(cluster, "folder", "f1", "alice");

        var first = await writer.WaitForNext(TimeSpan.FromSeconds(10), watchTask);
        cts.Cancel();
        await watchTask;

        Assert.True(first.IsCheckpoint);
        Assert.Empty(first.Updates);
    }

    [Fact]
    public async Task Watch_with_checkpoints_only_still_emits_checkpoints_with_no_content()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new CollectingStreamWriter<V1::WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);

        // Requesting checkpoints ONLY (no IncludeRelationshipUpdates, no IncludeSchemaUpdates): the
        // resulting WatchContent has just the Checkpoints bit set. Content selection is additive in
        // name only here — checkpoint emission is driven by commit activity, not by the content mask
        // matching anything, so this must still see a checkpoint (and never relationship content).
        var request = new V1::WatchRequest();
        request.OptionalUpdateKinds.Add(V1::WatchKind.IncludeCheckpoints);
        var watchTask = service.Watch(request, writer, ctx);

        await WriteViewer(cluster, "document", "doc1", "alice");

        var response = await writer.WaitForNext(TimeSpan.FromSeconds(10), watchTask);
        cts.Cancel();
        await watchTask;

        Assert.True(response.IsCheckpoint);
        Assert.Empty(response.Updates);
        Assert.False(string.IsNullOrEmpty(response.ChangesThrough.Token));
    }

    private static AuthzedPermissionsV1Service Permissions(MeshTestCluster cluster) =>
        new(cluster.Checker, cluster.GrainFactory, cluster.ReverseOps, cluster.RelationshipReads,
            cluster.Services.GetRequiredService<ISchemaProvider>());

    private static V1::Relationship ViewerProto(string type, string id, string user) => new()
    {
        Resource = new V1::ObjectReference { ObjectType = type, ObjectId = id },
        Relation = "viewer",
        Subject = new V1::SubjectReference { Object = new V1::ObjectReference { ObjectType = "user", ObjectId = user } },
    };

    private static V1::WriteRelationshipsRequest TouchViewer(string type, string id, string user) => new()
    {
        Updates =
        {
            new V1::RelationshipUpdate
            {
                Operation = V1::RelationshipUpdate.Types.Operation.Touch,
                Relationship = ViewerProto(type, id, user),
            },
        },
    };

    private static Google.Protobuf.WellKnownTypes.Struct Metadata(string json) =>
        Google.Protobuf.JsonParser.Default.Parse<Google.Protobuf.WellKnownTypes.Struct>(json);

    /// <summary>
    /// SpiceDB (internal/services/v1/watch.go): every response for a revision carries the transaction
    /// metadata the write supplied as <c>optional_transaction_metadata</c> (set when the revision has
    /// exactly one) and the revision's whole list as <c>full_revision_metadata</c>.
    /// </summary>
    [Fact]
    public async Task Watch_returns_the_writes_transaction_metadata_on_its_response()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new CollectingStreamWriter<V1::WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);
        var watchTask = service.Watch(new V1::WatchRequest(), writer, ctx);

        var metadata = Metadata("""{"source":"importer","attempt":2,"tags":["a","b"],"nested":{"ok":true}}""");
        var write = TouchViewer("document", "doc1", "alice");
        write.OptionalTransactionMetadata = metadata;
        await Permissions(cluster).WriteRelationships(write, ctx);

        var response = await writer.WaitForNext(TimeSpan.FromSeconds(10), watchTask);
        cts.Cancel();
        await watchTask;

        Assert.Single(response.Updates);
        Assert.Equal(metadata, response.OptionalTransactionMetadata);
        Assert.Equal(metadata, Assert.Single(response.FullRevisionMetadata));
    }

    [Fact]
    public async Task Watch_returns_the_deletes_transaction_metadata_with_its_deletions()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);
        var permissions = Permissions(cluster);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var ctx = new FakeServerCallContext(cts.Token);
        var seed = TouchViewer("document", "doc1", "alice");
        seed.Updates.Add(new V1::RelationshipUpdate
        {
            Operation = V1::RelationshipUpdate.Types.Operation.Touch,
            Relationship = ViewerProto("document", "doc1", "bob"),
        });
        await permissions.WriteRelationships(seed, ctx);

        var writer = new CollectingStreamWriter<V1::WatchResponse>();
        var watchTask = service.Watch(new V1::WatchRequest(), writer, ctx);

        var metadata = Metadata("""{"reason":"offboarding"}""");
        await permissions.DeleteRelationships(new V1::DeleteRelationshipsRequest
        {
            RelationshipFilter = new V1::RelationshipFilter { ResourceType = "document", OptionalResourceId = "doc1" },
            OptionalTransactionMetadata = metadata,
        }, ctx);

        var response = await writer.WaitForNext(TimeSpan.FromSeconds(10), watchTask);
        cts.Cancel();
        await watchTask;

        Assert.Equal(2, response.Updates.Count);
        Assert.All(response.Updates, u => Assert.Equal(V1::RelationshipUpdate.Types.Operation.Delete, u.Operation));
        Assert.Equal(metadata, response.OptionalTransactionMetadata);
        Assert.Equal(metadata, Assert.Single(response.FullRevisionMetadata));
    }

    /// <summary>
    /// No metadata, and an empty struct (SpiceDB stores none for it), leave both fields unset; a
    /// checkpoint never carries metadata, because SpiceDB's datastores emit checkpoints as their own
    /// metadata-free changes.
    /// </summary>
    [Fact]
    public async Task Watch_leaves_metadata_unset_without_metadata_for_an_empty_struct_and_on_checkpoints()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);
        var permissions = Permissions(cluster);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new CollectingStreamWriter<V1::WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);
        var request = new V1::WatchRequest();
        request.OptionalUpdateKinds.Add(V1::WatchKind.IncludeRelationshipUpdates);
        request.OptionalUpdateKinds.Add(V1::WatchKind.IncludeCheckpoints);
        var watchTask = service.Watch(request, writer, ctx);

        var tagged = TouchViewer("document", "doc1", "alice");
        tagged.OptionalTransactionMetadata = Metadata("""{"k":"v"}""");
        await permissions.WriteRelationships(tagged, ctx);
        await permissions.WriteRelationships(TouchViewer("document", "doc2", "bob"), ctx);
        var empty = TouchViewer("document", "doc3", "carol");
        empty.OptionalTransactionMetadata = new Google.Protobuf.WellKnownTypes.Struct();
        await permissions.WriteRelationships(empty, ctx);

        var responses = new List<V1::WatchResponse>();
        // Every change is followed by a checkpoint, so read through the checkpoint after the third.
        while (responses.Count(r => !r.IsCheckpoint) < 3 || !responses[^1].IsCheckpoint)
            responses.Add(await writer.WaitForNext(TimeSpan.FromSeconds(10), watchTask));
        cts.Cancel();
        await watchTask;

        var content = responses.Where(r => !r.IsCheckpoint).ToList();
        Assert.Equal(Metadata("""{"k":"v"}"""), content[0].OptionalTransactionMetadata);
        Assert.Null(content[1].OptionalTransactionMetadata);
        Assert.Empty(content[1].FullRevisionMetadata);
        Assert.Null(content[2].OptionalTransactionMetadata);
        Assert.Empty(content[2].FullRevisionMetadata);

        var checkpoints = responses.Where(r => r.IsCheckpoint).ToList();
        Assert.NotEmpty(checkpoints);
        Assert.All(checkpoints, c =>
        {
            Assert.Null(c.OptionalTransactionMetadata);
            Assert.Empty(c.FullRevisionMetadata);
        });
    }

    private const string ObjectTypePattern =
        "\"^([a-z][a-z0-9_]{1,62}[a-z0-9]/)*[a-z][a-z0-9_]{1,62}[a-z0-9]$\"";

    private static V1::WatchRequest Request(
        string[]? objectTypes = null,
        V1::RelationshipFilter[]? filters = null,
        string? startCursor = null,
        V1::WatchKind[]? kinds = null)
    {
        var request = new V1::WatchRequest();
        request.OptionalObjectTypes.Add(objectTypes ?? []);
        request.OptionalRelationshipFilters.Add(filters ?? []);
        request.OptionalUpdateKinds.Add(kinds ?? []);
        if (startCursor is not null)
            request.OptionalStartCursor = new V1::ZedToken { Token = startCursor };
        return request;
    }

    private static V1::RelationshipFilter Filter(string resourceType) => new() { ResourceType = resourceType };

    /// <summary>
    /// Malformed requests and the status SpiceDB v1.49.2 answers each with, observed from a running
    /// <c>spicedb serve</c> over memdb: the protoc-gen-validate interceptor (<c>ValidateAll</c>) first, then
    /// the handler's own checks in order (object types with relationship filters, the start cursor, each
    /// filter against the schema).
    /// </summary>
    public static TheoryData<string, V1::WatchRequest, StatusCode, string> MalformedRequests() => new()
    {
        {
            "object type breaks its pattern",
            Request(objectTypes: ["Doc"]),
            StatusCode.InvalidArgument,
            $"invalid WatchRequest.OptionalObjectTypes[0]: value does not match regex pattern {ObjectTypePattern}"
        },
        {
            "every bad object type is reported",
            Request(objectTypes: ["Doc", "x"]),
            StatusCode.InvalidArgument,
            $"invalid WatchRequest.OptionalObjectTypes[0]: value does not match regex pattern {ObjectTypePattern}; " +
            $"invalid WatchRequest.OptionalObjectTypes[1]: value does not match regex pattern {ObjectTypePattern}"
        },
        {
            "object type too long",
            Request(objectTypes: [new string('a', 129)]),
            StatusCode.InvalidArgument,
            "invalid WatchRequest.OptionalObjectTypes[0]: value length must be at most 128 bytes; " +
            $"invalid WatchRequest.OptionalObjectTypes[0]: value does not match regex pattern {ObjectTypePattern}"
        },
        {
            "filter resource type breaks its pattern",
            Request(filters: [Filter("Doc")]),
            StatusCode.InvalidArgument,
            "invalid WatchRequest.OptionalRelationshipFilters[0]: embedded message failed validation | caused by: " +
            "invalid RelationshipFilter.ResourceType: value does not match regex pattern " +
            "\"^(([a-z][a-z0-9_]{1,61}[a-z0-9]/)*[a-z][a-z0-9_]{1,62}[a-z0-9])?$\""
        },
        {
            "filter resource id is a wildcard",
            Request(filters: [new V1::RelationshipFilter { ResourceType = "document", OptionalResourceId = "*" }]),
            StatusCode.InvalidArgument,
            "invalid WatchRequest.OptionalRelationshipFilters[0]: embedded message failed validation | caused by: " +
            "invalid RelationshipFilter.OptionalResourceId: value does not match regex pattern " +
            "\"^([a-zA-Z0-9/_|\\\\-=+]{1,})?$\""
        },
        {
            "field validation precedes the object types and filters check",
            Request(objectTypes: ["Doc"], filters: [Filter("document")]),
            StatusCode.InvalidArgument,
            $"invalid WatchRequest.OptionalObjectTypes[0]: value does not match regex pattern {ObjectTypePattern}"
        },
        {
            "object types and filters precede schema validation of the filters",
            Request(objectTypes: ["document"], filters: [Filter("nope")]),
            StatusCode.InvalidArgument,
            "cannot specify both object types and relationship filters"
        },
        {
            "object types and filters together are allowed when relationships are not watched",
            Request(objectTypes: ["document"], filters: [Filter("nope")], kinds: [V1::WatchKind.IncludeSchemaUpdates]),
            StatusCode.FailedPrecondition,
            "object definition `nope` not found"
        },
        {
            "empty start cursor fails field validation",
            Request(startCursor: ""),
            StatusCode.InvalidArgument,
            "invalid WatchRequest.OptionalStartCursor: embedded message failed validation | caused by: " +
            "invalid ZedToken.Token: value length must be at least 1 bytes"
        },
        {
            "field violations are all reported",
            Request(objectTypes: ["Doc"], startCursor: ""),
            StatusCode.InvalidArgument,
            $"invalid WatchRequest.OptionalObjectTypes[0]: value does not match regex pattern {ObjectTypePattern}; " +
            "invalid WatchRequest.OptionalStartCursor: embedded message failed validation | caused by: " +
            "invalid ZedToken.Token: value length must be at least 1 bytes"
        },
        {
            "start cursor that is not base64",
            Request(startCursor: "garbage"),
            StatusCode.InvalidArgument,
            "failed to decode start revision: error decoding zedtoken: illegal base64 data at input byte 4"
        },
        {
            // SpiceDB reports its protobuf unmarshal error here (proto: illegal wireType 6 for this input);
            // Spiceport's own token format cannot, so it keeps the frame with its own cause.
            "start cursor that is base64 but not a token",
            Request(startCursor: "Zm9v"),
            StatusCode.InvalidArgument,
            "failed to decode start revision: error decoding zedtoken: malformed zedtoken"
        },
        {
            "field validation precedes the start cursor",
            Request(objectTypes: ["Doc"], startCursor: "garbage"),
            StatusCode.InvalidArgument,
            $"invalid WatchRequest.OptionalObjectTypes[0]: value does not match regex pattern {ObjectTypePattern}"
        },
        {
            "object types and filters precede the start cursor",
            Request(objectTypes: ["document"], filters: [Filter("document")], startCursor: "garbage"),
            StatusCode.InvalidArgument,
            "cannot specify both object types and relationship filters"
        },
        {
            "the start cursor precedes schema validation of the filters",
            Request(filters: [Filter("nope")], startCursor: "garbage"),
            StatusCode.InvalidArgument,
            "failed to decode start revision: error decoding zedtoken: illegal base64 data at input byte 4"
        },
        {
            "a wildcard subject filter with a relation is not rejected by handwritten validation",
            Request(filters:
            [
                new V1::RelationshipFilter
                {
                    ResourceType = "document",
                    OptionalSubjectFilter = new V1::SubjectFilter
                    {
                        SubjectType = "user",
                        OptionalSubjectId = "*",
                        OptionalRelation = new V1::SubjectFilter.Types.RelationFilter { Relation = "viewer" },
                    },
                },
            ]),
            StatusCode.FailedPrecondition,
            "relation/permission `viewer` not found under definition `user`"
        },
    };

    [Theory]
    [MemberData(nameof(MalformedRequests))]
    public async Task Watch_rejects_a_malformed_request_as_SpiceDB_does(
        string because, V1::WatchRequest request, StatusCode expectedCode, string expectedDetail)
    {
        _ = because;
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var writer = new CollectingStreamWriter<V1::WatchResponse>();
        var ctx = new FakeServerCallContext(cts.Token);

        var ex = await Assert.ThrowsAsync<RpcException>(() => service.Watch(request, writer, ctx));

        Assert.Equal(expectedCode, ex.StatusCode);
        Assert.Equal(expectedDetail, ex.Status.Detail);
        Assert.Empty(writer.Collected);
    }

    [Fact]
    public async Task Watch_rejects_a_start_cursor_from_another_datastore()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);
        var head = await cluster.Datastore.HeadRevision(CancellationToken.None);
        var foreign = ZedTokens.FromRevision(head.Revision, head.SchemaHash, "another-datastore");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var ex = await Assert.ThrowsAsync<RpcException>(() => service.Watch(
            Request(startCursor: foreign.Token), new CollectingStreamWriter<V1::WatchResponse>(),
            new FakeServerCallContext(cts.Token)));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal("start revision was generated by a different datastore instance", ex.Status.Detail);
    }

    /// <summary>
    /// SpiceDB's <c>InvalidFilterError</c> carries ERROR_REASON_INVALID_FILTER with the filter's prototext as
    /// <c>filter</c>; the schema errors carry their definition (and relation) names.
    /// </summary>
    [Fact]
    public async Task Watch_filter_errors_carry_SpiceDBs_error_info()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = Service(cluster);

        async Task<RpcException> Rejected(V1::RelationshipFilter filter)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            return await Assert.ThrowsAsync<RpcException>(() => service.Watch(
                Request(filters: [filter]), new CollectingStreamWriter<V1::WatchResponse>(),
                new FakeServerCallContext(cts.Token)));
        }

        ErrorInfoTests.AssertErrorInfo(
            await Rejected(new V1::RelationshipFilter()),
            StatusCode.InvalidArgument, "ERROR_REASON_INVALID_FILTER", ("filter", ""));
        ErrorInfoTests.AssertErrorInfo(
            await Rejected(new V1::RelationshipFilter
            {
                ResourceType = "document", OptionalResourceId = "a", OptionalResourceIdPrefix = "b",
            }),
            StatusCode.InvalidArgument, "ERROR_REASON_INVALID_FILTER",
            ("filter", "resource_type:\"document\" optional_resource_id:\"a\" optional_resource_id_prefix:\"b\""));
        ErrorInfoTests.AssertErrorInfo(
            await Rejected(new V1::RelationshipFilter
            {
                ResourceType = "document",
                OptionalResourceId = "a",
                OptionalResourceIdPrefix = "b",
                OptionalRelation = "viewer",
                OptionalSubjectFilter = new V1::SubjectFilter
                {
                    SubjectType = "user",
                    OptionalRelation = new V1::SubjectFilter.Types.RelationFilter(),
                },
            }),
            StatusCode.InvalidArgument, "ERROR_REASON_INVALID_FILTER",
            ("filter", "resource_type:\"document\" optional_resource_id:\"a\" optional_resource_id_prefix:\"b\" " +
                "optional_relation:\"viewer\" optional_subject_filter:{subject_type:\"user\" optional_relation:{}}"));
        ErrorInfoTests.AssertErrorInfo(
            await Rejected(Filter("nope")),
            StatusCode.FailedPrecondition, "ERROR_REASON_UNKNOWN_DEFINITION", ("definition_name", "nope"));
        ErrorInfoTests.AssertErrorInfo(
            await Rejected(new V1::RelationshipFilter { ResourceType = "document", OptionalRelation = "nope" }),
            StatusCode.FailedPrecondition, "ERROR_REASON_UNKNOWN_RELATION_OR_PERMISSION",
            ("definition_name", "document"), ("relation_or_permission_name", "nope"));
    }

    private sealed class CollectingStreamWriter<T> : IServerStreamWriter<T>
    {
        private readonly Channel<T> _channel = Channel.CreateUnbounded<T>();
        public List<T> Collected { get; } = [];

        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(T message)
        {
            Collected.Add(message);
            _channel.Writer.TryWrite(message);
            return Task.CompletedTask;
        }

        public async Task<T> WaitForNext(TimeSpan timeout, Task? watchTask = null)
        {
            using var cts = new CancellationTokenSource(timeout);
            var read = _channel.Reader.ReadAsync(cts.Token).AsTask();
            if (watchTask is not null)
            {
                var done = await Task.WhenAny(read, watchTask);
                if (done == watchTask)
                    await watchTask;
            }
            return await read;
        }
    }

    private sealed class FakeServerCallContext(CancellationToken cancellationToken) : ServerCallContext
    {
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
