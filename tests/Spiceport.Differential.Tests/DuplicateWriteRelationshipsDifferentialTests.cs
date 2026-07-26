using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Spiceport.Api;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine.Tests;
using Spiceport.Grains;
using Spiceport.Grains.Tests;
using V1 = Authzed.Api.V1;

namespace Spiceport.Differential.Tests;

/// <summary>
/// Directed differential regression for issue #34: a request with two updates for the same relationship
/// (varying only operation kind and/or caveat) must be rejected as <c>InvalidArgument</c>, not silently
/// tolerated. Runs the SAME requests against a real <c>authzed/spicedb</c> container and against
/// Spiceport's in-process <see cref="AuthzedPermissionsV1Service"/> and asserts both reject with the SAME
/// gRPC status.
/// </summary>
/// <remarks>
/// Real SpiceDB (authzed/spicedb v1.49.2), empirically observed by driving <c>WriteRelationships</c>
/// directly over gRPC:
/// <list type="bullet">
/// <item>two TOUCH updates for the identical tuple -&gt; <c>InvalidArgument</c>: "found more than one
/// update with relationship `document:readme#editor@user:alice` in this request; a relationship can only
/// be specified in an update once per overall WriteRelationships request".</item>
/// <item>a TOUCH and a DELETE for the same tuple -&gt; the same <c>InvalidArgument</c> message (operation
/// kind is NOT part of the dedup key).</item>
/// <item>a CREATE and a TOUCH for the same tuple -&gt; the same <c>InvalidArgument</c> message.</item>
/// <item>two updates for the same (resource, relation, subject) differing only in caveat name/context -&gt;
/// the same <c>InvalidArgument</c> message (the dedup key excludes the caveat, matching SpiceDB's
/// <c>V1StringRelationshipWithoutCaveatOrExpiration</c>).</item>
/// </list>
/// By contrast, <c>ImportBulkRelationships</c> (a distinct RPC, distinct client-streaming semantics) does
/// NOT apply this same-request dedup check: two identical rows in one streamed batch instead surface as a
/// CREATE-conflict <c>AlreadyExists</c> ("could not CREATE relationship ..., as it already existed"),
/// because SpiceDB's bulk-import path applies each row with create semantics rather than pre-validating
/// the whole batch for duplicates. Spiceport's <c>BulkImportRelationships</c> matches that CREATE-style
/// bulk import (issue #35); <see cref="ImportBulkRelationshipsDifferentialTests"/> is the differential
/// gate for it.
/// </remarks>
[Collection(SpiceDbCollection.Name)]
public sealed class DuplicateWriteRelationshipsDifferentialTests
{
    private const string Schema = """
        definition user {}

        caveat somecaveat(value int) {
            value > 0
        }

        definition document {
            relation viewer: user | user with somecaveat
            relation editor: user
        }
        """;

    private readonly SpiceDbContainerFixture _spiceDb;

    public DuplicateWriteRelationshipsDifferentialTests(SpiceDbContainerFixture spiceDb) => _spiceDb = spiceDb;

    public static IEnumerable<object[]> DuplicateCases()
    {
        yield return
        [
            "two_touch_same_tuple",
            (Func<V1::WriteRelationshipsRequest>)(() => Req(
                Upd(V1::RelationshipUpdate.Types.Operation.Touch, Rel("editor")),
                Upd(V1::RelationshipUpdate.Types.Operation.Touch, Rel("editor")))),
        ];
        yield return
        [
            "touch_and_delete_same_tuple",
            (Func<V1::WriteRelationshipsRequest>)(() => Req(
                Upd(V1::RelationshipUpdate.Types.Operation.Touch, Rel("editor")),
                Upd(V1::RelationshipUpdate.Types.Operation.Delete, Rel("editor")))),
        ];
        yield return
        [
            "create_then_touch_same_tuple",
            (Func<V1::WriteRelationshipsRequest>)(() => Req(
                Upd(V1::RelationshipUpdate.Types.Operation.Create, Rel("editor")),
                Upd(V1::RelationshipUpdate.Types.Operation.Touch, Rel("editor")))),
        ];
        yield return
        [
            "differ_only_by_caveat_context",
            (Func<V1::WriteRelationshipsRequest>)(() => Req(
                Upd(V1::RelationshipUpdate.Types.Operation.Touch, Rel("viewer", "somecaveat", 1)),
                Upd(V1::RelationshipUpdate.Types.Operation.Touch, Rel("viewer", "somecaveat", 2)))),
        ];
    }

    [SkippableTheory]
    [MemberData(nameof(DuplicateCases))]
    public async Task WriteRelationships_duplicate_update_rejected_by_both_systems(
        string caseName, Func<V1::WriteRelationshipsRequest> buildRequest)
    {
        Skip.IfNot(_spiceDb.Available, _spiceDb.SkipReason ?? "Docker/SpiceDB container unavailable");
        _ = caseName;

        using var spiceDbClient = new SpiceDbGrpcClient(_spiceDb.Address, _spiceDb.PreSharedKey);
        // Residual relationships from sibling classes in the shared container would make this schema
        // write fail on data-orphan grounds, order-dependently — see SpiceDbReset.
        await SpiceDbReset.ResetAsync(spiceDbClient);
        await spiceDbClient.WriteSchemaAsync(new V1::WriteSchemaRequest { Schema = Schema });

        var spiceDbEx = await Assert.ThrowsAsync<RpcException>(
            () => spiceDbClient.WriteRelationshipsAsync(buildRequest()));
        Assert.Equal(StatusCode.InvalidArgument, spiceDbEx.StatusCode);
        Assert.Contains("more than one update with relationship", spiceDbEx.Status.Detail);

        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var permissionsService = new AuthzedPermissionsV1Service(
            cluster.Services.GetRequiredService<IPermissionChecker>(), cluster.GrainFactory,
            cluster.ReverseOps, cluster.RelationshipReads,
            cluster.Services.GetRequiredService<ISchemaProvider>());

        var spiceportEx = await Assert.ThrowsAsync<RpcException>(
            () => permissionsService.WriteRelationships(buildRequest(), FakeServerCallContext.Default));
        Assert.Equal(StatusCode.InvalidArgument, spiceportEx.StatusCode);
        Assert.Contains("more than one update with relationship", spiceportEx.Status.Detail);
    }

    private static V1::WriteRelationshipsRequest Req(params V1::RelationshipUpdate[] updates)
    {
        var req = new V1::WriteRelationshipsRequest();
        req.Updates.AddRange(updates);
        return req;
    }

    private static V1::RelationshipUpdate Upd(V1::RelationshipUpdate.Types.Operation op, V1::Relationship rel) =>
        new() { Operation = op, Relationship = rel };

    private static V1::Relationship Rel(string relation, string? caveatName = null, int? caveatValue = null)
    {
        var rel = new V1::Relationship
        {
            Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = "readme" },
            Relation = relation,
            Subject = new V1::SubjectReference
            {
                Object = new V1::ObjectReference { ObjectType = "user", ObjectId = "alice" },
            },
        };
        if (caveatName is not null)
        {
            var caveat = new V1::ContextualizedCaveat { CaveatName = caveatName };
            if (caveatValue is not null)
            {
                var s = new Google.Protobuf.WellKnownTypes.Struct();
                s.Fields["value"] = Google.Protobuf.WellKnownTypes.Value.ForNumber(caveatValue.Value);
                caveat.Context = s;
            }
            rel.OptionalCaveat = caveat;
        }
        return rel;
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
