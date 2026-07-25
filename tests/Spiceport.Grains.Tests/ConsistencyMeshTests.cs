using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Spiceport.Api;
using Spiceport.Core;
using Spiceport.Protos;
using RelationshipUpdate = Spiceport.Protos.RelationshipUpdate;
using Relationship = Spiceport.Protos.Relationship;
using ZedToken = Spiceport.Protos.ZedToken;

namespace Spiceport.Grains.Tests;

/// <summary>
/// End-to-end consistency verification through the in-process gRPC <see cref="PermissionsGrpcService"/>
/// over the real Orleans grain mesh (no Kestrel): the four <c>Consistency</c> modes, read-your-writes,
/// exact-snapshot isolation from later writes, the cache-not-stale-under-exact seam, mismatched-datastore
/// handling, and response-token chaining. All assertions are deterministic (no timing flake): they rely on
/// the head-pinning / exact-key invariants, not on wall-clock windows.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class ConsistencyMeshTests
{
    private const string ViewerSchema = """
        definition user {}

        definition document {
            relation viewer: user
            permission view = viewer
        }
        """;

    private static PermissionsGrpcService Service(MeshTestCluster cluster) =>
        new(cluster.Services.GetRequiredService<IPermissionChecker>(), cluster.GrainFactory, cluster.ReverseOps, cluster.RelationshipReads);

    private static RelationshipUpdate TouchViewer(string res, string subj) => new()
    {
        Operation = RelationshipUpdate.Types.Operation.Touch,
        Relationship = new Relationship
        {
            Resource = new ObjectReference { ObjectType = "document", ObjectId = res },
            ResourceRelation = "viewer",
            Subject = new SubjectReference
            {
                Object = new ObjectReference { ObjectType = "user", ObjectId = subj },
            },
        },
    };

    private static CheckPermissionRequest Check(string res, string subj, Consistency? consistency = null)
    {
        var req = new CheckPermissionRequest
        {
            Resource = new ObjectReference { ObjectType = "document", ObjectId = res },
            Permission = "view",
            Subject = new SubjectReference
            {
                Object = new ObjectReference { ObjectType = "user", ObjectId = subj },
            },
        };
        if (consistency is not null)
            req.Consistency = consistency;
        return req;
    }

    private static async Task<ZedToken> WriteViewer(PermissionsGrpcService service, string res, string subj)
    {
        var req = new WriteRelationshipsRequest();
        req.Updates.Add(TouchViewer(res, subj));
        var resp = await service.WriteRelationships(req, FakeContext.Instance);
        return resp.WrittenAt;
    }

    // ---- 1. ZedToken round-trip through the datastore parser. ----

    [Fact]
    public async Task WrittenAt_token_decodes_to_a_snapshotable_revision()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerSchema);
        var service = Service(cluster);

        var written = await WriteViewer(service, "readme", "alice");

        var parser = await cluster.Datastore.GetRevisionParser(CancellationToken.None);
        var decoded = ZedTokens.DecodeRevision(new Spiceport.Core.ZedToken(written.Token), parser);

        Assert.Equal(ZedTokenStatus.Valid, decoded.Status);
        // The decoded revision is snapshot-able (does not throw / is valid).
        Assert.True(await cluster.Datastore.CheckRevision(decoded.Revision, CancellationToken.None));
        _ = cluster.Datastore.SnapshotReader(decoded.Revision);
    }

    // ---- 2. Read-your-writes via at_least_as_fresh. ----

    [Fact]
    public async Task AtLeastAsFresh_with_write_token_sees_the_write()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerSchema);
        var service = Service(cluster);

        var written = await WriteViewer(service, "readme", "alice");

        var resp = await service.CheckPermission(
            Check("readme", "alice", new Consistency { AtLeastAsFresh = written }),
            FakeContext.Instance);

        Assert.Equal(CheckPermissionResponse.Types.Permissionship.HasPermission, resp.Permissionship);
    }

    // ---- 3. FullyConsistent immediately after a write is always fresh. ----

    [Fact]
    public async Task FullyConsistent_after_write_is_always_fresh()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerSchema);
        var service = Service(cluster);

        await WriteViewer(service, "readme", "alice");

        var resp = await service.CheckPermission(
            Check("readme", "alice", new Consistency { FullyConsistent = true }),
            FakeContext.Instance);

        Assert.Equal(CheckPermissionResponse.Types.Permissionship.HasPermission, resp.Permissionship);
    }

    // ---- 4. AtExactSnapshot ignores writes after the captured snapshot. ----

    [Fact]
    public async Task AtExactSnapshot_reads_exactly_the_captured_snapshot()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerSchema);
        var service = Service(cluster);

        // Capture a pre-write snapshot token (a FullyConsistent check returns the head it evaluated).
        var t0 = (await service.CheckPermission(
            Check("readme", "alice", new Consistency { FullyConsistent = true }),
            FakeContext.Instance)).CheckedAt;

        // Now grant the permission (advances head past t0).
        var t1 = await WriteViewer(service, "readme", "alice");

        // Exact snapshot at t0 must NOT see the later write.
        var atT0 = await service.CheckPermission(
            Check("readme", "alice", new Consistency { AtExactSnapshot = t0 }),
            FakeContext.Instance);
        Assert.Equal(CheckPermissionResponse.Types.Permissionship.NoPermission, atT0.Permissionship);

        // Exact snapshot at the post-write token sees it.
        var atT1 = await service.CheckPermission(
            Check("readme", "alice", new Consistency { AtExactSnapshot = t1 }),
            FakeContext.Instance);
        Assert.Equal(CheckPermissionResponse.Types.Permissionship.HasPermission, atT1.Permissionship);
    }

    // ---- 5. Cache-not-stale-under-exact: the §4 correctness seam. ----

    [Fact]
    public async Task FullyConsistent_is_not_shadowed_by_a_stale_optimized_bucket_entry()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerSchema);
        var service = Service(cluster);

        // A MinimizeLatency check (the default) populates the optimized-bucket branch cache for this
        // sub-problem with NO_PERMISSION (alice is not yet a viewer).
        var before = await service.CheckPermission(Check("readme", "alice"), FakeContext.Instance);
        Assert.Equal(CheckPermissionResponse.Types.Permissionship.NoPermission, before.Permissionship);

        // Grant the permission. Head advances but (within the 5s window) stays in the SAME quantized
        // bucket, so a MinimizeLatency check could read the stale NO_PERMISSION entry.
        await WriteViewer(service, "readme", "alice");

        // FullyConsistent is keyed by the EXACT head revision, so it cannot read the stale optimized
        // bucket entry: it MUST return HAS_PERMISSION.
        var fresh = await service.CheckPermission(
            Check("readme", "alice", new Consistency { FullyConsistent = true }),
            FakeContext.Instance);
        Assert.Equal(CheckPermissionResponse.Types.Permissionship.HasPermission, fresh.Permissionship);
    }

    // ---- 6. Mismatched datastore id. ----

    [Fact]
    public async Task AtExactSnapshot_with_foreign_datastore_token_is_InvalidArgument()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerSchema);
        var service = Service(cluster);

        var head = await cluster.Datastore.HeadRevision(CancellationToken.None);
        var foreign = ZedTokens.FromRevision(head.Revision, head.SchemaHash, "some-other-datastore-id");

        var ex = await Assert.ThrowsAsync<RpcException>(() => service.CheckPermission(
            Check("readme", "alice", new Consistency { AtExactSnapshot = new ZedToken { Token = foreign.Token } }),
            FakeContext.Instance));
        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
    }

    [Fact]
    public async Task AtLeastAsFresh_with_foreign_datastore_token_falls_to_full_consistency()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerSchema);
        var service = Service(cluster);

        await WriteViewer(service, "readme", "alice");

        var head = await cluster.Datastore.HeadRevision(CancellationToken.None);
        var foreign = ZedTokens.FromRevision(head.Revision, head.SchemaHash, "some-other-datastore-id");

        // No error: it falls back to full consistency (head) and returns the fresh result.
        var resp = await service.CheckPermission(
            Check("readme", "alice", new Consistency { AtLeastAsFresh = new ZedToken { Token = foreign.Token } }),
            FakeContext.Instance);
        Assert.Equal(CheckPermissionResponse.Types.Permissionship.HasPermission, resp.Permissionship);
    }

    // ---- 7. Response-token chaining never goes backwards. ----

    [Fact]
    public async Task CheckedAt_token_chains_into_AtLeastAsFresh_without_going_backwards()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ViewerSchema);
        var service = Service(cluster);

        await WriteViewer(service, "readme", "alice");

        // First (MinimizeLatency) check returns a checked_at token.
        var first = await service.CheckPermission(Check("readme", "alice"), FakeContext.Instance);
        Assert.False(string.IsNullOrEmpty(first.CheckedAt.Token));

        // Feed it back as at_least_as_fresh: resolves without error and is at least as fresh.
        var second = await service.CheckPermission(
            Check("readme", "alice", new Consistency { AtLeastAsFresh = first.CheckedAt }),
            FakeContext.Instance);
        Assert.Equal(CheckPermissionResponse.Types.Permissionship.HasPermission, second.Permissionship);

        var parser = await cluster.Datastore.GetRevisionParser(CancellationToken.None);
        var firstRev = ZedTokens.DecodeRevision(new Spiceport.Core.ZedToken(first.CheckedAt.Token), parser).Revision;
        var secondRev = ZedTokens.DecodeRevision(new Spiceport.Core.ZedToken(second.CheckedAt.Token), parser).Revision;
        Assert.True(secondRev.CompareTo(firstRev) >= 0, "the chained check must not go backwards in revision");
    }

    // ---- 8. The closed-timestamp gate across silos. ----

    /// <summary>
    /// On a two-silo mesh, a write is observed by a FullyConsistent and an AtLeastAsFresh(token) Check
    /// IMMEDIATELY — the shard grain serving the sub-problem's graph reads blocks until its watermark
    /// covers the pinned revision (the per-shard closed-timestamp gate) rather than serving a stale
    /// snapshot. Repeated to shake out any catch-up race. (Adapted from the retired projection-mesh
    /// suite: the invariant — fresh reads never serve a stale prefix, on whichever silo the sub-problem
    /// lands — transfers unchanged to the sharded read path.)
    /// </summary>
    [Fact]
    public async Task ExactReads_SeeWritesImmediately_AcrossSilos()
    {
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(ViewerSchema, siloCount: 2);
        var service = Service(cluster);

        for (var i = 0; i < 12; i++)
        {
            var doc = $"doc{i}";
            var token = await WriteViewer(service, doc, "alice");

            var fully = await service.CheckPermission(
                Check(doc, "alice", new Consistency { FullyConsistent = true }), FakeContext.Instance);
            Assert.Equal(CheckPermissionResponse.Types.Permissionship.HasPermission, fully.Permissionship);

            var fresh = await service.CheckPermission(
                Check(doc, "alice", new Consistency { AtLeastAsFresh = token }), FakeContext.Instance);
            Assert.Equal(CheckPermissionResponse.Types.Permissionship.HasPermission, fresh.Permissionship);
        }
    }

    /// <summary>A no-op <see cref="ServerCallContext"/>; the service only reads CancellationToken.</summary>
    private sealed class FakeContext : ServerCallContext
    {
        public static readonly FakeContext Instance = new();

        protected override string MethodCore => string.Empty;
        protected override string HostCore => string.Empty;
        protected override string PeerCore => string.Empty;
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override Metadata RequestHeadersCore => [];
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override Metadata ResponseTrailersCore => [];
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => new(string.Empty, new Dictionary<string, List<AuthProperty>>());
        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) => null!;
        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => Task.CompletedTask;
    }
}
