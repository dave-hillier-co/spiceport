using System.Collections.Immutable;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.TestingHost;
using Spiceport.Api;
using Spiceport.Conformance.Tests;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;
using Spiceport.Protos;
using Relationship = Spiceport.Core.Relationship;
using ProtoRelationship = Spiceport.Protos.Relationship;
using RelationshipUpdate = Spiceport.Core.RelationshipUpdate;
using ProtoRelationshipUpdate = Spiceport.Protos.RelationshipUpdate;
using ZedToken = Spiceport.Protos.ZedToken;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Stage-2 gates for the per-silo materialized projection (<see cref="SiloProjection"/>), the only read
/// path of <see cref="GrainBackedDatastore"/>. Proves (a) ORACLE EQUIVALENCE: the same conformance corpus
/// subset that the in-process engine asserts stays green when every read serves from the projection;
/// (b) READER FIDELITY: at every committed revision the projection reader returns exactly the rows an
/// independent <see cref="ReferenceDatastore"/> oracle returns; (c) the CLOSED-TIMESTAMP gate: a cross-silo
/// exact / at-least-as-fresh read observes a write immediately (the projection blocks for catch-up), never
/// serving a stale prefix.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class Stage2ProjectionMeshTests
{
    // The same representative spread ConformanceMeshTests runs, now with reads served from the projection.
    public static IEnumerable<object[]> MeshFiles() =>
    [
        ["multipleops.yaml"],
        ["teamwitharrow.yaml"],
        ["simplewildcard.yaml"],
        ["indirectnestedgroups.yaml"],
        ["simplerecursive.yaml"],
        ["basiccaveat.yaml"],
        ["caveatlr.yaml"],
        ["caveatip.yaml"],
    ];

    /// <summary>Gate (a): the corpus oracle stays green with the projection reader on the read path.</summary>
    [Theory]
    [MemberData(nameof(MeshFiles))]
    public async Task Conformance_Through_Projection_Mesh(string fileName)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "TestData", fileName);
        Assert.True(File.Exists(path), $"Linked corpus file missing from output: {path}");

        var file = ValidationFileLoader.LoadFromFile(path);

        await using var cluster = await MeshTestCluster.CreateAsync(file.SchemaText);

        if (file.Relationships.Count > 0)
            await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(
                file.Relationships.Select(r => new RelationshipUpdate(r, UpdateOperation.Create)).ToList()));

        var failures = new List<string>();
        foreach (var assertion in file.Assertions)
        {
            var result = await cluster.Checker.Check(
                assertion.Resource.ObjectType,
                assertion.Resource.ObjectId,
                assertion.Resource.Relation,
                assertion.Subject,
                assertion.CaveatContext);

            if (result.Verdict != assertion.ExpectedMembership)
                failures.Add($"  {assertion.SourceText} => expected {assertion.ExpectedMembership}, got {result.Verdict}");
        }

        Assert.True(
            failures.Count == 0,
            $"{fileName} (through projection mesh): {failures.Count}/{file.Assertions.Count} assertion(s) failed:\n{string.Join('\n', failures)}");
    }

    /// <summary>
    /// Gate (b): at every committed revision, a projection reader on a SEPARATE datastore instance (so the
    /// rows can only have arrived by folding the singleton grain's log, never by local write echo) exposes
    /// the same live relationship set as an independent in-memory oracle replaying the same ops.
    /// </summary>
    [Fact]
    public async Task ProjectionReader_MatchesOracle_AtEveryRevision()
    {
        await using var scope = new Scope(await NewDatastoreClusterAsync());
        var gf = scope.Cluster.GrainFactory;
        // Two ISOLATED hosts: "projected" must observe every row purely by folding the singleton grain's log
        // (via its OWN SiloProjection), never by sharing "writer"'s in-memory state.
        await using var writerHost = new PrivateProjectionHost(gf);
        await using var projectedHost = new PrivateProjectionHost(gf);
        IDatastore writer = new GrainBackedDatastore(gf, writerHost);
        IDatastore projected = new GrainBackedDatastore(gf, projectedHost);
        var oracle = new ReferenceDatastore();

        // Drive the SAME ordered workload through the grain (writer) and the oracle, capturing each backend's
        // commit revision so we can compare the readers revision-by-revision.
        var grainRevs = new List<IRevision>();
        var oracleRevs = new List<IRevision>();
        foreach (var step in Workload())
        {
            grainRevs.Add(await writer.ReadWriteTx(step));
            oracleRevs.Add(await oracle.ReadWriteTx(step));
        }

        for (var i = 0; i < grainRevs.Count; i++)
        {
            var viaProjection = await LiveIds(projected.SnapshotReader(grainRevs[i]));
            var viaOracle = await LiveIds(oracle.SnapshotReader(oracleRevs[i]));

            Assert.Equal(viaOracle, viaProjection);
        }
    }

    /// <summary>
    /// Gate (c): the closed-timestamp gate across silos. On a two-silo projection-on mesh, a write is
    /// observed by a FullyConsistent and an AtLeastAsFresh(token) Check IMMEDIATELY — the projection on the
    /// silo that owns the sub-problem blocks until its watermark covers the write rather than serving a stale
    /// snapshot. Repeated to shake out any catch-up race.
    /// </summary>
    [Fact]
    public async Task ExactReads_SeeWritesImmediately_AcrossSilos()
    {
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(ViewerSchema, siloCount: 2);
        var service = new PermissionsGrpcService(
            cluster.Services.GetRequiredService<IPermissionChecker>(), cluster.GrainFactory);

        for (var i = 0; i < 12; i++)
        {
            var doc = $"doc{i}";
            var token = await WriteViewer(service, doc, "alice");

            var fully = await service.CheckPermission(
                CheckReq(doc, "alice", new Consistency { FullyConsistent = true }), FakeContext.Instance);
            Assert.Equal(CheckPermissionResponse.Types.Permissionship.HasPermission, fully.Permissionship);

            var fresh = await service.CheckPermission(
                CheckReq(doc, "alice", new Consistency { AtLeastAsFresh = token }), FakeContext.Instance);
            Assert.Equal(CheckPermissionResponse.Types.Permissionship.HasPermission, fresh.Permissionship);
        }
    }

    // --- workload + helpers ---

    private const string ViewerSchema = """
        definition user {}
        definition document {
            relation viewer: user
            permission view = viewer
        }
        """;

    private static Relationship Rel(string rid, string sid) =>
        Relationship.Create(new ObjectAndRelation("doc", rid, "viewer"), new ObjectAndRelation("user", sid, CoreConstants.Ellipsis));

    /// <summary>A spread of commits: schema, creates, a touch-over, and a delete (multiple revisions).</summary>
    private static IEnumerable<Func<IReadWriteTransaction, Task>> Workload() =>
    [
        async tx =>
        {
            await tx.WriteStoredSchema(Encoding.UTF8.GetBytes("definition user {}\ndefinition doc { relation viewer: user }"));
            await tx.WriteRelationships(new[]
            {
                new RelationshipUpdate(Rel("a", "alice"), UpdateOperation.Create),
                new RelationshipUpdate(Rel("b", "bob"), UpdateOperation.Create),
            });
        },
        tx => tx.WriteRelationships(new[]
        {
            new RelationshipUpdate(Rel("c", "carol"), UpdateOperation.Create),
            new RelationshipUpdate(Rel("d", "dave"), UpdateOperation.Create),
        }),
        tx => tx.WriteRelationships(new[]
        {
            new RelationshipUpdate(Rel("a", "alice"), UpdateOperation.Touch),
            new RelationshipUpdate(Rel("b", "bob"), UpdateOperation.Delete),
        }),
    ];

    private static async Task<SortedSet<string>> LiveIds(IDatastoreReader reader)
    {
        var set = new SortedSet<string>();
        await foreach (var r in reader.QueryRelationships(new RelationshipsFilter()))
            set.Add($"{r.Resource.ObjectId}#{r.Resource.Relation}@{r.Subject.ObjectId}");
        return set;
    }

    private static ProtoRelationshipUpdate TouchViewer(string res, string subj) => new()
    {
        Operation = ProtoRelationshipUpdate.Types.Operation.Touch,
        Relationship = new ProtoRelationship
        {
            Resource = new ObjectReference { ObjectType = "document", ObjectId = res },
            ResourceRelation = "viewer",
            Subject = new SubjectReference { Object = new ObjectReference { ObjectType = "user", ObjectId = subj } },
        },
    };

    private static async Task<ZedToken> WriteViewer(PermissionsGrpcService service, string res, string subj)
    {
        var req = new WriteRelationshipsRequest();
        req.Updates.Add(TouchViewer(res, subj));
        return (await service.WriteRelationships(req, FakeContext.Instance)).WrittenAt;
    }

    private static CheckPermissionRequest CheckReq(string res, string subj, Consistency consistency) => new()
    {
        Resource = new ObjectReference { ObjectType = "document", ObjectId = res },
        Permission = "view",
        Subject = new SubjectReference { Object = new ObjectReference { ObjectType = "user", ObjectId = subj } },
        Consistency = consistency,
    };

    private static async Task<TestCluster> NewDatastoreClusterAsync()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<DatastoreSiloConfigurator>();
        var cluster = builder.Build();
        await cluster.DeployAsync();
        return cluster;
    }

    private sealed class DatastoreSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder b)
        {
            b.AddMemoryGrainStorage("datastore");
            b.AddCustomStorageBasedLogConsistencyProvider("CustomStorage");
        }
    }

    private sealed class Scope(TestCluster cluster) : IAsyncDisposable
    {
        public TestCluster Cluster { get; } = cluster;
        public async ValueTask DisposeAsync() => await Cluster.DisposeAsync();
    }

    private sealed class FakeContext : Grpc.Core.ServerCallContext
    {
        public static readonly FakeContext Instance = new();
        protected override string MethodCore => string.Empty;
        protected override string HostCore => string.Empty;
        protected override string PeerCore => string.Empty;
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override Grpc.Core.Metadata RequestHeadersCore => [];
        protected override CancellationToken CancellationTokenCore => CancellationToken.None;
        protected override Grpc.Core.Metadata ResponseTrailersCore => [];
        protected override Grpc.Core.Status StatusCore { get; set; }
        protected override Grpc.Core.WriteOptions? WriteOptionsCore { get; set; }
        protected override Grpc.Core.AuthContext AuthContextCore => new(string.Empty, new Dictionary<string, List<Grpc.Core.AuthProperty>>());
        protected override Grpc.Core.ContextPropagationToken CreatePropagationTokenCore(Grpc.Core.ContextPropagationOptions? options) => null!;
        protected override Task WriteResponseHeadersAsyncCore(Grpc.Core.Metadata responseHeaders) => Task.CompletedTask;
    }
}
