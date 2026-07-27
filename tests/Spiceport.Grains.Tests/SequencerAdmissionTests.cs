using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Spiceport.Api;
using Spiceport.Core;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;
using V1 = Authzed.Api.V1;

namespace Spiceport.Grains.Tests;

/// <summary>
/// The per-silo sequencer write admission gate (issue #36): offered write load beyond the configured
/// in-flight bound is SHED with <see cref="SequencerOverloadedException"/> instead of queueing without
/// bound on the sequencer's single non-reentrant activation (where it would die as an opaque Orleans
/// response timeout). Direct gate semantics are proven on a plain instance; the mesh tests prove the
/// production wiring end to end — the DI-registered gate is the one the data-plane grain enters, the
/// exception round-trips the grain boundary, the gRPC front door maps it to
/// <c>RESOURCE_EXHAUSTED</c>, and a released slot readmits writes.
/// </summary>
public class SequencerAdmissionGateTests
{
    [Fact]
    public void Full_gate_sheds_and_a_released_slot_readmits()
    {
        var metrics = new SequencerMetrics();
        var gate = new SequencerAdmission(new SequencerAdmissionOptions { MaxInFlightCommits = 2 }, metrics);

        var first = gate.Enter();
        var second = gate.Enter();

        Assert.Throws<SequencerOverloadedException>(() => gate.Enter());
        Assert.Equal(1, metrics.Snapshot().CommitShed);

        first.Dispose();
        using var readmitted = gate.Enter();

        second.Dispose();
        Assert.Equal(1, metrics.Snapshot().CommitShed);
    }

    [Fact]
    public void Disposing_a_slot_twice_releases_it_only_once()
    {
        var gate = new SequencerAdmission(
            new SequencerAdmissionOptions { MaxInFlightCommits = 1 }, new SequencerMetrics());

        var slot = gate.Enter();
        slot.Dispose();
        slot.Dispose();

        // A double release would have grown capacity past the bound: both entries would now succeed.
        using var only = gate.Enter();
        Assert.Throws<SequencerOverloadedException>(() => gate.Enter());
    }

    [Fact]
    public void Non_positive_limit_disables_the_gate()
    {
        var metrics = new SequencerMetrics();
        var gate = new SequencerAdmission(new SequencerAdmissionOptions { MaxInFlightCommits = 0 }, metrics);

        for (var i = 0; i < 1000; i++)
            gate.Enter().Dispose();

        Assert.Equal(0, metrics.Snapshot().CommitShed);
    }
}

/// <summary>
/// End-to-end admission behavior over the real grain mesh: the test saturates the silo's DI-registered
/// gate by holding every slot, exactly the state a write burst beyond the sequencer's capacity produces.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class SequencerAdmissionMeshTests
{
    private const string Schema = """
        definition user {}

        definition doc {
          relation viewer: user
        }
        """;

    private static WriteRelationshipsArgs OneWrite(string docId) => new(
        new List<RelationshipUpdateWire>
        {
            new(RelationshipUpdateOpWire.Touch,
                new RelationshipWire("doc", docId, "viewer", "user", "alice", "...", null, null, null)),
        },
        Preconditions: null);

    /// <summary>Takes every slot of the (single) silo's admission gate, returning them for release.</summary>
    private static List<IDisposable> Saturate(MeshTestCluster cluster)
    {
        var gate = cluster.Services.GetRequiredService<SequencerAdmission>();
        var limit = cluster.Services.GetRequiredService<SequencerAdmissionOptions>().MaxInFlightCommits;
        Assert.True(limit > 0, "the production default must have the gate enabled");
        var slots = new List<IDisposable>(limit);
        for (var i = 0; i < limit; i++)
            slots.Add(gate.Enter());
        return slots;
    }

    [Fact]
    public async Task Saturated_gate_sheds_a_write_and_a_freed_slot_readmits_it()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var slots = Saturate(cluster);
        try
        {
            // The shed surfaces through the data-plane grain — the exception crosses the grain
            // boundary as itself (the [GenerateSerializer] contract), never as a timeout.
            await Assert.ThrowsAsync<SequencerOverloadedException>(
                () => cluster.Relationships.WriteRelationships(OneWrite("shed")));

            Assert.True(cluster.SequencerMetricsSnapshot().CommitShed >= 1, "the shed must be counted");
        }
        finally
        {
            foreach (var slot in slots)
                slot.Dispose();
        }

        // Overload over: the same write is admitted and commits.
        var reply = await cluster.Relationships.WriteRelationships(OneWrite("shed"));
        Assert.False(string.IsNullOrEmpty(reply.WrittenAtToken));
    }

    [Fact]
    public async Task Shed_write_maps_to_resource_exhausted_at_the_grpc_front_door()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var service = new AuthzedPermissionsV1Service(
            cluster.Checker, cluster.GrainFactory, cluster.ReverseOps, cluster.RelationshipReads,
            cluster.SchemaProvider);

        var request = new V1::WriteRelationshipsRequest();
        request.Updates.Add(new V1::RelationshipUpdate
        {
            Operation = V1::RelationshipUpdate.Types.Operation.Touch,
            Relationship = new V1::Relationship
            {
                Resource = new V1::ObjectReference { ObjectType = "doc", ObjectId = "d1" },
                Relation = "viewer",
                Subject = new V1::SubjectReference
                {
                    Object = new V1::ObjectReference { ObjectType = "user", ObjectId = "alice" },
                },
            },
        });

        var slots = Saturate(cluster);
        try
        {
            var ex = await Assert.ThrowsAsync<RpcException>(
                () => service.WriteRelationships(request, FakeServerCallContext.Default));
            Assert.Equal(StatusCode.ResourceExhausted, ex.StatusCode);
        }
        finally
        {
            foreach (var slot in slots)
                slot.Dispose();
        }
    }

    [Fact]
    public async Task Saturated_gate_still_serves_checks()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        // Commit the data BEFORE saturating (the compatibility ReadWriteTx path is deliberately
        // ungated, but going through the production write proves the gate passes normal load).
        await cluster.Relationships.WriteRelationships(OneWrite("d-read"));

        var slots = Saturate(cluster);
        try
        {
            // Reads are untouched by write admission: the check dispatches through the mesh and the
            // sequencer's interleaving read surface as usual.
            var result = await cluster.Checker.Check(
                "doc", "d-read", "viewer",
                new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis),
                caveatContext: null,
                consistency: ConsistencyRequirement.FullyConsistent);
            Assert.Equal(Membership.Member, result.Verdict);
        }
        finally
        {
            foreach (var slot in slots)
                slot.Dispose();
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
