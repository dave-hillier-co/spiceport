using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>Regression coverage for the Orleans-native dispatch plumbing.</summary>
[Collection(MeshClusterCollection.Name)]
public class CancellationAndImmutabilityTests
{
    private const string Schema = """
        definition user {}

        definition document {
            relation viewer: user
            permission view = viewer
        }
        """;

    private const string CyclicSchema = """
        definition user {}

        definition group {
            relation parent: group
            permission member = parent->member
        }
        """;

    [Fact]
    public async Task Dispatcher_bridges_caller_cancellation_to_the_remote_grain_call()
    {
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(
            CyclicSchema,
            siloCount: 1,
            localRecurseEnabled: false);
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships([
            new RelationshipUpdate(
                Relationship.Create(
                    new ObjectAndRelation("group", "loop", "parent"),
                    new ObjectAndRelation("group", "loop", CoreConstants.Ellipsis)),
                UpdateOperation.Create),
        ]));
        var head = await cluster.Datastore.HeadRevision();
        var dispatcher = cluster.Services.GetRequiredService<ISiloDispatcher>().Dispatcher;
        var request = new DispatchCheckRequest(
            new ObjectAndRelation("group", "loop", "member"),
            new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis),
            new ResolverMeta(head.Revision, 100_000, TraversalBloom.Empty));

        using var cancellation = new CancellationTokenSource();
        var dispatch = dispatcher.DispatchCheck(request, cancellation.Token);
        cancellation.Cancel();

        var error = await Assert.ThrowsAsync<DispatchFailedException>(
            () => dispatch.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.Equal(DispatchErrorCode.Cancelled, error.Code);
    }

    [Fact]
    public async Task Check_grain_honours_a_cancellation_token_received_across_the_grain_boundary()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var head = await cluster.Datastore.HeadRevision();
        var schemaHash = cluster.SchemaProvider.Current.SchemaHash;
        var key = GrainKey.Build(
            new ObjectAndRelation("document", "readme", "view"),
            new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis),
            head.Revision.ToString(),
            schemaHash,
            RevisionMode.Optimized);
        var grain = cluster.GrainFactory.GetGrain<ICheckGrain>(key);

        using var cancellation = new GrainCancellationTokenSource();
        await cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => grain.DispatchCheck(
                new DispatchCheckArgs(50, TraversalBloom.Empty.ToBytes(), TraversalBloom.Empty.Hashes),
                cancellation.Token));
    }

    [Theory]
    [InlineData(typeof(DispatchCheckArgs))]
    [InlineData(typeof(DispatchCheckReply))]
    [InlineData(typeof(LogEvent))]
    public void Same_silo_wire_values_are_declared_immutable(Type wireType)
    {
        Assert.True(
            wireType.IsDefined(typeof(ImmutableAttribute), inherit: false),
            $"{wireType.Name} must remain [Immutable] so same-silo grain calls do not defensively copy it.");
    }
}
