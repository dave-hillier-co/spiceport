using Orleans;
using Orleans.Runtime;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;
using static Spiceport.Grains.Tests.DispatchContextTestHelper;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Filter-level coverage for <see cref="CheckDispatchIncomingCallFilter"/> /
/// <see cref="CheckDispatchOutgoingCallFilter"/>: the depth-budget boundary guard rejects an
/// already-exhausted call before the grain body runs at all — observable both as "no memo bookkeeping
/// happened" and as "the filter itself still counted a real dispatch hop" — and a check that instead
/// exhausts its budget mid-recursion (a genuine cycle) still surfaces the SAME
/// <see cref="MaxDepthExceededException"/> the engine's own in-step guard has always thrown, now that the
/// error-classification try/catch has moved out of <see cref="OrleansDispatcher"/> and into
/// <see cref="CheckDispatchOutgoingCallFilter"/>.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class CheckDispatchFiltersTests
{
    private const string DocumentSchema = """
        definition user {}

        definition document {
            relation viewer: user
            permission view = viewer
        }
        """;

    private const string CycleSchema = """
        definition user {}

        definition group {
            relation member: user | group#member
        }
        """;

    [Fact]
    public async Task Mid_recursion_depth_exhaustion_still_surfaces_MaxDepthExceededException()
    {
        // group:a#member -> group:b#member -> group:a#member: a genuine cycle. Depth is exhausted
        // several grain hops in, well after the incoming filter has already accepted (and counted) many
        // dispatch hops along the way — this is the "consumed mid-recursion" case, as opposed to the
        // "already exhausted on arrival" case covered below.
        await using var cluster = await MeshTestCluster.CreateAsync(CycleSchema);
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(
        [
            new RelationshipUpdate(
                Relationship.Create(
                    new ObjectAndRelation("group", "a", "member"),
                    new ObjectAndRelation("group", "b", "member")),
                UpdateOperation.Create),
            new RelationshipUpdate(
                Relationship.Create(
                    new ObjectAndRelation("group", "b", "member"),
                    new ObjectAndRelation("group", "a", "member")),
                UpdateOperation.Create),
        ]));

        cluster.ResetMetrics();

        await Assert.ThrowsAsync<MaxDepthExceededException>(() => cluster.Checker.Check(
            "group", "a", "member",
            new ObjectAndRelation("user", "x", CoreConstants.Ellipsis), null));

        // The cycle burns through many real grain hops before the budget runs out — each one crossed the
        // incoming filter's boundary and was counted, proving this genuinely ran the mesh rather than
        // failing before a single dispatch happened.
        var snapshot = cluster.MetricsSnapshot();
        Assert.True(snapshot.Dispatch > 1,
            $"Expected the cycle to burn through several real dispatch hops before exhausting depth; saw {snapshot.Dispatch}.");
    }

    [Fact]
    public async Task Incoming_filter_rejects_an_already_exhausted_call_before_the_grain_body_runs()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(DocumentSchema);
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(
        [
            new RelationshipUpdate(
                Relationship.Create(
                    new ObjectAndRelation("document", "readme", "viewer"),
                    new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis)),
                UpdateOperation.Create),
        ]));

        var head = await cluster.Datastore.HeadRevision();
        var schemaHash = cluster.SchemaProvider.Current.SchemaHash;
        var key = GrainKey.Build(
            new ObjectAndRelation("document", "readme", "view"),
            new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis),
            head.Revision.ToString(),
            schemaHash);
        var grain = cluster.GrainFactory.GetGrain<ICheckGrain>(key);

        cluster.ResetMetrics();
        using var cancellation = new GrainCancellationTokenSource();

        // DepthRemaining = 0: the incoming filter must reject this BEFORE the grain body (and hence its
        // activation-memo lookup) ever runs. If the filter did not exist, this would instead reach
        // CheckGrain.DispatchCheck, record a memo miss, and only THEN hit LocalDispatcher's own guard.
        SetDispatchContext(0);
        await Assert.ThrowsAsync<MaxDepthExceededException>(() => grain.DispatchCheck(cancellation.Token));

        var snapshot = cluster.MetricsSnapshot();
        // No memo bookkeeping happened at all: the grain body never ran.
        Assert.Equal(0, snapshot.MemoHit);
        Assert.Equal(0, snapshot.MemoMiss);
        // ...but the filter itself still counted the boundary crossing: a message genuinely reached this
        // grain, even though it was rejected before doing any work.
        Assert.Equal(1, snapshot.Dispatch);
    }

    // --- CheckDispatchOutgoingCallFilter, driven directly (no mesh) -----------------------------------
    //
    // The mesh tests above only ever provoke the DOMAIN passthrough branch (MaxDepthExceededException),
    // because the calls in this file all originate from the TestCluster's CLIENT grain factory, which has
    // no CheckDispatchOutgoingCallFilter registered (only the silo's DI container does, via
    // AddSpiceportGrainServices) — so those calls never actually pass through the outgoing filter at all;
    // only the SILO's incoming filter runs. The MAPPED (non-domain) branch — the one that actually
    // collapses a transport/unknown failure into a DispatchFailedException — is exercised here instead,
    // directly against the filter's own Invoke, since a genuine Orleans transport failure cannot be
    // injected into the in-process TestCluster (see DispatchErrorMapperTests).

    [Fact]
    public async Task Outgoing_filter_collapses_a_mapped_non_domain_exception_into_DispatchFailedException()
    {
        var filter = new CheckDispatchOutgoingCallFilter();
        var context = new FakeOutgoingGrainCallContext(
            () => throw new TimeoutException("response timed out"));

        var error = await Assert.ThrowsAsync<DispatchFailedException>(() => filter.Invoke(context));

        Assert.Equal(DispatchErrorCode.Unavailable, error.Code);
    }

    [Fact]
    public async Task Outgoing_filter_re_throws_a_domain_exception_unchanged()
    {
        var filter = new CheckDispatchOutgoingCallFilter();
        var context = new FakeOutgoingGrainCallContext(
            () => throw new MaxDepthExceededException());

        await Assert.ThrowsAsync<MaxDepthExceededException>(() => filter.Invoke(context));
    }

    [Fact]
    public async Task Outgoing_filter_leaves_calls_to_other_methods_untranslated()
    {
        // A call to some other grain interface's method must pass straight through — no translation, even
        // though the underlying exception is exactly the kind the filter would otherwise collapse.
        var filter = new CheckDispatchOutgoingCallFilter();
        var otherMethod = typeof(IRelationshipsGrain).GetMethod(nameof(IRelationshipsGrain.WriteSchema))!;
        var context = new FakeOutgoingGrainCallContext(
            () => throw new TimeoutException("unrelated hop"),
            interfaceMethod: otherMethod);

        await Assert.ThrowsAsync<TimeoutException>(() => filter.Invoke(context));
    }

    // --- CheckDispatchIncomingCallFilter / DispatchContext, driven directly (no mesh) -----------------

    [Fact]
    public async Task Incoming_filter_throws_loudly_when_the_ambient_depth_context_was_never_set()
    {
        // A caller that reaches ICheckGrain.DispatchCheck without going through DispatchContext.Set (the
        // dispatcher in production, SetDispatchContext in a direct-grain test) is a bug: the filter must
        // surface a loud InvalidOperationException, never silently default to some depth.
        RequestContext.Clear();
        var filter = new CheckDispatchIncomingCallFilter();
        var context = new FakeIncomingGrainCallContext();

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => filter.Invoke(context));

        Assert.Contains("depthRemaining", error.Message, StringComparison.OrdinalIgnoreCase);
    }
}
