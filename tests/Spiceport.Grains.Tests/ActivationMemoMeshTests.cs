using Orleans;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Stage (a) of "Activation-as-cache" (<c>docs/future-work.md</c> item 1.3): <see cref="CheckGrain"/>
/// memoizes its computed pre-context reply in activation state, so a re-dispatch of the same canonical
/// sub-problem to a warm activation is served without re-expanding the relation graph. Every test here
/// resolves the <see cref="ICheckGrain"/> directly by its <see cref="GrainKey"/> and calls
/// <see cref="ICheckGrain.DispatchCheck"/> itself — bypassing the silo-wide <see cref="CachingDispatcher"/>
/// entirely — so only the grain's OWN memo behaviour is under test.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class ActivationMemoMeshTests
{
    private const string DocumentSchema = """
        definition user {}

        definition document {
            relation viewer: user
            permission view = viewer
        }
        """;

    private const string ChainSchema = """
        definition user {}

        definition group {
            relation direct_member: user
            relation parent: group
            permission member = direct_member + parent->member
        }
        """;

    // A finite (acyclic) two-hop schema used to provoke the OrleansDispatcher's bloom loop-bypass
    // WITHOUT a genuine relation cycle: doc1 has no direct viewer but a parent doc2 that does. A true
    // self/mutual cycle only ever terminates via MaxDepthExceededException (see LocalDispatcher's
    // remarks: "a true cycle simply consumes depth ... until it throws" — there is no visited-set cut on
    // the verdict path), so it can never surface a graceful CycleCut=true reply to assert against. Instead
    // this test hand-seeds the dispatched TraversalBloom with the SECOND hop's own visit key before
    // making the call, so the dispatcher believes (falsely, but harmlessly per the bloom's false-positive
    // contract) that hop is already in flight and takes the local bypass — which resolves immediately
    // (doc2 has a direct viewer tuple, no further recursion needed) and is unconditionally tagged
    // CycleCut = true by OrleansDispatcher, regardless of whether that resolution itself needed the loop
    // guard. This reaches a genuinely successful CycleCut = true reply out of a normal, finite graph.
    private const string ArrowSchema = """
        definition user {}

        definition document {
            relation viewer: user
            relation parent: document
            permission view = viewer + parent->view
        }
        """;

    private static ObjectAndRelation Resource(string type, string id, string relation) =>
        new(type, id, relation);

    private static ObjectAndRelation Subject(string id) =>
        new("user", id, CoreConstants.Ellipsis);

    private static async Task<(ICheckGrain Grain, string Revision)> ResolveGrain(
        MeshTestCluster cluster, ObjectAndRelation resource, ObjectAndRelation subject)
    {
        var head = await cluster.Datastore.HeadRevision();
        var schemaHash = cluster.SchemaProvider.Current.SchemaHash;
        var key = GrainKey.Build(
            resource, subject, head.Revision.ToString(), schemaHash, RevisionMode.Optimized);
        return (cluster.GrainFactory.GetGrain<ICheckGrain>(key), head.Revision.ToString());
    }

    private static DispatchCheckArgs Args(int depthRemaining, TraversalBloom? bloom = null) =>
        new(depthRemaining, (bloom ?? TraversalBloom.Empty).ToBytes(), (bloom ?? TraversalBloom.Empty).Hashes);

    [Fact]
    public async Task Warm_activation_serves_the_second_identical_call_from_the_memo()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(DocumentSchema);
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships([
            new RelationshipUpdate(
                Relationship.Create(
                    Resource("document", "readme", "viewer"),
                    Subject("alice")),
                UpdateOperation.Create),
        ]));

        var (grain, _) = await ResolveGrain(
            cluster, Resource("document", "readme", "view"), Subject("alice"));

        using var ct1 = new GrainCancellationTokenSource();
        using var ct2 = new GrainCancellationTokenSource();

        var before = cluster.MetricsSnapshot();
        var first = await grain.DispatchCheck(Args(50), ct1.Token);
        var afterFirst = cluster.MetricsSnapshot();
        var second = await grain.DispatchCheck(Args(50), ct2.Token);
        var afterSecond = cluster.MetricsSnapshot();

        Assert.Equal(first, second);
        Assert.True(first.Member);

        // First call: cold activation, no memo yet -> a miss (and nothing to hit).
        Assert.Equal(before.MemoMiss + 1, afterFirst.MemoMiss);
        Assert.Equal(before.MemoHit, afterFirst.MemoHit);

        // Second call: identical sub-problem on the SAME warm activation -> served from the memo.
        Assert.Equal(afterFirst.MemoHit + 1, afterSecond.MemoHit);
        Assert.Equal(afterFirst.MemoMiss, afterSecond.MemoMiss);
    }

    [Fact]
    public async Task Depth_guard_recomputes_rather_than_serving_a_memo_primed_at_a_tighter_budget()
    {
        // A deep, distinctly-keyed chain so DepthRequired at the root is > 1 (deterministic: this schema
        // has exactly one path, direct_member at the bottom of a linear parent chain, so DepthRequired
        // equals the chain depth).
        const int depth = 5;
        await using var cluster = await MeshTestCluster.CreateAsync(ChainSchema);

        var updates = new List<RelationshipUpdate>();
        for (var i = 0; i < depth - 1; i++)
        {
            updates.Add(new RelationshipUpdate(
                Relationship.Create(
                    Resource("group", $"g{i}", "parent"),
                    Resource("group", $"g{i + 1}", CoreConstants.Ellipsis)),
                UpdateOperation.Create));
        }
        updates.Add(new RelationshipUpdate(
            Relationship.Create(
                Resource("group", $"g{depth - 1}", "direct_member"),
                Subject("u")),
            UpdateOperation.Create));
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(updates));

        var (grain, _) = await ResolveGrain(
            cluster, Resource("group", "g0", "member"), Subject("u"));

        // Prime the memo with a generous budget so it genuinely completes and records the sub-problem's
        // real DepthRequired (D).
        using var ctPrime = new GrainCancellationTokenSource();
        var primed = await grain.DispatchCheck(Args(1_000), ctPrime.Token);
        Assert.True(primed.Member);
        var required = primed.DepthRequired;
        Assert.True(required > 1, "expected the chain to require more than one hop of depth.");

        // Served: a budget exactly equal to what the memo required is sufficient (DepthRemaining >=
        // DepthRequired), so the memo answers without touching the datastore/graph again.
        using var ctServed = new GrainCancellationTokenSource();
        var beforeServed = cluster.MetricsSnapshot();
        var served = await grain.DispatchCheck(Args(required), ctServed.Token);
        var afterServed = cluster.MetricsSnapshot();
        Assert.Equal(primed, served);
        Assert.Equal(beforeServed.MemoHit + 1, afterServed.MemoHit);

        // Not served: a budget ONE SHORT of what the memo required must fall through and recompute under
        // the tighter budget — proven by the recompute legitimately exhausting depth and throwing
        // MaxDepthExceededException (the memo, had it been (wrongly) served, would have returned
        // successfully instead).
        using var ctTight = new GrainCancellationTokenSource();
        var beforeTight = cluster.MetricsSnapshot();
        await Assert.ThrowsAsync<MaxDepthExceededException>(
            () => grain.DispatchCheck(Args(required - 1), ctTight.Token));
        var afterTight = cluster.MetricsSnapshot();
        Assert.Equal(beforeTight.MemoMiss + 1, afterTight.MemoMiss);
        Assert.Equal(beforeTight.MemoHit, afterTight.MemoHit);
    }

    [Fact]
    public async Task Cycle_cut_replies_are_never_memoized()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(ArrowSchema);
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships([
            new RelationshipUpdate(
                Relationship.Create(
                    Resource("document", "doc1", "parent"),
                    Resource("document", "doc2", CoreConstants.Ellipsis)),
                UpdateOperation.Create),
            new RelationshipUpdate(
                Relationship.Create(
                    Resource("document", "doc2", "viewer"),
                    Subject("alice")),
                UpdateOperation.Create),
        ]));

        var (grain, _) = await ResolveGrain(
            cluster, Resource("document", "doc1", "view"), Subject("alice"));

        // Hand-seed the bloom with the SECOND hop's own visit key (doc2/view/alice), so the dispatcher's
        // loop-bypass fires when the root's parent->view arrow reaches it, and OrleansDispatcher
        // unconditionally tags the resulting reply CycleCut = true (see the class remarks on ArrowSchema).
        var seeded = TraversalBloom.Empty.Add(
            VisitKey.Of(Resource("document", "doc2", "view"), Subject("alice")));

        using var ct1 = new GrainCancellationTokenSource();
        var before = cluster.MetricsSnapshot();
        var first = await grain.DispatchCheck(Args(50, seeded), ct1.Token);
        var afterFirst = cluster.MetricsSnapshot();

        Assert.True(first.Member);
        Assert.True(first.CycleCut, "expected the pre-seeded bloom to provoke a cycle-cut reply.");
        // A cycle-cut reply is a miss (it was computed), but it must NOT populate the memo.
        Assert.Equal(before.MemoMiss + 1, afterFirst.MemoMiss);

        // A second call with an EMPTY bloom (no seeding) asks the exact same sub-problem; if the
        // cycle-cut reply had been (wrongly) memoized, this would be served from it. It must instead
        // recompute — observable as a second miss, not a hit.
        using var ct2 = new GrainCancellationTokenSource();
        var second = await grain.DispatchCheck(Args(50), ct2.Token);
        var afterSecond = cluster.MetricsSnapshot();

        Assert.True(second.Member);
        Assert.False(second.CycleCut);
        Assert.Equal(afterFirst.MemoMiss + 1, afterSecond.MemoMiss);
        Assert.Equal(afterFirst.MemoHit, afterSecond.MemoHit);
    }

    [Fact]
    public async Task Disabled_memo_never_hits_or_misses()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(DocumentSchema, useActivationMemo: false);
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships([
            new RelationshipUpdate(
                Relationship.Create(
                    Resource("document", "readme", "viewer"),
                    Subject("alice")),
                UpdateOperation.Create),
        ]));

        var (grain, _) = await ResolveGrain(
            cluster, Resource("document", "readme", "view"), Subject("alice"));

        cluster.ResetMetrics();
        using var ct1 = new GrainCancellationTokenSource();
        using var ct2 = new GrainCancellationTokenSource();
        var first = await grain.DispatchCheck(Args(50), ct1.Token);
        var second = await grain.DispatchCheck(Args(50), ct2.Token);
        var snapshot = cluster.MetricsSnapshot();

        Assert.True(first.Member);
        Assert.True(second.Member);
        Assert.Equal(0, snapshot.MemoHit);
        Assert.Equal(0, snapshot.MemoMiss);
    }
}
