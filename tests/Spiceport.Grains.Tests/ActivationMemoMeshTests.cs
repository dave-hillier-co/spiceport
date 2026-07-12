using System.Collections.Immutable;
using Orleans;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;
using static Spiceport.Grains.Tests.DispatchContextTestHelper;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Stage (a) of "Activation-as-cache" (<c>docs/future-work.md</c> item 1.3): <see cref="CheckGrain"/>
/// memoizes its computed pre-context reply in activation state, so a re-dispatch of the same canonical
/// sub-problem to a warm activation is served without re-expanding the relation graph. Every test here
/// resolves the <see cref="ICheckGrain"/> directly by its <see cref="GrainKey"/> and calls
/// <see cref="ICheckGrain.DispatchCheck"/> itself — bypassing the silo-wide <see cref="OrleansDispatcher"/>
/// entry point entirely — so only the grain's OWN memo behaviour is under test.
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

    // A finite (acyclic) two-hop schema used to provoke the OrleansDispatcher's visited-set loop-bypass
    // WITHOUT a genuine relation cycle: doc1 has no direct viewer but a parent doc2 that does. A true
    // self/mutual cycle only ever terminates via MaxDepthExceededException (see LocalDispatcher's
    // remarks: "a true cycle simply consumes depth ... until it throws" — there is no visited-set cut on
    // the verdict path), so it can never surface a graceful CycleCut=true reply to assert against. Instead
    // this test hand-seeds the dispatched visited set with the SECOND hop's own visit key before making
    // the call, so the dispatcher DETERMINISTICALLY sees that hop as already in flight and takes the
    // loop-bypass path — the grain call still happens normally (doc2 has a direct viewer tuple, no
    // further recursion needed) but the RETURNED result is unconditionally tagged CycleCut = true by
    // OrleansDispatcher, regardless of whether that resolution itself needed the loop guard. This reaches
    // a genuinely successful CycleCut = true reply out of a normal, finite graph.
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
        var key = GrainKey.Build(resource, subject, head.Revision.ToString(), schemaHash);
        return (cluster.GrainFactory.GetGrain<ICheckGrain>(key), head.Revision.ToString());
    }

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

        using var ct1 = new CancellationTokenSource();
        using var ct2 = new CancellationTokenSource();

        var before = cluster.MetricsSnapshot();
        SetDispatchContext(50);
        var first = await grain.DispatchCheck(ct1.Token);
        var afterFirst = cluster.MetricsSnapshot();
        SetDispatchContext(50);
        var second = await grain.DispatchCheck(ct2.Token);
        var afterSecond = cluster.MetricsSnapshot();

        Assert.Equal(first, second);
        Assert.True(first.Member);

        // First call: TWO cold activations, not one — "view = viewer" compiles the bare "viewer" reference
        // as a ComputedUsersetChild, so evaluating it is a genuine Sub-dispatch (a real grain call, now
        // that there is no in-process local-recurse shortcut) to the document's OWN "viewer" grain, in
        // addition to the root "view" grain itself.
        Assert.Equal(before.MemoMiss + 2, afterFirst.MemoMiss);
        Assert.Equal(before.MemoHit, afterFirst.MemoHit);

        // Second call: the ROOT's own memo answers immediately without re-expanding the relation graph at
        // all, so the "viewer" child grain is never touched again — exactly one hit (the root), no misses.
        Assert.Equal(afterFirst.MemoHit + 1, afterSecond.MemoHit);
        Assert.Equal(afterFirst.MemoMiss, afterSecond.MemoMiss);
    }

    private const string CaveatSchema = """
        caveat over_age(age int, min_age int) {
          age >= min_age
        }

        definition user {}

        definition document {
          relation viewer: user with over_age
          permission view = viewer
        }
        """;

    [Fact]
    public async Task Memoized_branch_collapses_correctly_under_different_caveat_contexts()
    {
        // (b) A pre-context branch served from a WARM activation's memo is not itself the verdict: the
        // request-time caveat context is applied per-request at Collapse, so two callers of the exact
        // same memoized sub-problem with different contexts correctly get different verdicts.
        await using var cluster = await MeshTestCluster.CreateAsync(CaveatSchema);
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships([
            new RelationshipUpdate(
                Relationship.Create(
                    Resource("document", "doc1", "viewer"),
                    Subject("alice"),
                    new ContextualizedCaveat("over_age", new Dictionary<string, object?> { ["min_age"] = 18 })),
                UpdateOperation.Create),
        ]));

        var (grain, _) = await ResolveGrain(
            cluster, Resource("document", "doc1", "view"), Subject("alice"));

        using var ct1 = new CancellationTokenSource();
        using var ct2 = new CancellationTokenSource();

        var before = cluster.MetricsSnapshot();
        SetDispatchContext(50);
        var first = await grain.DispatchCheck(ct1.Token);
        var afterFirst = cluster.MetricsSnapshot();
        // Second call to the SAME warm activation: served from the memo (a miss, then a hit).
        SetDispatchContext(50);
        var second = await grain.DispatchCheck(ct2.Token);
        var afterSecond = cluster.MetricsSnapshot();

        // (Not asserting first == second by record equality: the caveat's Context dictionary has no
        // value equality, so two independently-marshalled grain replies compare unequal by reference even
        // when memoized. The memo-hit counter below is the real proof of reuse.)
        Assert.True(first.Member);
        Assert.True(second.Member);
        // Same shape as the plain document schema: "view = viewer" dispatches the bare "viewer" reference
        // as a genuine Sub to a second grain, so the first call is TWO misses (root + viewer), not one.
        Assert.Equal(before.MemoMiss + 2, afterFirst.MemoMiss);
        // Second call: served entirely from the root's own memo (no re-expansion, no touching the viewer
        // grain again) — exactly one hit.
        Assert.Equal(afterFirst.MemoHit + 1, afterSecond.MemoHit);

        // Collapse the SAME memoized reply with two different request-time contexts.
        var engine = new CheckEngine(cluster.SchemaProvider.Current.Namespaces, cluster.SchemaProvider.Current.Caveats);
        var caveat = CaveatWire.FromWire(second.Caveat);

        var over = engine.Collapse(
            new DispatchCheckResult(second.Member, caveat, second.CycleCut, second.DepthRequired),
            new Dictionary<string, object?> { ["age"] = 21 });
        var under = engine.Collapse(
            new DispatchCheckResult(second.Member, caveat, second.CycleCut, second.DepthRequired),
            new Dictionary<string, object?> { ["age"] = 16 });

        Assert.Equal(Membership.Member, over.Verdict);
        Assert.Equal(Membership.NotMember, under.Verdict);
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
        using var ctPrime = new CancellationTokenSource();
        SetDispatchContext(1_000);
        var primed = await grain.DispatchCheck(ctPrime.Token);
        Assert.True(primed.Member);
        var required = primed.DepthRequired;
        Assert.True(required > 1, "expected the chain to require more than one hop of depth.");

        // Served: a budget exactly equal to what the memo required is sufficient (DepthRemaining >=
        // DepthRequired), so the memo answers without touching the datastore/graph again.
        using var ctServed = new CancellationTokenSource();
        var beforeServed = cluster.MetricsSnapshot();
        SetDispatchContext(required);
        var served = await grain.DispatchCheck(ctServed.Token);
        var afterServed = cluster.MetricsSnapshot();
        Assert.Equal(primed, served);
        Assert.Equal(beforeServed.MemoHit + 1, afterServed.MemoHit);

        // Not served: a budget ONE SHORT of what the memo required must fall through and recompute under
        // the tighter budget — proven by the recompute legitimately exhausting depth and throwing
        // MaxDepthExceededException (the memo, had it been (wrongly) served, would have returned
        // successfully instead).
        using var ctTight = new CancellationTokenSource();
        var beforeTight = cluster.MetricsSnapshot();
        SetDispatchContext(required - 1);
        await Assert.ThrowsAsync<MaxDepthExceededException>(
            () => grain.DispatchCheck(ctTight.Token));
        var afterTight = cluster.MetricsSnapshot();

        // The root's own memo must NOT serve this call (its DepthRequired is one more than the offered
        // budget), so the root itself recomputes — at least one new miss. (The chain's intermediate
        // grains are each a REAL grain now that there is no local-recurse shortcut, and each one's own
        // depth guard is re-evaluated against the ever-tighter budget the recompute passes down; whether
        // any given intermediate hop is served from ITS OWN warm memo or itself falls through and misses
        // is an implementation detail of how the tightened budget lines up with each grain's cached
        // DepthRequired — not something this test needs to pin down. The one invariant that matters is
        // proven above by the caught exception: a too-tight budget is never wrongly served from a memo.)
        Assert.True(afterTight.MemoMiss > beforeTight.MemoMiss,
            "a budget one short of what the root's memo required must fall through the memo and recompute.");
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

        // Hand-seed the visited set with the SECOND hop's own visit key (doc2/view/alice), so the
        // dispatcher's loop-bypass fires when the root's parent->view arrow reaches it, and
        // OrleansDispatcher unconditionally tags the resulting reply CycleCut = true (see the class
        // remarks on ArrowSchema).
        var seeded = ImmutableHashSet<VisitKey>.Empty.Add(
            VisitKey.Of(Resource("document", "doc2", "view"), Subject("alice")));

        using var ct1 = new CancellationTokenSource();
        var before = cluster.MetricsSnapshot();
        SetDispatchContext(50, seeded);
        var first = await grain.DispatchCheck(ct1.Token);
        var afterFirst = cluster.MetricsSnapshot();

        Assert.True(first.Member);
        Assert.True(first.CycleCut, "expected the pre-seeded visited set to provoke a cycle-cut reply.");
        // Every hop of this check is now a genuine grain, since there is no in-process local-recurse
        // shortcut: the root (doc1/view), the root's bare "viewer" reference (doc1/viewer, a
        // ComputedUsersetChild Sub), the arrow target (doc2/view, the loop-bypassed hop) and ITS bare
        // "viewer" reference (doc2/viewer) — four cold activations, four misses. The root's own reply is
        // a miss (it was computed) but must NOT populate ITS memo, because it inherited CycleCut = true
        // from the force-tagged doc2/view branch.
        Assert.Equal(before.MemoMiss + 4, afterFirst.MemoMiss);

        // A second call with an EMPTY visited set (no seeding) asks the exact same sub-problem; if the root's
        // cycle-cut reply had been (wrongly) memoized, this would be served from it. It must instead
        // recompute at the root (a miss) — but doc1/viewer and doc2/view are each warm from the first
        // call (their OWN replies were computed with CycleCut = false, since the force-tag is applied only
        // to what OrleansDispatcher hands back to the CALLER, never to a callee's own memo decision), so
        // both are served from their own memos (two hits) without re-touching doc2/viewer at all.
        using var ct2 = new CancellationTokenSource();
        SetDispatchContext(50);
        var second = await grain.DispatchCheck(ct2.Token);
        var afterSecond = cluster.MetricsSnapshot();

        Assert.True(second.Member);
        Assert.False(second.CycleCut);
        Assert.Equal(afterFirst.MemoMiss + 1, afterSecond.MemoMiss);
        Assert.Equal(afterFirst.MemoHit + 2, afterSecond.MemoHit);
    }

    [Fact]
    public async Task Optimized_and_exact_checks_at_the_same_revision_share_one_activation_memo()
    {
        // The post-CachingDispatcher invariant this pins (see GrainKey's remarks): once a revision
        // string is resolved, an exact-snapshot check and a minimize-latency check that happen to name
        // the SAME revision string compute the identical answer and legitimately share the CheckGrain
        // activation (and its memo) — there is no mode segment left to keep them apart.
        await using var cluster = await MeshTestCluster.CreateAsync(DocumentSchema);
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships([
            new RelationshipUpdate(
                Relationship.Create(
                    Resource("document", "readme", "viewer"),
                    Subject("alice")),
                UpdateOperation.Create),
        ]));

        var checker = cluster.Checker;
        var subject = new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis);

        cluster.ResetMetrics();
        SetDispatchContext(50);

        // First call: MinimizeLatency (Optimized resolution). No prior write happened since, so the
        // datastore's cached "optimized" revision IS the real current head — a cold pair of activations
        // (root "view" + its bare "viewer" Sub, exactly as Warm_activation_serves_the_second_identical_call
        // observes for this schema).
        var optimized = await checker.Check(
            "document", "readme", "view", subject, caveatContext: null,
            consistency: ConsistencyRequirement.MinimizeLatency, ct: CancellationToken.None);
        var afterOptimized = cluster.MetricsSnapshot();
        Assert.Equal(Membership.Member, optimized.Verdict);

        // Second call: AtExactSnapshot pinned to the token the FIRST call itself just evaluated at. The
        // decoded revision round-trips to the identical string, so this must hit the very same ROOT grain
        // activation warmed a moment ago — served entirely from its memo (which, as
        // Warm_activation_serves_the_second_identical_call establishes for this schema, answers without
        // re-expanding the relation graph at all, so the "viewer" child grain is never touched again).
        SetDispatchContext(50);
        var exact = await checker.Check(
            "document", "readme", "view", subject, caveatContext: null,
            consistency: ConsistencyRequirement.AtExactSnapshot(new ZedToken(optimized.EvaluatedToken)),
            ct: CancellationToken.None);
        var afterExact = cluster.MetricsSnapshot();

        Assert.Equal(Membership.Member, exact.Verdict);
        Assert.Equal(optimized.EvaluatedRevision, exact.EvaluatedRevision);

        // Shared activation memo: the second call adds a hit, not a miss.
        Assert.Equal(afterOptimized.MemoHit + 1, afterExact.MemoHit);
        Assert.Equal(afterOptimized.MemoMiss, afterExact.MemoMiss);
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
        using var ct1 = new CancellationTokenSource();
        using var ct2 = new CancellationTokenSource();
        SetDispatchContext(50);
        var first = await grain.DispatchCheck(ct1.Token);
        SetDispatchContext(50);
        var second = await grain.DispatchCheck(ct2.Token);
        var snapshot = cluster.MetricsSnapshot();

        Assert.True(first.Member);
        Assert.True(second.Member);
        Assert.Equal(0, snapshot.MemoHit);
        Assert.Equal(0, snapshot.MemoMiss);
    }
}
