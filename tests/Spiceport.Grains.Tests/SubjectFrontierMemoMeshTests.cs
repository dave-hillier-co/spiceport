using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// <see cref="SubjectFrontierGrain"/>'s per-activation LookupSubjects frontier memo — the LookupSubjects
/// analogue of stage (a) of "Activation-as-cache" (<c>docs/future-work.md</c> item 1.3). Every test
/// resolves the <see cref="ISubjectFrontierGrain"/> directly by its <see cref="SubjectFrontierKey"/> (or
/// drives <see cref="IReverseOpsStreamGrain.StreamLookupSubjects"/>, for the caveat-context test), so only
/// this feature's own memo/consumer behaviour is under test.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class SubjectFrontierMemoMeshTests
{
    private const string DocumentSchema = """
        definition user {}

        definition document {
            relation viewer: user
            permission view = viewer
        }
        """;

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

    private static ObjectAndRelation Resource(string type, string id, string relation) =>
        new(type, id, relation);

    private static ObjectAndRelation Subject(string id) =>
        new("user", id, CoreConstants.Ellipsis);

    private static async Task<ISubjectFrontierGrain> ResolveGrain(
        MeshTestCluster cluster, ObjectAndRelation resource, string subjectType,
        string subjectRelation = CoreConstants.Ellipsis)
    {
        var head = await cluster.Datastore.HeadRevision();
        var schemaHash = cluster.SchemaProvider.Current.SchemaHash;
        var key = SubjectFrontierKey.Build(
            resource, subjectType, subjectRelation, head.Revision.ToString(), schemaHash);
        return cluster.GrainFactory.GetGrain<ISubjectFrontierGrain>(key);
    }

    [Fact]
    public async Task Warm_activation_serves_the_second_identical_call_from_the_memo()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(DocumentSchema);
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships([
            new RelationshipUpdate(
                Relationship.Create(Resource("document", "readme", "viewer"), Subject("alice")),
                UpdateOperation.Create),
        ]));

        var grain = await ResolveGrain(cluster, Resource("document", "readme", "view"), "user");

        using var ct1 = new CancellationTokenSource();
        using var ct2 = new CancellationTokenSource();

        var before = cluster.MetricsSnapshot();
        var first = await grain.GetFrontier(ct1.Token);
        var afterFirst = cluster.MetricsSnapshot();
        var second = await grain.GetFrontier(ct2.Token);
        var afterSecond = cluster.MetricsSnapshot();

        // Not asserting record equality directly: each grain call returns an independently marshalled
        // list, so IReadOnlyList<T>'s default (reference) equality makes two equal-content replies compare
        // unequal even when served from the same memo — the same caveat this file's CheckGrain counterpart
        // documents for its own reply's Context dictionary. Compare by subject id instead.
        Assert.Equal(
            first.Subjects.Select(s => s.SubjectId).OrderBy(s => s, StringComparer.Ordinal),
            second.Subjects.Select(s => s.SubjectId).OrderBy(s => s, StringComparer.Ordinal));
        Assert.Contains(first.Subjects, s => s.SubjectId == "alice");

        Assert.Equal(before.FrontierMemoMiss + 1, afterFirst.FrontierMemoMiss);
        Assert.Equal(before.FrontierMemoHit, afterFirst.FrontierMemoHit);

        Assert.Equal(afterFirst.FrontierMemoHit + 1, afterSecond.FrontierMemoHit);
        Assert.Equal(afterFirst.FrontierMemoMiss, afterSecond.FrontierMemoMiss);
    }

    [Fact]
    public async Task Memoized_frontier_collapses_differently_under_different_request_contexts_via_the_stream()
    {
        // (b) The memoized frontier is the pre-context shape: caveat context is applied per-request at
        // ReverseOpsStreamGrain, so two StreamLookupSubjects calls sharing the SAME warm memoized frontier
        // correctly collapse to different verdicts for the caveated subject.
        await using var cluster = await MeshTestCluster.CreateAsync(CaveatSchema);
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships([
            new RelationshipUpdate(
                Relationship.Create(
                    Resource("document", "doc1", "viewer"),
                    Subject("alice"),
                    new ContextualizedCaveat("over_age", new Dictionary<string, object?> { ["min_age"] = 18 })),
                UpdateOperation.Create),
        ]));

        async Task<Permissionship?> StreamOnce(IReadOnlyDictionary<string, object?>? context)
        {
            var args = new LookupSubjectsArgs(
                "document", "doc1", "view", "user", CoreConstants.Ellipsis,
                Context: context, Limit: null, Cursor: null);
            await foreach (var item in cluster.GrainFactory
                .GetGrain<IReverseOpsStreamGrain>(Guid.NewGuid()).StreamLookupSubjects(args))
            {
                if (item.Subject.SubjectId == "alice")
                    return item.Subject.Permissionship;
            }
            return null;
        }

        var before = cluster.MetricsSnapshot();
        var over = await StreamOnce(new Dictionary<string, object?> { ["age"] = 21 });
        var afterFirst = cluster.MetricsSnapshot();
        var under = await StreamOnce(new Dictionary<string, object?> { ["age"] = 16 });
        var afterSecond = cluster.MetricsSnapshot();

        Assert.NotNull(over);
        Assert.False(over!.IsCaveated); // age 21 satisfies min_age 18 -> definite member.

        Assert.Null(under); // age 16 fails min_age 18 -> definitely excluded, sheared off entirely.

        // Same memoized activation served both requests: one miss (cold), then a hit.
        Assert.Equal(before.FrontierMemoMiss + 1, afterFirst.FrontierMemoMiss);
        Assert.Equal(afterFirst.FrontierMemoHit + 1, afterSecond.FrontierMemoHit);
        Assert.Equal(afterFirst.FrontierMemoMiss, afterSecond.FrontierMemoMiss);
    }

    [Fact]
    public async Task Over_cap_frontier_is_served_but_not_retained()
    {
        // (c) A tiny MaxMemoSubjects with a frontier that exceeds it: nothing is retained, so BOTH calls
        // register as misses even though they hit the SAME warm activation.
        await using var cluster = await MeshTestCluster.CreateAsync(DocumentSchema, subjectFrontierMaxMemoSubjects: 1);
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships([
            new RelationshipUpdate(
                Relationship.Create(Resource("document", "readme", "viewer"), Subject("alice")),
                UpdateOperation.Create),
            new RelationshipUpdate(
                Relationship.Create(Resource("document", "readme", "viewer"), Subject("bob")),
                UpdateOperation.Create),
        ]));

        var grain = await ResolveGrain(cluster, Resource("document", "readme", "view"), "user");

        using var ct1 = new CancellationTokenSource();
        using var ct2 = new CancellationTokenSource();

        var before = cluster.MetricsSnapshot();
        var first = await grain.GetFrontier(ct1.Token);
        var afterFirst = cluster.MetricsSnapshot();
        var second = await grain.GetFrontier(ct2.Token);
        var afterSecond = cluster.MetricsSnapshot();

        Assert.Equal(2, first.Subjects.Count);
        Assert.Equal(2, second.Subjects.Count);

        Assert.Equal(before.FrontierMemoMiss + 1, afterFirst.FrontierMemoMiss);
        Assert.Equal(afterFirst.FrontierMemoMiss + 1, afterSecond.FrontierMemoMiss);
        Assert.Equal(before.FrontierMemoHit, afterSecond.FrontierMemoHit);
    }

    [Fact]
    public async Task Disabled_memo_never_hits_or_misses()
    {
        // (d) useSubjectFrontierMemo: false -> zero frontier hits+misses across a run that would
        // otherwise (via StreamLookupSubjects's memo-consulting branch) produce some.
        await using var cluster = await MeshTestCluster.CreateAsync(DocumentSchema, useSubjectFrontierMemo: false);
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships([
            new RelationshipUpdate(
                Relationship.Create(Resource("document", "readme", "viewer"), Subject("alice")),
                UpdateOperation.Create),
        ]));

        cluster.ResetMetrics();

        var args = new LookupSubjectsArgs(
            "document", "readme", "view", "user", CoreConstants.Ellipsis,
            Context: null, Limit: null, Cursor: null);

        var found = new List<string>();
        await foreach (var item in cluster.GrainFactory
            .GetGrain<IReverseOpsStreamGrain>(Guid.NewGuid()).StreamLookupSubjects(args))
        {
            found.Add(item.Subject.SubjectId);
        }
        await foreach (var item in cluster.GrainFactory
            .GetGrain<IReverseOpsStreamGrain>(Guid.NewGuid()).StreamLookupSubjects(args))
        {
            found.Add(item.Subject.SubjectId);
        }

        var snapshot = cluster.MetricsSnapshot();

        Assert.Contains("alice", found);
        Assert.Equal(0, snapshot.FrontierMemoHit);
        Assert.Equal(0, snapshot.FrontierMemoMiss);
    }
}
