using Microsoft.Extensions.DependencyInjection;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Exercises <see cref="IPermissionChecker.BatchCheck"/> THROUGH the real grain mesh: every item's root
/// sub-problem fans out over the shared Caching-over-Orleans dispatcher against ONE pinned revision.
/// Verifies (1) per-item verdicts equal the per-item <see cref="IPermissionChecker.Check"/> for each item
/// (including a caveated item and a not-member item), all index-aligned and sharing ONE evaluated token;
/// and (2) a batch sharing a common sub-problem performs materially fewer underlying dispatches/datastore
/// reads than the naive sum of independent checks, observed via the shared dispatch cache's hit/miss
/// counters.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class BatchCheckMeshTests
{
    // Each document's `view` depends on the SAME shared sub-problem: group:eng#member@user:alice.
    // `restricted` additionally gates membership on a caveat so we can exercise a per-item caveated verdict.
    private const string SchemaText = """
        definition user {}

        caveat allow_region(region string) {
            region == "eu"
        }

        definition group {
            relation member: user
        }

        definition document {
            relation parent: group
            relation restricted: group with allow_region
            permission view = parent->member
            permission restricted_view = restricted->member
        }
        """;

    private static InMemoryDispatchCache Cache(MeshTestCluster cluster) =>
        (InMemoryDispatchCache)cluster.Services.GetRequiredService<IDispatchCache>();

    private static async Task SeedSharedGroupFixture(MeshTestCluster cluster, int docCount)
    {
        var updates = new List<RelationshipUpdate>
        {
            // The shared sub-problem every document depends on.
            new(Relationship.Create(
                new ObjectAndRelation("group", "eng", "member"),
                new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis)), UpdateOperation.Touch),
        };

        for (var i = 0; i < docCount; i++)
        {
            updates.Add(new(Relationship.Create(
                new ObjectAndRelation("document", $"doc{i}", "parent"),
                new ObjectAndRelation("group", "eng", CoreConstants.Ellipsis)), UpdateOperation.Touch));
        }

        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(updates));
    }

    [Fact]
    public async Task BatchCheck_per_item_equals_Check_with_caveated_and_not_member_items_and_one_token()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(SchemaText);
        await SeedSharedGroupFixture(cluster, docCount: 3);

        // restricted edge gated on the caveat, so restricted_view collapses per item context.
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(
        [
            new(Relationship.Create(
                new ObjectAndRelation("document", "doc0", "restricted"),
                new ObjectAndRelation("group", "eng", CoreConstants.Ellipsis),
                new ContextualizedCaveat("allow_region")), UpdateOperation.Touch),
        ]));

        var alice = new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis);
        var bob = new ObjectAndRelation("user", "bob", CoreConstants.Ellipsis);
        var euContext = new Dictionary<string, object?> { ["region"] = "eu" };

        var items = new List<BatchCheckItem>
        {
            // Member: alice via group on doc1.
            new("document", "doc1", "view", alice, null),
            // Not a member: bob is in no group.
            new("document", "doc1", "view", bob, null),
            // Caveated member: alice via the restricted edge, NO context supplied -> Caveated.
            new("document", "doc0", "restricted_view", alice, null),
            // Same restricted edge, WITH eu context -> Member (shared branch, per-item context collapse).
            new("document", "doc0", "restricted_view", alice, euContext),
        };

        var batch = await cluster.Checker.BatchCheck(items);

        // Index-aligned + per-item correctness.
        Assert.Equal(items.Count, batch.Items.Count);
        Assert.Equal(Membership.Member, batch.Items[0].Verdict);
        Assert.Equal(Membership.NotMember, batch.Items[1].Verdict);
        Assert.Equal(Membership.Caveated, batch.Items[2].Verdict);
        Assert.Contains("region", batch.Items[2].MissingFields);
        Assert.Equal(Membership.Member, batch.Items[3].Verdict);

        // ONE shared evaluated token for the whole batch.
        Assert.False(string.IsNullOrEmpty(batch.EvaluatedToken));

        // Batch result equals the per-item Check for each item.
        for (var i = 0; i < items.Count; i++)
        {
            var it = items[i];
            var single = await cluster.Checker.Check(
                it.ResourceType, it.ResourceId, it.Permission, it.Subject, it.CaveatContext);
            Assert.Equal(single.Verdict, batch.Items[i].Verdict);
            Assert.Equal(single.MissingFields, batch.Items[i].MissingFields);
        }
    }

    [Fact]
    public async Task BatchCheck_sharing_a_common_subproblem_does_fewer_dispatches_than_naive_sum()
    {
        const int docCount = 20;

        // Serialize the fan-out (concurrency = 1) so the shared-branch cache behaviour is deterministic:
        // no two items race on the same key before either stores it.
        await using var cluster = await MeshTestCluster.CreateAsync(SchemaText, batchConcurrency: 1);
        await SeedSharedGroupFixture(cluster, docCount);
        var alice = new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis);

        // docCount distinct documents whose `view` arrow ALL depend on the SAME shared sub-problem
        // group:eng#member@user:alice.
        var items = Enumerable.Range(0, docCount)
            .Select(i => new BatchCheckItem("document", $"doc{i}", "view", alice, null))
            .ToList();

        var cache = Cache(cluster);
        var (hitsBefore, missesBefore) = (cache.Hits, cache.Misses);

        var batch = await cluster.Checker.BatchCheck(items);
        Assert.All(batch.Items, item => Assert.Equal(Membership.Member, item.Verdict));

        var batchHits = cache.Hits - hitsBefore;
        var batchMisses = cache.Misses - missesBefore;

        // The NAIVE sum: each of the docCount documents independently expands its `view` arrow, which
        // dispatches the shared group:eng#member@user:alice leaf once per document => docCount underlying
        // leaf dispatches, on top of each document's own distinct root sub-problem.
        const int naiveSharedLeafDispatches = docCount;

        // Because all items share ONE pinned revision + schema hash, the common group leaf is COMPUTED
        // ONCE (a single miss) and served from the shared branch cache for the other docCount-1 documents
        // (docCount-1 hits). That is materially fewer underlying dispatches than the naive sum.
        Assert.Equal(docCount - 1, batchHits);
        Assert.True(batchHits > 0, $"expected batch cache hits > 0, got {batchHits}");

        // Distinct misses (= distinct sub-problems actually dispatched) is the docCount distinct document
        // roots plus the ONE shared leaf = docCount + 1, strictly fewer than the naive sum which would
        // dispatch the shared leaf docCount times (docCount + naiveSharedLeafDispatches total for it).
        Assert.True(batchMisses < docCount + naiveSharedLeafDispatches,
            $"expected batch misses ({batchMisses}) materially below the naive dispatch sum");
        Assert.Equal(docCount + 1, batchMisses);

        // Corroborating single-revision proof: every item shares ONE evaluated token.
        Assert.False(string.IsNullOrEmpty(batch.EvaluatedToken));
    }
}
