using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Proves the Phase 0 sequencer observability seam end to end (docs/scalability-program.md):
/// <see cref="ISequencerMetrics"/> is wired into the production DI
/// (<c>AddSpiceportGrainServices</c>), the singleton <c>DatastoreGrain</c> records into it, and
/// <see cref="MeshTestCluster.SequencerMetricsSnapshot"/> sums it across silos exactly like the
/// dispatch counters — the seam the bench harness reads.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public class SequencerMetricsTests
{
    private const string Schema = """
        definition user {}

        definition doc {
          relation viewer: user
        }
        """;

    [Fact]
    public async Task Write_and_fully_consistent_check_surface_in_the_sequencer_counters()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        // One write: exactly one declarative Commit through the sequencer.
        var rel = Relationship.Create(
            new ObjectAndRelation("doc", "d1", "viewer"),
            new ObjectAndRelation("user", "u1", CoreConstants.Ellipsis));
        await cluster.Datastore.ReadWriteTx(
            tx => tx.WriteRelationships([new RelationshipUpdate(rel, UpdateOperation.Create)]));

        // One fully-consistent check: the revision resolver must sample the real head (GetHead), and
        // the engine's graph read hits a COLD GraphShardGrain (fresh cluster, first activation for
        // f/doc/d1), which always hydrates via the sequencer's ReadShard — the forced cold shard read.
        var result = await cluster.Checker.Check(
            "doc", "d1", "viewer",
            new ObjectAndRelation("user", "u1", CoreConstants.Ellipsis),
            caveatContext: null,
            consistency: ConsistencyRequirement.FullyConsistent);
        Assert.Equal(Membership.Member, result.Verdict);

        var m = cluster.SequencerMetricsSnapshot();

        Assert.True(m.Commit >= 1, $"expected at least the test's own commit; saw Commit = {m.Commit}");
        Assert.True(m.GetHead >= 1, $"expected the fully-consistent head sample; saw GetHead = {m.GetHead}");
        Assert.True(m.ReadShard >= 1, $"expected the cold shard hydration; saw ReadShard = {m.ReadShard}");

        // The Commit-only observables ride along: a single-relationship commit resolves exactly one
        // candidate key (its resource's forward key), and a real commit's serialized turn takes
        // measurable time, so the duration totals are nonzero and internally consistent.
        Assert.True(
            m.CommitCandidates1 >= 1,
            $"expected the single-update commit in the at-most-one-candidate bucket; saw {m.CommitCandidates1}");
        Assert.True(m.CommitMicrosTotal > 0, "expected a nonzero total commit duration");
        Assert.True(
            m.CommitMicrosTotal >= m.CommitMicrosMax,
            $"total ({m.CommitMicrosTotal}) must cover the max ({m.CommitMicrosMax})");
    }

    /// <summary>
    /// The seed window (schema embedded at silo startup, no WriteSchema persisted) must resolve schemas
    /// from the seeded per-silo cache, never by calling the sequencer: before the fix the resolver missed
    /// on the seed hash on EVERY dispatch, fetched null from <c>ReadSchemaAt</c>, and fell back without
    /// caching — a per-dispatch singleton hop the measurement harness observed at ~16k calls/second.
    /// </summary>
    [Fact]
    public async Task Seed_window_checks_never_call_ReadSchemaAt()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);

        var rel = Relationship.Create(
            new ObjectAndRelation("doc", "d2", "viewer"),
            new ObjectAndRelation("user", "u2", CoreConstants.Ellipsis));
        await cluster.Datastore.ReadWriteTx(
            tx => tx.WriteRelationships([new RelationshipUpdate(rel, UpdateOperation.Create)]));

        for (var i = 0; i < 10; i++)
        {
            var result = await cluster.Checker.Check(
                "doc", "d2", "viewer",
                new ObjectAndRelation("user", "u2", CoreConstants.Ellipsis),
                caveatContext: null,
                consistency: ConsistencyRequirement.FullyConsistent);
            Assert.Equal(Membership.Member, result.Verdict);
        }

        var m = cluster.SequencerMetricsSnapshot();
        Assert.Equal(0, m.ReadSchemaAt);
    }
}
