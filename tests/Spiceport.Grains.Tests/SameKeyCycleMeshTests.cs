using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Schema;

namespace Spiceport.Grains.Tests;

/// <summary>
/// THE regression test for the re-entrant same-key grain deadlock (issue #2, parts B1 + B2).
/// </summary>
/// <remarks>
/// A genuine same-key cycle (<c>group:a#member -&gt; group:b#member -&gt; group:a#member</c>) re-addresses
/// the SAME grain key on the way back round, because the grain key excludes the visited set/depth. On the
/// pre-change code (non-reentrant grain, visited-set cycle-cut living below the grain hop) the outer grain
/// call blocks awaiting the inner call to its own key — a deadlock that only breaks on a timeout. After the
/// change: (B1) the dispatcher detects the genuine loop via the exact visited set and recurses IN-PROCESS
/// instead of re-entering the busy grain, and (B2) <c>CheckGrain</c> is <c>[Reentrant]</c> so even a
/// re-entry that slips past B1 is accepted, never blocked. With correctness now resting solely on the
/// depth budget (A1), the cycle terminates deterministically with <see cref="MaxDepthExceededException"/>
/// rather than a confident (wrong) NotMember. The TIGHT timeout is load-bearing: a deadlock would blow it.
/// </remarks>
[Collection(MeshClusterCollection.Name)]
public class SameKeyCycleMeshTests
{
    private const string CycleSchema = """
        definition user {}

        definition group {
            relation member: user | group#member
        }
        """;

    [Fact]
    public async Task SameKeyCycle_terminates_with_error_and_never_deadlocks_the_mesh()
    {
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(CycleSchema, siloCount: 3);

        // group:a#member -> group:b#member -> group:a#member: a true cycle, no real member inside.
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

        // TIGHT timeout: a deadlock (the pre-B1/B2 behaviour) would hang here; the depth-bounded cycle
        // must complete (by throwing) well inside it.
        var check = cluster.Checker.Check(
            "group", "a", "member",
            new ObjectAndRelation("user", "x", CoreConstants.Ellipsis), null);

        var completed = await Task.WhenAny(check, Task.Delay(TimeSpan.FromSeconds(20)));
        Assert.True(ReferenceEquals(completed, check),
            "The same-key cycle did not terminate within the timeout — the grain mesh deadlocked.");

        // It must surface an ERROR (depth exhausted), never a confident NotMember. The exception
        // round-trips the Orleans grain boundary as MaxDepthExceededException (it is [GenerateSerializer]).
        await Assert.ThrowsAsync<MaxDepthExceededException>(() => check);
    }

    [Fact]
    public async Task DeepAcyclicChain_within_budget_resolves_member_across_the_mesh()
    {
        // A linear chain of 49 hops (under maxDepth 50) must resolve Member across the real grain mesh:
        // the exact visited-set loop guard must not falsely terminate a legitimately deep, acyclic graph.
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(CycleSchema, siloCount: 3);

        var updates = new List<RelationshipUpdate>();
        for (var i = 0; i < 48; i++)
        {
            updates.Add(new RelationshipUpdate(
                Relationship.Create(
                    new ObjectAndRelation("group", $"g{i}", "member"),
                    new ObjectAndRelation("group", $"g{i + 1}", "member")),
                UpdateOperation.Create));
        }
        updates.Add(new RelationshipUpdate(
            Relationship.Create(
                new ObjectAndRelation("group", "g48", "member"),
                new ObjectAndRelation("user", "x", CoreConstants.Ellipsis)),
            UpdateOperation.Create));
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(updates));

        var result = await cluster.Checker.Check(
            "group", "g0", "member",
            new ObjectAndRelation("user", "x", CoreConstants.Ellipsis), null);

        Assert.Equal(Membership.Member, result.Verdict);
    }

    /// <summary>
    /// Step 2 mesh gate for the local-recurse deletion: a genuine same-key grain re-entry (the SAME
    /// group:a &lt;-&gt; group:b cycle as above) must terminate through the multi-silo mesh with EXACTLY the
    /// verdict the <see cref="ReferenceDatastore"/> reference model’s in-process <see cref="CheckEngine"/> produces
    /// for the identical schema/relationships/request — an error (<see cref="MaxDepthExceededException"/>),
    /// never a confident (wrong) verdict on either side. Now that <see cref="OrleansDispatcher"/> has no
    /// in-process local-recurse shortcut, every hop of the cycle — including the one that re-addresses the
    /// SAME grain key it started from — is a real grain call; termination rests solely on the depth
    /// budget, and the grain's <c>[Reentrant]</c> attribute (not a visited-set-based in-process bypass) is
    /// what keeps a same-key re-entry from deadlocking.
    /// </summary>
    [Fact]
    public async Task SameKeyCycle_mesh_verdict_matches_the_ReferenceDatastore_reference_model()
    {
        var relationships = new List<RelationshipUpdate>
        {
            new(
                Relationship.Create(
                    new ObjectAndRelation("group", "a", "member"),
                    new ObjectAndRelation("group", "b", "member")),
                UpdateOperation.Create),
            new(
                Relationship.Create(
                    new ObjectAndRelation("group", "b", "member"),
                    new ObjectAndRelation("group", "a", "member")),
                UpdateOperation.Create),
        };
        var subject = new ObjectAndRelation("user", "x", CoreConstants.Ellipsis);

        // The reference model: a plain in-process CheckEngine over a ReferenceDatastore, no grains, no visited-set wiring.
        var compiled = SchemaCompiler.CompileSchema(CycleSchema);
        var referenceStore = new ReferenceDatastore();
        var referenceRevision = await referenceStore.ReadWriteTx(tx => tx.WriteRelationships(relationships));
        var referenceReader = referenceStore.SnapshotReader(referenceRevision);
        var referenceEngine = new CheckEngine(compiled.Namespaces, compiled.Caveats);

        await Assert.ThrowsAsync<MaxDepthExceededException>(
            () => referenceEngine.Check(referenceReader, "group", "a", "member", subject, caveatContext: null));

        // The mesh: the same schema/relationships/request across a real 3-silo Orleans cluster, where the
        // cycle's second hop re-addresses the SAME grain key the check started from.
        await using var cluster = await MeshTestCluster.CreateMultiSiloAsync(CycleSchema, siloCount: 3);
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(relationships));

        var meshCheck = cluster.Checker.Check("group", "a", "member", subject, null);
        var completed = await Task.WhenAny(meshCheck, Task.Delay(TimeSpan.FromSeconds(20)));
        Assert.True(ReferenceEquals(completed, meshCheck),
            "The same-key cycle did not terminate within the timeout — the grain mesh deadlocked.");

        await Assert.ThrowsAsync<MaxDepthExceededException>(() => meshCheck);
    }
}
