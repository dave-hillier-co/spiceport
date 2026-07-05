using Spiceport.Core;
using Spiceport.Datastore;

namespace Spiceport.Datastore.Tests;

public class RevisionResolverTests
{
    private static Relationship Rel(string id) =>
        Relationship.Create(
            new ObjectAndRelation("document", id, "viewer"),
            new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis));

    private static async Task<IRevision> Write(ReferenceDatastore ds, string id) =>
        await ds.ReadWriteTx(async tx =>
            await tx.WriteRelationships([new RelationshipUpdate(Rel(id), UpdateOperation.Create)]));

    // No quantization: OptimizedRevision == HeadRevision, so revision comparisons are exact and deterministic.
    private static ReferenceDatastore NewExactDatastore() => new(quantization: TimeSpan.Zero);

    private static async Task<ZedToken> TokenFor(ReferenceDatastore ds, IRevision rev)
    {
        var id = await ds.GetUniqueId();
        return ZedTokens.FromRevision(rev, schemaHash: null, datastoreUniqueId: id);
    }

    // --- Token round-trip ---

    [Fact]
    public async Task TokenRoundTrip_MintedTokenDecodesToCommittedRevision_AndSnapshotReads()
    {
        var ds = new ReferenceDatastore();
        var committed = await Write(ds, "doc1");
        var token = await TokenFor(ds, committed);

        var parser = await ds.GetRevisionParser();
        var decoded = ZedTokens.DecodeRevision(token, parser);

        Assert.Equal(ZedTokenStatus.Valid, decoded.Status);
        Assert.True(decoded.Revision.Equals(committed));
        // The decoded revision must be snapshot-readable by the datastore.
        var reader = ds.SnapshotReader(decoded.Revision);
        Assert.True(reader.IsValid);
    }

    [Fact]
    public async Task TokenRoundTrip_DifferentDatastoreId_DecodesAsMismatched()
    {
        var ds = new ReferenceDatastore();
        var committed = await Write(ds, "doc1");
        var foreign = ZedTokens.FromRevision(committed, schemaHash: null, datastoreUniqueId: "some-other-id");

        var parser = await ds.GetRevisionParser();
        var decoded = ZedTokens.DecodeRevision(foreign, parser);

        Assert.Equal(ZedTokenStatus.MismatchedDatastoreId, decoded.Status);
    }

    // --- MinimizeLatency ---

    [Fact]
    public async Task MinimizeLatency_ResolvesToOptimizedRevision_ModeOptimized()
    {
        var ds = NewExactDatastore();
        await Write(ds, "doc1");
        var opt = await ds.OptimizedRevision();

        var resolved = await RevisionResolver.Resolve(ds, ConsistencyRequirement.MinimizeLatency);

        Assert.Equal(RevisionMode.Optimized, resolved.Mode);
        Assert.True(resolved.Revision.Equals(opt.Revision));
    }

    // --- FullyConsistent ---

    [Fact]
    public async Task FullyConsistent_ResolvesToHeadRevision_ModeExact()
    {
        var ds = new ReferenceDatastore();
        var head = await Write(ds, "doc1");

        var resolved = await RevisionResolver.Resolve(ds, ConsistencyRequirement.FullyConsistent);

        Assert.Equal(RevisionMode.Exact, resolved.Mode);
        Assert.True(resolved.Revision.Equals(head));
    }

    // --- AtLeastAsFresh = max(token, optimized) ---

    [Fact]
    public async Task AtLeastAsFresh_OptimizedFresher_PicksOptimized_ModeOptimized()
    {
        var ds = NewExactDatastore();
        // Old token captured before later writes; optimized (== head) is strictly fresher.
        var oldRev = await Write(ds, "doc1");
        var oldToken = await TokenFor(ds, oldRev);
        await Write(ds, "doc2"); // advances head past the token revision
        var opt = await ds.OptimizedRevision();
        Assert.True(opt.Revision.CompareTo(oldRev) > 0); // precondition

        var resolved = await RevisionResolver.Resolve(ds, ConsistencyRequirement.AtLeastAsFresh(oldToken));

        Assert.Equal(RevisionMode.Optimized, resolved.Mode);
        Assert.True(resolved.Revision.Equals(opt.Revision));
    }

    [Fact]
    public async Task AtLeastAsFresh_TokenFresher_PicksToken_ModeExact()
    {
        var ds = NewExactDatastore();
        var rev = await Write(ds, "doc1");
        var opt = await ds.OptimizedRevision();
        // Mint a token strictly fresher than the current optimized/head revision (future read-your-writes case).
        var fresherRev = new TimestampRevision(((TimestampRevision)opt.Revision).TimestampNanosSinceEpoch + 1_000_000);
        var fresherToken = await TokenFor(ds, fresherRev);

        var resolved = await RevisionResolver.Resolve(ds, ConsistencyRequirement.AtLeastAsFresh(fresherToken));

        Assert.Equal(RevisionMode.Exact, resolved.Mode);
        Assert.True(resolved.Revision.Equals(fresherRev));
    }

    [Fact]
    public async Task AtLeastAsFresh_EqualRevision_PicksToken_ModeExact()
    {
        var ds = NewExactDatastore();
        await Write(ds, "doc1");
        var opt = await ds.OptimizedRevision();
        var token = await TokenFor(ds, opt.Revision);

        var resolved = await RevisionResolver.Resolve(ds, ConsistencyRequirement.AtLeastAsFresh(token));

        // token >= optimized -> token wins (exact). Boundary of the max.
        Assert.Equal(RevisionMode.Exact, resolved.Mode);
        Assert.True(resolved.Revision.Equals(opt.Revision));
    }

    [Fact]
    public async Task AtLeastAsFresh_MismatchedDatastore_FallsToFullConsistency()
    {
        var ds = new ReferenceDatastore();
        var rev = await Write(ds, "doc1");
        var foreign = ZedTokens.FromRevision(rev, schemaHash: null, datastoreUniqueId: "other-instance");

        var resolved = await RevisionResolver.Resolve(ds, ConsistencyRequirement.AtLeastAsFresh(foreign));
        var head = await ds.HeadRevision();

        Assert.Equal(RevisionMode.Exact, resolved.Mode);
        Assert.True(resolved.Revision.Equals(head.Revision));
    }

    [Fact]
    public async Task AtLeastAsFresh_MismatchedDatastore_AsError_Throws()
    {
        var ds = new ReferenceDatastore();
        var rev = await Write(ds, "doc1");
        var foreign = ZedTokens.FromRevision(rev, schemaHash: null, datastoreUniqueId: "other-instance");

        await Assert.ThrowsAsync<InvalidConsistencyTokenException>(() =>
            RevisionResolver.Resolve(ds, ConsistencyRequirement.AtLeastAsFresh(foreign),
                MismatchingTokenOption.TreatAsError));
    }

    [Fact]
    public async Task AtLeastAsFresh_MalformedToken_Throws()
    {
        var ds = new ReferenceDatastore();
        await Write(ds, "doc1");
        var garbage = new ZedToken("!!!not-base64!!!");

        await Assert.ThrowsAsync<InvalidConsistencyTokenException>(() =>
            RevisionResolver.Resolve(ds, ConsistencyRequirement.AtLeastAsFresh(garbage)));
    }

    // --- AtExactSnapshot ---

    [Fact]
    public async Task AtExactSnapshot_ResolvesToTokenRevision_ModeExact_IgnoringLaterWrites()
    {
        var ds = NewExactDatastore();
        var rev = await Write(ds, "doc1");
        var token = await TokenFor(ds, rev);
        await Write(ds, "doc2"); // advances head; must not change the resolved exact snapshot

        var resolved = await RevisionResolver.Resolve(ds, ConsistencyRequirement.AtExactSnapshot(token));

        Assert.Equal(RevisionMode.Exact, resolved.Mode);
        Assert.True(resolved.Revision.Equals(rev));
    }

    [Fact]
    public async Task AtExactSnapshot_MismatchedDatastore_Throws()
    {
        var ds = new ReferenceDatastore();
        var rev = await Write(ds, "doc1");
        var foreign = ZedTokens.FromRevision(rev, schemaHash: null, datastoreUniqueId: "other-instance");

        await Assert.ThrowsAsync<InvalidConsistencyTokenException>(() =>
            RevisionResolver.Resolve(ds, ConsistencyRequirement.AtExactSnapshot(foreign)));
    }

    [Fact]
    public async Task AtExactSnapshot_RevisionOutsideGcWindow_Throws()
    {
        // Very tight GC window so an old revision is no longer available.
        var ds = new ReferenceDatastore(gcWindow: TimeSpan.FromMilliseconds(1));
        var staleRev = new TimestampRevision(1); // far in the past, before the GC floor
        var token = await TokenFor(ds, staleRev);

        await Assert.ThrowsAsync<InvalidConsistencyTokenException>(() =>
            RevisionResolver.Resolve(ds, ConsistencyRequirement.AtExactSnapshot(token)));
    }

    [Fact]
    public async Task AtExactSnapshot_MalformedToken_Throws()
    {
        var ds = new ReferenceDatastore();
        await Write(ds, "doc1");
        var garbage = new ZedToken("###");

        await Assert.ThrowsAsync<InvalidConsistencyTokenException>(() =>
            RevisionResolver.Resolve(ds, ConsistencyRequirement.AtExactSnapshot(garbage)));
    }
}
