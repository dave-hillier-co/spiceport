using Spiceport.Datastore.Postgres;

namespace Spiceport.Datastore.Postgres.Tests;

/// <summary>
/// Pure (Docker-free) tests for the snapshot "more information" comparison, which underpins
/// revision-window checks and pick-best-revision. The key correctness property mirrors SpiceDB's
/// <c>pgSnapshot.GreaterThan</c>: two CONCURRENT (incomparable) snapshots are NOT greater than one
/// another, so concurrent revisions must not be wrongly classified as future/stale (CheckRevision) or
/// preferred over a read-your-writes token (pickBestRevision). The legacy <c>Compare</c> returns
/// <c>int.MaxValue</c> for the concurrent case — &gt; 0, hence the bug — whereas <c>GreaterThan</c>
/// returns false.
/// </summary>
public class PgSnapshotComparisonTests
{
    [Fact]
    public void Compare_OrdersSnapshotsBySettledInformation()
    {
        // "5:5:" sees everything below xid 5; "6:6:" additionally sees xid 5 -> strictly greater.
        var older = PgSnapshot.Parse("5:5:");
        var newer = PgSnapshot.Parse("6:6:");

        Assert.Equal(1, newer.Compare(older));
        Assert.Equal(-1, older.Compare(newer));
        Assert.Equal(0, older.Compare(PgSnapshot.Parse("5:5:")));
    }

    [Fact]
    public void Compare_ReturnsMaxValueSentinel_ForConcurrentSnapshots()
    {
        // Each snapshot has settled a transaction the other has not: A sees xid 5 (but 6 in-progress),
        // B sees xid 6 (but 5 in-progress). Neither dominates -> concurrent.
        var a = PgSnapshot.Parse("5:7:6");
        var b = PgSnapshot.Parse("5:7:5");

        Assert.Equal(int.MaxValue, a.Compare(b));
        Assert.Equal(int.MaxValue, b.Compare(a));
    }

    [Fact]
    public void GreaterThan_IsStrict_AndFalseForEqualAndConcurrent()
    {
        var newer = PgSnapshot.Parse("6:6:");
        var older = PgSnapshot.Parse("5:5:");
        var concurrentA = PgSnapshot.Parse("5:7:6");
        var concurrentB = PgSnapshot.Parse("5:7:5");

        Assert.True(newer.GreaterThan(older));
        Assert.False(older.GreaterThan(newer));
        Assert.False(newer.GreaterThan(PgSnapshot.Parse("6:6:"))); // equal -> not greater
        // The load-bearing fix: concurrent snapshots are NOT greater than one another, even though
        // Compare's concurrent sentinel (int.MaxValue) is > 0.
        Assert.False(concurrentA.GreaterThan(concurrentB));
        Assert.False(concurrentB.GreaterThan(concurrentA));
    }

    [Fact]
    public void RevisionGreaterThan_TreatsConcurrentAsNotGreater_UnlikeCompareToTiebreak()
    {
        // Two revisions over concurrent snapshots: CompareTo falls back to an ordinal string tiebreak
        // (so one is arbitrarily "greater"), but GreaterThan must report false for BOTH directions.
        var revA = PostgresRevision.Parse("5:7:6|0|0");
        var revB = PostgresRevision.Parse("5:7:5|0|0");

        Assert.False(revA.GreaterThan(revB));
        Assert.False(revB.GreaterThan(revA));

        // A strictly-newer revision IS greater.
        var newer = PostgresRevision.Parse("6:6:|0|0");
        var older = PostgresRevision.Parse("5:5:|0|0");
        Assert.True(newer.GreaterThan(older));
        Assert.False(older.GreaterThan(newer));
    }
}
