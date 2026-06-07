using System.Globalization;
using Spiceport.Core;

namespace Spiceport.Datastore.Postgres;

/// <summary>
/// A Postgres datastore revision, mirroring SpiceDB's <c>postgresRevision</c>. A revision is a
/// <see cref="PgSnapshot"/> (the set of transactions settled at this point in time), plus an optional
/// transaction id (<see cref="OptionalXid"/>) identifying the writing transaction when this revision
/// names a single commit.
/// </summary>
/// <remarks>
/// Visibility: a relationship row with <c>created_xid = c</c> and <c>deleted_xid = d</c> is visible at
/// this revision iff <c>Snapshot.TxVisible(c)</c> is true AND (<c>d</c> is the live sentinel OR
/// <c>Snapshot.TxVisible(d)</c> is false). This is exactly Postgres' <c>pg_visible_in_snapshot</c>.
///
/// Ordering uses the snapshot "more information" comparison. Two snapshots may be concurrent
/// (incomparable); to satisfy <see cref="IComparable{T}"/>'s total-order requirement, concurrent
/// revisions fall back to comparing their string form so ordering is at least deterministic.
/// </remarks>
public sealed class PostgresRevision : IRevision
{
    /// <summary>The xid8 sentinel used for the <c>deleted_xid</c> of a live (not-yet-deleted) row.</summary>
    public const ulong LiveDeletedXid = 9223372036854775807UL; // = max_xid8 used by SpiceDB

    internal PgSnapshot Snapshot { get; }

    /// <summary>The transaction id this revision names, when it names a single commit (0 = unset).</summary>
    public ulong OptionalXid { get; }

    /// <summary>Inexact commit time (unix nanos) for lag estimation; not used for ordering.</summary>
    public ulong OptionalTimestampNanos { get; }

    internal PostgresRevision(PgSnapshot snapshot, ulong optionalXid = 0, ulong optionalTimestampNanos = 0)
    {
        Snapshot = snapshot;
        OptionalXid = optionalXid;
        OptionalTimestampNanos = optionalTimestampNanos;
    }

    /// <inheritdoc />
    public bool ByteSortable => false;

    /// <summary>True if a row created at <paramref name="createdXid"/> and deleted at
    /// <paramref name="deletedXid"/> is visible at this revision.</summary>
    internal bool IsVisible(ulong createdXid, ulong deletedXid)
    {
        if (!Snapshot.TxVisible(createdXid))
            return false;
        if (deletedXid == LiveDeletedXid)
            return true;
        return !Snapshot.TxVisible(deletedXid);
    }

    /// <inheritdoc />
    public int CompareTo(IRevision? other)
    {
        if (other is null)
            return 1;
        if (other is not PostgresRevision pg)
            return string.CompareOrdinal(ToString(), other.ToString());

        var cmp = Snapshot.Compare(pg.Snapshot);
        if (cmp == int.MaxValue)
            return string.CompareOrdinal(ToString(), pg.ToString()); // concurrent: deterministic tiebreak
        return cmp;
    }

    /// <inheritdoc />
    public bool Equals(IRevision? other) =>
        other is PostgresRevision pg && Snapshot.Compare(pg.Snapshot) == 0;

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as IRevision);

    /// <inheritdoc />
    public override int GetHashCode() => Snapshot.Xmin.GetHashCode() ^ Snapshot.Xmax.GetHashCode();

    /// <summary>
    /// The opaque, round-trippable string form. Encodes the snapshot plus the optional xid and
    /// timestamp as <c>xmin:xmax:xips|optXid|optTsNanos</c>. Parsed back by
    /// <see cref="PostgresRevisionParser"/> so ZedTokens decode losslessly.
    /// </summary>
    public override string ToString() =>
        string.Create(CultureInfo.InvariantCulture, $"{Snapshot}|{OptionalXid}|{OptionalTimestampNanos}");

    /// <summary>Parses the string form produced by <see cref="ToString"/>.</summary>
    public static PostgresRevision Parse(string value)
    {
        var parts = value.Split('|');
        if (parts.Length != 3)
            throw new FormatException($"invalid postgres revision: {value}");
        var snapshot = PgSnapshot.Parse(parts[0]);
        var optXid = ulong.Parse(parts[1], CultureInfo.InvariantCulture);
        var optTs = ulong.Parse(parts[2], CultureInfo.InvariantCulture);
        return new PostgresRevision(snapshot, optXid, optTs);
    }
}
