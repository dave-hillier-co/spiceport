using System.Globalization;

namespace Spiceport.Datastore.Postgres;

/// <summary>
/// A Postgres <c>pg_snapshot</c> value (<c>xmin:xmax:xip_list</c>). Mirrors SpiceDB's
/// <c>pgSnapshot</c>: it captures which transaction ids are settled (committed or aborted) from the
/// perspective of a point-in-time read. Visibility of a transaction <c>t</c> is:
/// <list type="bullet">
/// <item><c>t &lt; xmin</c> =&gt; visible (settled before this snapshot began)</item>
/// <item><c>t &gt;= xmax</c> =&gt; not visible (not yet started when this snapshot was taken)</item>
/// <item><c>xmin &lt;= t &lt; xmax</c> =&gt; visible iff <c>t</c> is NOT in the in-progress (xip) list</item>
/// </list>
/// Ordering between snapshots follows SpiceDB's "more information" comparison so two snapshots can be
/// equal, ordered, or concurrent. This is the basis of the Postgres revision's <see cref="IComparable{T}"/>.
/// </summary>
internal sealed record PgSnapshot(ulong Xmin, ulong Xmax, IReadOnlyList<ulong> XipList)
{
    /// <summary>Parses the canonical Postgres text form <c>xmin:xmax:xip,xip,...</c>.</summary>
    public static PgSnapshot Parse(string text)
    {
        var parts = text.Split(':');
        if (parts.Length != 3)
            throw new FormatException($"invalid pg_snapshot: {text}");

        var xmin = ulong.Parse(parts[0], CultureInfo.InvariantCulture);
        var xmax = ulong.Parse(parts[1], CultureInfo.InvariantCulture);
        var xips = parts[2].Length == 0
            ? Array.Empty<ulong>()
            : parts[2].Split(',').Select(s => ulong.Parse(s, CultureInfo.InvariantCulture)).OrderBy(x => x).ToArray();

        return new PgSnapshot(xmin, xmax, xips);
    }

    /// <summary>The canonical Postgres text form, round-trippable via <see cref="Parse"/>.</summary>
    public override string ToString() =>
        string.Create(CultureInfo.InvariantCulture, $"{Xmin}:{Xmax}:{string.Join(',', XipList)}");

    /// <summary>True if <paramref name="txid"/> is settled (visible) from this snapshot's perspective.</summary>
    public bool TxVisible(ulong txid)
    {
        if (txid < Xmin)
            return true;
        if (txid >= Xmax)
            return false;
        return !XipList.Contains(txid);
    }

    /// <summary>
    /// Returns a new snapshot in which <paramref name="txid"/> is marked settled/visible. Mirrors
    /// SpiceDB's <c>markComplete</c>: used to fold a transaction's own xid into the snapshot it
    /// observed, so the writer's optimized/head revision sees its own commit.
    /// </summary>
    public PgSnapshot MarkComplete(ulong txid)
    {
        if (txid < Xmin)
            return this;

        var xips = new List<ulong>(XipList);
        var xmax = Xmax;

        if (txid >= xmax)
        {
            for (var ip = xmax; ip < txid + 1; ip++)
                xips.Add(ip);
            xmax = txid + 1;
        }

        xips.Remove(txid);
        xips.Sort();

        var xmin = xips.Count > 0 ? xips[0] : xmax;
        return new PgSnapshot(xmin, xmax, xips.Count > 0 ? xips : Array.Empty<ulong>());
    }

    /// <summary>True if <paramref name="other"/> knows the disposition of a transaction this snapshot does not.</summary>
    private bool OtherHasMoreInfo(PgSnapshot other)
    {
        foreach (var xip in XipList)
        {
            if (other.TxVisible(xip))
                return true;
        }

        if (other.Xmax > Xmax)
        {
            var rangeSize = other.Xmax - Xmax;
            ulong inRange = 0;
            foreach (var xip in other.XipList)
            {
                if (xip >= Xmax && xip < other.Xmax)
                    inRange++;
            }
            if (inRange < rangeSize)
                return true;
        }

        return false;
    }

    /// <summary>
    /// SpiceDB-style three-way (plus concurrent) comparison. Returns -1 (&lt;), 0 (=), 1 (&gt;), or
    /// <c>int.MaxValue</c> for concurrent (incomparable) snapshots.
    /// </summary>
    public int Compare(PgSnapshot other)
    {
        var otherMore = OtherHasMoreInfo(other);
        var thisMore = other.OtherHasMoreInfo(this);
        return (otherMore, thisMore) switch
        {
            (true, true) => int.MaxValue,
            (true, false) => -1,
            (false, true) => 1,
            _ => 0,
        };
    }
}
