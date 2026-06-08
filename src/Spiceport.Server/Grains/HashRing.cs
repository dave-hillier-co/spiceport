using System.Collections.Concurrent;
using System.Text;
using Orleans.Runtime;

namespace Spiceport.Grains;

/// <summary>
/// A deterministic consistent-hash ring over a set of <see cref="SiloAddress"/>es. This is the single
/// source of placement truth: both <see cref="ConsistentHashPlacementDirector"/> (which decides where a
/// grain activates) and <see cref="ISiloOwnership"/> (which lets the dispatcher predict that owner
/// without sending a message) compute ownership through this one function, so they always agree for a
/// given membership view.
/// </summary>
/// <remarks>
/// Hashing uses FNV-1a over UTF-8 bytes — a stable, process-independent non-cryptographic hash — NOT
/// <see cref="string.GetHashCode()"/>, which is randomized per process and would make two silos
/// disagree. Each silo is placed on the ring at <see cref="VirtualNodes"/> points (mirroring the
/// replication factor used by authzed/consistent) so removing or adding one silo only reassigns the
/// keys in the arcs adjacent to that silo's points (~1/N of all keys), the minimal-reassignment
/// property of a consistent hash ring.
/// </remarks>
internal static class HashRing
{
    /// <summary>Number of ring points per silo (replication factor), mirroring authzed/consistent.</summary>
    public const int VirtualNodes = 100;

    private static readonly ConcurrentDictionary<string, RingSnapshot> Cache = new();

    /// <summary>
    /// Returns the owner silo for <paramref name="key"/> given the active <paramref name="silos"/>:
    /// the silo whose ring point is the first one at or after the key's hash (wrapping past the end).
    /// Pure function of (key, silo set) — no activation, no message.
    /// </summary>
    /// <param name="key">The canonical sub-problem key (the <c>ICheckGrain</c> string key).</param>
    /// <param name="silos">The current membership view (active, compatible silos). Must be non-empty.</param>
    public static SiloAddress Owner(string key, IReadOnlyList<SiloAddress> silos)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(silos);
        if (silos.Count == 0)
            throw new ArgumentException("Cannot resolve an owner from an empty silo set.", nameof(silos));

        var ring = GetOrBuild(silos);
        var keyHash = Fnv1a64(key);

        // First ring point >= keyHash; wrap to index 0 if the key hashes past the last point.
        var idx = LowerBound(ring.Hashes, keyHash);
        if (idx == ring.Hashes.Length)
            idx = 0;
        return ring.Owners[idx];
    }

    private static RingSnapshot GetOrBuild(IReadOnlyList<SiloAddress> silos)
    {
        // Cache keyed by the ORDERED set of parsable silo strings so a steady membership reuses the
        // sorted ring; a membership change yields a different cache key and a fresh build.
        var parsable = silos.Select(s => s.ToParsableString()).OrderBy(s => s, StringComparer.Ordinal).ToArray();
        var cacheKey = string.Join("\n", parsable);
        return Cache.GetOrAdd(cacheKey, _ => Build(silos));
    }

    private static RingSnapshot Build(IReadOnlyList<SiloAddress> silos)
    {
        var points = new List<(ulong Hash, SiloAddress Silo)>(silos.Count * VirtualNodes);
        foreach (var silo in silos)
        {
            var parsable = silo.ToParsableString();
            for (var v = 0; v < VirtualNodes; v++)
                points.Add((Fnv1a64(parsable + "#" + v.ToString(System.Globalization.CultureInfo.InvariantCulture)), silo));
        }

        // Sort by hash; ties broken by the silo's parsable string so the ring is fully deterministic.
        points.Sort((a, b) =>
        {
            var c = a.Hash.CompareTo(b.Hash);
            return c != 0 ? c : string.CompareOrdinal(a.Silo.ToParsableString(), b.Silo.ToParsableString());
        });

        var hashes = new ulong[points.Count];
        var owners = new SiloAddress[points.Count];
        for (var i = 0; i < points.Count; i++)
        {
            hashes[i] = points[i].Hash;
            owners[i] = points[i].Silo;
        }
        return new RingSnapshot(hashes, owners);
    }

    /// <summary>Index of the first element in the sorted array that is >= value, or array length.</summary>
    private static int LowerBound(ulong[] sorted, ulong value)
    {
        var lo = 0;
        var hi = sorted.Length;
        while (lo < hi)
        {
            var mid = lo + ((hi - lo) >> 1);
            if (sorted[mid] < value)
                lo = mid + 1;
            else
                hi = mid;
        }
        return lo;
    }

    /// <summary>FNV-1a 64-bit over the UTF-8 bytes of <paramref name="s"/>. Stable across processes.</summary>
    private static ulong Fnv1a64(string s)
    {
        const ulong offset = 14695981039346656037UL;
        const ulong prime = 1099511628211UL;
        var hash = offset;
        var bytes = Encoding.UTF8.GetBytes(s);
        foreach (var b in bytes)
        {
            hash ^= b;
            hash *= prime;
        }
        return hash;
    }

    private sealed record RingSnapshot(ulong[] Hashes, SiloAddress[] Owners);
}
