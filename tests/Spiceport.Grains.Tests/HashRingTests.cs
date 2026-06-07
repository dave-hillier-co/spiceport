using System.Net;
using Orleans.Runtime;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Pure-function unit tests for the consistent-hash <see cref="HashRing"/>: determinism (same key + same
/// silo set always resolves to the same silo) and the minimal-reassignment property (removing one silo
/// from N moves only roughly 1/N of keys).
/// </summary>
public class HashRingTests
{
    private static SiloAddress Silo(int port) =>
        SiloAddress.New(IPAddress.Loopback, port, 1);

    private static IReadOnlyList<SiloAddress> Silos(params int[] ports) =>
        ports.Select(Silo).ToArray();

    [Fact]
    public void Owner_is_deterministic_for_the_same_key_and_silo_set()
    {
        var silos = Silos(11111, 22222, 33333);
        for (var i = 0; i < 200; i++)
        {
            var key = $"document/doc{i}/view/user/u{i}/.../rev1/h/0";
            var a = HashRing.Owner(key, silos);
            var b = HashRing.Owner(key, silos);
            Assert.Equal(a, b);
        }
    }

    [Fact]
    public void Owner_is_independent_of_silo_ordering()
    {
        var ordered = Silos(11111, 22222, 33333);
        var shuffled = Silos(33333, 11111, 22222);
        for (var i = 0; i < 200; i++)
        {
            var key = $"k{i}";
            Assert.Equal(HashRing.Owner(key, ordered), HashRing.Owner(key, shuffled));
        }
    }

    [Fact]
    public void Single_silo_owns_every_key()
    {
        var silos = Silos(40000);
        for (var i = 0; i < 50; i++)
            Assert.Equal(silos[0], HashRing.Owner($"k{i}", silos));
    }

    [Fact]
    public void Owner_throws_on_empty_silo_set()
    {
        Assert.Throws<ArgumentException>(() => HashRing.Owner("k", Array.Empty<SiloAddress>()));
    }

    [Fact]
    public void Removing_one_silo_reassigns_only_about_one_over_N_of_keys()
    {
        var four = Silos(11111, 22222, 33333, 44444);
        var three = Silos(11111, 22222, 33333); // dropped 44444

        const int n = 20000;
        var moved = 0;
        var toRemovedSilo = 0;
        var removed = Silo(44444);
        for (var i = 0; i < n; i++)
        {
            var key = $"document/doc{i}/view/user/u{i}/.../rev/h/0";
            var before = HashRing.Owner(key, four);
            var after = HashRing.Owner(key, three);
            if (before.Equals(removed)) toRemovedSilo++;
            if (!before.Equals(after)) moved++;
        }

        var movedFraction = (double)moved / n;
        // Ideal consistent hash: removing 1 of 4 silos moves ~1/4 of keys. Allow a generous band for
        // virtual-node variance; the key property is that it is NOT a full reshuffle.
        Assert.InRange(movedFraction, 0.15, 0.35);

        // And every moved key must have been one that the removed silo previously owned (keys not on the
        // removed silo are untouched) — the hallmark of consistent hashing.
        Assert.Equal(toRemovedSilo, moved);
    }
}
