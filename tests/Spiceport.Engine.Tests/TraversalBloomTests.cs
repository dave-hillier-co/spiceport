using Spiceport.Core;
using Spiceport.Engine;

namespace Spiceport.Engine.Tests;

/// <summary>
/// Unit tests for the bounded traversal Bloom filter that replaces the exact visited set as the cycle
/// guard: no false negatives (an added key always reports present), conservative behaviour, immutability
/// (copy-on-add), wire round-trip stability, default sizing within the 1 KB ceiling, and a measured
/// false-positive rate at the design point (n=50) below the SpiceDB target.
/// </summary>
public class TraversalBloomTests
{
    private static VisitKey Key(int i) =>
        new("document", $"doc{i}", "view", "user", $"u{i}", "...");

    [Fact]
    public void Empty_contains_nothing()
    {
        var b = TraversalBloom.Empty;
        for (var i = 0; i < 50; i++)
            Assert.False(b.Contains(Key(i)));
    }

    [Fact]
    public void Added_key_is_always_present_no_false_negatives()
    {
        var b = TraversalBloom.Empty;
        for (var i = 0; i < 50; i++)
            b = b.Add(Key(i));
        for (var i = 0; i < 50; i++)
            Assert.True(b.Contains(Key(i)), $"Key {i} must report present (no false negatives).");
    }

    [Fact]
    public void Add_is_copy_on_write_parent_is_unchanged()
    {
        var parent = TraversalBloom.Empty;
        var child = parent.Add(Key(1));

        Assert.True(child.Contains(Key(1)));
        Assert.False(parent.Contains(Key(1)));
    }

    [Fact]
    public void Default_sizing_is_within_the_one_kilobyte_ceiling()
    {
        var b = TraversalBloom.Empty;
        Assert.Equal(TraversalBloom.DefaultBits, b.Bits);
        Assert.True(b.ToBytes().Length <= 1024, "Bloom wire size must be <= 1 KB.");
        Assert.Equal(128, b.ToBytes().Length); // 1024 bits = 128 bytes
    }

    [Fact]
    public void ForDepth_50_is_byte_aligned_within_ceiling_with_k_10()
    {
        // SpiceDB-exact sizing for n=50, p=0.001: optimal m ≈ 719 bits → byte-aligned 720, k=10.
        var b = TraversalBloom.ForDepth(50);
        Assert.Equal(720, b.Bits);
        Assert.Equal(10, b.Hashes);
        Assert.True(b.ToBytes().Length <= 1024);
    }

    [Fact]
    public void ForDepth_never_exceeds_the_one_kilobyte_ceiling()
    {
        // Even an absurd depth is capped at the 1 KB wire ceiling.
        var b = TraversalBloom.ForDepth(100_000);
        Assert.True(b.ToBytes().Length <= 1024, $"ForDepth must cap at 1 KB; got {b.ToBytes().Length} bytes.");
    }

    [Fact]
    public void Wire_round_trip_preserves_membership()
    {
        var b = TraversalBloom.Empty.Add(Key(7)).Add(Key(13));
        var round = TraversalBloom.FromBytes(b.ToBytes(), b.Hashes);

        Assert.True(round.Contains(Key(7)));
        Assert.True(round.Contains(Key(13)));
        Assert.False(round.Contains(Key(99)));
        Assert.Equal(b, round);
    }

    [Fact]
    public void Hashing_is_deterministic_across_instances()
    {
        // Same key set added in a different order yields the SAME bits (process-stable hashing).
        var a = TraversalBloom.Empty.Add(Key(1)).Add(Key(2)).Add(Key(3));
        var c = TraversalBloom.Empty.Add(Key(3)).Add(Key(1)).Add(Key(2));
        Assert.Equal(a, c);
    }

    [Fact]
    public void False_positive_rate_at_design_point_is_below_target()
    {
        // Fill to the design capacity (n=50), then probe a large disjoint key population. The conservative
        // guard tolerates false positives; assert the measured rate is below SpiceDB's 0.1% target.
        var b = TraversalBloom.Empty;
        for (var i = 0; i < 50; i++)
            b = b.Add(Key(i));

        var falsePositives = 0;
        const int probes = 100_000;
        for (var i = 0; i < probes; i++)
        {
            // Keys far outside the added range; none were added.
            if (b.Contains(new VisitKey("document", $"probe{i}", "view", "user", $"p{i}", "...")))
                falsePositives++;
        }

        var rate = (double)falsePositives / probes;
        Assert.True(rate < 0.001, $"Measured FP rate {rate:P3} should be below the 0.1% target.");
    }
}
