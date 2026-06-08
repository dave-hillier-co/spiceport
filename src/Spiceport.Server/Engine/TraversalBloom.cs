using System.Text;

namespace Spiceport.Engine;

/// <summary>
/// A bounded, immutable Bloom filter carrying the traversal loop hint across dispatch (and grain)
/// boundaries at a fixed wire/CPU cost.
/// </summary>
/// <remarks>
/// <para>
/// This mirrors SpiceDB's traversal bloom (<c>ResolverMeta.RecordTraversal</c> + the singleflight loop
/// guard): the filter is sized for the maximum number of distinct in-flight visit keys on a single path
/// (≈ the max recursion depth) at a low false-positive rate. It is NOT on the correctness path —
/// termination rests solely on the depth budget. A positive <see cref="Contains"/> only tells the
/// dispatcher "this is a LIKELY loop", which it uses to recurse in-process instead of re-entering a busy
/// same-key grain (a deadlock guard). A bloom false-positive can therefore only force a (correct) local
/// step; it can never change a verdict, and depth/loop-affected results are never cached anyway.
/// </para>
/// <para>
/// Sizing (default, matching SpiceDB's <c>bloom.NewWithEstimates(maxDepth, 0.001)</c> for maxDepth = 50):
/// <c>m = 1024 bits = 128 bytes</c>, <c>k = 10</c>. Optimal m for n=50, p=0.001 is ≈ 719 bits; rounding
/// up to 1024 bits gives an actual false-positive rate at n=50 of ≈ 2.3e-4 — better than SpiceDB's target
/// and far under the 1 KB wire ceiling. The bit count and k are carried so a larger filter (e.g. 2048
/// bits / 256 bytes, still ≤ 1 KB) can be selected if the corpus ever demands it without a wire change.
/// </para>
/// <para>
/// Hashing is process-stable (FNV-1a over the canonical UTF-8 key string), NOT
/// <see cref="object.GetHashCode"/>, so a filter built on one silo is interpreted identically on another.
/// The k bit indices are derived by Kirsch–Mitzenmacher double hashing (<c>h_i = h1 + i*h2 mod m</c>).
/// The value is immutable: <see cref="Add"/> returns a NEW instance (copy-on-add), so each sub-problem
/// extends its own filter without mutating its parent's, exactly like the immutable set it replaces.
/// </para>
/// </remarks>
public readonly struct TraversalBloom : IEquatable<TraversalBloom>
{
    /// <summary>Default filter width in bits (128 bytes), sized for maxDepth ≈ 50 at p ≈ 2.3e-4.</summary>
    public const int DefaultBits = 1024;

    /// <summary>Default number of hash functions, optimal for <see cref="DefaultBits"/> at n ≈ 50.</summary>
    public const int DefaultHashes = 10;

    private readonly byte[]? _bits;
    private readonly int _m;
    private readonly int _k;

    private TraversalBloom(byte[] bits, int m, int k)
    {
        _bits = bits;
        _m = m;
        _k = k;
    }

    /// <summary>Number of bits in the filter.</summary>
    public int Bits => _m == 0 ? DefaultBits : _m;

    /// <summary>Number of hash functions (k).</summary>
    public int Hashes => _k == 0 ? DefaultHashes : _k;

    /// <summary>The raw bit array (immutable; the same reference is shared by readers, never mutated).</summary>
    public byte[] ToBytes() => _bits ?? new byte[DefaultBits / 8];

    /// <summary>An empty filter at the default sizing (m = 1024 bits, k = 10), tuned for maxDepth ≈ 50.</summary>
    public static TraversalBloom Empty => Create(DefaultBits, DefaultHashes);

    /// <summary>
    /// An empty filter sized for <paramref name="expectedElements"/> distinct keys at false-positive rate
    /// <paramref name="p"/>, mirroring SpiceDB's <c>NewWithEstimates(n, 0.001)</c>:
    /// <c>m = ceil(-n·ln p / (ln 2)²)</c> bits (byte-aligned, capped at the 1 KB / 8192-bit ceiling) and
    /// <c>k = round((m/n)·ln 2)</c> (≥1). For the default depth (50) this resolves to m=1024 bits, k=10,
    /// identical to <see cref="Empty"/>.
    /// </summary>
    /// <param name="expectedElements">The expected number of additions (≈ the max recursion depth).</param>
    /// <param name="p">The target false-positive rate (default 0.001, as SpiceDB).</param>
    public static TraversalBloom ForDepth(int expectedElements, double p = 0.001)
    {
        if (expectedElements <= 0) throw new ArgumentOutOfRangeException(nameof(expectedElements));
        if (p is <= 0 or >= 1) throw new ArgumentOutOfRangeException(nameof(p));

        var ln2 = Math.Log(2);
        var m = (int)Math.Ceiling(-expectedElements * Math.Log(p) / (ln2 * ln2));
        m = ((m + 7) / 8) * 8;            // byte-align
        const int ceilingBits = 8 * 1024;  // 1 KB wire ceiling
        if (m > ceilingBits) m = ceilingBits;
        var k = Math.Max(1, (int)Math.Round((double)m / expectedElements * ln2));
        return Create(m, k);
    }

    /// <summary>Creates an empty filter of the given width and hash count.</summary>
    /// <param name="bits">Filter width in bits; rounded up to a whole byte.</param>
    /// <param name="hashes">Number of hash functions (k).</param>
    public static TraversalBloom Create(int bits, int hashes)
    {
        if (bits <= 0) throw new ArgumentOutOfRangeException(nameof(bits));
        if (hashes <= 0) throw new ArgumentOutOfRangeException(nameof(hashes));
        var byteLen = (bits + 7) / 8;
        return new TraversalBloom(new byte[byteLen], byteLen * 8, hashes);
    }

    /// <summary>Reconstructs a filter from its wire form (raw bits + k), e.g. after a grain hop.</summary>
    public static TraversalBloom FromBytes(byte[] bits, int hashes)
    {
        ArgumentNullException.ThrowIfNull(bits);
        if (hashes <= 0) throw new ArgumentOutOfRangeException(nameof(hashes));
        return new TraversalBloom((byte[])bits.Clone(), bits.Length * 8, hashes);
    }

    /// <summary>True if <paramref name="key"/> is possibly in the set (no false negatives, rare false positives).</summary>
    public bool Contains(VisitKey key)
    {
        var bits = _bits ?? throw NotInitialized();
        var (h1, h2) = Hash(key);
        for (var i = 0; i < Hashes; i++)
        {
            var bit = (int)(((h1 + (ulong)i * h2) % (ulong)_m));
            if ((bits[bit >> 3] & (1 << (bit & 7))) == 0)
                return false;
        }
        return true;
    }

    /// <summary>Returns a NEW filter that additionally records <paramref name="key"/> (copy-on-add).</summary>
    public TraversalBloom Add(VisitKey key)
    {
        var src = _bits ?? throw NotInitialized();
        var copy = (byte[])src.Clone();
        var (h1, h2) = Hash(key);
        for (var i = 0; i < Hashes; i++)
        {
            var bit = (int)(((h1 + (ulong)i * h2) % (ulong)_m));
            copy[bit >> 3] |= (byte)(1 << (bit & 7));
        }
        return new TraversalBloom(copy, _m, _k);
    }

    private static InvalidOperationException NotInitialized() =>
        new("TraversalBloom was default-constructed; use Empty / Create / FromBytes.");

    /// <summary>
    /// Two independent 64-bit hashes of the key's canonical string, used to derive k indices by double
    /// hashing. h1 = FNV-1a; h2 = a salted FNV-1a (with h2 forced odd/non-zero so the index walk spans m).
    /// </summary>
    private static (ulong H1, ulong H2) Hash(VisitKey key)
    {
        var s = Canonical(key);
        var bytes = Encoding.UTF8.GetBytes(s);
        var h1 = Fnv1a64(bytes, 14695981039346656037UL);
        var h2 = Fnv1a64(bytes, 1099511628211UL) | 1UL; // force odd so successive indices differ
        return (h1, h2);
    }

    /// <summary>
    /// Canonical, unit-separated key string. The separator (U+001F) cannot appear in an ONR field,
    /// so no two distinct keys collide on the concatenation.
    /// </summary>
    private static string Canonical(VisitKey key)
    {
        var sep = (char)0x1F;
        return string.Join(
            sep,
            key.ResourceType, key.ResourceId, key.ResourceRelation,
            key.SubjectType, key.SubjectId, key.SubjectRelation);
    }

    private static ulong Fnv1a64(byte[] bytes, ulong offset)
    {
        const ulong prime = 1099511628211UL;
        var hash = offset;
        foreach (var b in bytes)
        {
            hash ^= b;
            hash *= prime;
        }
        return hash;
    }

    /// <inheritdoc />
    public bool Equals(TraversalBloom other)
    {
        if (Hashes != other.Hashes || Bits != other.Bits)
            return false;
        return ToBytes().AsSpan().SequenceEqual(other.ToBytes());
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is TraversalBloom b && Equals(b);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Hashes);
        foreach (var b in ToBytes())
            hash.Add(b);
        return hash.ToHashCode();
    }
}
