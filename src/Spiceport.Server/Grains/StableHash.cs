namespace Spiceport.Grains;

/// <summary>
/// Stable, process-independent, non-cryptographic string hashing shared by everything that needs the
/// SAME answer for the same string on every silo and across restarts: the graph-locality placement
/// director (silo choice per locality key) and the datastore grain's durable key-index bucketing
/// (<c>indexb/{version}/{dir}/{bucket}</c> rows — the bucket of a key is part of the durable layout, so
/// it must never change across processes or runtimes). <see cref="string.GetHashCode()"/> is
/// deliberately NOT used — it is randomized per process.
/// </summary>
internal static class StableHash
{
    /// <summary>FNV-1a 64-bit over the string's UTF-16 code units.</summary>
    internal static ulong Fnv1a64(string value)
    {
        const ulong offsetBasis = 14695981039346656037UL;
        const ulong prime = 1099511628211UL;

        var hash = offsetBasis;
        foreach (var ch in value)
        {
            unchecked
            {
                hash ^= ch;
                hash *= prime;
            }
        }

        return hash;
    }
}
