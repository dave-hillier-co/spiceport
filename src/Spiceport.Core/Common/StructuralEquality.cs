using System.Collections;
using System.Text.Json;

namespace Spiceport.Core;

/// <summary>
/// Content comparison for the record members whose own <c>Equals</c> is reference-based (byte arrays,
/// dictionaries, immutable lists), so the records holding them keep the value equality a record
/// advertises.
/// </summary>
internal static class StructuralEquality
{
    /// <summary>Whether two byte arrays hold the same bytes.</summary>
    public static bool BytesEqual(byte[]? a, byte[]? b) =>
        a is null || b is null ? ReferenceEquals(a, b) : a.AsSpan().SequenceEqual(b);

    /// <summary>A content hash consistent with <see cref="BytesEqual"/>.</summary>
    public static int BytesHash(byte[]? bytes)
    {
        if (bytes is null)
            return 0;
        var hash = new HashCode();
        hash.AddBytes(bytes);
        return hash.ToHashCode();
    }

    /// <summary>
    /// Whether two caveat context maps hold the same entries, comparing values deeply. A null map and an
    /// empty map both mean "no context" and compare equal.
    /// </summary>
    public static bool ContextEqual(IReadOnlyDictionary<string, object?>? a, IReadOnlyDictionary<string, object?>? b)
    {
        if (ReferenceEquals(a, b))
            return true;
        if ((a?.Count ?? 0) != (b?.Count ?? 0))
            return false;
        if (a is null || b is null)
            return true; // both empty
        foreach (var (key, value) in a)
        {
            if (!b.TryGetValue(key, out var other) || !ValueEqual(value, other))
                return false;
        }
        return true;
    }

    /// <summary>
    /// A hash consistent with <see cref="ContextEqual"/>: order-independent over the keys only, so values
    /// that compare equal by content never need a matching value hash.
    /// </summary>
    public static int ContextHash(IReadOnlyDictionary<string, object?>? context)
    {
        if (context is null || context.Count == 0)
            return 0;
        var keys = 0;
        foreach (var key in context.Keys)
            keys += StringComparer.Ordinal.GetHashCode(key);
        return HashCode.Combine(context.Count, keys);
    }

    /// <summary>Whether two context values are equal by content: maps by entry, sequences by element.</summary>
    public static bool ValueEqual(object? a, object? b)
    {
        if (ReferenceEquals(a, b))
            return true;
        if (a is null || b is null)
            return false;

        switch (a, b)
        {
            case (JsonElement x, JsonElement y):
                return JsonElement.DeepEquals(x, y);
            case (IReadOnlyDictionary<string, object?> x, IReadOnlyDictionary<string, object?> y):
                return ContextEqual(x, y);
            case (string x, string y):
                return string.Equals(x, y, StringComparison.Ordinal);
            case (IEnumerable x, IEnumerable y) when a is not string && b is not string:
            {
                var ex = x.GetEnumerator();
                var ey = y.GetEnumerator();
                while (true)
                {
                    var hasX = ex.MoveNext();
                    var hasY = ey.MoveNext();
                    if (hasX != hasY)
                        return false;
                    if (!hasX)
                        return true;
                    if (!ValueEqual(ex.Current, ey.Current))
                        return false;
                }
            }
            default:
                return a.Equals(b);
        }
    }
}
