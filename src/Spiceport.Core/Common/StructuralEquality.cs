using System.Collections;

namespace Spiceport.Core;

/// <summary>
/// Content-based equality and hashing for the collection-typed members of Core records. The
/// compiler-generated record <c>Equals</c>/<c>GetHashCode</c> compare <c>byte[]</c>, dictionary and
/// list members by reference, so two records with identical content would otherwise be unequal.
/// Dictionaries compare as unordered key sets; lists and byte sequences compare in order.
/// </summary>
internal static class StructuralEquality
{
    public static bool BytesEqual(byte[]? a, byte[]? b) =>
        ReferenceEquals(a, b) || (a is not null && b is not null && a.AsSpan().SequenceEqual(b));

    public static int BytesHash(byte[]? bytes)
    {
        if (bytes is null)
        {
            return 0;
        }

        var hash = new HashCode();
        hash.AddBytes(bytes);
        return hash.ToHashCode();
    }

    public static bool SequenceEqual<T>(IReadOnlyList<T>? a, IReadOnlyList<T>? b)
    {
        if (ReferenceEquals(a, b))
        {
            return true;
        }

        if (a is null || b is null || a.Count != b.Count)
        {
            return false;
        }

        var comparer = EqualityComparer<T>.Default;
        for (var i = 0; i < a.Count; i++)
        {
            if (!comparer.Equals(a[i], b[i]))
            {
                return false;
            }
        }

        return true;
    }

    public static int SequenceHash<T>(IReadOnlyList<T>? items)
    {
        if (items is null)
        {
            return 0;
        }

        var hash = new HashCode();
        hash.Add(items.Count);
        foreach (var item in items)
        {
            hash.Add(item);
        }

        return hash.ToHashCode();
    }

    /// <summary>Unordered dictionary equality, comparing values with <see cref="DeepEquals"/>.</summary>
    public static bool DictionaryEqual<TValue>(
        IReadOnlyDictionary<string, TValue>? a, IReadOnlyDictionary<string, TValue>? b)
    {
        if (ReferenceEquals(a, b))
        {
            return true;
        }

        if (a is null || b is null || a.Count != b.Count)
        {
            return false;
        }

        foreach (var (key, value) in a)
        {
            if (!b.TryGetValue(key, out var other) || !DeepEquals(value, other))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Order-independent dictionary hash, consistent with <see cref="DictionaryEqual{TValue}"/>.</summary>
    public static int DictionaryHash<TValue>(IReadOnlyDictionary<string, TValue>? dictionary)
    {
        if (dictionary is null)
        {
            return 0;
        }

        var sum = dictionary.Count;
        foreach (var (key, value) in dictionary)
        {
            sum = unchecked(sum + HashCode.Combine(key, DeepHash(value)));
        }

        return sum;
    }

    /// <summary>
    /// Recursive equality over JSON-like values: dictionaries compare as unordered maps, other
    /// non-string enumerables compare in order, and everything else uses <see cref="object.Equals(object?, object?)"/>.
    /// </summary>
    public static bool DeepEquals(object? a, object? b)
    {
        if (ReferenceEquals(a, b))
        {
            return true;
        }

        if (a is null || b is null)
        {
            return false;
        }

        if (a is IDictionary da)
        {
            if (b is not IDictionary db || da.Count != db.Count)
            {
                return false;
            }

            foreach (DictionaryEntry entry in da)
            {
                if (!db.Contains(entry.Key) || !DeepEquals(entry.Value, db[entry.Key]))
                {
                    return false;
                }
            }

            return true;
        }

        if (IsSequence(a))
        {
            if (!IsSequence(b) || b is IDictionary)
            {
                return false;
            }

            var ea = ((IEnumerable)a).GetEnumerator();
            var eb = ((IEnumerable)b).GetEnumerator();
            while (true)
            {
                var hasA = ea.MoveNext();
                var hasB = eb.MoveNext();
                if (hasA != hasB)
                {
                    return false;
                }

                if (!hasA)
                {
                    return true;
                }

                if (!DeepEquals(ea.Current, eb.Current))
                {
                    return false;
                }
            }
        }

        return !IsSequence(b) && b is not IDictionary && a.Equals(b);
    }

    /// <summary>A hash consistent with <see cref="DeepEquals"/>.</summary>
    public static int DeepHash(object? value)
    {
        switch (value)
        {
            case null:
                return 0;
            case IDictionary dictionary:
            {
                var sum = dictionary.Count;
                foreach (DictionaryEntry entry in dictionary)
                {
                    sum = unchecked(sum + HashCode.Combine(entry.Key, DeepHash(entry.Value)));
                }

                return sum;
            }
            case IEnumerable sequence when value is not string:
            {
                var hash = new HashCode();
                foreach (var item in sequence)
                {
                    hash.Add(DeepHash(item));
                }

                return hash.ToHashCode();
            }
            default:
                return value.GetHashCode();
        }
    }

    private static bool IsSequence(object value) => value is IEnumerable and not string and not IDictionary;
}
