using System.Collections.Concurrent;

namespace Spiceport.Engine;

/// <summary>
/// A thread-safe store for pre-context dispatch branches, keyed by a stable string key.
/// </summary>
/// <remarks>
/// Stores only the pre-context <see cref="DispatchCheckResult"/> (membership + caveat expression),
/// never the final collapsed verdict. The caveat context is applied per-request outside the
/// dispatcher, so two requests with identical relationships but different contexts can correctly
/// share a cached branch and still collapse to different verdicts.
/// </remarks>
public interface IDispatchCache
{
    /// <summary>Tries to get a cached branch for the key.</summary>
    bool TryGet(string key, out DispatchCheckResult result);

    /// <summary>Stores a branch for the key.</summary>
    void Set(string key, DispatchCheckResult result);
}

/// <summary>
/// A simple thread-safe in-memory <see cref="IDispatchCache"/> with optional size bound and TTL.
/// </summary>
/// <remarks>
/// Backed by a <see cref="ConcurrentDictionary{TKey,TValue}"/>. When a maximum size is set, an
/// over-capacity insert evicts an arbitrary existing entry (approximate, lock-free bounding rather
/// than strict LRU). Entries older than the TTL are treated as misses and dropped on access.
/// </remarks>
public sealed class InMemoryDispatchCache : IDispatchCache
{
    private readonly ConcurrentDictionary<string, Entry> _entries = new();
    private readonly int _maxSize;
    private readonly TimeSpan? _ttl;
    private readonly Func<DateTimeOffset> _clock;

    /// <summary>Creates an in-memory cache.</summary>
    /// <param name="maxSize">Maximum number of entries (0 or less = unbounded).</param>
    /// <param name="ttl">Entry time-to-live, or null for no expiry.</param>
    /// <param name="clock">Clock used for TTL (defaults to system UTC).</param>
    public InMemoryDispatchCache(int maxSize = 10_000, TimeSpan? ttl = null, Func<DateTimeOffset>? clock = null)
    {
        _maxSize = maxSize;
        _ttl = ttl;
        _clock = clock ?? (() => DateTimeOffset.UtcNow);
    }

    /// <summary>The current number of stored entries (including any not-yet-evicted expired ones).</summary>
    public int Count => _entries.Count;

    /// <inheritdoc/>
    public bool TryGet(string key, out DispatchCheckResult result)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (_entries.TryGetValue(key, out var entry))
        {
            if (_ttl is { } ttl && _clock() - entry.StoredAt > ttl)
            {
                _entries.TryRemove(key, out _);
                result = default;
                return false;
            }
            result = entry.Result;
            return true;
        }
        result = default;
        return false;
    }

    /// <inheritdoc/>
    public void Set(string key, DispatchCheckResult result)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (_maxSize > 0 && _entries.Count >= _maxSize && !_entries.ContainsKey(key))
        {
            // Approximate bounding: drop an arbitrary entry to make room.
            foreach (var existing in _entries.Keys)
            {
                _entries.TryRemove(existing, out _);
                break;
            }
        }
        _entries[key] = new Entry(result, _clock());
    }

    private readonly record struct Entry(DispatchCheckResult Result, DateTimeOffset StoredAt);
}
