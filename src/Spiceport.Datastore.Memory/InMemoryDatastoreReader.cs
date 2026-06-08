using System.Runtime.CompilerServices;
using Spiceport.Core;

namespace Spiceport.Datastore.Memory;

/// <summary>Read-only snapshot accessor over an immutable <see cref="DatastoreState"/> at a fixed revision.</summary>
internal sealed class InMemoryDatastoreReader : IDatastoreReader
{
    private readonly DatastoreState _state;
    private readonly long _revision;
    private readonly Func<long, bool> _isValid;

    public InMemoryDatastoreReader(DatastoreState state, long revision, Func<long, bool> isValid)
    {
        _state = state;
        _revision = revision;
        _isValid = isValid;
    }

    public bool IsValid => _isValid(_revision);

    public async IAsyncEnumerable<Relationship> QueryRelationships(
        RelationshipsFilter filter,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var now = DateTimeOffset.UtcNow;
        foreach (var rel in _state.LiveAt(_revision))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (IsExpired(rel, now))
                continue;
            if (filter.Matches(rel))
                yield return rel;
        }
        await Task.CompletedTask;
    }

    public async IAsyncEnumerable<Relationship> ReverseQueryRelationships(
        SubjectsFilter subjectsFilter,
        ReverseQueryOptions? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var now = DateTimeOffset.UtcNow;

        if (options is null || options.Sort == ReverseQuerySort.Unsorted)
        {
            // Fast path: stream in storage order, no materialization.
            foreach (var rel in _state.LiveAt(_revision))
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (IsExpired(rel, now))
                    continue;
                if (subjectsFilter.Matches(rel))
                    yield return rel;
            }
            await Task.CompletedTask;
            yield break;
        }

        // Ordered path: materialize matching rows, sort by the requested key, then apply the exclusive
        // keyset so resumption sees only strictly-greater rows. The six-tuple is unique, so there are no
        // ties and the order is total.
        var matches = new List<Relationship>();
        foreach (var rel in _state.LiveAt(_revision))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (IsExpired(rel, now))
                continue;
            if (subjectsFilter.Matches(rel))
                matches.Add(rel);
        }

        matches.Sort(ReverseQueryOptions.CompareBySubject);

        foreach (var rel in matches)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (options.After is { } after &&
                ReverseQueryOptions.CompareBySubject(rel.Reference, after) <= 0)
                continue;
            yield return rel;
        }
        await Task.CompletedTask;
    }

    public Task<byte[]?> ReadStoredSchema(CancellationToken cancellationToken = default) =>
        Task.FromResult(_state.SchemaAt(_revision));

    public Task<RelationshipsFilter?> ReadCounterFilter(string name, CancellationToken cancellationToken = default) =>
        Task.FromResult(_state.CounterFilterAt(name, _revision));

    public async Task<ulong> CountRelationships(string name, CancellationToken cancellationToken = default)
    {
        var filter = _state.CounterFilterAt(name, _revision)
            ?? throw new CounterNotRegisteredException(name);
        ulong count = 0;
        await foreach (var _ in QueryRelationships(filter, cancellationToken).ConfigureAwait(false))
            count++;
        return count;
    }

    public async IAsyncEnumerable<RegisteredCounter> LookupCounters(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var counter in _state.LiveCountersAt(_revision))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return counter;
        }
        await Task.CompletedTask;
    }

    private static bool IsExpired(Relationship rel, DateTimeOffset now) =>
        rel.OptionalExpiration is { } exp && exp <= now;
}
