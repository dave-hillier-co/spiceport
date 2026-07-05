using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using Spiceport.Core;

namespace Spiceport.Datastore;

/// <summary>
/// A read-write transaction. Reads see prior committed state plus this transaction's own staged
/// mutations. On successful completion the datastore atomically commits the resulting state.
/// </summary>
internal sealed class MvccReadWriteTransaction : IReadWriteTransaction
{
    private readonly DatastoreState _baseState;
    private readonly long _newRevision;

    // Live relationships keyed by identity, as visible inside this transaction.
    private readonly Dictionary<RelationshipKey, Relationship> _live = new();
    // Identity keys that existed in the base state and have been deleted in this transaction.
    private readonly HashSet<RelationshipKey> _deleted = new();
    // Identity keys created (not previously present) within this transaction.
    private readonly HashSet<RelationshipKey> _created = new();

    private byte[]? _pendingSchema;

    // Staged counter mutations: name -> filter to register; names to tombstone.
    private readonly Dictionary<string, RelationshipsFilter> _pendingCounterWrites = new();
    private readonly HashSet<string> _pendingCounterDeletes = new();

    public MvccReadWriteTransaction(DatastoreState baseState, long newRevision)
    {
        _baseState = baseState;
        _newRevision = newRevision;
        foreach (var rel in baseState.LiveAt(baseState.HeadRevision))
            _live[RelationshipKey.From(rel)] = rel;
    }

    public IRevision NewRevision => new TimestampRevision(_newRevision);

    public bool IsValid => true;

    public Task WriteRelationships(IReadOnlyList<RelationshipUpdate> mutations, CancellationToken cancellationToken = default)
    {
        foreach (var update in mutations)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var rel = update.Relationship;
            rel.Validate();
            var key = RelationshipKey.From(rel);

            switch (update.Operation)
            {
                case UpdateOperation.Create:
                    if (_live.ContainsKey(key))
                        throw new CreateRelationshipExistsException(rel.ToString());
                    Apply(key, rel);
                    break;
                case UpdateOperation.Touch:
                    Apply(key, rel);
                    break;
                case UpdateOperation.Delete:
                    Remove(key);
                    break;
            }
        }
        return Task.CompletedTask;
    }

    public Task<(ulong Count, bool ReachedLimit)> DeleteRelationships(
        RelationshipsFilter filter,
        ulong? limit = null,
        CancellationToken cancellationToken = default)
    {
        var matched = new List<RelationshipKey>();
        foreach (var (key, rel) in _live)
        {
            if (filter.Matches(rel))
                matched.Add(key);
        }

        var reachedLimit = false;
        var toRemove = matched;
        if (limit is { } lim && (ulong)matched.Count > lim)
        {
            toRemove = matched.GetRange(0, (int)lim);
            reachedLimit = true;
        }

        foreach (var key in toRemove)
            Remove(key);

        return Task.FromResult(((ulong)toRemove.Count, reachedLimit));
    }

    public Task WriteStoredSchema(byte[] schemaBytes, CancellationToken cancellationToken = default)
    {
        _pendingSchema = schemaBytes;
        return Task.CompletedTask;
    }

    public async Task<ulong> BulkLoad(IAsyncEnumerable<Relationship> relationships, CancellationToken cancellationToken = default)
    {
        ulong count = 0;
        await foreach (var rel in relationships.WithCancellation(cancellationToken))
        {
            rel.Validate();
            var key = RelationshipKey.From(rel);
            Apply(key, rel);
            count++;
        }
        return count;
    }

    // --- Reads (snapshot = prior committed state + staged mutations) ---

    public async IAsyncEnumerable<Relationship> QueryRelationships(
        RelationshipsFilter filter,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var now = DateTimeOffset.UtcNow;
        foreach (var rel in _live.Values)
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
            foreach (var rel in _live.Values)
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

        var matches = new List<Relationship>();
        foreach (var rel in _live.Values)
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
        Task.FromResult(_pendingSchema ?? _baseState.SchemaAt(_baseState.HeadRevision));

    // --- Counter reads / mutations (snapshot = base committed + staged ops) ---

    public Task<RelationshipsFilter?> ReadCounterFilter(string name, CancellationToken cancellationToken = default) =>
        Task.FromResult(CounterFilterNow(name));

    public async Task<ulong> CountRelationships(string name, CancellationToken cancellationToken = default)
    {
        var filter = CounterFilterNow(name) ?? throw new CounterNotRegisteredException(name);
        ulong count = 0;
        await foreach (var _ in QueryRelationships(filter, cancellationToken).ConfigureAwait(false))
            count++;
        return count;
    }

    public async IAsyncEnumerable<RegisteredCounter> LookupCounters(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var names = new HashSet<string>();
        foreach (var counter in _baseState.LiveCountersAt(_baseState.HeadRevision))
            names.Add(counter.Name);
        foreach (var name in _pendingCounterWrites.Keys)
            names.Add(name);

        foreach (var name in names)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var filter = CounterFilterNow(name);
            if (filter is not null)
                yield return new RegisteredCounter(name, filter);
        }
        await Task.CompletedTask;
    }

    public Task WriteCounter(string name, RelationshipsFilter filter, CancellationToken cancellationToken = default)
    {
        if (CounterFilterNow(name) is not null)
            throw new CounterAlreadyRegisteredException(name);
        _pendingCounterDeletes.Remove(name);
        _pendingCounterWrites[name] = filter;
        return Task.CompletedTask;
    }

    public Task DeleteCounter(string name, CancellationToken cancellationToken = default)
    {
        if (CounterFilterNow(name) is null)
            throw new CounterNotRegisteredException(name);
        _pendingCounterWrites.Remove(name);
        _pendingCounterDeletes.Add(name);
        return Task.CompletedTask;
    }

    /// <summary>Resolves the counter filter as visible inside this transaction (base committed + staged).</summary>
    private RelationshipsFilter? CounterFilterNow(string name)
    {
        if (_pendingCounterWrites.TryGetValue(name, out var staged))
            return staged;
        if (_pendingCounterDeletes.Contains(name))
            return null;
        return _baseState.CounterFilterAt(name, _baseState.HeadRevision);
    }

    // --- Commit ---

    /// <summary>Produces the committed state by applying staged mutations to the base state.</summary>
    public DatastoreState Commit()
    {
        var relationships = _baseState.Relationships;

        // Close out deleted base rows.
        if (_deleted.Count > 0)
        {
            var builder = relationships.ToBuilder();
            for (var i = 0; i < builder.Count; i++)
            {
                var row = builder[i];
                if (row.DeletedRevision is null && _deleted.Contains(RelationshipKey.From(row.Relationship)))
                    builder[i] = row with { DeletedRevision = _newRevision };
            }
            relationships = builder.ToImmutable();
        }

        // Append created / touched rows. A touch on an existing live row closes the old and adds new.
        var additions = new List<StoredRelationship>();
        foreach (var key in _created)
        {
            if (_live.TryGetValue(key, out var rel))
                additions.Add(new StoredRelationship(rel, _newRevision, null));
        }

        if (additions.Count > 0)
            relationships = relationships.AddRange(additions);

        var schemas = _baseState.Schemas;
        if (_pendingSchema is not null)
            schemas = schemas.Add(new SchemaVersion(_newRevision, _pendingSchema, ComputeHash(_pendingSchema)));

        var counters = _baseState.Counters;
        foreach (var (name, filter) in _pendingCounterWrites)
            counters = counters.Add(new CounterVersion(_newRevision, name, filter));
        foreach (var name in _pendingCounterDeletes)
            counters = counters.Add(new CounterVersion(_newRevision, name, null));

        return new DatastoreState(_newRevision, relationships, schemas, counters);
    }

    private void Apply(RelationshipKey key, Relationship rel)
    {
        var existedInBase = !_created.Contains(key) && BaseHas(key);
        if (existedInBase)
        {
            // Touch over a base row: mark base row deleted and re-create with new payload.
            _deleted.Add(key);
            _created.Add(key);
        }
        else
        {
            _created.Add(key);
        }
        _live[key] = rel;
    }

    private void Remove(RelationshipKey key)
    {
        if (!_live.Remove(key))
            return;
        if (_created.Remove(key))
        {
            // Was created in this transaction; if it also shadowed a base row, that base row deletion stands.
        }
        if (BaseHas(key))
            _deleted.Add(key);
    }

    private bool BaseHas(RelationshipKey key)
    {
        foreach (var row in _baseState.Relationships)
        {
            if (row.DeletedRevision is null && RelationshipKey.From(row.Relationship) == key)
                return true;
        }
        return false;
    }

    private static bool IsExpired(Relationship rel, DateTimeOffset now) =>
        rel.OptionalExpiration is { } exp && exp <= now;

    private static string ComputeHash(byte[] bytes) =>
        Convert.ToHexStringLower(SHA256.HashData(bytes));
}
