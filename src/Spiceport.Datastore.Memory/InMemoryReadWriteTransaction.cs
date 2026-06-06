using System.Collections.Immutable;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using Spiceport.Core;

namespace Spiceport.Datastore.Memory;

/// <summary>
/// A read-write transaction. Reads see prior committed state plus this transaction's own staged
/// mutations. On successful completion the datastore atomically commits the resulting state.
/// </summary>
internal sealed class InMemoryReadWriteTransaction : IReadWriteTransaction
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

    public InMemoryReadWriteTransaction(DatastoreState baseState, long newRevision)
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
                        throw new SerializationException($"relationship already exists: {rel}");
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
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var now = DateTimeOffset.UtcNow;
        foreach (var rel in _live.Values)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (IsExpired(rel, now))
                continue;
            if (subjectsFilter.Matches(rel))
                yield return rel;
        }
        await Task.CompletedTask;
    }

    public Task<byte[]?> ReadStoredSchema(CancellationToken cancellationToken = default) =>
        Task.FromResult(_pendingSchema ?? _baseState.SchemaAt(_baseState.HeadRevision));

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

        return new DatastoreState(_newRevision, relationships, schemas);
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
