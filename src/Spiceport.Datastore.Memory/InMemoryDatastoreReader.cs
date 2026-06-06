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
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var now = DateTimeOffset.UtcNow;
        foreach (var rel in _state.LiveAt(_revision))
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
        Task.FromResult(_state.SchemaAt(_revision));

    private static bool IsExpired(Relationship rel, DateTimeOffset now) =>
        rel.OptionalExpiration is { } exp && exp <= now;
}
