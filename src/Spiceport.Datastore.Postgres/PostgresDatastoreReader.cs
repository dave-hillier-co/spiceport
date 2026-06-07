using System.Runtime.CompilerServices;
using Npgsql;
using Spiceport.Core;

namespace Spiceport.Datastore.Postgres;

using static PostgresSchema;

/// <summary>
/// Read-only snapshot accessor at a fixed <see cref="PostgresRevision"/>. Every query opens a short-lived
/// connection from the data source and filters by MVCC visibility against the revision's snapshot. The
/// residual structured filter (caveat presence/name, expiration, subject-relation nuances) is re-applied
/// in memory over the visible rows so results are byte-for-byte identical to the in-memory datastore.
/// </summary>
internal sealed class PostgresDatastoreReader : IDatastoreReader
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly PostgresRevision _revision;
    private readonly Func<PostgresRevision, Task<bool>> _checkValid;

    public PostgresDatastoreReader(
        NpgsqlDataSource dataSource,
        PostgresRevision revision,
        Func<PostgresRevision, Task<bool>> checkValid)
    {
        _dataSource = dataSource;
        _revision = revision;
        _checkValid = checkValid;
    }

    // Synchronous IsValid for the interface; the datastore validates the revision when the reader is
    // created, so this reflects that point-in-time decision plus a best-effort live check.
    public bool IsValid => _checkValid(_revision).GetAwaiter().GetResult();

    public async IAsyncEnumerable<Relationship> QueryRelationships(
        RelationshipsFilter filter,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var now = DateTimeOffset.UtcNow;
        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = RelationshipQuery.BuildForward(conn, null, _revision.Snapshot, filter);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var rel = RelationshipQuery.ReadRelationship(reader);
            if (IsExpired(rel, now))
                continue;
            if (filter.Matches(rel))
                yield return rel;
        }
    }

    public async IAsyncEnumerable<Relationship> ReverseQueryRelationships(
        SubjectsFilter subjectsFilter,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var now = DateTimeOffset.UtcNow;
        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = RelationshipQuery.BuildReverse(conn, null, _revision.Snapshot, subjectsFilter);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var rel = RelationshipQuery.ReadRelationship(reader);
            if (IsExpired(rel, now))
                continue;
            if (subjectsFilter.Matches(rel))
                yield return rel;
        }
    }

    public async Task<byte[]?> ReadStoredSchema(CancellationToken cancellationToken = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        return await ReadSchemaBytes(conn, null, _revision.Snapshot, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Reads the schema bytes live at the given snapshot, or null. Shared with the write tx reader.</summary>
    internal static async Task<byte[]?> ReadSchemaBytes(
        NpgsqlConnection conn, NpgsqlTransaction? tx, PgSnapshot snapshot, CancellationToken cancellationToken)
    {
        await using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText =
            $"SELECT bytes FROM {TableStoredSchema} " +
            $"WHERE pg_visible_in_snapshot({ColCreatedXid}, @s::pg_snapshot) = true " +
            $"AND ({ColDeletedXid} = '{PostgresRevision.LiveDeletedXid}'::xid8 " +
            $"OR pg_visible_in_snapshot({ColDeletedXid}, @s::pg_snapshot) = false) " +
            $"ORDER BY {ColCreatedXid} DESC LIMIT 1";
        cmd.Parameters.AddWithValue("s", snapshot.ToString());
        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is byte[] bytes ? bytes : null;
    }

    private static bool IsExpired(Relationship rel, DateTimeOffset now) =>
        rel.OptionalExpiration is { } exp && exp <= now;
}
