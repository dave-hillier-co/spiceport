using System.Runtime.CompilerServices;
using Npgsql;
using NpgsqlTypes;
using Spiceport.Core;

namespace Spiceport.Datastore.Postgres;

using static PostgresSchema;

/// <summary>
/// A read-write transaction backed by a single serializable Postgres transaction. On open it allocates a
/// new revision row in <c>relation_tuple_transaction</c> (yielding this transaction's <c>xid</c> and the
/// snapshot it observes). All mutations stamp that xid:
/// <list type="bullet">
/// <item><b>Create</b>: INSERT a living row; the unique living index makes a duplicate fail
/// (surfaced as <see cref="SerializationException"/>), enforcing CREATE-conflict semantics.</item>
/// <item><b>Touch</b>: close the existing living row (set its <c>deleted_xid</c>) then INSERT the new row.</item>
/// <item><b>Delete</b>: set the living row's <c>deleted_xid</c> to this xid.</item>
/// </list>
/// Reads inside the transaction see prior committed state plus this transaction's own staged writes,
/// because the observed snapshot has this transaction's xid marked complete and the staged rows carry it.
/// </summary>
internal sealed class PostgresReadWriteTransaction : IReadWriteTransaction
{
    private readonly NpgsqlConnection _conn;
    private readonly NpgsqlTransaction _tx;
    private readonly ulong _newXid;
    private readonly PgSnapshot _readSnapshot; // base snapshot with our own xid folded in

    public PostgresReadWriteTransaction(NpgsqlConnection conn, NpgsqlTransaction tx, ulong newXid, PgSnapshot observedSnapshot)
    {
        _conn = conn;
        _tx = tx;
        _newXid = newXid;
        _readSnapshot = observedSnapshot.MarkComplete(newXid);
    }

    /// <summary>The revision this transaction will commit as: its snapshot (with own xid complete) + xid.</summary>
    public IRevision NewRevision => new PostgresRevision(_readSnapshot, _newXid);

    public bool IsValid => true;

    public async Task WriteRelationships(IReadOnlyList<RelationshipUpdate> mutations, CancellationToken cancellationToken = default)
    {
        foreach (var update in mutations)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var rel = update.Relationship;
            rel.Validate();

            switch (update.Operation)
            {
                case UpdateOperation.Create:
                    await InsertRow(rel, cancellationToken).ConfigureAwait(false);
                    break;
                case UpdateOperation.Touch:
                    await CloseLivingRow(rel, cancellationToken).ConfigureAwait(false);
                    await InsertRow(rel, cancellationToken).ConfigureAwait(false);
                    break;
                case UpdateOperation.Delete:
                    await CloseLivingRow(rel, cancellationToken).ConfigureAwait(false);
                    break;
            }
        }
    }

    public async Task<(ulong Count, bool ReachedLimit)> DeleteRelationships(
        RelationshipsFilter filter,
        ulong? limit = null,
        CancellationToken cancellationToken = default)
    {
        // Identify the living, matching rows by reading through the transaction reader (sees own writes),
        // then close each by identity. Re-using the reader guarantees identical filter semantics.
        var matched = new List<Relationship>();
        await foreach (var rel in QueryRelationships(filter, cancellationToken).ConfigureAwait(false))
            matched.Add(rel);

        var reachedLimit = false;
        var toDelete = matched;
        if (limit is { } lim && (ulong)matched.Count > lim)
        {
            toDelete = matched.GetRange(0, (int)lim);
            reachedLimit = true;
        }

        foreach (var rel in toDelete)
            await CloseLivingRow(rel, cancellationToken).ConfigureAwait(false);

        return ((ulong)toDelete.Count, reachedLimit);
    }

    public async Task WriteStoredSchema(byte[] schemaBytes, CancellationToken cancellationToken = default)
    {
        var hash = Convert.ToHexStringLower(System.Security.Cryptography.SHA256.HashData(schemaBytes));

        await using (var close = _conn.CreateCommand())
        {
            close.Transaction = _tx;
            close.CommandText =
                $"UPDATE {TableStoredSchema} SET {ColDeletedXid} = @xid " +
                $"WHERE {ColDeletedXid} = '{PostgresRevision.LiveDeletedXid}'::xid8";
            close.Parameters.AddWithValue("xid", NpgsqlDbType.Xid8, _newXid);
            await close.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await using var insert = _conn.CreateCommand();
        insert.Transaction = _tx;
        insert.CommandText =
            $"INSERT INTO {TableStoredSchema} (bytes, hash, {ColCreatedXid}) VALUES (@b, @h, @xid)";
        insert.Parameters.AddWithValue("b", schemaBytes);
        insert.Parameters.AddWithValue("h", hash);
        insert.Parameters.AddWithValue("xid", NpgsqlDbType.Xid8, _newXid);
        await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<ulong> BulkLoad(IAsyncEnumerable<Relationship> relationships, CancellationToken cancellationToken = default)
    {
        ulong count = 0;
        // Use COPY for throughput; bulk load assumes fresh inserts of living rows.
        var copySql =
            $"COPY {TableTuple} ({ColResourceNamespace}, {ColResourceObjectId}, {ColResourceRelation}, " +
            $"{ColSubjectNamespace}, {ColSubjectObjectId}, {ColSubjectRelation}, " +
            $"{ColCaveatName}, {ColCaveatContext}, {ColExpiration}, " +
            $"{ColIntegrityKeyId}, {ColIntegrityHash}, {ColIntegrityHashedAt}, {ColCreatedXid}) FROM STDIN (FORMAT BINARY)";

        await using var writer = await _conn.BeginBinaryImportAsync(copySql, cancellationToken).ConfigureAwait(false);
        await foreach (var rel in relationships.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            rel.Validate();
            await writer.StartRowAsync(cancellationToken).ConfigureAwait(false);
            await WriteRowValues(writer, rel, cancellationToken).ConfigureAwait(false);
            await writer.WriteAsync(_newXid, NpgsqlDbType.Xid8, cancellationToken).ConfigureAwait(false);
            count++;
        }
        await writer.CompleteAsync(cancellationToken).ConfigureAwait(false);
        return count;
    }

    // --- Reads (snapshot = observed base + own staged writes) ---

    public IAsyncEnumerable<Relationship> QueryRelationships(
        RelationshipsFilter filter,
        CancellationToken cancellationToken = default)
        => Query(RelationshipQuery.BuildForward(_conn, _tx, _readSnapshot, filter), filter.Matches, cancellationToken);

    public IAsyncEnumerable<Relationship> ReverseQueryRelationships(
        SubjectsFilter subjectsFilter,
        CancellationToken cancellationToken = default)
        => Query(RelationshipQuery.BuildReverse(_conn, _tx, _readSnapshot, subjectsFilter), subjectsFilter.Matches, cancellationToken);

    private static async IAsyncEnumerable<Relationship> Query(
        NpgsqlCommand cmd, Func<Relationship, bool> residual,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;
        await using (cmd)
        {
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var rel = RelationshipQuery.ReadRelationship(reader);
                if (rel.OptionalExpiration is { } exp && exp <= now)
                    continue;
                if (residual(rel))
                    yield return rel;
            }
        }
    }

    public Task<byte[]?> ReadStoredSchema(CancellationToken cancellationToken = default) =>
        PostgresDatastoreReader.ReadSchemaBytes(_conn, _tx, _readSnapshot, cancellationToken);

    // --- Counter reads / mutations (see own staged writes via the folded read snapshot) ---

    public Task<RelationshipsFilter?> ReadCounterFilter(string name, CancellationToken cancellationToken = default) =>
        PostgresDatastoreReader.ReadCounterFilter(_conn, _tx, _readSnapshot, name, cancellationToken);

    public async Task<ulong> CountRelationships(string name, CancellationToken cancellationToken = default)
    {
        var filter = await PostgresDatastoreReader.ReadCounterFilter(_conn, _tx, _readSnapshot, name, cancellationToken).ConfigureAwait(false)
            ?? throw new CounterNotRegisteredException(name);
        return await PostgresDatastoreReader.CountForFilter(_conn, _tx, _readSnapshot, filter, cancellationToken).ConfigureAwait(false);
    }

    public IAsyncEnumerable<RegisteredCounter> LookupCounters(CancellationToken cancellationToken = default) =>
        PostgresDatastoreReader.LookupCounters(_conn, _tx, _readSnapshot, cancellationToken);

    public async Task WriteCounter(string name, RelationshipsFilter filter, CancellationToken cancellationToken = default)
    {
        var existing = await PostgresDatastoreReader.ReadCounterFilter(_conn, _tx, _readSnapshot, name, cancellationToken).ConfigureAwait(false);
        if (existing is not null)
            throw new CounterAlreadyRegisteredException(name);

        await using var cmd = _conn.CreateCommand();
        cmd.Transaction = _tx;
        cmd.CommandText =
            $"INSERT INTO {TableRelationshipCounter} ({ColCounterName}, {ColCounterFilter}, {ColCreatedXid}) " +
            "VALUES (@n, @f, @xid)";
        cmd.Parameters.AddWithValue("n", name);
        cmd.Parameters.AddWithValue("f", CounterFilterJson.Serialize(filter));
        cmd.Parameters.AddWithValue("xid", NpgsqlDbType.Xid8, _newXid);
        try
        {
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UniqueViolation)
        {
            // The living unique index guards a concurrent register of the same name.
            throw new CounterAlreadyRegisteredException(name);
        }
    }

    public async Task DeleteCounter(string name, CancellationToken cancellationToken = default)
    {
        await using var cmd = _conn.CreateCommand();
        cmd.Transaction = _tx;
        cmd.CommandText =
            $"UPDATE {TableRelationshipCounter} SET {ColDeletedXid} = @xid " +
            $"WHERE {ColCounterName} = @n AND {ColDeletedXid} = '{PostgresRevision.LiveDeletedXid}'::xid8";
        cmd.Parameters.AddWithValue("n", name);
        cmd.Parameters.AddWithValue("xid", NpgsqlDbType.Xid8, _newXid);
        var rows = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        if (rows == 0)
            throw new CounterNotRegisteredException(name);
    }

    // --- mutation helpers ---

    private async Task InsertRow(Relationship rel, CancellationToken cancellationToken)
    {
        await using var cmd = _conn.CreateCommand();
        cmd.Transaction = _tx;
        cmd.CommandText =
            $"INSERT INTO {TableTuple} (" +
            $"{ColResourceNamespace}, {ColResourceObjectId}, {ColResourceRelation}, " +
            $"{ColSubjectNamespace}, {ColSubjectObjectId}, {ColSubjectRelation}, " +
            $"{ColCaveatName}, {ColCaveatContext}, {ColExpiration}, " +
            $"{ColIntegrityKeyId}, {ColIntegrityHash}, {ColIntegrityHashedAt}, {ColCreatedXid}) " +
            "VALUES (@rn, @ro, @rr, @sn, @so, @sr, @cn, @cc, @exp, @ik, @ih, @iat, @xid)";
        AddRelationshipParams(cmd, rel);
        cmd.Parameters.AddWithValue("xid", NpgsqlDbType.Xid8, _newXid);

        try
        {
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UniqueViolation)
        {
            // A living row with this identity already exists: CREATE conflict.
            throw new SerializationException($"relationship already exists: {rel}");
        }
    }

    private async Task CloseLivingRow(Relationship rel, CancellationToken cancellationToken)
    {
        await using var cmd = _conn.CreateCommand();
        cmd.Transaction = _tx;
        cmd.CommandText =
            $"UPDATE {TableTuple} SET {ColDeletedXid} = @xid WHERE " +
            $"{ColResourceNamespace} = @rn AND {ColResourceObjectId} = @ro AND {ColResourceRelation} = @rr AND " +
            $"{ColSubjectNamespace} = @sn AND {ColSubjectObjectId} = @so AND {ColSubjectRelation} = @sr AND " +
            $"{ColDeletedXid} = '{PostgresRevision.LiveDeletedXid}'::xid8";
        cmd.Parameters.AddWithValue("rn", rel.Resource.ObjectType);
        cmd.Parameters.AddWithValue("ro", rel.Resource.ObjectId);
        cmd.Parameters.AddWithValue("rr", rel.Resource.Relation);
        cmd.Parameters.AddWithValue("sn", rel.Subject.ObjectType);
        cmd.Parameters.AddWithValue("so", rel.Subject.ObjectId);
        cmd.Parameters.AddWithValue("sr", rel.Subject.Relation);
        cmd.Parameters.AddWithValue("xid", NpgsqlDbType.Xid8, _newXid);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static void AddRelationshipParams(NpgsqlCommand cmd, Relationship rel)
    {
        cmd.Parameters.AddWithValue("rn", rel.Resource.ObjectType);
        cmd.Parameters.AddWithValue("ro", rel.Resource.ObjectId);
        cmd.Parameters.AddWithValue("rr", rel.Resource.Relation);
        cmd.Parameters.AddWithValue("sn", rel.Subject.ObjectType);
        cmd.Parameters.AddWithValue("so", rel.Subject.ObjectId);
        cmd.Parameters.AddWithValue("sr", rel.Subject.Relation);
        cmd.Parameters.AddWithValue("cn", (object?)rel.OptionalCaveat?.CaveatName ?? DBNull.Value);
        cmd.Parameters.Add(new NpgsqlParameter("cc", NpgsqlDbType.Jsonb)
        {
            Value = rel.OptionalCaveat?.Context is { Count: > 0 } ctx
                ? CaveatContextJson.Serialize(ctx)
                : DBNull.Value,
        });
        cmd.Parameters.AddWithValue("exp", (object?)rel.OptionalExpiration ?? DBNull.Value);
        cmd.Parameters.AddWithValue("ik", (object?)rel.OptionalIntegrity?.KeyId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("ih", (object?)rel.OptionalIntegrity?.Hash ?? DBNull.Value);
        cmd.Parameters.AddWithValue("iat", (object?)rel.OptionalIntegrity?.HashedAt ?? DBNull.Value);
    }

    private static async Task WriteRowValues(NpgsqlBinaryImporter writer, Relationship rel, CancellationToken cancellationToken)
    {
        await writer.WriteAsync(rel.Resource.ObjectType, cancellationToken).ConfigureAwait(false);
        await writer.WriteAsync(rel.Resource.ObjectId, cancellationToken).ConfigureAwait(false);
        await writer.WriteAsync(rel.Resource.Relation, cancellationToken).ConfigureAwait(false);
        await writer.WriteAsync(rel.Subject.ObjectType, cancellationToken).ConfigureAwait(false);
        await writer.WriteAsync(rel.Subject.ObjectId, cancellationToken).ConfigureAwait(false);
        await writer.WriteAsync(rel.Subject.Relation, cancellationToken).ConfigureAwait(false);

        if (rel.OptionalCaveat is { } cav)
            await writer.WriteAsync(cav.CaveatName, cancellationToken).ConfigureAwait(false);
        else
            await writer.WriteNullAsync(cancellationToken).ConfigureAwait(false);

        if (rel.OptionalCaveat?.Context is { Count: > 0 } ctx)
            await writer.WriteAsync(CaveatContextJson.Serialize(ctx), NpgsqlDbType.Jsonb, cancellationToken).ConfigureAwait(false);
        else
            await writer.WriteNullAsync(cancellationToken).ConfigureAwait(false);

        if (rel.OptionalExpiration is { } exp)
            await writer.WriteAsync(exp, cancellationToken).ConfigureAwait(false);
        else
            await writer.WriteNullAsync(cancellationToken).ConfigureAwait(false);

        if (rel.OptionalIntegrity is { } integ)
        {
            await writer.WriteAsync(integ.KeyId, cancellationToken).ConfigureAwait(false);
            await writer.WriteAsync(integ.Hash, cancellationToken).ConfigureAwait(false);
            await writer.WriteAsync(integ.HashedAt, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await writer.WriteNullAsync(cancellationToken).ConfigureAwait(false);
            await writer.WriteNullAsync(cancellationToken).ConfigureAwait(false);
            await writer.WriteNullAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
