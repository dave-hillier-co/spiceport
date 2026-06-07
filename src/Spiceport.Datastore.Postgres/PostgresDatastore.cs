using System.Runtime.CompilerServices;
using Npgsql;
using NpgsqlTypes;
using Spiceport.Core;

namespace Spiceport.Datastore.Postgres;

using static PostgresSchema;

/// <summary>
/// A Postgres-backed MVCC datastore implementing <see cref="IDatastore"/>, modelled on SpiceDB's Postgres
/// backend: relationships carry <c>created_xid</c>/<c>deleted_xid</c> (xid8) columns, each revision is a row
/// in <c>relation_tuple_transaction</c> (an xid8 + <c>pg_snapshot</c> + timestamp), and visibility uses
/// <c>pg_visible_in_snapshot</c>. Read-write transactions run as a single serializable Postgres transaction
/// that allocates exactly one revision.
/// </summary>
public sealed class PostgresDatastore : IDatastore, IAsyncDisposable
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly bool _ownsDataSource;
    private readonly long _quantizationNanos;
    private readonly TimeSpan _gcWindow;
    private string? _uniqueId;

    private PostgresDatastore(NpgsqlDataSource dataSource, bool ownsDataSource, TimeSpan quantization, TimeSpan gcWindow)
    {
        _dataSource = dataSource;
        _ownsDataSource = ownsDataSource;
        _quantizationNanos = (long)quantization.TotalMilliseconds * 1_000_000L;
        _gcWindow = gcWindow;
    }

    /// <summary>
    /// Creates and bootstraps a Postgres datastore against the given connection string. Runs
    /// <see cref="Migrate"/> so the tables exist before use.
    /// </summary>
    public static async Task<PostgresDatastore> Create(
        string connectionString,
        TimeSpan? quantization = null,
        TimeSpan? gcWindow = null,
        CancellationToken cancellationToken = default)
    {
        var dataSource = NpgsqlDataSource.Create(connectionString);
        var ds = new PostgresDatastore(
            dataSource,
            ownsDataSource: true,
            quantization ?? TimeSpan.FromSeconds(5),
            gcWindow ?? TimeSpan.FromHours(24));
        await ds.Migrate(cancellationToken).ConfigureAwait(false);
        return ds;
    }

    /// <summary>Runs the idempotent bootstrap DDL. Safe to call repeatedly; creates tables if absent.</summary>
    public async Task Migrate(CancellationToken cancellationToken = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = PostgresSchema.BootstrapDdl;
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public IDatastoreReader SnapshotReader(IRevision revision)
    {
        var pg = AsPostgres(revision);
        // Validate up front (GC-window bound). Throws if stale; matches the in-memory contract.
        if (!CheckRevisionSync(pg))
            throw new RevisionNotFoundException(revision);
        return new PostgresDatastoreReader(_dataSource, pg, r => CheckRevision(r, default));
    }

    public async Task<RevisionWithSchemaHash> HeadRevision(CancellationToken cancellationToken = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT pg_current_snapshot()::text";
        var snapText = (string)(await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
        var snapshot = PgSnapshot.Parse(snapText);
        var revision = new PostgresRevision(snapshot);
        var hash = await ReadSchemaHash(conn, snapshot, cancellationToken).ConfigureAwait(false);
        return new RevisionWithSchemaHash(revision, hash);
    }

    public async Task<RevisionWithSchemaHash> OptimizedRevision(CancellationToken cancellationToken = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        // Find the first transaction at/after the quantized floor; if none, fall back to the latest.
        // The selected transaction's snapshot (with its own xid folded in) becomes a cacheable revision.
        await using var cmd = conn.CreateCommand();
        var floorNanos = _quantizationNanos > 0
            ? "TO_TIMESTAMP(FLOOR((EXTRACT(EPOCH FROM NOW() AT TIME ZONE 'utc') * 1000000000)/ " +
              _quantizationNanos + ") * " + _quantizationNanos + " / 1000000000) AT TIME ZONE 'utc'"
            : "NOW()";
        cmd.CommandText =
            $"WITH selected AS (" +
            $"  SELECT {ColXid}, {ColSnapshot} FROM {TableTransaction} " +
            $"  WHERE {ColTimestamp} >= {floorNanos} ORDER BY {ColTimestamp} ASC LIMIT 1" +
            $") " +
            $"SELECT {ColXid}::text, {ColSnapshot}::text FROM selected " +
            $"UNION ALL " +
            $"SELECT {ColXid}::text, {ColSnapshot}::text FROM {TableTransaction} " +
            $"WHERE NOT EXISTS (SELECT 1 FROM selected) ORDER BY 1 DESC LIMIT 1";

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            // Empty (should not happen post-migration): fall back to head.
            await reader.DisposeAsync().ConfigureAwait(false);
            return await HeadRevision(cancellationToken).ConfigureAwait(false);
        }

        var xid = ulong.Parse(reader.GetString(0), System.Globalization.CultureInfo.InvariantCulture);
        var snapshot = PgSnapshot.Parse(reader.GetString(1)).MarkComplete(xid);
        await reader.DisposeAsync().ConfigureAwait(false);

        var hash = await ReadSchemaHash(conn, snapshot, cancellationToken).ConfigureAwait(false);
        return new RevisionWithSchemaHash(new PostgresRevision(snapshot, xid), hash);
    }

    public async Task<IRevision> ReadWriteTx(
        Func<IReadWriteTransaction, Task> transaction,
        CancellationToken cancellationToken = default)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var tx = await conn.BeginTransactionAsync(System.Data.IsolationLevel.Serializable, cancellationToken)
            .ConfigureAwait(false);

        ulong newXid;
        PgSnapshot observed;
        await using (var alloc = conn.CreateCommand())
        {
            alloc.Transaction = tx;
            alloc.CommandText =
                $"INSERT INTO {TableTransaction} DEFAULT VALUES RETURNING {ColXid}::text, {ColSnapshot}::text";
            await using var r = await alloc.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await r.ReadAsync(cancellationToken).ConfigureAwait(false);
            newXid = ulong.Parse(r.GetString(0), System.Globalization.CultureInfo.InvariantCulture);
            observed = PgSnapshot.Parse(r.GetString(1));
        }

        var rwt = new PostgresReadWriteTransaction(conn, tx, newXid, observed);
        await transaction(rwt).ConfigureAwait(false);

        try
        {
            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.SerializationFailure
                                           || ex.SqlState == PostgresErrorCodes.UniqueViolation)
        {
            throw new SerializationException();
        }

        return rwt.NewRevision;
    }

    public async Task<bool> CheckRevision(IRevision revision, CancellationToken cancellationToken = default)
    {
        if (revision is not PostgresRevision pg)
            return false;

        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        // currentSnapshot bounds the future; earliestValid bounds the GC window past.
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT pg_current_snapshot()::text, " +
            $"(SELECT {ColSnapshot}::text FROM {TableTransaction} " +
            $" WHERE {ColTimestamp} >= NOW() - @win ORDER BY {ColTimestamp} ASC LIMIT 1), " +
            $"(SELECT {ColXid}::text FROM {TableTransaction} " +
            $" WHERE {ColTimestamp} >= NOW() - @win ORDER BY {ColTimestamp} ASC LIMIT 1)";
        cmd.Parameters.AddWithValue("win", _gcWindow);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return false;

        var current = PgSnapshot.Parse(reader.GetString(0));
        if (reader.IsDBNull(1))
            return false;
        var earliest = PgSnapshot.Parse(reader.GetString(1)).MarkComplete(
            ulong.Parse(reader.GetString(2), System.Globalization.CultureInfo.InvariantCulture));

        // Not from the future, and not older than the earliest retained revision.
        if (pg.Snapshot.Compare(current) > 0)
            return false;
        if (earliest.Compare(pg.Snapshot) > 0)
            return false;
        return true;
    }

    private bool CheckRevisionSync(PostgresRevision pg) =>
        CheckRevision(pg, default).GetAwaiter().GetResult();

    public async IAsyncEnumerable<RevisionChange> Watch(
        IRevision afterRevision,
        WatchOptions options,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(afterRevision);
        ArgumentNullException.ThrowIfNull(options);

        var cursor = AsPostgres(afterRevision);
        if (!await CheckRevision(cursor, cancellationToken).ConfigureAwait(false))
            throw new RevisionNotFoundException(afterRevision);

        // The cursor's snapshot defines what is already seen. Poll the transaction table for committed
        // transactions whose xid is not yet visible to the cursor, emit their relationship diffs in xid
        // order, and advance the cursor by folding each emitted xid into its snapshot.
        var seen = cursor.Snapshot;

        while (!cancellationToken.IsCancellationRequested)
        {
            var pending = await PollNewTransactions(seen, cancellationToken).ConfigureAwait(false);

            foreach (var xid in pending)
            {
                var change = await BuildChange(xid, seen, options, cancellationToken).ConfigureAwait(false);
                seen = seen.MarkComplete(xid);
                if (change is not null)
                    yield return change;
            }

            if (pending.Count == 0)
            {
                try
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    yield break;
                }
            }
        }
    }

    private async Task<List<ulong>> PollNewTransactions(PgSnapshot seen, CancellationToken cancellationToken)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = conn.CreateCommand();
        // Transactions with xid >= seen.xmax are guaranteed unseen; those in [xmin,xmax) may be in the
        // xip list. Fetch xids at/after xmin and filter by the snapshot's visibility (NOT yet visible).
        cmd.CommandText =
            $"SELECT {ColXid}::text FROM {TableTransaction} WHERE {ColXid} >= @minxid::text::xid8 ORDER BY {ColXid} ASC";
        cmd.Parameters.AddWithValue("minxid", seen.Xmin.ToString(System.Globalization.CultureInfo.InvariantCulture));

        var result = new List<ulong>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var xid = ulong.Parse(reader.GetString(0), System.Globalization.CultureInfo.InvariantCulture);
            if (!seen.TxVisible(xid))
                result.Add(xid);
        }
        return result;
    }

    private async Task<RevisionChange?> BuildChange(
        ulong xid, PgSnapshot seenBefore, WatchOptions options, CancellationToken cancellationToken)
    {
        var includeRels = (options.Content & WatchContent.Relationships) != 0;
        var includeSchema = (options.Content & WatchContent.Schema) != 0;

        var changes = new List<RelationshipUpdate>();
        var schemaChanged = false;

        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        if (includeRels)
        {
            await using var cmd = conn.CreateCommand();
            // Rows created at this xid surface as Touch; rows deleted at this xid (and not also created
            // here) surface as Delete. This mirrors the in-memory ChangesAt semantics.
            cmd.CommandText =
                $"SELECT {ColResourceNamespace}, {ColResourceObjectId}, {ColResourceRelation}, " +
                $"{ColSubjectNamespace}, {ColSubjectObjectId}, {ColSubjectRelation}, " +
                $"{ColCaveatName}, {ColCaveatContext}, {ColExpiration}, " +
                $"{ColIntegrityKeyId}, {ColIntegrityHash}, {ColIntegrityHashedAt}, " +
                $"({ColCreatedXid} = @xid) AS created_here, ({ColDeletedXid} = @xid) AS deleted_here " +
                $"FROM {TableTuple} WHERE {ColCreatedXid} = @xid OR {ColDeletedXid} = @xid";
            cmd.Parameters.AddWithValue("xid", NpgsqlDbType.Xid8, xid);

            var deletes = new List<RelationshipUpdate>();
            var createdKeys = new HashSet<(string, string, string, string, string, string)>();
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var rel = RelationshipQuery.ReadRelationship(reader);
                var createdHere = reader.GetBoolean(12);
                var deletedHere = reader.GetBoolean(13);
                var key = (rel.Resource.ObjectType, rel.Resource.ObjectId, rel.Resource.Relation,
                           rel.Subject.ObjectType, rel.Subject.ObjectId, rel.Subject.Relation);
                if (createdHere)
                {
                    changes.Add(new RelationshipUpdate(rel, UpdateOperation.Touch));
                    createdKeys.Add(key);
                }
                else if (deletedHere)
                {
                    deletes.Add(new RelationshipUpdate(rel, UpdateOperation.Delete));
                }
            }
            foreach (var del in deletes)
            {
                var key = (del.Relationship.Resource.ObjectType, del.Relationship.Resource.ObjectId, del.Relationship.Resource.Relation,
                           del.Relationship.Subject.ObjectType, del.Relationship.Subject.ObjectId, del.Relationship.Subject.Relation);
                if (!createdKeys.Contains(key))
                    changes.Add(del);
            }
        }

        if (includeSchema)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT EXISTS (SELECT 1 FROM {TableStoredSchema} WHERE {ColCreatedXid} = @xid)";
            cmd.Parameters.AddWithValue("xid", NpgsqlDbType.Xid8, xid);
            schemaChanged = (bool)(await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
        }

        if (changes.Count == 0 && !schemaChanged)
            return null;

        // The revision naming this change: the seen snapshot with this xid folded in.
        var revision = new PostgresRevision(seenBefore.MarkComplete(xid), xid);
        return new RevisionChange(revision, changes, schemaChanged);
    }

    public async Task<string> GetUniqueId(CancellationToken cancellationToken = default)
    {
        if (_uniqueId is not null)
            return _uniqueId;

        await using var conn = await _dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT value FROM {TableMetadata} WHERE key = 'unique_id'";
        _uniqueId = (string)(await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
        return _uniqueId;
    }

    public async Task<IRevisionParser> GetRevisionParser(CancellationToken cancellationToken = default) =>
        new PostgresRevisionParser(await GetUniqueId(cancellationToken).ConfigureAwait(false));

    public Task Close() => DisposeAsync().AsTask();

    public async ValueTask DisposeAsync()
    {
        if (_ownsDataSource)
            await _dataSource.DisposeAsync().ConfigureAwait(false);
    }

    private static async Task<string?> ReadSchemaHash(NpgsqlConnection conn, PgSnapshot snapshot, CancellationToken cancellationToken)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText =
            $"SELECT hash FROM {TableStoredSchema} " +
            $"WHERE pg_visible_in_snapshot({ColCreatedXid}, @s::pg_snapshot) = true " +
            $"AND ({ColDeletedXid} = '{PostgresRevision.LiveDeletedXid}'::xid8 " +
            $"OR pg_visible_in_snapshot({ColDeletedXid}, @s::pg_snapshot) = false) " +
            $"ORDER BY {ColCreatedXid} DESC LIMIT 1";
        cmd.Parameters.AddWithValue("s", snapshot.ToString());
        return await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) as string;
    }

    private static PostgresRevision AsPostgres(IRevision revision) => revision switch
    {
        PostgresRevision pg => pg,
        _ => throw new InvalidRevisionException($"unsupported revision type: {revision.GetType().Name}"),
    };
}
