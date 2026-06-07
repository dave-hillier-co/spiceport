using System.Text;
using Npgsql;
using NpgsqlTypes;
using Spiceport.Core;

namespace Spiceport.Datastore.Postgres;

using static PostgresSchema;

/// <summary>
/// Builds parameterized SELECT statements over <c>relation_tuple</c> for forward and reverse queries,
/// applying both the structured filter columns and the MVCC visibility predicate. Visibility is pushed
/// into SQL via <c>pg_visible_in_snapshot(created_xid, $snapshot)</c> and a deleted-xid check, exactly
/// mirroring SpiceDB. Caveat-presence, expiration, and subject-relation nuances that are awkward to
/// express precisely in SQL are applied by re-running the in-memory filter on the materialized rows,
/// guaranteeing identical semantics to the reference datastore.
/// </summary>
internal static class RelationshipQuery
{
    private const string AllColumns =
        ColResourceNamespace + ", " + ColResourceObjectId + ", " + ColResourceRelation + ", " +
        ColSubjectNamespace + ", " + ColSubjectObjectId + ", " + ColSubjectRelation + ", " +
        ColCaveatName + ", " + ColCaveatContext + ", " + ColExpiration + ", " +
        ColIntegrityKeyId + ", " + ColIntegrityHash + ", " + ColIntegrityHashedAt;

    /// <summary>Builds the forward (resource-side) query for a snapshot read.</summary>
    public static NpgsqlCommand BuildForward(NpgsqlConnection conn, NpgsqlTransaction? tx, PgSnapshot snapshot, RelationshipsFilter filter)
    {
        var sql = new StringBuilder($"SELECT {AllColumns} FROM {TableTuple} WHERE ");
        var cmd = NewCommand(conn, tx);
        AppendVisibility(sql, cmd, snapshot);

        if (filter.OptionalResourceType is { } rt)
            AppendEq(sql, cmd, ColResourceNamespace, rt);
        if (filter.OptionalResourceRelation is { } rr)
            AppendEq(sql, cmd, ColResourceRelation, rr);
        if (filter.OptionalResourceIds is { Count: > 0 } ids)
            AppendIn(sql, cmd, ColResourceObjectId, ids);
        if (!string.IsNullOrEmpty(filter.OptionalResourceIdPrefix))
            AppendPrefix(sql, cmd, ColResourceObjectId, filter.OptionalResourceIdPrefix);

        cmd.CommandText = sql.ToString();
        return cmd;
    }

    /// <summary>Builds the reverse (subject-side) query for a snapshot read.</summary>
    public static NpgsqlCommand BuildReverse(NpgsqlConnection conn, NpgsqlTransaction? tx, PgSnapshot snapshot, SubjectsFilter filter)
    {
        var sql = new StringBuilder($"SELECT {AllColumns} FROM {TableTuple} WHERE ");
        var cmd = NewCommand(conn, tx);
        AppendVisibility(sql, cmd, snapshot);

        AppendEq(sql, cmd, ColSubjectNamespace, filter.SubjectType);
        if (filter.OptionalSubjectIds is { Count: > 0 } ids)
            AppendIn(sql, cmd, ColSubjectObjectId, ids);
        if (filter.OptionalResourceType is { } rt)
            AppendEq(sql, cmd, ColResourceNamespace, rt);
        if (filter.OptionalResourceRelation is { } rr)
            AppendEq(sql, cmd, ColResourceRelation, rr);

        cmd.CommandText = sql.ToString();
        return cmd;
    }

    private static NpgsqlCommand NewCommand(NpgsqlConnection conn, NpgsqlTransaction? tx)
    {
        var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        return cmd;
    }

    private static void AppendVisibility(StringBuilder sql, NpgsqlCommand cmd, PgSnapshot snapshot)
    {
        var p = $"@p{cmd.Parameters.Count}";
        cmd.Parameters.Add(new NpgsqlParameter(p.TrimStart('@'), NpgsqlDbType.Unknown) { Value = snapshot.ToString() });
        // pg_visible_in_snapshot needs an actual pg_snapshot value; cast the text param.
        sql.Append($"pg_visible_in_snapshot({ColCreatedXid}, {p}::pg_snapshot) = true ")
           .Append($"AND ({ColDeletedXid} = '{PostgresRevision.LiveDeletedXid}'::xid8 ")
           .Append($"OR pg_visible_in_snapshot({ColDeletedXid}, {p}::pg_snapshot) = false)");
    }

    private static void AppendEq(StringBuilder sql, NpgsqlCommand cmd, string column, string value)
    {
        var name = $"p{cmd.Parameters.Count}";
        cmd.Parameters.AddWithValue(name, value);
        sql.Append($" AND {column} = @{name}");
    }

    private static void AppendIn(StringBuilder sql, NpgsqlCommand cmd, string column, IReadOnlyList<string> values)
    {
        var name = $"p{cmd.Parameters.Count}";
        cmd.Parameters.AddWithValue(name, values.ToArray());
        sql.Append($" AND {column} = ANY(@{name})");
    }

    private static void AppendPrefix(StringBuilder sql, NpgsqlCommand cmd, string column, string prefix)
    {
        var name = $"p{cmd.Parameters.Count}";
        cmd.Parameters.AddWithValue(name, prefix + "%");
        sql.Append($" AND {column} LIKE @{name}");
    }

    /// <summary>Materializes the current reader row into a <see cref="Relationship"/>.</summary>
    public static Relationship ReadRelationship(NpgsqlDataReader reader)
    {
        var resource = new ObjectAndRelation(reader.GetString(0), reader.GetString(1), reader.GetString(2));
        var subject = new ObjectAndRelation(reader.GetString(3), reader.GetString(4), reader.GetString(5));

        ContextualizedCaveat? caveat = null;
        if (!reader.IsDBNull(6))
        {
            var ctx = reader.IsDBNull(7) ? null : CaveatContextJson.Deserialize(reader.GetString(7));
            caveat = new ContextualizedCaveat(reader.GetString(6), ctx);
        }

        DateTimeOffset? expiration = reader.IsDBNull(8) ? null : reader.GetFieldValue<DateTimeOffset>(8);

        RelationshipIntegrity? integrity = null;
        if (!reader.IsDBNull(9))
        {
            integrity = new RelationshipIntegrity(
                reader.GetString(9),
                reader.GetFieldValue<byte[]>(10),
                reader.GetFieldValue<DateTimeOffset>(11));
        }

        return Relationship.Create(resource, subject, caveat, expiration, integrity);
    }
}
