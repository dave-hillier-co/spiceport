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

    /// <summary>
    /// True if the filter has a component that the structured SQL forward query cannot express on its own
    /// (so a COUNT(*) would be wrong and we must fall back to streaming + in-memory residual matching).
    /// The forward SQL pushes only resource type/id/prefix/relation; everything else is residual.
    /// </summary>
    public static bool HasResidualForward(RelationshipsFilter filter) =>
        filter.OptionalSubjectsSelectors is { Count: > 0 }
        || filter.OptionalCaveatNameFilter is not null
        || filter.OptionalExpirationOption != ExpirationFilterOption.None;

    /// <summary>
    /// Builds a <c>SELECT COUNT(*)</c> over <c>relation_tuple</c> applying the SAME visibility predicate and
    /// structured forward filter columns as <see cref="BuildForward"/>. Only valid when
    /// <see cref="HasResidualForward"/> is false. Note: expiration is NOT filtered here (no residual), so
    /// callers must guarantee the filter places no expiration constraint; expired-but-live rows are not
    /// excluded by this fast path. For registered counters (which never set expiration/caveat/subject
    /// residuals) this matches SpiceDB's <c>countRels</c> query.
    /// </summary>
    public static NpgsqlCommand BuildForwardCount(NpgsqlConnection conn, NpgsqlTransaction? tx, PgSnapshot snapshot, RelationshipsFilter filter)
    {
        var sql = new StringBuilder($"SELECT COUNT(*) FROM {TableTuple} WHERE ");
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

        // Exclude expired-but-not-yet-GC'd rows to match the in-memory reader's now-based skip.
        var nowParam = $"p{cmd.Parameters.Count}";
        cmd.Parameters.AddWithValue(nowParam, DateTimeOffset.UtcNow);
        sql.Append($" AND ({ColExpiration} IS NULL OR {ColExpiration} > @{nowParam})");

        cmd.CommandText = sql.ToString();
        return cmd;
    }

    // BySubject keyset columns, in order. Ordinal/byte ordering is forced with COLLATE "C" so the SQL sort
    // and keyset comparison match the in-memory reader's StringComparer.Ordinal exactly (SpiceDB object ids
    // are ASCII-only, where byte order == UTF-16 ordinal).
    private static readonly string[] BySubjectColumns =
    [
        ColSubjectNamespace, ColSubjectObjectId, ColSubjectRelation,
        ColResourceNamespace, ColResourceObjectId, ColResourceRelation,
    ];

    /// <summary>Builds the reverse (subject-side) query for a snapshot read.</summary>
    public static NpgsqlCommand BuildReverse(
        NpgsqlConnection conn, NpgsqlTransaction? tx, PgSnapshot snapshot, SubjectsFilter filter,
        ReverseQueryOptions? options = null)
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

        if (options is { Sort: ReverseQuerySort.BySubject })
        {
            if (options.After is { } after)
                AppendBySubjectKeyset(sql, cmd, after);
            sql.Append(" ORDER BY ")
               .Append(string.Join(", ", BySubjectColumns.Select(c => $"{c} COLLATE \"C\"")));
        }

        cmd.CommandText = sql.ToString();
        return cmd;
    }

    // Exclusive keyset: (subj_ns, subj_id, subj_rel, res_ns, res_id, res_rel) > (a0..a5) under "C" collation.
    private static void AppendBySubjectKeyset(StringBuilder sql, NpgsqlCommand cmd, RelationshipReference after)
    {
        string[] values =
        [
            after.Subject.ObjectType, after.Subject.ObjectId, after.Subject.Relation,
            after.Resource.ObjectType, after.Resource.ObjectId, after.Resource.Relation,
        ];

        var lhs = string.Join(", ", BySubjectColumns.Select(c => $"{c} COLLATE \"C\""));
        var rhs = new StringBuilder();
        for (var i = 0; i < values.Length; i++)
        {
            var name = $"p{cmd.Parameters.Count}";
            cmd.Parameters.AddWithValue(name, values[i]);
            if (i > 0)
                rhs.Append(", ");
            rhs.Append('@').Append(name);
        }
        sql.Append($" AND ({lhs}) > ({rhs})");
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
