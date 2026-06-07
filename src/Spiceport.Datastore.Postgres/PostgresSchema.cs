namespace Spiceport.Datastore.Postgres;

/// <summary>
/// Table and column names plus the bootstrap DDL for the Postgres datastore. The schema mirrors
/// SpiceDB's Postgres backend MVCC model:
/// <list type="bullet">
/// <item><c>relation_tuple_transaction</c>: the revision table. Each row is a transaction, identified
/// by an <c>xid8</c> (from <c>pg_current_xact_id()</c>) and stamped with the snapshot it observed and a
/// commit timestamp. Revisions are derived from these rows.</item>
/// <item><c>relation_tuple</c>: relationships, each with <c>created_xid</c>/<c>deleted_xid</c> (xid8)
/// columns for MVCC. A live row has <c>deleted_xid = </c><see cref="PostgresRevision.LiveDeletedXid"/>.
/// Writes never UPDATE payload in place: a delete stamps <c>deleted_xid</c>; a create inserts a new row;
/// a touch closes the old living row and inserts a new one.</item>
/// <item><c>stored_schema</c>: the unified schema bytes, versioned the same MVCC way
/// (created_xid/deleted_xid), so a snapshot read returns the schema live at its revision.</item>
/// </list>
/// </summary>
internal static class PostgresSchema
{
    public const string TableTransaction = "relation_tuple_transaction";
    public const string TableTuple = "relation_tuple";
    public const string TableStoredSchema = "stored_schema";
    public const string TableMetadata = "datastore_metadata";
    public const string TableRelationshipCounter = "relationship_counter";

    public const string ColCounterName = "name";
    public const string ColCounterFilter = "serialized_filter";

    public const string ColXid = "xid";
    public const string ColSnapshot = "snapshot";
    public const string ColTimestamp = "timestamp";

    public const string ColResourceNamespace = "namespace";
    public const string ColResourceObjectId = "object_id";
    public const string ColResourceRelation = "relation";
    public const string ColSubjectNamespace = "userset_namespace";
    public const string ColSubjectObjectId = "userset_object_id";
    public const string ColSubjectRelation = "userset_relation";
    public const string ColCaveatName = "caveat_name";
    public const string ColCaveatContext = "caveat_context";
    public const string ColExpiration = "expiration";
    public const string ColIntegrityKeyId = "integrity_key_id";
    public const string ColIntegrityHash = "integrity_hash";
    public const string ColIntegrityHashedAt = "integrity_hashed_at";
    public const string ColCreatedXid = "created_xid";
    public const string ColDeletedXid = "deleted_xid";

    /// <summary>
    /// Idempotent bootstrap DDL. Creates the revision table, relationship table, stored-schema table,
    /// and a metadata table holding the datastore's stable unique id. Indexes support forward (resource)
    /// and reverse (subject) visibility-filtered queries, plus the watch scan over created/deleted xids.
    /// Seeds an initial transaction row so HeadRevision works on an empty store.
    /// </summary>
    public static string BootstrapDdl => $@"
CREATE TABLE IF NOT EXISTS {TableTransaction} (
    {ColXid}        xid8 PRIMARY KEY DEFAULT pg_current_xact_id(),
    {ColSnapshot}   pg_snapshot NOT NULL DEFAULT pg_current_snapshot(),
    {ColTimestamp}  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {TableTuple} (
    {ColResourceNamespace}  varchar NOT NULL,
    {ColResourceObjectId}   varchar NOT NULL,
    {ColResourceRelation}   varchar NOT NULL,
    {ColSubjectNamespace}   varchar NOT NULL,
    {ColSubjectObjectId}    varchar NOT NULL,
    {ColSubjectRelation}    varchar NOT NULL,
    {ColCaveatName}         varchar NULL,
    {ColCaveatContext}      jsonb NULL,
    {ColExpiration}         timestamptz NULL,
    {ColIntegrityKeyId}     varchar NULL,
    {ColIntegrityHash}      bytea NULL,
    {ColIntegrityHashedAt}  timestamptz NULL,
    {ColCreatedXid}         xid8 NOT NULL,
    {ColDeletedXid}         xid8 NOT NULL DEFAULT '{PostgresRevision.LiveDeletedXid}'::xid8
);

-- At most one living row per relationship identity (enforces CREATE-conflict semantics).
CREATE UNIQUE INDEX IF NOT EXISTS uq_relation_tuple_living
    ON {TableTuple} ({ColResourceNamespace}, {ColResourceObjectId}, {ColResourceRelation},
                     {ColSubjectNamespace}, {ColSubjectObjectId}, {ColSubjectRelation}, {ColDeletedXid});

-- Forward query: resource type + id + relation, with the xids for visibility filtering.
CREATE INDEX IF NOT EXISTS ix_relation_tuple_by_resource
    ON {TableTuple} ({ColResourceNamespace}, {ColResourceObjectId}, {ColResourceRelation},
                     {ColCreatedXid}, {ColDeletedXid});

-- Reverse query: subject type + id + relation, with the xids for visibility filtering.
CREATE INDEX IF NOT EXISTS ix_relation_tuple_by_subject
    ON {TableTuple} ({ColSubjectNamespace}, {ColSubjectObjectId}, {ColSubjectRelation},
                     {ColResourceNamespace}, {ColResourceRelation}, {ColCreatedXid}, {ColDeletedXid});

-- Watch scan: relationships by the transaction that created/deleted them.
CREATE INDEX IF NOT EXISTS ix_relation_tuple_by_created_xid ON {TableTuple} ({ColCreatedXid});
CREATE INDEX IF NOT EXISTS ix_relation_tuple_by_deleted_xid ON {TableTuple} ({ColDeletedXid});

-- Revision table scan for OptimizedRevision / GC window bounds.
CREATE INDEX IF NOT EXISTS ix_transaction_by_timestamp ON {TableTransaction} ({ColTimestamp});

CREATE TABLE IF NOT EXISTS {TableStoredSchema} (
    bytes        bytea NOT NULL,
    hash         varchar NOT NULL,
    {ColCreatedXid} xid8 NOT NULL,
    {ColDeletedXid} xid8 NOT NULL DEFAULT '{PostgresRevision.LiveDeletedXid}'::xid8
);
CREATE INDEX IF NOT EXISTS ix_stored_schema_by_created_xid ON {TableStoredSchema} ({ColCreatedXid});

-- Relationship counters, versioned MVCC exactly like stored_schema.
CREATE TABLE IF NOT EXISTS {TableRelationshipCounter} (
    {ColCounterName}    varchar NOT NULL,
    {ColCounterFilter}  bytea NOT NULL,
    {ColCreatedXid}     xid8 NOT NULL,
    {ColDeletedXid}     xid8 NOT NULL DEFAULT '{PostgresRevision.LiveDeletedXid}'::xid8
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_relationship_counter_living
    ON {TableRelationshipCounter} ({ColCounterName}, {ColDeletedXid});
CREATE INDEX IF NOT EXISTS ix_relationship_counter_by_created_xid
    ON {TableRelationshipCounter} ({ColCreatedXid});

CREATE TABLE IF NOT EXISTS {TableMetadata} (
    key   varchar PRIMARY KEY,
    value varchar NOT NULL
);

-- Seed a stable datastore unique id (one-time) and an initial transaction row.
INSERT INTO {TableMetadata} (key, value)
    VALUES ('unique_id', gen_random_uuid()::text)
    ON CONFLICT (key) DO NOTHING;

INSERT INTO {TableTransaction} DEFAULT VALUES;
";
}
