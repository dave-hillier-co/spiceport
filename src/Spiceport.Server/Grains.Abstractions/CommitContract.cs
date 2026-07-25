using Orleans.Concurrency;

namespace Spiceport.Grains.Abstractions;

/// <summary>
/// A precondition inside a <see cref="CommitRequest"/>: an existence-only probe of
/// <see cref="Filter"/> against the transaction snapshot the commit would apply at.
/// <see cref="MustMatch"/> <c>true</c> requires at least one matching relationship;
/// <c>false</c> requires none. Carried as the lossless <see cref="FullRelationshipsFilterWire"/> so the
/// filter round-trips the grain boundary exactly (the same form the counter filters already use).
/// </summary>
/// <remarks>
/// Named <c>CommitPreconditionWire</c> (not <c>PreconditionWire</c>) because the data-plane RPC surface
/// already defines <see cref="PreconditionWire"/> in this namespace.
/// </remarks>
[GenerateSerializer, Immutable]
public sealed record CommitPreconditionWire(
    [property: Id(0)] FullRelationshipsFilterWire Filter,
    [property: Id(1)] bool MustMatch);

/// <summary>
/// A bulk delete-by-filter inside a <see cref="CommitRequest"/>: close every live relationship matching
/// <see cref="Filter"/>, up to <see cref="Limit"/> rows when non-null (the reply reports the deleted
/// count and whether the limit was reached, mirroring the DeleteRelationships RPC semantics).
/// </summary>
[GenerateSerializer, Immutable]
public sealed record DeleteByFilterWire(
    [property: Id(0)] FullRelationshipsFilterWire Filter,
    [property: Id(1)] ulong? Limit);

/// <summary>
/// The write wire contract of the graph-sharded design (docs/graph-sharded-datastore.md section 3):
/// a fully DECLARATIVE description of one commit, executed inside the sequencer grain
/// (<see cref="IDatastoreGrain.Commit"/>) whose single-threaded, non-reentrant activation is the
/// serialization point — the request replaces the caller-side read-transact-CAS loop, so a declarative
/// commit needs no retry. The lambda compatibility path (<c>GrainBackedDatastore.ReadWriteTx</c>)
/// survives by passing <see cref="ExpectedHead"/>, keeping the old caller-evaluated CAS semantics.
/// </summary>
/// <param name="Preconditions">
/// Evaluated in request order BEFORE any mutation, against the same snapshot the mutations commit at
/// (the semantics the data-plane write path historically evaluated inline, now the sequencer's job).
/// </param>
/// <param name="Updates">Relationship mutations (Create/Touch/Delete), applied in request order.</param>
/// <param name="DeleteByFilter">Optional bulk delete applied after <paramref name="Updates"/>.</param>
/// <param name="SchemaBytes">Optional new schema source bytes written at the minted revision.</param>
/// <param name="ExpectedSchemaHash">
/// When non-null: schema-write serializability — the commit is rejected
/// (<see cref="CommitFailureKind.SchemaHashMoved"/>) unless the schema hash effective at the grain's
/// head (<c>DatastoreGrainState.SchemaHashAt(head)</c>) still matches. A null current hash (the
/// pre-first-schema seed window) matches only a null/empty expected hash.
/// </param>
/// <param name="CounterChanges">
/// Counter deltas: a non-null <see cref="CounterDeltaWire.Filter"/> registers, a null filter
/// unregisters. Their semantics are keyed off <see cref="ExpectedHead"/>, like the CAS itself: on a
/// declarative commit (null <see cref="ExpectedHead"/>) each delta is a guarded INTENT run through the
/// transaction's register/unregister preconditions, so already-registered / not-registered are rejected
/// as reply failures; on the compatibility path (non-null <see cref="ExpectedHead"/>) each delta is the
/// already-RESOLVED net counter version the caller's lambda produced against that exact base, applied by
/// direct append (never re-guarded — a same-commit register+unregister nets to a delta whose guard is
/// false in the base, the same reason the log fold appends counter versions directly).
/// </param>
/// <param name="ExpectedHead">
/// When non-null: compare-and-swap — the commit is rejected (<see cref="CommitFailureKind.HeadMoved"/>)
/// if the grain's head differs (the lambda compatibility path, whose preconditions were evaluated
/// caller-side against this head). When null the grain serializes the commit unconditionally.
/// </param>
[GenerateSerializer, Immutable]
public sealed record CommitRequest(
    [property: Id(0)] IReadOnlyList<CommitPreconditionWire> Preconditions,
    [property: Id(1)] IReadOnlyList<RelationshipUpdateWire> Updates,
    [property: Id(2)] DeleteByFilterWire? DeleteByFilter,
    [property: Id(3)] byte[]? SchemaBytes,
    [property: Id(4)] string? ExpectedSchemaHash,
    [property: Id(5)] IReadOnlyList<CounterDeltaWire> CounterChanges,
    [property: Id(6)] long? ExpectedHead);

/// <summary>
/// How a <see cref="CommitRequest"/> was rejected. Failures cross the grain boundary as STRUCTURED
/// REPLY DATA (<see cref="CommitFailureWire"/>), never as serialized exceptions, so the client rethrows
/// the exact same typed exceptions the write surface throws today and every gRPC status mapping is
/// preserved: <see cref="PreconditionFailed"/> maps to <c>PreconditionFailedException</c>
/// (FailedPrecondition), <see cref="CreateAlreadyExists"/> to <c>CreateRelationshipExistsException</c>
/// (AlreadyExists), the counter kinds to <c>CounterAlreadyRegisteredException</c> /
/// <c>CounterNotRegisteredException</c>, and <see cref="HeadMoved"/> / <see cref="SchemaHashMoved"/>
/// to the caller's CAS-conflict handling (retry, or <c>SerializationException</c> — Aborted — on
/// exhaustion).
/// </summary>
[GenerateSerializer]
public enum CommitFailureKind
{
    /// <summary>The grain's head no longer equals <see cref="CommitRequest.ExpectedHead"/>.</summary>
    HeadMoved = 0,

    /// <summary>The schema hash at head no longer matches <see cref="CommitRequest.ExpectedSchemaHash"/>.</summary>
    SchemaHashMoved = 1,

    /// <summary>A precondition was violated; nothing was applied.</summary>
    PreconditionFailed = 2,

    /// <summary>A Create update targeted a relationship that already exists.</summary>
    CreateAlreadyExists = 3,

    /// <summary>A counter register targeted a name that is already live.</summary>
    CounterAlreadyRegistered = 4,

    /// <summary>A counter unregister targeted a name that is not registered.</summary>
    CounterNotRegistered = 5,
}

/// <summary>
/// A rejected commit as reply data. <see cref="Detail"/> carries exactly what the client needs to
/// rethrow its existing typed exception unchanged: the canonical relationship string for
/// <see cref="CommitFailureKind.CreateAlreadyExists"/> (the <c>CreateRelationshipExistsException</c>
/// constructor argument), the counter name for the counter kinds (the exceptions' constructor
/// argument), and the full precondition-failure message for
/// <see cref="CommitFailureKind.PreconditionFailed"/> (the shared <c>PreconditionMessages</c> text the
/// client parses back into an exact <c>PreconditionFailedException</c>).
/// </summary>
[GenerateSerializer, Immutable]
public sealed record CommitFailureWire(
    [property: Id(0)] CommitFailureKind Kind,
    [property: Id(1)] string? Detail);

/// <summary>
/// The result of <see cref="IDatastoreGrain.Commit"/>. On success <see cref="Revision"/> is the
/// grain-minted authoritative revision and <see cref="Failure"/> is null; on rejection
/// <see cref="Revision"/> is null, <see cref="Failure"/> says why, and nothing was applied (the whole
/// request is atomic). <see cref="DeletedCount"/> / <see cref="ReachedLimit"/> report the
/// <see cref="CommitRequest.DeleteByFilter"/> outcome (0/false when no delete-by-filter was requested).
/// </summary>
[GenerateSerializer, Immutable]
public sealed record CommitReply(
    [property: Id(0)] long? Revision,
    [property: Id(1)] CommitFailureWire? Failure,
    [property: Id(2)] ulong DeletedCount,
    [property: Id(3)] bool ReachedLimit);
