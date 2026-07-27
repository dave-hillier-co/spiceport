using System.Text;
using Orleans.Concurrency;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// Stateless-worker implementation of <see cref="IRelationshipsGrain"/>: the data-plane write side.
/// Schema writes compile-and-swap the live <see cref="ISchemaProvider"/> and persist to the datastore;
/// relationship writes/deletes are DECLARATIVE commits executed inside the sequencer grain
/// (<see cref="IDatastoreGrain.Commit"/> — one hop, no client retry loop, the single-threaded activation
/// is the serialization point); reads pin the optimized revision and page over a snapshot. Replies carry
/// opaque revision tokens minted by <see cref="ZedTokens"/>.
/// </summary>
/// <remarks>
/// The grain is <c>[StatelessWorker]</c> so the silo scales activations with load; it carries no per-key
/// identity, so callers always use <see cref="IRelationshipsGrain.Key"/>. Read paging is deterministic
/// by the canonical tuple string and resumes strictly after the cursor's tuple, requesting one extra row
/// to set the continuation cursor (the same convention as the reverse-ops grain). Commit rejections
/// arrive as STRUCTURED REPLY DATA (<see cref="CommitReply.Failure"/>), and this grain rethrows exactly
/// the typed exceptions the inline write path historically threw — same types, same messages — so every
/// gRPC status mapping in the front door is preserved unchanged.
/// </remarks>
[StatelessWorker]
public sealed class RelationshipsGrain(
    IDatastore datastore,
    ISchemaProvider schemaProvider,
    ISchemaSource schemaSource,
    ISnapshotScanner scanner,
    LogWatchHub hub,
    SequencerAdmission admission) : Grain, IRelationshipsGrain
{
    // Orleans grain code must not ConfigureAwait(false); keep the captured context.
    private const ConfigureAwaitOptions ContinueOnCapturedContext = ConfigureAwaitOptions.ContinueOnCapturedContext;

    /// <summary>
    /// Bound on schema-write validate-and-commit retries (a retry happens only when the schema hash or
    /// the guarded data moved under the validation — both grain-detected races) AND on the declarative
    /// commit paths' HeadMoved retries (<see cref="CommitDeclarative"/>). Mirrors the compatibility
    /// write path's CAS bound (<c>GrainBackedDatastore.MaxCasAttempts</c>); on exhaustion the same
    /// retryable serialization conflict surfaces.
    /// </summary>
    private const int MaxCommitAttempts = 50;

    /// <summary>The cluster-singleton sequencer grain every declarative commit executes inside.</summary>
    private IDatastoreGrain Sequencer => GrainFactory.GetGrain<IDatastoreGrain>(IDatastoreGrain.Key);

    /// <summary>
    /// Submits one commit to the sequencer through the per-silo admission gate: a slot is held only for
    /// the duration of the grain call (each retry attempt re-enters, so a retry storm is shed like any
    /// other offered load). A full gate throws <see cref="SequencerOverloadedException"/> — the write is
    /// shed before it can join the sequencer's activation queue (issue #36).
    /// </summary>
    private async Task<CommitReply> SubmitCommit(CommitRequest request)
    {
        using var slot = admission.Enter();
        return await Sequencer.Commit(request).ConfigureAwait(ContinueOnCapturedContext);
    }

    /// <inheritdoc />
    public async Task<WriteSchemaReply> WriteSchema(WriteSchemaArgs args)
    {
        ArgumentNullException.ThrowIfNull(args);

        // Compile the proposed schema FIRST, but do NOT swap yet. A compile failure is surfaced as a
        // serializable ArgumentException (the underlying SchemaCompileException is not an Orleans-
        // serializable type across the grain boundary) carrying the original message so the gRPC layer
        // can map it to InvalidArgument.
        Spiceport.Schema.CompiledSchema nextCompiled;
        try
        {
            nextCompiled = Spiceport.Schema.SchemaCompiler.CompileSchema(args.SchemaText);
        }
        catch (Spiceport.Schema.SchemaCompileException ex)
        {
            throw new ArgumentException(ex.Message, nameof(args));
        }

        // Run SpiceDB's type-system + caveat-definition validation on the freshly compiled schema BEFORE
        // touching the datastore. A type error (undefined reference, permission-on-left-of-arrow, wildcard
        // in arrow, self-reference, missing allowed types, undefined caveat, duplicate/reused name, or an
        // unparseable/parameterless/unused-parameter caveat) is rejected at write time as a
        // FailedPrecondition — re-wrapped in the serializable carrier below.
        try
        {
            Spiceport.Engine.SchemaTypeValidator.Validate(nextCompiled);
        }
        catch (Spiceport.Engine.SchemaTypeException ex)
        {
            throw new SchemaWriteValidationException(ex.Message);
        }

        var schemaBytes = Encoding.UTF8.GetBytes(args.SchemaText);

        // Validate-and-commit loop. The change validation (compile + diff) stays CLIENT-SIDE against a
        // pinned snapshot — it produces the descriptive SchemaWriteValidationException messages and the
        // unconditional caveat rejections — while its data-existence guards ALSO ride the commit as
        // MUST_NOT_MATCH preconditions, and the commit carries ExpectedSchemaHash (the stored-schema hash
        // the diff base was read at). The sequencer therefore re-proves, atomically at the commit
        // snapshot, both that no other schema landed (SchemaHashMoved) and that no conflicting data
        // landed (PreconditionFailed) since validation; either race re-runs the whole loop against a
        // fresh base, where the client-side pass rethrows the descriptive rejection if the conflict is
        // real, or passes and retries the commit if it receded.
        for (var attempt = 0; ; attempt++)
        {
            // Pin the validation base: the head revision and the stored-schema hash effective at it (the
            // grain's own SHA-256-of-bytes hash — the same value the ExpectedSchemaHash gate compares).
            var head = await datastore.HeadRevision(CancellationToken.None).ConfigureAwait(ContinueOnCapturedContext);

            // The CURRENT schema for the diff must be the one the pinned hash represents, so the gate and
            // the validation can never disagree: compile the stored bytes at the pinned revision (they
            // always compile — they were validated when written), read through the ISchemaSource seam.
            // Pre-first-schema (nothing stored yet) falls back to the host-seeded live snapshot, matching
            // the historical behavior; the gate then expects the empty hash (which is what a null stored
            // hash matches).
            var storedBytes = await schemaSource.ReadSchemaAt(head.Revision, CancellationToken.None)
                .ConfigureAwait(ContinueOnCapturedContext);
            Spiceport.Schema.CompiledSchema current;
            if (storedBytes is null)
            {
                current = schemaProvider.Current.Schema;
            }
            else
            {
                // This compile is of the STORED schema (the diff base), never the caller's input: those
                // bytes were validated when written, so a compile failure here is server-side corruption.
                // It must surface as InvalidOperationException (gRPC Internal) — the ArgumentException
                // mapping to InvalidArgument at the top of this method is reserved for the caller's NEW
                // schema, and blaming the caller for a corrupt stored schema would be wrong.
                try
                {
                    current = Spiceport.Schema.SchemaCompiler.CompileSchema(Encoding.UTF8.GetString(storedBytes));
                }
                catch (Spiceport.Schema.SchemaCompileException ex)
                {
                    throw new InvalidOperationException(
                        $"stored schema failed to compile; the persisted schema at the pinned revision is corrupt: {ex.Message}",
                        ex);
                }
            }

            var checks = SchemaChangeValidator.ComputeChecks(current, nextCompiled);
            await SchemaChangeValidator.EvaluateAsync(checks, scanner, head.Revision, CancellationToken.None)
                .ConfigureAwait(ContinueOnCapturedContext);

            var preconditions = checks
                .OfType<SchemaChangeCheck.NoOrphans>()
                .Select(c => new CommitPreconditionWire(WireConvert.ToFullFilter(c.Filter), MustMatch: false))
                .ToList();

            var reply = await SubmitCommit(new CommitRequest(
                preconditions,
                Array.Empty<RelationshipUpdateWire>(),
                DeleteByFilter: null,
                SchemaBytes: schemaBytes,
                ExpectedSchemaHash: head.SchemaHash ?? string.Empty,
                CounterChanges: Array.Empty<CounterDeltaWire>(),
                ExpectedHead: null)).ConfigureAwait(ContinueOnCapturedContext);

            if (reply.Revision is { } revision)
            {
                // Same-silo Watch pulse parity with the ReadWriteTx path: the sequencer's observer push
                // is best-effort (a missed push costs a full heartbeat of latency, the only backstop),
                // so a local commit wakes this silo's parked Watch streams directly.
                hub.Pulse(revision);

                // The persist committed: only now swap the live snapshot so the datastore and the live
                // schema never diverge and a rejected change leaves the live schema intact.
                var snapshot = schemaProvider.Update(args.SchemaText);
                var token = await MintToken(new TimestampRevision(revision), snapshot.SchemaHash)
                    .ConfigureAwait(ContinueOnCapturedContext);
                return new WriteSchemaReply(token);
            }

            switch (reply.Failure!.Kind)
            {
                case CommitFailureKind.SchemaHashMoved:
                case CommitFailureKind.PreconditionFailed:
                    break; // a grain-detected race with another schema/data write: re-validate and retry.
                case CommitFailureKind.HeadMoved:
                    // Near-impossible on this path (no ExpectedHead rides the request; the sequencer's
                    // single-writer activation cannot lose its own CAS) — it exists only for
                    // duplicate-activation churn during cluster membership changes, when a second
                    // activation of the singleton briefly races the storage version check. Retryable,
                    // like the pre-declarative write loop treated it.
                    break;
                default:
                    throw new InvalidOperationException(
                        $"unexpected schema-commit failure {reply.Failure.Kind}: {reply.Failure.Detail}");
            }

            if (attempt + 1 >= MaxCommitAttempts)
                throw new SerializationException();
        }
    }

    /// <inheritdoc />
    public async Task<ReadSchemaReply> ReadSchema()
    {
        var snapshot = schemaProvider.Current;
        var optimized = await datastore.OptimizedRevision(CancellationToken.None)
            .ConfigureAwait(ContinueOnCapturedContext);
        var token = await MintToken(optimized.Revision, snapshot.SchemaHash).ConfigureAwait(ContinueOnCapturedContext);
        return new ReadSchemaReply(snapshot.SourceText, token);
    }

    /// <inheritdoc />
    public async Task<WriteRelationshipsReply> WriteRelationships(WriteRelationshipsArgs args)
    {
        ArgumentNullException.ThrowIfNull(args);

        // One declarative commit: the sequencer evaluates the preconditions against the same snapshot
        // the updates commit at (the semantics the inline transaction had) and applies the updates with
        // Create preserved, so a duplicate create is rejected there and nothing commits.
        var reply = await CommitDeclarative(new CommitRequest(
            ToCommitPreconditions(args.Preconditions),
            args.Updates,
            DeleteByFilter: null,
            SchemaBytes: null,
            ExpectedSchemaHash: null,
            CounterChanges: Array.Empty<CounterDeltaWire>(),
            ExpectedHead: null)).ConfigureAwait(ContinueOnCapturedContext);

        if (reply.Failure is { } failure)
            throw RelationshipWriteFailure(failure);

        var token = await MintToken(new TimestampRevision(reply.Revision!.Value), schemaProvider.Current.SchemaHash)
            .ConfigureAwait(ContinueOnCapturedContext);
        return new WriteRelationshipsReply(token);
    }

    /// <inheritdoc />
    public async Task<DeleteRelationshipsReply> DeleteRelationships(DeleteRelationshipsArgs args)
    {
        ArgumentNullException.ThrowIfNull(args);

        var reply = await CommitDeclarative(new CommitRequest(
            ToCommitPreconditions(args.Preconditions),
            Array.Empty<RelationshipUpdateWire>(),
            new DeleteByFilterWire(WireConvert.ToFullFilter(ToFilter(args.Filter)), args.OptionalLimit),
            SchemaBytes: null,
            ExpectedSchemaHash: null,
            CounterChanges: Array.Empty<CounterDeltaWire>(),
            ExpectedHead: null)).ConfigureAwait(ContinueOnCapturedContext);

        if (reply.Failure is { } failure)
            throw RelationshipWriteFailure(failure);

        var token = await MintToken(new TimestampRevision(reply.Revision!.Value), schemaProvider.Current.SchemaHash)
            .ConfigureAwait(ContinueOnCapturedContext);
        return new DeleteRelationshipsReply(reply.DeletedCount, reply.ReachedLimit, token);
    }

    /// <inheritdoc />
    public async Task<BulkImportRelationshipsReply> BulkImportRelationships(BulkImportRelationshipsArgs args)
    {
        ArgumentNullException.ThrowIfNull(args);

        // CREATE semantics per row, matching real SpiceDB's ImportBulkRelationships (observed v1.49.2):
        // a row that already exists in the store, or appears twice in the import (the MVCC staging makes
        // the first Create visible to the second's conflict check), rejects the whole import with the
        // CREATE-conflict failure — never a silent upsert. The entire import loads in ONE declarative
        // commit, executed inside the sequencer like every other production write, so a rejected import
        // applies nothing (SpiceDB's whole-stream atomicity, which the gRPC services preserve by
        // buffering the client stream and calling this once). The grain itself stays request/response.
        var updates = args.Relationships
            .Select(r => new RelationshipUpdateWire(RelationshipUpdateOpWire.Create, r))
            .ToList();

        var reply = await CommitDeclarative(new CommitRequest(
            Array.Empty<CommitPreconditionWire>(),
            updates,
            DeleteByFilter: null,
            SchemaBytes: null,
            ExpectedSchemaHash: null,
            CounterChanges: Array.Empty<CounterDeltaWire>(),
            ExpectedHead: null)).ConfigureAwait(ContinueOnCapturedContext);

        // A duplicate row surfaces as CreateAlreadyExists and maps through the shared mapper to the
        // same typed WriteConflictException (-> gRPC AlreadyExists) the WriteRelationships path throws,
        // with the SpiceDB-verbatim message. HeadMoved is retried inside CommitDeclarative (exhaustion
        // throws the Serialization WriteConflictException the ReadWriteTx path surfaced).
        if (reply.Failure is { } failure)
            throw RelationshipWriteFailure(failure);

        var token = await MintToken(new TimestampRevision(reply.Revision!.Value), schemaProvider.Current.SchemaHash)
            .ConfigureAwait(ContinueOnCapturedContext);
        return new BulkImportRelationshipsReply((ulong)updates.Count, token);
    }

    /// <inheritdoc />
    public async Task<RegisterCounterReply> RegisterRelationshipCounter(RegisterCounterArgs args)
    {
        ArgumentNullException.ThrowIfNull(args);

        var reply = await CommitDeclarative(CounterCommit(
            new CounterDeltaWire(args.Name, WireConvert.ToFullFilter(ToFilter(args.Filter)))))
            .ConfigureAwait(ContinueOnCapturedContext);

        if (reply.Failure is { } failure)
            throw CounterFailure(failure);

        return new RegisterCounterReply();
    }

    /// <inheritdoc />
    public async Task<UnregisterCounterReply> UnregisterRelationshipCounter(UnregisterCounterArgs args)
    {
        ArgumentNullException.ThrowIfNull(args);

        // A null filter is the unregister tombstone (the guarded DeleteCounter op grain-side).
        var reply = await CommitDeclarative(CounterCommit(new CounterDeltaWire(args.Name, null)))
            .ConfigureAwait(ContinueOnCapturedContext);

        if (reply.Failure is { } failure)
            throw CounterFailure(failure);

        return new UnregisterCounterReply();
    }

    /// <inheritdoc />
    public async Task<CountRelationshipsReply> CountRelationships(CountRelationshipsArgs args)
    {
        ArgumentNullException.ThrowIfNull(args);

        // The proto request carries no consistency, so (like SpiceDB's on-demand path, which uses
        // HeadRevision) resolve a fully-consistent revision and count at that pinned snapshot.
        var resolved = await RevisionResolver
            .Resolve(datastore, ConsistencyRequirement.FullyConsistent, cancellationToken: CancellationToken.None)
            .ConfigureAwait(ContinueOnCapturedContext);

        ulong count;
        try
        {
            // The count is a broad filter scan at the pinned revision — the storage-direct scan seam's
            // workload, not the shard mesh's (see ISnapshotScanner).
            count = await scanner.CountRelationships(args.Name, resolved.Revision, CancellationToken.None)
                .ConfigureAwait(ContinueOnCapturedContext);
        }
        catch (CounterNotRegisteredException ex)
        {
            throw new CounterOperationException(CounterErrorKind.NotRegistered, ex.Message);
        }

        var token = await MintToken(resolved.Revision, resolved.SchemaHash ?? schemaProvider.Current.SchemaHash)
            .ConfigureAwait(ContinueOnCapturedContext);
        return new CountRelationshipsReply(count, token);
    }

    // --- declarative commit submission ---

    /// <summary>
    /// Submits a declarative commit (null <c>ExpectedHead</c>) with a bounded retry on
    /// <see cref="CommitFailureKind.HeadMoved"/> ONLY; every other outcome (success or failure) returns
    /// to the caller for its own mapping. HeadMoved is near-impossible here — a declarative commit
    /// carries no head CAS and the sequencer's single-writer, non-reentrant activation cannot lose its
    /// own conditional append — it exists only for duplicate-activation churn during cluster membership
    /// changes, when a second activation of the singleton briefly races the storage version check. That
    /// is a transient condition the pre-declarative write path retried (its ReadWriteTx CAS loop), so
    /// the declarative path must stay retryable too: bounded by <see cref="MaxCommitAttempts"/> (the
    /// same bound as <c>GrainBackedDatastore.MaxCasAttempts</c>), and on exhaustion it throws the same
    /// retryable exception the old path surfaced — <see cref="WriteConflictException"/> with
    /// <see cref="WriteConflictKind.Serialization"/>, which the gRPC front door maps to Aborted so the
    /// client retries the whole transaction.
    /// </summary>
    private async Task<CommitReply> CommitDeclarative(CommitRequest request)
    {
        for (var attempt = 0; ; attempt++)
        {
            var reply = await SubmitCommit(request).ConfigureAwait(ContinueOnCapturedContext);
            if (reply.Failure is not { Kind: CommitFailureKind.HeadMoved })
            {
                // Same-silo Watch pulse parity with GrainBackedDatastore.ReadWriteTx: the sequencer's
                // observer push is best-effort (missed pushes are recovered only by the hub's slow
                // heartbeat), so a successful local commit wakes this silo's parked Watch streams
                // immediately instead of costing up to one heartbeat of latency.
                if (reply.Revision is { } revision)
                    hub.Pulse(revision);
                return reply;
            }

            if (attempt + 1 >= MaxCommitAttempts)
                throw new WriteConflictException(
                    WriteConflictKind.Serialization, new SerializationException().Message);
        }
    }

    // --- commit failure mapping (reply data -> the exact historical typed exceptions) ---

    /// <summary>
    /// Maps a relationship-write commit rejection back to the exact exception the inline write path threw:
    /// a precondition failure rethrows <see cref="PreconditionFailedException"/> (kind/index recovered from
    /// the shared message text — see <see cref="PreconditionMessages.TryParseFailure"/>), and a duplicate
    /// create rethrows <see cref="WriteConflictException"/> with
    /// <see cref="WriteConflictKind.CreateExisting"/> carrying the message
    /// <see cref="CreateRelationshipExistsException"/> derives from the conflicting relationship (the
    /// reply Detail). HeadMoved never reaches this mapper — <see cref="CommitDeclarative"/> retries it —
    /// and no other kind can occur on a declarative relationship commit (there is no
    /// ExpectedHead/ExpectedSchemaHash and no counter delta), so anything else is surfaced loudly.
    /// </summary>
    private static Exception RelationshipWriteFailure(CommitFailureWire failure) => failure.Kind switch
    {
        CommitFailureKind.PreconditionFailed => PreconditionFailure(failure.Detail),
        CommitFailureKind.CreateAlreadyExists => new WriteConflictException(
            WriteConflictKind.CreateExisting,
            new CreateRelationshipExistsException(failure.Detail ?? string.Empty).Message),
        _ => new InvalidOperationException(
            $"unexpected relationship-commit failure {failure.Kind}: {failure.Detail}"),
    };

    private static PreconditionFailedException PreconditionFailure(string? detail)
    {
        var message = detail ?? string.Empty;
        // The Detail is always a PreconditionMessages-formatted text (the single shared copy), so the
        // parse recovers the exact kind and index the inline evaluation stamped on the exception.
        return PreconditionMessages.TryParseFailure(message, out var kind, out var index)
            ? new PreconditionFailedException(kind, index, message)
            : new PreconditionFailedException(PreconditionFailureKind.MustMatchFoundNone, 0, message);
    }

    /// <summary>A commit carrying exactly one counter register/unregister delta and nothing else.</summary>
    private static CommitRequest CounterCommit(CounterDeltaWire delta) => new(
        Array.Empty<CommitPreconditionWire>(),
        Array.Empty<RelationshipUpdateWire>(),
        DeleteByFilter: null,
        SchemaBytes: null,
        ExpectedSchemaHash: null,
        CounterChanges: new[] { delta },
        ExpectedHead: null);

    /// <summary>
    /// Maps a counter commit rejection back to the serializable
    /// <see cref="CounterOperationException"/> the counter RPCs have always thrown, with the message the
    /// underlying datastore exception derives from the counter name (the reply Detail) — so the gRPC
    /// front door's FailedPrecondition mapping and message are byte-identical to the inline path.
    /// </summary>
    private static Exception CounterFailure(CommitFailureWire failure) => failure.Kind switch
    {
        CommitFailureKind.CounterAlreadyRegistered => new CounterOperationException(
            CounterErrorKind.AlreadyRegistered,
            new CounterAlreadyRegisteredException(failure.Detail ?? string.Empty).Message),
        CommitFailureKind.CounterNotRegistered => new CounterOperationException(
            CounterErrorKind.NotRegistered,
            new CounterNotRegisteredException(failure.Detail ?? string.Empty).Message),
        _ => new InvalidOperationException(
            $"unexpected counter-commit failure {failure.Kind}: {failure.Detail}"),
    };

    // --- wire conversions ---

    /// <summary>
    /// Converts the RPC-surface preconditions to the commit contract's form: the lossless full filter
    /// (round-tripping the same core <see cref="RelationshipsFilter"/> the inline evaluation built via
    /// <see cref="ToFilter"/>) plus the MUST_MATCH/MUST_NOT_MATCH flag.
    /// </summary>
    private static IReadOnlyList<CommitPreconditionWire> ToCommitPreconditions(
        IReadOnlyList<PreconditionWire>? preconditions)
    {
        if (preconditions is not { Count: > 0 })
            return Array.Empty<CommitPreconditionWire>();

        return preconditions
            .Select(p => new CommitPreconditionWire(
                WireConvert.ToFullFilter(ToFilter(p.Filter)),
                MustMatch: p.Operation == PreconditionOpWire.MustMatch))
            .ToList();
    }

    private async Task<string> MintToken(IRevision revision, string schemaHash)
    {
        var datastoreId = await datastore.GetUniqueId(CancellationToken.None)
            .ConfigureAwait(ContinueOnCapturedContext);
        return ZedTokens.FromRevision(revision, schemaHash, datastoreId).Token;
    }

    private static RelationshipsFilter ToFilter(RelationshipsFilterWire wire)
    {
        IReadOnlyList<SubjectsSelector>? selectors = null;
        if (wire.SubjectType is { Length: > 0 } || wire.SubjectIds is { Count: > 0 } || wire.SubjectRelation is { Length: > 0 })
        {
            var relFilter = wire.SubjectRelation is { Length: > 0 } sr
                ? new SubjectRelationFilter(NonEllipsisRelation: sr)
                : null;
            selectors = [new SubjectsSelector(wire.SubjectType, wire.SubjectIds, relFilter)];
        }

        return new RelationshipsFilter
        {
            OptionalResourceType = wire.ResourceType,
            OptionalResourceIds = wire.ResourceIds,
            OptionalResourceIdPrefix = wire.ResourceIdPrefix,
            OptionalResourceRelation = wire.ResourceRelation,
            OptionalSubjectsSelectors = selectors,
        };
    }
}
