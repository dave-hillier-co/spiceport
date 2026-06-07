using System.Text;
using Orleans.Concurrency;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// Stateless-worker implementation of <see cref="IRelationshipsGrain"/>: the data-plane write side.
/// Schema writes compile-and-swap the live <see cref="ISchemaProvider"/> and persist to the datastore;
/// relationship writes/deletes run a single read-write transaction; reads pin the optimized revision and
/// page over a snapshot. Replies carry opaque revision tokens minted by <see cref="ZedTokens"/>.
/// </summary>
/// <remarks>
/// The grain is <c>[StatelessWorker]</c> so the silo scales activations with load; it carries no per-key
/// identity, so callers always use <see cref="IRelationshipsGrain.Key"/>. Read paging is deterministic
/// by the canonical tuple string and resumes strictly after the cursor's tuple, requesting one extra row
/// to set the continuation cursor (the same convention as the reverse-ops grain).
/// </remarks>
[StatelessWorker]
public sealed class RelationshipsGrain(
    IDatastore datastore,
    ISchemaProvider schemaProvider) : Grain, IRelationshipsGrain
{
    // Orleans grain code must not ConfigureAwait(false); keep the captured context.
    private const ConfigureAwaitOptions ContinueOnCapturedContext = ConfigureAwaitOptions.ContinueOnCapturedContext;

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

        var current = schemaProvider.Current.Schema;

        // Validate the diff against existing data and persist in ONE transaction: the change-validation
        // reads run against the transaction reader (the snapshot the swap commits at), so a concurrent
        // write either is seen here or fails the serialization check on commit. A removal that would
        // orphan an existing relationship throws SchemaWriteValidationException, abandoning the tx — the
        // datastore is unchanged and (because the swap happens only AFTER commit) the live schema too.
        var committed = await datastore.ReadWriteTx(async tx =>
        {
            await SchemaChangeValidator
                .ValidateAsync(current, nextCompiled, tx, CancellationToken.None)
                .ConfigureAwait(ContinueOnCapturedContext);
            await tx.WriteStoredSchema(Encoding.UTF8.GetBytes(args.SchemaText))
                .ConfigureAwait(ContinueOnCapturedContext);
        }).ConfigureAwait(ContinueOnCapturedContext);

        // The persist committed: only now swap the live snapshot so the datastore and the live snapshot
        // never diverge and a rejected change leaves the live schema intact.
        var snapshot = schemaProvider.Update(args.SchemaText);

        var token = await MintToken(committed, snapshot.SchemaHash).ConfigureAwait(ContinueOnCapturedContext);
        return new WriteSchemaReply(token);
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

        var updates = args.Updates.Select(ToUpdate).ToList();
        IRevision committed;
        try
        {
            committed = await datastore.ReadWriteTx(async tx =>
            {
                // Preconditions are evaluated INSIDE the write transaction, against the same snapshot the
                // writes commit at: the tx reader sees prior committed state and any staged mutations. If a
                // precondition fails we throw, which abandons the transaction so nothing commits.
                await CheckPreconditions(tx, args.Preconditions).ConfigureAwait(ContinueOnCapturedContext);
                await tx.WriteRelationships(updates).ConfigureAwait(ContinueOnCapturedContext);
            }).ConfigureAwait(ContinueOnCapturedContext);
        }
        catch (CreateRelationshipExistsException ex)
        {
            // The datastore exceptions are not Orleans-serializable across the grain boundary; re-wrap
            // in a [GenerateSerializer] exception that preserves the conflict kind for the gRPC mapping.
            throw new WriteConflictException(WriteConflictKind.CreateExisting, ex.Message);
        }
        catch (SerializationException ex)
        {
            throw new WriteConflictException(WriteConflictKind.Serialization, ex.Message);
        }

        var token = await MintToken(committed, schemaProvider.Current.SchemaHash)
            .ConfigureAwait(ContinueOnCapturedContext);
        return new WriteRelationshipsReply(token);
    }

    /// <inheritdoc />
    public async Task<DeleteRelationshipsReply> DeleteRelationships(DeleteRelationshipsArgs args)
    {
        ArgumentNullException.ThrowIfNull(args);

        var filter = ToFilter(args.Filter);
        var count = 0UL;
        var reachedLimit = false;
        var committed = await datastore.ReadWriteTx(async tx =>
        {
            await CheckPreconditions(tx, args.Preconditions).ConfigureAwait(ContinueOnCapturedContext);
            (count, reachedLimit) = await tx.DeleteRelationships(filter, args.OptionalLimit)
                .ConfigureAwait(ContinueOnCapturedContext);
        }).ConfigureAwait(ContinueOnCapturedContext);

        var token = await MintToken(committed, schemaProvider.Current.SchemaHash)
            .ConfigureAwait(ContinueOnCapturedContext);
        return new DeleteRelationshipsReply(count, reachedLimit, token);
    }

    /// <inheritdoc />
    public async Task<ReadRelationshipsReply> ReadRelationships(ReadRelationshipsArgs args)
    {
        ArgumentNullException.ThrowIfNull(args);

        // Honour the request consistency; null (the default) is MinimizeLatency → the optimized
        // revision, identical to the prior behaviour. The read-at token is minted from the revision
        // actually evaluated, not unconditionally the optimized one.
        var requirement = (args.Consistency ?? ConsistencyWire.MinimizeLatency).ToRequirement();
        var resolved = await RevisionResolver
            .Resolve(datastore, requirement, cancellationToken: CancellationToken.None)
            .ConfigureAwait(ContinueOnCapturedContext);
        var reader = datastore.SnapshotReader(resolved.Revision);
        var filter = ToFilter(args.Filter);
        var after = args.Cursor;
        var limit = args.Limit is { } l && l > 0 ? l : (int?)null;

        // Materialize and order deterministically by canonical tuple string so paging is stable.
        var matched = new List<(string Tuple, Relationship Rel)>();
        await foreach (var rel in reader.QueryRelationships(filter))
        {
            var tuple = TupleStrings.FormatRelationship(rel);
            if (after is { } a && string.CompareOrdinal(tuple, a) <= 0)
                continue;
            matched.Add((tuple, rel));
        }

        matched.Sort((x, y) => string.CompareOrdinal(x.Tuple, y.Tuple));

        var page = new List<RelationshipWire>();
        string? cursor = null;
        for (var i = 0; i < matched.Count; i++)
        {
            if (limit is { } cap && page.Count >= cap)
            {
                // More rows remain past the page: resume strictly after the last emitted tuple.
                cursor = matched[i - 1].Tuple;
                break;
            }

            page.Add(ToWire(matched[i].Rel));
        }

        var token = await MintToken(resolved.Revision, resolved.SchemaHash ?? schemaProvider.Current.SchemaHash)
            .ConfigureAwait(ContinueOnCapturedContext);
        return new ReadRelationshipsReply(page, cursor, token);
    }

    /// <inheritdoc />
    public async Task<BulkImportRelationshipsReply> BulkImportRelationships(BulkImportRelationshipsArgs args)
    {
        ArgumentNullException.ThrowIfNull(args);

        // Efficient-insert semantics (create-or-overwrite, NO per-row already-exists check), matching
        // SpiceDB ImportBulk. The whole batch loads in ONE write transaction: all-or-nothing per batch.
        // The gRPC service drives the client stream and calls this once per inbound batch, so the grain
        // itself stays request/response.
        var relationships = args.Relationships.Select(ToRelationship).ToList();
        ulong loaded = 0;
        IRevision committed;
        try
        {
            committed = await datastore.ReadWriteTx(async tx =>
            {
                loaded = await tx.BulkLoad(ToAsync(relationships)).ConfigureAwait(ContinueOnCapturedContext);
            }).ConfigureAwait(ContinueOnCapturedContext);
        }
        catch (CreateRelationshipExistsException ex)
        {
            throw new WriteConflictException(WriteConflictKind.CreateExisting, ex.Message);
        }
        catch (SerializationException ex)
        {
            throw new WriteConflictException(WriteConflictKind.Serialization, ex.Message);
        }

        var token = await MintToken(committed, schemaProvider.Current.SchemaHash)
            .ConfigureAwait(ContinueOnCapturedContext);
        return new BulkImportRelationshipsReply(loaded, token);

        static async IAsyncEnumerable<Relationship> ToAsync(IReadOnlyList<Relationship> rels)
        {
            foreach (var rel in rels)
                yield return rel;
            await Task.CompletedTask;
        }
    }

    /// <inheritdoc />
    public async Task<BulkExportRelationshipsReply> BulkExportRelationships(BulkExportRelationshipsArgs args)
    {
        ArgumentNullException.ThrowIfNull(args);

        // Pin the snapshot ONCE: with no cursor resolve from the request consistency; with a cursor read
        // the exact revision the cursor encodes. The pinned revision is carried in every returned cursor,
        // so resuming reads the same snapshot and never sees writes committed after the export began.
        IRevision pinned;
        string? after;
        if (BulkExportCursor.TryDecode(args.Cursor, out var decoded))
        {
            pinned = decoded.Revision;
            after = decoded.AfterTuple;
        }
        else
        {
            var requirement = (args.Consistency ?? ConsistencyWire.MinimizeLatency).ToRequirement();
            var resolved = await RevisionResolver
                .Resolve(datastore, requirement, cancellationToken: CancellationToken.None)
                .ConfigureAwait(ContinueOnCapturedContext);
            pinned = resolved.Revision;
            after = null;
        }

        var reader = datastore.SnapshotReader(pinned);
        var filter = ToFilter(args.Filter);
        var limit = args.Limit > 0 ? args.Limit : DefaultExportPageSize;

        // Materialize matching tuples ordered by canonical string (same deterministic paging convention
        // as ReadRelationships), skip past the cursor, and take one page.
        var matched = new List<(string Tuple, Relationship Rel)>();
        await foreach (var rel in reader.QueryRelationships(filter))
        {
            var tuple = TupleStrings.FormatRelationship(rel);
            if (after is { } a && string.CompareOrdinal(tuple, a) <= 0)
                continue;
            matched.Add((tuple, rel));
        }

        matched.Sort((x, y) => string.CompareOrdinal(x.Tuple, y.Tuple));

        var page = new List<RelationshipWire>(Math.Min(limit, matched.Count));
        string? lastTuple = null;
        for (var i = 0; i < matched.Count && page.Count < limit; i++)
        {
            page.Add(ToWire(matched[i].Rel));
            lastTuple = matched[i].Tuple;
        }

        // A full page that exactly drains the remaining rows still emits a cursor; the next call returns
        // an empty page with a null cursor, signalling exhaustion. This mirrors the export's "keep going
        // while pages are full" contract.
        var cursor = page.Count == limit && lastTuple is not null
            ? BulkExportCursor.Encode(pinned, lastTuple)
            : null;

        return new BulkExportRelationshipsReply(page, cursor);
    }

    /// <inheritdoc />
    public async Task<RegisterCounterReply> RegisterRelationshipCounter(RegisterCounterArgs args)
    {
        ArgumentNullException.ThrowIfNull(args);

        var filter = ToFilter(args.Filter);
        try
        {
            await datastore.ReadWriteTx(tx => tx.WriteCounter(args.Name, filter))
                .ConfigureAwait(ContinueOnCapturedContext);
        }
        catch (CounterAlreadyRegisteredException ex)
        {
            // The datastore exception is not Orleans-serializable across the grain boundary; rethrow a
            // serializable carrier the gRPC front door maps to the right status code.
            throw new CounterOperationException(CounterErrorKind.AlreadyRegistered, ex.Message);
        }

        return new RegisterCounterReply();
    }

    /// <inheritdoc />
    public async Task<UnregisterCounterReply> UnregisterRelationshipCounter(UnregisterCounterArgs args)
    {
        ArgumentNullException.ThrowIfNull(args);

        try
        {
            await datastore.ReadWriteTx(tx => tx.DeleteCounter(args.Name))
                .ConfigureAwait(ContinueOnCapturedContext);
        }
        catch (CounterNotRegisteredException ex)
        {
            throw new CounterOperationException(CounterErrorKind.NotRegistered, ex.Message);
        }

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
        var reader = datastore.SnapshotReader(resolved.Revision);

        ulong count;
        try
        {
            count = await reader.CountRelationships(args.Name).ConfigureAwait(ContinueOnCapturedContext);
        }
        catch (CounterNotRegisteredException ex)
        {
            throw new CounterOperationException(CounterErrorKind.NotRegistered, ex.Message);
        }

        var token = await MintToken(resolved.Revision, resolved.SchemaHash ?? schemaProvider.Current.SchemaHash)
            .ConfigureAwait(ContinueOnCapturedContext);
        return new CountRelationshipsReply(count, token);
    }

    /// <summary>The default bulk-export page size when the request leaves the limit unset.</summary>
    private const int DefaultExportPageSize = 1000;

    /// <summary>
    /// Evaluates each precondition against the transaction reader (the snapshot the writes will commit
    /// at). A MUST_MATCH that finds nothing, or a MUST_NOT_MATCH that finds something, throws
    /// <see cref="PreconditionFailedException"/>, which abandons the transaction so nothing commits.
    /// </summary>
    private static async Task CheckPreconditions(
        IReadWriteTransaction tx,
        IReadOnlyList<PreconditionWire>? preconditions)
    {
        if (preconditions is not { Count: > 0 })
            return;

        for (var i = 0; i < preconditions.Count; i++)
        {
            var precond = preconditions[i];
            var filter = ToFilter(precond.Filter);

            var matched = false;
            await foreach (var _ in tx.QueryRelationships(filter))
            {
                matched = true;
                break; // existence check only; one row is enough.
            }

            switch (precond.Operation)
            {
                case PreconditionOpWire.MustMatch when !matched:
                    throw new PreconditionFailedException(
                        PreconditionFailureKind.MustMatchFoundNone, i,
                        $"precondition {i} failed: MUST_MATCH filter [{DescribeFilter(filter)}] matched no relationships");
                case PreconditionOpWire.MustNotMatch when matched:
                    throw new PreconditionFailedException(
                        PreconditionFailureKind.MustNotMatchFoundOne, i,
                        $"precondition {i} failed: MUST_NOT_MATCH filter [{DescribeFilter(filter)}] matched at least one relationship");
            }
        }
    }

    private static string DescribeFilter(RelationshipsFilter f)
    {
        var sb = new StringBuilder();
        if (f.OptionalResourceType is { } rt) sb.Append("resource_type=").Append(rt).Append(' ');
        if (f.OptionalResourceIds is { Count: > 0 } ids) sb.Append("resource_ids=").Append(string.Join(",", ids)).Append(' ');
        if (f.OptionalResourceIdPrefix is { Length: > 0 } p) sb.Append("resource_id_prefix=").Append(p).Append(' ');
        if (f.OptionalResourceRelation is { } rr) sb.Append("relation=").Append(rr).Append(' ');
        if (f.OptionalSubjectsSelectors is { Count: > 0 } sels)
        {
            foreach (var s in sels)
            {
                if (s.OptionalSubjectType is { } st) sb.Append("subject_type=").Append(st).Append(' ');
                if (s.OptionalSubjectIds is { Count: > 0 } sids) sb.Append("subject_ids=").Append(string.Join(",", sids)).Append(' ');
            }
        }
        return sb.ToString().TrimEnd();
    }

    private async Task<string> MintToken(IRevision revision, string schemaHash)
    {
        var datastoreId = await datastore.GetUniqueId(CancellationToken.None)
            .ConfigureAwait(ContinueOnCapturedContext);
        return ZedTokens.FromRevision(revision, schemaHash, datastoreId).Token;
    }

    private static RelationshipUpdate ToUpdate(RelationshipUpdateWire wire)
    {
        var op = wire.Operation switch
        {
            RelationshipUpdateOpWire.Create => UpdateOperation.Create,
            RelationshipUpdateOpWire.Delete => UpdateOperation.Delete,
            _ => UpdateOperation.Touch,
        };
        return new RelationshipUpdate(ToRelationship(wire.Relationship), op);
    }

    private static Relationship ToRelationship(RelationshipWire wire)
    {
        var resource = new ObjectAndRelation(wire.ResourceType, wire.ResourceId, wire.ResourceRelation);
        var subjectRelation = string.IsNullOrEmpty(wire.SubjectRelation) ? CoreConstants.Ellipsis : wire.SubjectRelation;
        var subject = new ObjectAndRelation(wire.SubjectType, wire.SubjectId, subjectRelation);
        var caveat = wire.CaveatName is { Length: > 0 }
            ? new ContextualizedCaveat(wire.CaveatName, wire.CaveatContext)
            : null;
        return Relationship.Create(resource, subject, caveat, wire.Expiration);
    }

    private static RelationshipWire ToWire(Relationship rel) => new(
        rel.Resource.ObjectType, rel.Resource.ObjectId, rel.Resource.Relation,
        rel.Subject.ObjectType, rel.Subject.ObjectId, rel.Subject.Relation,
        rel.OptionalCaveat?.CaveatName, rel.OptionalCaveat?.Context, rel.OptionalExpiration);

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
