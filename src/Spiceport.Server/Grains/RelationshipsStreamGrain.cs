using System.Runtime.CompilerServices;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// Default-placement, Guid-keyed implementation of <see cref="IRelationshipsStreamGrain"/>: streams the
/// data-plane read ops as native Orleans <see cref="IAsyncEnumerable{T}"/> iterators over a single pinned
/// snapshot. The write side and all other data-plane ops stay on the stateless-worker
/// <see cref="RelationshipsGrain"/>.
/// </summary>
/// <remarks>
/// Deliberately NOT a <c>[StatelessWorker]</c> — native streaming holds the enumerator on one activation
/// keyed by the caller-minted request Guid (see the interface remarks). The datastore's
/// <see cref="IDatastoreReader.QueryRelationships"/> does not guarantee canonical-tuple order, so each
/// iterator materializes its matches once and sorts before yielding — the deterministic order the client
/// cursor depends on. The deleted round-trips are the per-page service→grain hops, not this in-memory sort:
/// the whole read is now ONE grain call whose result streams back with backpressure.
/// </remarks>
public sealed class RelationshipsStreamGrain(
    IDatastore datastore,
    ISchemaProvider schemaProvider) : Grain, IRelationshipsStreamGrain
{
    private const ConfigureAwaitOptions ContinueOnCapturedContext = ConfigureAwaitOptions.ContinueOnCapturedContext;

    /// <inheritdoc />
    public async IAsyncEnumerable<RelationshipStreamItem> StreamReadRelationships(
        ReadRelationshipsArgs args,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        cancellationToken.ThrowIfCancellationRequested();

        // Resolve the revision ONCE (no more paging): null consistency (the default) is MinimizeLatency → the
        // optimized revision, identical to the prior behaviour. The read-at token is minted from the revision
        // actually evaluated.
        var requirement = (args.Consistency ?? ConsistencyWire.MinimizeLatency).ToRequirement();
        var resolved = await RevisionResolver
            .Resolve(datastore, requirement, cancellationToken: cancellationToken)
            .ConfigureAwait(ContinueOnCapturedContext);
        var reader = datastore.SnapshotReader(resolved.Revision);
        var filter = ToFilter(args.Filter);
        var after = args.Cursor;
        var token = await MintToken(resolved.Revision, resolved.SchemaHash ?? schemaProvider.Current.SchemaHash)
            .ConfigureAwait(ContinueOnCapturedContext);

        // Materialize and order deterministically by canonical tuple string so the stream (and any client
        // resume from a per-item cursor) is stable, skipping rows at or before the cursor.
        var matched = new List<(string Tuple, Relationship Rel)>();
        await foreach (var rel in reader.QueryRelationships(filter, cancellationToken)
            .WithCancellation(cancellationToken))
        {
            var tuple = TupleStrings.FormatRelationship(rel);
            if (after is { } a && string.CompareOrdinal(tuple, a) <= 0)
                continue;
            matched.Add((tuple, rel));
        }

        matched.Sort((x, y) => string.CompareOrdinal(x.Tuple, y.Tuple));

        foreach (var (tuple, rel) in matched)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return new RelationshipStreamItem(ToWire(rel), tuple, token);
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RelationshipStreamItem> StreamBulkExportRelationships(
        BulkExportRelationshipsArgs args,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        cancellationToken.ThrowIfCancellationRequested();

        // Pin the snapshot ONCE: with no cursor resolve from the request consistency; with a cursor read the
        // exact revision the cursor encodes. The pinned revision is carried in every emitted cursor, so a
        // reconnect reads the same snapshot and never sees writes committed after the export began.
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
                .Resolve(datastore, requirement, cancellationToken: cancellationToken)
                .ConfigureAwait(ContinueOnCapturedContext);
            pinned = resolved.Revision;
            after = null;
        }

        var reader = datastore.SnapshotReader(pinned);
        var filter = ToFilter(args.Filter);

        var matched = new List<(string Tuple, Relationship Rel)>();
        await foreach (var rel in reader.QueryRelationships(filter, cancellationToken)
            .WithCancellation(cancellationToken))
        {
            var tuple = TupleStrings.FormatRelationship(rel);
            if (after is { } a && string.CompareOrdinal(tuple, a) <= 0)
                continue;
            matched.Add((tuple, rel));
        }

        matched.Sort((x, y) => string.CompareOrdinal(x.Tuple, y.Tuple));

        foreach (var (tuple, rel) in matched)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return new RelationshipStreamItem(ToWire(rel), BulkExportCursor.Encode(pinned, tuple));
        }
    }

    private async Task<string> MintToken(IRevision revision, string schemaHash)
    {
        var datastoreId = await datastore.GetUniqueId(CancellationToken.None)
            .ConfigureAwait(ContinueOnCapturedContext);
        return ZedTokens.FromRevision(revision, schemaHash, datastoreId).Token;
    }

    private static RelationshipWire ToWire(Relationship rel) => WireConvert.ToWire(rel);

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
