using System.Runtime.CompilerServices;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// The data-plane READ ops (ReadRelationships, BulkExportRelationships) served IN-PROCESS over the local
/// silo's <see cref="IDatastore"/> projection — the same pattern <see cref="Spiceport.Api.AuthzedWatchV1Service"/>
/// uses deliberately for Watch (see its remarks). The write side and all other data-plane ops stay on the
/// stateless-worker <see cref="IRelationshipsGrain"/>.
/// </summary>
/// <remarks>
/// The datastore's <see cref="IDatastoreReader.QueryRelationships"/> does not guarantee canonical-tuple
/// order, so each read materializes its matches once and sorts before yielding — the deterministic order
/// the client cursor depends on. This is no longer grain code, so the streaming ops take the caller's plain
/// <see cref="CancellationToken"/> directly, with no Orleans grain-method plumbing.
/// </remarks>
public sealed class RelationshipReads(IDatastore datastore, ISchemaProvider schemaProvider)
{
    /// <summary>
    /// Streams relationships matching the filter, in ascending canonical-tuple order, over one revision
    /// resolved once at the start. Each item carries the canonical tuple as its resume cursor (resumption
    /// skips tuples at or before it) and the per-message read-at token. The caller applies any client limit
    /// by stopping enumeration.
    /// </summary>
    public async IAsyncEnumerable<RelationshipStreamItem> ReadRelationships(
        ReadRelationshipsArgs args,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        cancellationToken.ThrowIfCancellationRequested();

        // Resolve the revision ONCE (no paging): null consistency (the default) is MinimizeLatency → the
        // optimized revision. The read-at token is minted from the revision actually evaluated.
        var requirement = (args.Consistency ?? ConsistencyWire.MinimizeLatency).ToRequirement();
        var resolved = await RevisionResolver.Resolve(datastore, requirement, cancellationToken: cancellationToken);
        var reader = datastore.SnapshotReader(resolved.Revision);
        var filter = ToFilter(args.Filter);
        var after = args.Cursor;
        var token = await MintToken(resolved.Revision, resolved.SchemaHash ?? schemaProvider.Current.SchemaHash);

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

    /// <summary>
    /// Streams a bulk export over a single pinned snapshot: with no cursor this resolves and pins a
    /// revision from the request consistency; with a cursor it reads the exact revision the cursor encodes.
    /// Each item's resume cursor carries that pinned revision plus the last tuple, so a reconnect reads the
    /// same snapshot and never sees writes committed after the export began. The caller applies any client
    /// limit/batching by how it consumes the stream.
    /// </summary>
    public async IAsyncEnumerable<RelationshipStreamItem> BulkExportRelationships(
        BulkExportRelationshipsArgs args,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        cancellationToken.ThrowIfCancellationRequested();

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
            var resolved = await RevisionResolver.Resolve(datastore, requirement, cancellationToken: cancellationToken);
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
        var datastoreId = await datastore.GetUniqueId(CancellationToken.None);
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
