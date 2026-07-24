using System.Runtime.CompilerServices;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// An <see cref="IGraphReader"/> pinned at a revision that resolves each read to the matching
/// <see cref="IGraphShardGrain"/> — forward shards for resource-pinned queries, reverse shards for
/// subject-pinned queries — instead of the whole-graph silo projection
/// (<c>docs/graph-sharded-datastore.md</c> §2/§3, migration step 3).
/// </summary>
/// <remarks>
/// Deliberately NARROW: it serves only the two pinned call shapes the evaluation engines actually
/// produce (a <see cref="RelationshipsFilter"/> with explicit resource ids; a <see cref="SubjectsFilter"/>
/// with explicit subject ids) and throws <see cref="NotSupportedException"/> for anything scan-shaped —
/// broad scans stay on the <c>IDatastoreReader</c>/snapshot path (the design's scan seam), and the one
/// no-subject-ids reverse caller (<see cref="SchemaChangeValidator"/>) stays on <c>IDatastoreReader</c>.
/// Shards filter VISIBILITY at the pinned revision; EXPIRATION is filtered here at enumeration time
/// ("now" captured once per enumeration), and sorted reverse reads mirror
/// <c>MvccSnapshotReader.ReverseQueryRelationships</c>'s ordered path exactly (same comparator, same
/// exclusive keyset skip), so the two readers are row-for-row interchangeable — the property the
/// fold-equivalence gate asserts.
/// </remarks>
public sealed class ShardedGraphReader : IGraphReader
{
    private readonly IGrainFactory _grainFactory;
    private readonly long _revisionNanos;

    /// <summary>Creates a reader over the shard mesh, pinned at the given timestamp revision.</summary>
    public ShardedGraphReader(IGrainFactory grainFactory, long revisionNanos)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        _grainFactory = grainFactory;
        _revisionNanos = revisionNanos;
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<Relationship> QueryRelationships(
        RelationshipsFilter filter,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(filter);

        // The engine call-site inventory (design step 1) proves engines only produce pinned-resource
        // shapes; anything broader belongs on the snapshot path, not the shard mesh.
        if (filter.OptionalResourceType is null
            || filter.OptionalResourceIds is not { Count: > 0 } resourceIds
            || filter.OptionalResourceIdPrefix is not null)
        {
            throw new NotSupportedException(
                "scan-shaped filter on the sharded graph reader; broad scans stay on the IDatastoreReader/snapshot path");
        }

        // 'now' is captured ONCE per enumeration, mirroring MvccSnapshotReader: every row of one read
        // sees the same expiration cutoff.
        var now = DateTimeOffset.UtcNow;

        foreach (var id in resourceIds.Distinct())
        {
            cancellationToken.ThrowIfCancellationRequested();
            var shard = _grainFactory.GetGrain<IGraphShardGrain>(
                GraphShardGrainKey.Build(GraphShardKeyWire.ForResource(filter.OptionalResourceType, id)));
            var reply = await shard.RowsAt(_revisionNanos, cancellationToken).ConfigureAwait(false);

            foreach (var row in reply.Rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var rel = WireConvert.ToRelationship(row);
                if (IsExpired(rel, now))
                    continue;
                if (filter.Matches(rel))
                    yield return rel;
            }
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<Relationship> ReverseQueryRelationships(
        SubjectsFilter subjectsFilter,
        ReverseQueryOptions? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(subjectsFilter);

        // The only no-subject-ids caller is SchemaChangeValidator, which stays on IDatastoreReader.
        if (subjectsFilter.OptionalSubjectIds is not { Count: > 0 } subjectIds)
        {
            throw new NotSupportedException(
                "scan-shaped filter on the sharded graph reader; broad scans stay on the IDatastoreReader/snapshot path");
        }

        var now = DateTimeOffset.UtcNow;

        if (options is null || options.Sort == ReverseQuerySort.Unsorted)
        {
            // Fast path: stream shard by shard, no materialization (mirrors MvccSnapshotReader).
            foreach (var id in subjectIds.Distinct())
            {
                cancellationToken.ThrowIfCancellationRequested();
                var reply = await RowsOfSubject(subjectsFilter.SubjectType, id, cancellationToken)
                    .ConfigureAwait(false);
                foreach (var row in reply.Rows)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var rel = WireConvert.ToRelationship(row);
                    if (IsExpired(rel, now))
                        continue;
                    if (subjectsFilter.Matches(rel))
                        yield return rel;
                }
            }
            yield break;
        }

        // Ordered path — mirrors MvccSnapshotReader.ReverseQueryRelationships exactly: materialize the
        // matching rows, sort by the BySubject total order, then apply the exclusive keyset so
        // resumption sees only strictly-greater rows (the six-tuple is unique, so there are no ties).
        var matches = new List<Relationship>();
        foreach (var id in subjectIds.Distinct())
        {
            cancellationToken.ThrowIfCancellationRequested();
            var reply = await RowsOfSubject(subjectsFilter.SubjectType, id, cancellationToken)
                .ConfigureAwait(false);
            foreach (var row in reply.Rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var rel = WireConvert.ToRelationship(row);
                if (IsExpired(rel, now))
                    continue;
                if (subjectsFilter.Matches(rel))
                    matches.Add(rel);
            }
        }

        matches.Sort(ReverseQueryOptions.CompareBySubject);

        foreach (var rel in matches)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (options.After is { } after &&
                ReverseQueryOptions.CompareBySubject(rel.Reference, after) <= 0)
                continue;
            yield return rel;
        }
    }

    private Task<GraphShardRowsReply> RowsOfSubject(string subjectType, string subjectId, CancellationToken ct)
    {
        var shard = _grainFactory.GetGrain<IGraphShardGrain>(
            GraphShardGrainKey.Build(GraphShardKeyWire.ForSubject(subjectType, subjectId)));
        return shard.RowsAt(_revisionNanos, ct);
    }

    // Mirrors MvccSnapshotReader.IsExpired exactly: an expiration at or before 'now' excludes the row.
    private static bool IsExpired(Relationship rel, DateTimeOffset now) =>
        rel.OptionalExpiration is { } exp && exp <= now;
}
