using Spiceport.Core;

namespace Spiceport.Datastore;

/// <summary>
/// The graph-shaped read seam the evaluation engines consume: forward reads keyed by resource,
/// reverse reads keyed by subject, at a fixed pinned revision.
/// </summary>
/// <remarks>
/// This is the narrow surface the graph-sharded datastore (<c>docs/graph-sharded-datastore.md</c>)
/// serves per key. <see cref="IDatastoreReader"/> extends it with the snapshot-wide reads (schema,
/// counters, validity).
/// </remarks>
public interface IGraphReader
{
    /// <summary>Queries relationships from the resource side, matching the given filter.</summary>
    IAsyncEnumerable<Relationship> QueryRelationships(
        RelationshipsFilter filter,
        CancellationToken cancellationToken = default);

    /// <summary>Queries relationships from the subject side, matching the given subject filter.</summary>
    /// <param name="subjectsFilter">The subject-side filter to match.</param>
    /// <param name="options">
    /// Optional ordering and keyset-resume controls. When null the query is unordered and unbounded
    /// (the original behaviour). A <see cref="ReverseQuerySort.BySubject"/> sort yields a deterministic
    /// total order that <see cref="ReverseQueryOptions.After"/> can resume after.
    /// </param>
    /// <param name="cancellationToken">A cancellation token.</param>
    IAsyncEnumerable<Relationship> ReverseQueryRelationships(
        SubjectsFilter subjectsFilter,
        ReverseQueryOptions? options = null,
        CancellationToken cancellationToken = default);
}
