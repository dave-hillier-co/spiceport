namespace Spiceport.Grains.Abstractions;

/// <summary>
/// A stateless-worker grain that runs the whole-resource permission tree expansion (ExpandPermissionTree)
/// against a pinned datastore snapshot.
/// </summary>
/// <remarks>
/// Unlike <see cref="ICheckGrain"/> — which is keyed by a single canonical sub-problem so recursion can
/// cross grain boundaries and share the branch cache — Expand is a whole-traversal, not per-child dispatch.
/// It is therefore exposed on ONE grain keyed by the constant integer <c>0</c> and marked
/// <c>[StatelessWorker]</c> on the implementation so the silo scales activations with load without
/// fragmenting any cache keyspace. Expand pins the datastore's optimized (quantized) revision itself, so
/// callers need not thread a revision, and returns a whole tree with no cursor.
/// <para>
/// The reverse LOOKUP ops (LookupSubjects, LookupResources) are NOT here: they moved to the Guid-keyed
/// <see cref="IReverseOpsStreamGrain"/>, which streams them as native Orleans <see cref="IAsyncEnumerable{T}"/>
/// grain calls (a stateless worker cannot back native streaming — its activations are not individually
/// addressable, so a follow-up <c>MoveNext</c> could land on an activation that never started the stream).
/// </para>
/// </remarks>
public interface IReverseOpsGrain : IGrainWithIntegerKey
{
    /// <summary>The fixed key every caller uses; the grain is a stateless worker so this is not an identity.</summary>
    public const long Key = 0;

    /// <summary>Expands the resource's permission into a structural permission tree.</summary>
    Task<ExpandTreeReply> ExpandPermissionTree(ExpandTreeArgs args);
}
