namespace Spiceport.Grains.Abstractions;

/// <summary>
/// A stateless-worker grain that runs the whole-resource / whole-subject reverse and tree engine ops
/// (ExpandPermissionTree, LookupSubjects, LookupResources) against a pinned datastore snapshot.
/// </summary>
/// <remarks>
/// Unlike <see cref="ICheckGrain"/> — which is keyed by a single canonical sub-problem so recursion can
/// cross grain boundaries and share the branch cache — these three ops are whole-traversals, not
/// per-child dispatch. They are therefore exposed on ONE grain keyed by the constant integer <c>0</c>
/// and marked <c>[StatelessWorker]</c> on the implementation so the silo scales activations with load
/// without fragmenting any cache keyspace. The op runs its own forward/reverse walk over the snapshot;
/// where it issues Check sub-calls (LookupResources confirms intersection / exclusion candidates) those
/// still flow through the existing dispatcher mesh inside the engine. Each call pins the datastore's
/// optimized (quantized) revision itself, so callers need not thread a revision.
/// <para>
/// Results are returned as a bounded materialized page with an optional opaque cursor rather than via
/// Orleans streaming: the engine ops already materialize their working sets, a single unary page keeps
/// the grain reply, the gRPC surface and the in-process test path identical, and server streaming can be
/// added later without changing this seam. A non-empty <c>Cursor</c> on a reply means more results may
/// exist; pass it back to resume. Expand returns a whole tree and has no cursor.
/// </para>
/// </remarks>
public interface IReverseOpsGrain : IGrainWithIntegerKey
{
    /// <summary>The fixed key every caller uses; the grain is a stateless worker so this is not an identity.</summary>
    public const long Key = 0;

    /// <summary>Expands the resource's permission into a structural permission tree.</summary>
    Task<ExpandTreeReply> ExpandPermissionTree(ExpandTreeArgs args);

    /// <summary>Enumerates the subjects (of the requested type/subrelation) holding the resource's permission.</summary>
    Task<LookupSubjectsReply> LookupSubjects(LookupSubjectsArgs args);

    /// <summary>Enumerates the resources (of the requested type) on which the subject holds the permission.</summary>
    Task<LookupResourcesReply> LookupResources(LookupResourcesArgs args);
}
