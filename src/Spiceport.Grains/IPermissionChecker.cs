using System.Collections.Immutable;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;

namespace Spiceport.Grains;

/// <summary>The verdict of a top-level permission check, plus any unresolved caveat fields.</summary>
/// <param name="Verdict">The membership verdict.</param>
/// <param name="MissingFields">
/// When <see cref="Verdict"/> is <see cref="Membership.Caveated"/>, the caveat parameter names that
/// were missing from the supplied context; otherwise empty.
/// </param>
public sealed record PermissionCheckResult(Membership Verdict, IReadOnlyList<string> MissingFields);

/// <summary>
/// The top-level entry point used by the API: pins an optimized (quantized) revision, dispatches the
/// root sub-problem through the silo-wide Caching-over-Orleans dispatcher, then collapses the returned
/// pre-context branch against the request-time caveat context.
/// </summary>
public interface IPermissionChecker
{
    /// <summary>Checks whether the subject has the given permission on the resource.</summary>
    Task<PermissionCheckResult> Check(
        string resourceType,
        string resourceId,
        string permission,
        ObjectAndRelation subject,
        IReadOnlyDictionary<string, object?>? caveatContext,
        CancellationToken ct = default);
}

/// <summary>
/// Default <see cref="IPermissionChecker"/>. Mirrors the in-process <c>CheckEngine.Check</c> flow,
/// but the dispatch seam is the silo-wide root dispatcher, so the recursion runs across grains.
/// </summary>
/// <remarks>
/// The revision is pinned to the datastore's optimized (quantized) revision so that the revision
/// component of every grain key — and hence the shared branch cache — buckets near-in-time checks
/// together, while still being a real, snapshot-able revision each grain can resolve a reader for.
/// </remarks>
public sealed class PermissionChecker(
    IDatastore datastore,
    ISiloDispatcher root,
    ISchemaProvider schemaProvider,
    int maxDepth = CheckEngine.DefaultMaxDepth) : IPermissionChecker
{
    /// <inheritdoc />
    public async Task<PermissionCheckResult> Check(
        string resourceType,
        string resourceId,
        string permission,
        ObjectAndRelation subject,
        IReadOnlyDictionary<string, object?>? caveatContext,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(subject);

        var optimized = await datastore.OptimizedRevision(ct).ConfigureAwait(false);

        // Capture a single consistent schema snapshot for this check, so the collapse uses the same
        // caveats the dispatch ran under. Collapse is cheap and stateless, so building a per-call
        // engine over the live schema is sufficient (rebuild-on-version-change is a perf follow-up).
        var schema = schemaProvider.Current;
        var engine = new CheckEngine(schema.Namespaces, schema.Caveats, maxDepth);

        var resource = new ObjectAndRelation(resourceType, resourceId, permission);
        var meta = new ResolverMeta(optimized.Revision, maxDepth, ImmutableHashSet<VisitKey>.Empty);
        var request = new DispatchCheckRequest(resource, subject, meta);

        var branch = await root.Dispatcher.DispatchCheck(request, ct).ConfigureAwait(false);

        var collapsed = engine.Collapse(branch, caveatContext);
        return new PermissionCheckResult(collapsed.Verdict, collapsed.MissingExprFields);
    }
}
