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
/// <param name="EvaluatedRevision">The revision the check actually evaluated against.</param>
/// <param name="SchemaHash">The schema hash at the evaluated revision, if any.</param>
/// <param name="EvaluatedToken">
/// The ZedToken minted from <see cref="EvaluatedRevision"/> (with <see cref="SchemaHash"/> and the
/// datastore id), which a client can chain into a subsequent <c>at_least_as_fresh</c> check.
/// </param>
public sealed record PermissionCheckResult(
    Membership Verdict,
    IReadOnlyList<string> MissingFields,
    IRevision EvaluatedRevision,
    string? SchemaHash,
    string EvaluatedToken);

/// <summary>
/// One item of a <see cref="IPermissionChecker.BatchCheck"/> request: a single resource/permission/subject
/// triple with its OWN caveat context. There is deliberately NO per-item consistency — the whole batch
/// pins one revision (mirroring SpiceDB's <c>CheckBulkPermissions</c>, which resolves the revision once).
/// </summary>
/// <param name="ResourceType">The resource namespace.</param>
/// <param name="ResourceId">The resource object id.</param>
/// <param name="Permission">The permission (or relation) to check.</param>
/// <param name="Subject">The subject ONR (ellipsis subrelation for a direct subject).</param>
/// <param name="CaveatContext">The per-item request-time caveat context, or null.</param>
[GenerateSerializer]
public sealed record BatchCheckItem(
    [property: Id(0)] string ResourceType,
    [property: Id(1)] string ResourceId,
    [property: Id(2)] string Permission,
    [property: Id(3)] ObjectAndRelation Subject,
    [property: Id(4)] IReadOnlyDictionary<string, object?>? CaveatContext);

/// <summary>
/// The per-item verdict of a batch check, index-aligned to the request items. The evaluated
/// revision/token live ONCE on <see cref="BatchCheckResult"/>, not per item.
/// </summary>
/// <param name="Verdict">The membership verdict for this item.</param>
/// <param name="MissingFields">
/// When <paramref name="Verdict"/> is <see cref="Membership.Caveated"/>, the caveat parameter names that
/// were missing from this item's context; otherwise empty.
/// </param>
[GenerateSerializer]
public sealed record PermissionCheckResultItem(
    [property: Id(0)] Membership Verdict,
    [property: Id(1)] IReadOnlyList<string> MissingFields);

/// <summary>
/// The result of a batch check: per-item verdicts in request order, plus the SINGLE revision/schema-hash/
/// token the whole batch was evaluated against (every item shares it, proving the one-revision pin).
/// </summary>
/// <param name="Items">Per-item verdicts, index-aligned to the request items.</param>
/// <param name="EvaluatedRevision">The single revision the whole batch evaluated against.</param>
/// <param name="SchemaHash">The schema hash at the evaluated revision, if any.</param>
/// <param name="EvaluatedToken">The single ZedToken minted from <paramref name="EvaluatedRevision"/>.</param>
[GenerateSerializer]
public sealed record BatchCheckResult(
    [property: Id(0)] IReadOnlyList<PermissionCheckResultItem> Items,
    [property: Id(1)] IRevision EvaluatedRevision,
    [property: Id(2)] string? SchemaHash,
    [property: Id(3)] string EvaluatedToken);

/// <summary>
/// The top-level entry point used by the API: pins an optimized (quantized) revision, dispatches the
/// root sub-problem through the silo-wide Caching-over-Orleans dispatcher, then collapses the returned
/// pre-context branch against the request-time caveat context.
/// </summary>
public interface IPermissionChecker
{
    /// <summary>Checks whether the subject has the given permission on the resource.</summary>
    /// <param name="consistency">
    /// The consistency the read demands; null means <see cref="ConsistencyRequirement.MinimizeLatency"/>
    /// (the server default), so existing callers behave exactly as before.
    /// </param>
    Task<PermissionCheckResult> Check(
        string resourceType,
        string resourceId,
        string permission,
        ObjectAndRelation subject,
        IReadOnlyDictionary<string, object?>? caveatContext,
        ConsistencyRequirement? consistency = null,
        CancellationToken ct = default);

    /// <summary>
    /// Checks a batch of items against ONE pinned revision, fanning each item's root sub-problem out
    /// over the shared silo-wide dispatcher with bounded concurrency, then collapsing each against its
    /// own per-item caveat context. Returns per-item verdicts (index-aligned to <paramref name="items"/>)
    /// plus the single evaluated revision/token the whole batch shares.
    /// </summary>
    /// <param name="items">The batch items, each with its own caveat context (no per-item consistency).</param>
    /// <param name="consistency">
    /// The single consistency the whole batch demands; null means
    /// <see cref="ConsistencyRequirement.MinimizeLatency"/>.
    /// </param>
    /// <param name="ct">A cancellation token.</param>
    Task<BatchCheckResult> BatchCheck(
        IReadOnlyList<BatchCheckItem> items,
        ConsistencyRequirement? consistency = null,
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
    int maxDepth = CheckEngine.DefaultMaxDepth,
    int maxConcurrency = PermissionChecker.DefaultBatchConcurrency) : IPermissionChecker
{
    /// <summary>
    /// The default bounded fan-out width for <see cref="BatchCheck"/>, mirroring SpiceDB's bulk-check
    /// <c>maxConcurrency</c> (see <c>internal/services/v1/bulkcheck.go</c>).
    /// </summary>
    public const int DefaultBatchConcurrency = 50;
    /// <inheritdoc />
    public async Task<PermissionCheckResult> Check(
        string resourceType,
        string resourceId,
        string permission,
        ObjectAndRelation subject,
        IReadOnlyDictionary<string, object?>? caveatContext,
        ConsistencyRequirement? consistency = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(subject);

        // Resolve the consistency requirement to a concrete revision + cache mode. Default (null) is
        // MinimizeLatency → the optimized (quantized, head-pinned) revision, identical to the prior
        // behaviour, so existing tests are unchanged.
        var resolved = await RevisionResolver
            .Resolve(datastore, consistency ?? ConsistencyRequirement.MinimizeLatency, cancellationToken: ct)
            .ConfigureAwait(false);

        // Capture a single consistent schema snapshot for this check, so the collapse uses the same
        // caveats the dispatch ran under. Collapse is cheap and stateless, so building a per-call
        // engine over the live schema is sufficient (rebuild-on-version-change is a perf follow-up).
        var schema = schemaProvider.Current;
        var engine = new CheckEngine(schema.Namespaces, schema.Caveats, maxDepth);

        var resource = new ObjectAndRelation(resourceType, resourceId, permission);
        var meta = new ResolverMeta(
            resolved.Revision, maxDepth, ImmutableHashSet<VisitKey>.Empty, resolved.Mode);
        var request = new DispatchCheckRequest(resource, subject, meta);

        var branch = await root.Dispatcher.DispatchCheck(request, ct).ConfigureAwait(false);

        var collapsed = engine.Collapse(branch, caveatContext);
        var datastoreId = await datastore.GetUniqueId(ct).ConfigureAwait(false);
        var token = ZedTokens.FromRevision(resolved.Revision, resolved.SchemaHash, datastoreId).Token;
        return new PermissionCheckResult(
            collapsed.Verdict, collapsed.MissingExprFields, resolved.Revision, resolved.SchemaHash, token);
    }

    /// <inheritdoc />
    public async Task<BatchCheckResult> BatchCheck(
        IReadOnlyList<BatchCheckItem> items,
        ConsistencyRequirement? consistency = null,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(items);

        // ONE revision for the whole batch (mirrors SpiceDB's single RevisionFromContext). Every item's
        // ResolverMeta carries the same revision + mode, so structurally-identical sub-problems across
        // items collide on the same shared cache key and the same grain activation.
        var resolved = await RevisionResolver
            .Resolve(datastore, consistency ?? ConsistencyRequirement.MinimizeLatency, cancellationToken: ct)
            .ConfigureAwait(false);

        // ONE schema snapshot and ONE collapse engine for the whole batch, so every item collapses
        // against the same caveats the dispatch ran under.
        var schema = schemaProvider.Current;
        var engine = new CheckEngine(schema.Namespaces, schema.Caveats, maxDepth);

        var meta = new ResolverMeta(
            resolved.Revision, maxDepth, ImmutableHashSet<VisitKey>.Empty, resolved.Mode);

        // Dedup distinct sub-problems by their dispatch key (resource + subject; caveat context is
        // excluded from the dispatch key and applied per-item at collapse). Each distinct sub-problem is
        // dispatched ONCE and its pre-context branch mapped back to every input index that requested it.
        var distinct = new Dictionary<(string, string, string, string, string, string), List<int>>();
        for (var i = 0; i < items.Count; i++)
        {
            var it = items[i] ?? throw new ArgumentException("Batch item must not be null.", nameof(items));
            ArgumentNullException.ThrowIfNull(it.Subject);
            var key = (it.ResourceType, it.ResourceId, it.Permission,
                it.Subject.ObjectType, it.Subject.ObjectId, it.Subject.Relation);
            if (!distinct.TryGetValue(key, out var idxs))
            {
                idxs = [];
                distinct[key] = idxs;
            }
            idxs.Add(i);
        }

        // Bounded fan-out over the distinct sub-problems through the shared root dispatcher.
        var distinctList = distinct.Values.ToList();
        var branches = new DispatchCheckResult[distinctList.Count];
        using var gate = new SemaphoreSlim(Math.Max(1, maxConcurrency));
        await Task.WhenAll(distinctList.Select(async (indices, slot) =>
        {
            await gate.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var sample = items[indices[0]];
                var resource = new ObjectAndRelation(sample.ResourceType, sample.ResourceId, sample.Permission);
                var request = new DispatchCheckRequest(resource, sample.Subject, meta);
                branches[slot] = await root.Dispatcher.DispatchCheck(request, ct).ConfigureAwait(false);
            }
            finally
            {
                gate.Release();
            }
        })).ConfigureAwait(false);

        // Collapse each item against its OWN caveat context (a shared branch can collapse differently per
        // item), assembling results in request order.
        var resultItems = new PermissionCheckResultItem[items.Count];
        for (var slot = 0; slot < distinctList.Count; slot++)
        {
            var branch = branches[slot];
            foreach (var i in distinctList[slot])
            {
                var collapsed = engine.Collapse(branch, items[i].CaveatContext);
                resultItems[i] = new PermissionCheckResultItem(collapsed.Verdict, collapsed.MissingExprFields);
            }
        }

        var datastoreId = await datastore.GetUniqueId(ct).ConfigureAwait(false);
        var token = ZedTokens.FromRevision(resolved.Revision, resolved.SchemaHash, datastoreId).Token;
        return new BatchCheckResult(resultItems, resolved.Revision, resolved.SchemaHash, token);
    }
}
