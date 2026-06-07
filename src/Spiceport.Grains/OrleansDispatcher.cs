using System.Collections.Immutable;
using Orleans.Runtime;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;
using Spiceport.Schema;

namespace Spiceport.Grains;

/// <summary>
/// An <see cref="IDispatcher"/> that turns each sub-problem into a grain call: it derives the
/// canonical grain key from the request, resolves the keyed <see cref="ICheckGrain"/> via
/// <see cref="IGrainFactory"/>, and invokes <see cref="ICheckGrain.DispatchCheck"/>.
/// </summary>
/// <remarks>
/// <para>
/// This is how recursion crosses grain boundaries: a grain computing one sub-problem dispatches each
/// of its children through an <see cref="OrleansDispatcher"/>, so every child becomes a (potentially
/// remote) call to a different grain keyed by that child's identity. The dispatcher maps the engine's
/// <see cref="DispatchCheckRequest"/> / <see cref="DispatchCheckResult"/> to and from the serializable
/// grain <see cref="DispatchCheckArgs"/> / <see cref="DispatchCheckReply"/>.
/// </para>
/// <para>
/// HYBRID local-recurse-vs-grain-hop (when an <see cref="ISiloOwnership"/> and the local-recurse
/// dependencies are supplied and <see cref="OrleansDispatcherOptions.LocalRecurseEnabled"/> is on): the
/// dispatcher computes the sub-problem's consistent-hash owner without sending a message. If the owner
/// is a REMOTE silo it makes the grain call as before. If the owner is the LOCAL silo it runs the one
/// expansion step IN-PROCESS via a <see cref="LocalDispatcher"/> whose onward dispatcher is the same
/// silo-wide caching dispatcher the grain would have used — so the branch cache is consulted and
/// populated EXACTLY as on the grain path (no cache bypass, dedup preserved), but with no cross-silo
/// message and no grain-activation/scheduler overhead. Because consistent-hash placement guarantees a
/// grain activates on its hash owner, this is the cheapest correct path for a locally-owned key.
/// </para>
/// </remarks>
public sealed class OrleansDispatcher : IDispatcher
{
    private readonly IGrainFactory _grains;
    private readonly ISchemaHashSource _schemaHash;
    private readonly ISiloOwnership? _ownership;
    private readonly OrleansDispatcherOptions _options;
    private readonly IDispatchMetrics? _metrics;
    private readonly LocalRecurseContext? _localRecurse;

    /// <summary>Creates an Orleans dispatcher.</summary>
    /// <param name="grains">The grain factory used to resolve keyed check grains.</param>
    /// <param name="schemaHash">Supplies the live schema hash embedded in every grain key (scopes identity to the current schema).</param>
    /// <param name="ownership">
    /// Optional consistent-hash ownership oracle. When supplied (with <paramref name="localRecurse"/>),
    /// the dispatcher can compute which silo a sub-problem's grain would activate on, using the SAME
    /// membership view and hash ring as the placement director, and take the in-process local-recurse
    /// shortcut for locally-owned sub-problems.
    /// </param>
    /// <param name="options">Hybrid toggle and tunables; defaults to local-recurse ON.</param>
    /// <param name="metrics">Optional silo-wide hop counters for benchmarking.</param>
    /// <param name="localRecurse">
    /// Optional in-process recursion context (schema + datastore + onward silo dispatcher). Required for
    /// the local-recurse shortcut; when absent, the dispatcher always makes the grain call.
    /// </param>
    public OrleansDispatcher(
        IGrainFactory grains,
        ISchemaHashSource schemaHash,
        ISiloOwnership? ownership = null,
        OrleansDispatcherOptions? options = null,
        IDispatchMetrics? metrics = null,
        LocalRecurseContext? localRecurse = null)
    {
        ArgumentNullException.ThrowIfNull(grains);
        ArgumentNullException.ThrowIfNull(schemaHash);
        _grains = grains;
        _schemaHash = schemaHash;
        _ownership = ownership;
        _options = options ?? new OrleansDispatcherOptions();
        _metrics = metrics;
        _localRecurse = localRecurse;
    }

    /// <summary>
    /// The canonical grain key for a sub-problem, identical to the key used to address its
    /// <see cref="ICheckGrain"/>. Exposed so callers can ask the ownership oracle where it would land.
    /// </summary>
    public string KeyFor(ObjectAndRelation resource, ObjectAndRelation subject, string revision, RevisionMode mode) =>
        GrainKey.Build(resource, subject, revision, _schemaHash.CurrentSchemaHash, mode);

    /// <summary>
    /// The silo that the sub-problem identified by <paramref name="request"/> would activate on under
    /// consistent-hash placement, computed from the same membership view as the placement director —
    /// without activating the grain. Requires an <see cref="ISiloOwnership"/> to have been supplied.
    /// </summary>
    public SiloAddress OwnerOf(DispatchCheckRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (_ownership is null)
            throw new InvalidOperationException(
                "OrleansDispatcher was constructed without an ISiloOwnership; owner computation is unavailable.");

        var key = KeyFor(
            request.Resource, request.Subject, request.Meta.Revision.ToString(), request.Meta.Mode);
        return _ownership.OwnerOf(key);
    }

    /// <inheritdoc/>
    public async Task<DispatchCheckResult> DispatchCheck(DispatchCheckRequest request, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(request);
        ct.ThrowIfCancellationRequested();

        var key = GrainKey.Build(
            request.Resource,
            request.Subject,
            request.Meta.Revision.ToString(),
            _schemaHash.CurrentSchemaHash,
            request.Meta.Mode);

        // HYBRID: if local-recurse is enabled and we can compute ownership, take the in-process path for
        // a locally-owned sub-problem (no cross-silo message), else fall through to the grain call.
        if (_options.LocalRecurseEnabled
            && _ownership is not null
            && _localRecurse is not null
            && _ownership.IsLocal(key))
        {
            _metrics?.RecordLocalRecurse();
            return await LocalStep(request, ct).ConfigureAwait(false);
        }

        _metrics?.RecordRemoteGrainHop();

        var grain = _grains.GetGrain<ICheckGrain>(key);

        var args = new DispatchCheckArgs(
            request.Meta.DepthRemaining,
            request.Meta.Bloom.ToBytes(),
            request.Meta.Bloom.Hashes,
            request.Meta.Mode);

        var reply = await grain.DispatchCheck(args).ConfigureAwait(false);

        return new DispatchCheckResult(
            reply.Member, CaveatWire.FromWire(reply.Caveat), reply.CycleCut, reply.DepthRequired);
    }

    /// <summary>
    /// Runs ONE expansion step in-process for a locally-owned sub-problem, routing children back through
    /// the silo-wide caching dispatcher — identical onward wiring to <c>CheckGrain.DispatchCheck</c>, so
    /// the shared branch cache is consulted/populated exactly as on the grain path (no bypass).
    /// </summary>
    private Task<DispatchCheckResult> LocalStep(DispatchCheckRequest request, CancellationToken ct)
    {
        var ctx = _localRecurse!;
        var namespaces = ctx.SchemaProvider.Current.Namespaces.ToImmutableDictionary(ns => ns.Name);
        var local = new LocalDispatcher(
            namespaces,
            ctx.Datastore.SnapshotReader,
            DateTimeOffset.UtcNow,
            new CheckState())
        {
            // Children flow back through the silo-wide Caching-over-Orleans dispatcher (the same onward
            // path a grain uses), so non-local children still cross grain boundaries and the cache is shared.
            Dispatcher = ctx.Onward.Dispatcher,
        };

        return local.DispatchCheck(request, ct);
    }
}

/// <summary>
/// The in-process recursion dependencies the <see cref="OrleansDispatcher"/> needs to run a locally-owned
/// sub-problem's one expansion step without a grain hop: the live schema, the datastore (for a snapshot
/// reader) and the silo-wide onward dispatcher children route back through.
/// </summary>
/// <param name="SchemaProvider">The live schema provider (current namespaces per request).</param>
/// <param name="Datastore">The datastore singleton (resolves a snapshot reader at the request revision).</param>
/// <param name="Onward">The silo-wide caching-over-Orleans dispatcher children recurse through.</param>
public sealed record LocalRecurseContext(
    ISchemaProvider SchemaProvider,
    IDatastore Datastore,
    ISiloDispatcher Onward);
