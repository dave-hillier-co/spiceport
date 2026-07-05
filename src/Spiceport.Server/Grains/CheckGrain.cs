using System.Collections.Immutable;
using Orleans.Concurrency;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// A grain keyed by a single canonical sub-problem. It computes exactly ONE expansion step of that
/// sub-problem and dispatches every deeper sub-problem back out through the (Orleans) dispatcher, so
/// recursion crosses grain boundaries and the mesh is real.
/// </summary>
/// <remarks>
/// On <see cref="DispatchCheck"/> the grain decodes its identity from its string key (resource,
/// subject, revision, schema hash), resolves a snapshot reader at that revision from the injected
/// <see cref="IDatastore"/> singleton, and runs a <see cref="LocalDispatcher"/> whose onward
/// <see cref="LocalDispatcher.Dispatcher"/> is the silo-wide onward dispatcher
/// (<see cref="ISiloDispatcher"/> = Caching over Orleans). The local dispatcher performs the one step;
/// children flow through Orleans as further grain calls.
/// <para>
/// CACHING (stage (a) of "Activation-as-cache", <c>docs/future-work.md</c> item 1.3): the grain holds a
/// single memoized reply in <see cref="_memo"/> — the PRE-CONTEXT branch (membership + caveat wire),
/// never the collapsed verdict, mirroring exactly what <see cref="CachingDispatcher"/> caches one layer
/// up. On entry, when <see cref="ActivationMemoOptions.Enabled"/> and a memo exists whose
/// <see cref="DispatchCheckReply.DepthRequired"/> is at most the caller's remaining depth budget, it is
/// returned directly with no re-expansion of the relation graph (the same depth guard
/// <c>CachingDispatcher</c> uses: <c>DepthRemaining &gt;= DepthRequired</c>, else fall through and
/// recompute under the tighter budget). A freshly computed reply is stored ONLY when it is not
/// <see cref="DispatchCheckReply.CycleCut"/> (a cycle-cut result depends on the in-flight visited-set,
/// which is not part of this grain's identity, so caching it would be unsound on another path) and only
/// when it is at least as servable as any existing memo (a strictly lower <c>DepthRequired</c> replaces
/// it, so the activation keeps the most-reusable entry it has ever computed). The grain identity already
/// embeds the quantized revision and schema hash, so the keyspace rotates on its own every quantization
/// window; the activation's own idle-collection age (<see cref="ActivationMemoOptions.CollectionAge"/>,
/// applied via <see cref="SiloBuilderExtensions.AddActivationMemoCollectionAge"/>) IS the memo's eviction
/// policy, so no separate TTL bookkeeping is needed. This is therefore a bounded-staleness cache in the
/// same sense the branch cache's revision quantization already is: a memo entry can serve requests for up
/// to one collection-age window within one quantized-revision keyspace.
/// </para>
/// <para>
/// This memo is purely additive: the silo-wide <see cref="CachingDispatcher"/> remains the first-line
/// cache (consulted before a call ever reaches this grain, on the caller's silo), and every other seam —
/// the hybrid local-recurse shortcut, the Orleans dispatcher, the traversal-bloom cycle guard — is
/// unchanged. Disabling <see cref="ActivationMemoOptions.Enabled"/> reverts this grain to exactly its
/// pre-memo behaviour.
/// </para>
/// <para>
/// NOT a singleflight cache: concurrent calls never await one another's in-flight <see cref="Task"/>.
/// The grain is <see cref="ReentrantAttribute"/> precisely so that a same-key re-entry on a genuine cycle
/// is accepted rather than blocked; if a re-entrant call instead awaited a shared in-flight Task for the
/// same key, a same-key cyclic re-entry would deadlock against itself. Letting concurrent duplicate calls
/// recompute independently is benign — both read the same pinned snapshot and schema, so they produce the
/// identical result — and is exactly the miss behaviour <see cref="CachingDispatcher"/> already accepts
/// one layer up.
/// </para>
/// </remarks>
[ConsistentHashPlacement]
[Reentrant] // Accepts a same-key re-entry (a genuine cycle) rather than blocking it. The memo field below
            // is per-activation mutable state, but it is read-then-written with plain (non-atomic)
            // assignments deliberately: Orleans single-threads turn-based execution even for a reentrant
            // grain (interleaving only happens at await points), so there is no torn read/write here — the
            // only concurrency hazard reentrancy introduces is the singleflight deadlock this class avoids
            // by never sharing an in-flight Task (see the CACHING remarks above).
public sealed class CheckGrain(
    IDatastore datastore,
    ISchemaProvider schemaProvider,
    ISiloDispatcher onward,
    Orleans.Runtime.ILocalSiloDetails localSiloDetails,
    ActivationMemoOptions? memoOptions = null,
    IDispatchMetrics? metrics = null) : Grain, ICheckGrain
{
    private readonly ActivationMemoOptions _memoOptions = memoOptions ?? new ActivationMemoOptions();

    /// <summary>
    /// The most-servable pre-context reply computed on this activation so far (or null before the first
    /// compute, or if the memo is disabled). See the CACHING remarks on this class.
    /// </summary>
    private DispatchCheckReply? _memo;

    /// <inheritdoc />
    public Task<string> GetHostSilo() =>
        Task.FromResult(localSiloDetails.SiloAddress.ToParsableString());

    /// <inheritdoc />
    public async Task<DispatchCheckReply> DispatchCheck(
        DispatchCheckArgs args,
        GrainCancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(args);

        if (_memoOptions.Enabled
            && _memo is { } cached
            && args.DepthRemaining >= cached.DepthRequired)
        {
            metrics?.RecordMemoHit();
            return cached;
        }

        if (_memoOptions.Enabled)
            metrics?.RecordMemoMiss();

        var parts = GrainKey.Parse(this.GetPrimaryKeyString());
        var revision = RevisionCodec.Parse(parts.Revision);

        // 'now' is captured once per compute (not once per activation): the memo's staleness class is
        // bounded by the activation's idle-collection age within one quantized-revision keyspace, the
        // same staleness the branch cache's revision quantization window already accepts.
        var now = DateTimeOffset.UtcNow;

        // A LocalDispatcher does ONE expansion step; its onward Dispatcher (the silo-wide
        // Caching-over-Orleans dispatcher) turns each child sub-problem into a further grain call.
        var namespaces = schemaProvider.Current.Namespaces.ToImmutableDictionary(ns => ns.Name);
        var state = new CheckState();
        var local = new LocalDispatcher(
            namespaces,
            datastore.SnapshotReader,
            now,
            state)
        {
            Dispatcher = onward.Dispatcher,
        };

        var meta = new ResolverMeta(
            revision,
            args.DepthRemaining,
            TraversalBloom.FromBytes(args.BloomBits, args.BloomK),
            parts.Mode);
        var request = new DispatchCheckRequest(parts.Resource, parts.Subject, meta);

        var result = await local.DispatchCheck(request, cancellationToken.CancellationToken);

        var reply = new DispatchCheckReply(
            result.Member, CaveatWire.ToWire(result.Caveat), result.CycleCut, result.DepthRequired);

        // Never memoize a cycle-cut result (path-dependent on the in-flight visited-set, which is
        // excluded from this grain's identity); of the remaining candidates, keep the one that requires
        // the least depth, so the activation always holds its most-servable entry.
        if (_memoOptions.Enabled
            && !result.CycleCut
            && (_memo is not { } existing || reply.DepthRequired < existing.DepthRequired))
            _memo = reply;

        return reply;
    }
}
