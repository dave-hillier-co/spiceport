using System.Collections.Immutable;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>Toggle for the Leopard <see cref="MembershipIndex"/> accelerator. Default ON (opt-out).</summary>
public sealed class MembershipIndexOptions
{
    /// <summary>When false the index is never built or consulted; lookups run the live traversal.</summary>
    public bool Enabled { get; init; } = true;
}

/// <summary>
/// A per-silo cache of the flattened <see cref="MembershipIndex"/>, keyed by the silo schema's hash and the
/// revision it was built at. It mirrors <see cref="SiloProjection"/>'s bootstrap-once-then-fold-the-tail
/// pattern over the SAME <see cref="IDatastoreGrain"/> log feed: rather than a full re-scan every time a
/// request needs a fresher revision than the cached build, it pulls the log tail
/// (<see cref="IDatastoreLog.ReadFrom"/>) and folds the deltas into the existing index via
/// <see cref="MembershipIndex.Apply"/>. A full rebuild happens only when the schema changed, there is no
/// index yet, or the log has been compacted past the cache's watermark (the tail is no longer available). The
/// index is built from the SAME namespaces the lookup engine's confirming Check uses (the silo's current
/// schema), so the candidates it seeds and the verdicts that confirm them are always schema-consistent.
/// </summary>
/// <remarks>
/// Freshness rule: <see cref="MembershipIndex.Apply"/> folds deletes, so — unlike <see cref="SiloProjection"/>,
/// which retains full MVCC history and filters by visibility at read time — an index built/caught-up to
/// revision <c>R</c> is NOT a safe candidate source for every revision <c>&lt;= R</c>: a subject removed by a
/// delete folded in between an earlier requested revision and <c>R</c> would be silently missing, even though
/// it was a legitimate member at the earlier revision. So reuse requires an EXACT match on the built
/// revision. A request BEHIND the cache is served by folding the log tail up to (and clamped at) exactly that
/// revision (see <see cref="CatchUp"/>); a request for a revision the shared cache has already advanced PAST
/// is served by an independent full scan over the caller's own reader, without perturbing the shared cache
/// (see <see cref="TryGet"/>).
/// <para>
/// Single-flight: a <see cref="SemaphoreSlim"/> guards build/catch-up so concurrent callers converge on one
/// in-flight build/fold instead of racing separate full scans, exactly as <see cref="SiloProjection"/> does
/// for the relationship-read projection.
/// </para>
/// When disabled, <see cref="TryGet"/> always returns null and never touches the grain; the caller runs the
/// unchanged live traversal.
/// </remarks>
public sealed class MembershipIndexCache
{
    /// <summary>Log-tail page size for catch-up pulls (mirrors <see cref="SiloProjection"/>).</summary>
    private const int BatchSize = 256;

    // Bundles the built index with the revision it reflects into ONE reference so a lock-free reader always
    // observes a matched pair. Two separately-updated plain fields (as this cache used before) let a reader
    // pair an advanced watermark with a not-yet-visible (or, symmetrically, a stale) index; publishing them
    // together as a single immutable object, assigned via one `volatile` reference write/read, rules that out
    // — a reader either sees the old pair or the fully-formed new one, never a torn mix (mirrors the intent of
    // SiloProjection's `volatile _memoryState` + `Interlocked` watermark, but as one atomic unit here since the
    // two values must always be read together).
    private sealed record IndexSnapshot(MembershipIndex Index, long BuiltRevision);

    private readonly bool _enabled;
    private readonly IDatastoreGrain _grain;
    private readonly SemaphoreSlim _gate = new(1, 1);

    private volatile IndexSnapshot? _snapshot;

    /// <summary>Creates the cache. <paramref name="grainFactory"/> resolves the singleton log lazily (a grain reference costs nothing to hold).</summary>
    public MembershipIndexCache(MembershipIndexOptions options, IGrainFactory grainFactory)
        : this(options, RequireGrainFactory(grainFactory).GetGrain<IDatastoreGrain>(IDatastoreGrain.Key))
    {
    }

    /// <summary>Test seam: builds the cache directly over a hand-rolled <see cref="IDatastoreGrain"/> (no Orleans).</summary>
    internal MembershipIndexCache(MembershipIndexOptions options, IDatastoreGrain grain)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(grain);
        _enabled = options.Enabled;
        _grain = grain;
    }

    private static IGrainFactory RequireGrainFactory(IGrainFactory grainFactory)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        return grainFactory;
    }

    /// <summary>
    /// Returns a schema-consistent index usable to seed candidates for a request at <paramref name="revision"/>,
    /// or null when the accelerator is disabled (the caller then runs the live traversal). Builds the index (a
    /// full scan) on schema change / first use, or catches it up incrementally by folding the log tail when the
    /// schema is unchanged but a fresher revision is needed.
    /// </summary>
    public async Task<MembershipIndex?> TryGet(
        ImmutableList<NamespaceDefinition> namespaces,
        ImmutableList<CaveatDefinition> caveats,
        IDatastoreReader reader,
        long revision,
        CancellationToken cancellationToken = default)
    {
        if (!_enabled)
            return null;

        var hash = SchemaHash.Compute(namespaces, caveats);

        // Fast path (no await): one volatile read of the (index, revision) pair, so a concurrent writer can
        // never be observed half-published. Reuse requires an EXACT revision match — see the freshness rule
        // in the class remarks for why "built revision >= requested revision" is unsafe once deletes fold.
        if (_snapshot is { } fast && fast.Index.SchemaHash == hash && fast.BuiltRevision == revision)
            return fast.Index;

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Re-check after acquiring the gate: another caller may have already done the work.
            var snapshot = _snapshot;
            if (snapshot is { } cached && cached.Index.SchemaHash == hash && cached.BuiltRevision == revision)
                return cached.Index;

            if (snapshot is null || snapshot.Index.SchemaHash != hash)
            {
                // No index yet, or the schema moved: only a full scan reflects the new keyspace correctly.
                var built = await MembershipIndex.Build(namespaces, reader, hash, cancellationToken)
                    .ConfigureAwait(false);
                _snapshot = new IndexSnapshot(built, revision);
                return built;
            }

            if (snapshot.BuiltRevision < revision)
            {
                // Behind: fold the log tail forward (fold-equivalent to a fresh build at `revision`,
                // clamped there — see CatchUp).
                return await CatchUp(namespaces, reader, hash, revision, cancellationToken)
                    .ConfigureAwait(false);
            }

            // Ahead (snapshot.BuiltRevision > revision): the shared cache already folded past this request,
            // and that fold may have applied a delete that removed a subject who was still a legitimate
            // member at `revision`. Serve this one caller from an independent full scan over THEIR OWN
            // reader (pinned exactly at `revision`) instead of the shared, more-advanced index — and do not
            // overwrite the shared cache with it (that would regress it for callers needing the newer
            // revision).
            return await MembershipIndex.Build(namespaces, reader, hash, cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            _gate.Release();
        }
    }

    // Advances the cached index by pulling and folding the log tail from the current snapshot's watermark,
    // mirroring SiloProjection.PullOnce/StateAtLeast — but, unlike SiloProjection, CLAMPED exactly at
    // `revision`: only events with Revision <= revision are folded, and the published watermark never
    // exceeds `revision`, even when a fetched page reaches further. Folding (or publishing a watermark past)
    // anything beyond `revision` would risk applying a delete that removes a subject who was still a
    // legitimate member AT `revision` — the exact hazard the class remarks describe. A later caller that
    // needs the events this page carries past `revision` simply resumes the fold from `revision` on its own
    // call. Falls back to a full rebuild (via the passed reader, pinned at `revision`) if a schema change is
    // observed at/before `revision` or the tail is no longer available (compacted below our watermark).
    // Caller holds _gate.
    private async Task<MembershipIndex> CatchUp(
        ImmutableList<NamespaceDefinition> namespaces,
        IDatastoreReader reader,
        string hash,
        long revision,
        CancellationToken cancellationToken)
    {
        var snapshot = _snapshot!;
        var index = snapshot.Index;
        var watermark = snapshot.BuiltRevision;

        while (watermark < revision)
        {
            LogSegment segment;
            try
            {
                segment = await _grain.ReadFrom(watermark, BatchSize).ConfigureAwait(false);
            }
            catch (RevisionNotFoundException)
            {
                // Fell below the grain's retained log window: the fold cannot be resumed from here, so
                // rebuild fresh at the request revision (the same recovery SiloProjection performs).
                return await RebuildAndStore(namespaces, reader, hash, revision, cancellationToken)
                    .ConfigureAwait(false);
            }

            // Only events at or before the requested revision are safe to fold in for THIS call.
            var relevant = segment.Events.Where(e => e.Revision <= revision).ToList();

            if (relevant.Any(e => e.SchemaChange is not null))
            {
                // A schema change invalidates the fold no matter how the hash compares afterward: even an
                // A->B->A round-trip could change which base relations are scanned/yielded in between, and
                // this cache only ever tracks one hash's worth of history — conservative-but-correct.
                return await RebuildAndStore(namespaces, reader, hash, revision, cancellationToken)
                    .ConfigureAwait(false);
            }

            var updates = relevant
                .SelectMany(e => e.RelationshipChanges)
                .Select(WireConvert.ToUpdate)
                .ToList();
            if (updates.Count > 0)
                index = index.Apply(updates);

            if (relevant.Count > 0)
                watermark = relevant[^1].Revision;

            if (segment.Events.Count > 0 && segment.Events[^1].Revision >= revision)
            {
                // The fetched page already carries everything up to `revision` (anything past it is left
                // unfolded — see above); clamp the published watermark exactly there.
                watermark = revision;
                break;
            }

            if (segment.Events.Count < BatchSize)
            {
                // Short page: drained to the actual log head. Normally that head is >= the request
                // revision (the caller resolved `revision` from the datastore head), so clamping to
                // `revision` is exact; defensively cap at the observed head too in case that invariant
                // is ever violated, so we never publish a watermark we have not actually folded up to.
                watermark = Math.Min(revision, Math.Max(watermark, segment.HeadRevision));
                break;
            }
        }

        _snapshot = new IndexSnapshot(index, watermark);
        return index;
    }

    private async Task<MembershipIndex> RebuildAndStore(
        ImmutableList<NamespaceDefinition> namespaces,
        IDatastoreReader reader,
        string hash,
        long revision,
        CancellationToken cancellationToken)
    {
        var built = await MembershipIndex.Build(namespaces, reader, hash, cancellationToken).ConfigureAwait(false);
        _snapshot = new IndexSnapshot(built, revision);
        return built;
    }
}
