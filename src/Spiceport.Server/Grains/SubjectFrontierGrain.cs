using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// A grain keyed by "the pre-context subject frontier of resource#relation for subjectType(#subjectRelation)
/// at (quantizedRevision, schemaHash)". It runs a whole <see cref="LookupSubjectsEngine.LookupSubjects"/>
/// walk from its key's root, in-process, behind a single grain call, and memoizes the materialized result
/// in activation state — the LookupSubjects analogue of <see cref="CheckGrain"/>'s reply memo (stage (a)
/// of "Activation-as-cache", <c>docs/future-work.md</c> item 1.3).
/// </summary>
/// <remarks>
/// UNLIKE <see cref="CheckGrain"/>, this grain is deliberately NOT <see cref="Orleans.Concurrency.ReentrantAttribute"/>-marked.
/// <see cref="CheckGrain"/> must be reentrant because it dispatches sub-problems back out to OTHER
/// <see cref="ICheckGrain"/> activations, and a genuine relation cycle can re-enter the very activation
/// that started the recursion — without reentrancy that would deadlock. This grain has no dispatcher seam
/// at all: <see cref="LookupSubjectsEngine"/> is consumed unchanged and walks the whole sub-graph
/// in-process from this activation, with no cross-grain recursion to re-enter. Default (non-reentrant)
/// Orleans turn-based execution therefore gives this grain single-flight for free — two concurrent calls
/// to the same activation simply queue, and the second is served by the memo the first one just populated,
/// rather than recomputing independently.
/// <para>
/// The memoized value is the PRE-CONTEXT frontier (verbatim caveat expressions, exactly as the engine
/// yields them), never a collapsed verdict — caveat context is applied per-request at the caller
/// (<see cref="ReverseOps.StreamLookupSubjects"/>), matching <see cref="CheckGrain"/>'s memo
/// contract.
/// </para>
/// <para>
/// There is no depth guard / cycle-cut analogue here (unlike <see cref="CheckGrain"/>'s exact visited set
/// and <c>DepthRequired</c> gate): this grain always runs the WHOLE walk from its key's root in one call,
/// and the walk's own depth limit and visited-set cycle guard are entirely internal to
/// <see cref="LookupSubjectsEngine"/> — the result is a pure function of the key alone, so there is no
/// caller-supplied budget varying the completeness of what gets cached, and hence nothing here to guard.
/// </para>
/// </remarks>
public sealed class SubjectFrontierGrain(
    IDatastore datastore,
    ISchemaProvider schemaProvider,
    SchemaResolver schemaResolver,
    IGraphReaderSource readerSource,
    SubjectFrontierMemoOptions? memoOptions = null,
    IDispatchMetrics? metrics = null) : Grain, ISubjectFrontierGrain
{
    private readonly SubjectFrontierMemoOptions _memoOptions = memoOptions ?? new SubjectFrontierMemoOptions();

    /// <summary>The memoized frontier computed on this activation so far, or null before the first compute.</summary>
    private SubjectFrontierReply? _memo;

    /// <inheritdoc />
    public async Task<SubjectFrontierReply> GetFrontier(CancellationToken cancellationToken)
    {
        if (_memoOptions.Enabled && _memo is { } cached)
        {
            metrics?.RecordFrontierMemoHit();
            return cached;
        }

        if (_memoOptions.Enabled)
            metrics?.RecordFrontierMemoMiss();

        var parts = SubjectFrontierKey.Parse(this.GetPrimaryKeyString());
        var revision = RevisionCodec.Parse(parts.Revision);

        // 'now' is captured once per compute, not once per activation, exactly matching CheckGrain: the
        // memo's staleness class is bounded by the activation's idle-collection age within one
        // quantized-revision keyspace.
        var now = DateTimeOffset.UtcNow;

        var reader = datastore.SnapshotReader(revision);
        var schema = await schemaResolver.ResolveAsync(
            parts.SchemaHash,
            reader,
            schemaProvider.Current,
            cancellationToken);

        // The engine walk reads through the IGraphReaderSource seam (projection or shard mesh, per
        // GraphReaderOptions) at the same pinned revision; schema resolution above keeps the
        // projection-pinned reader — see IGraphReaderSource.
        var engine = new LookupSubjectsEngine(schema.Namespaces);
        var subjects = new List<FrontierSubjectWire>();
        await foreach (var found in engine.LookupSubjects(
            readerSource.GraphReaderAt(revision), parts.Resource, parts.SubjectType, parts.SubjectRelation, now,
            cancellationToken))
        {
            subjects.Add(FrontierWire.ToWire(found));
        }

        var reply = new SubjectFrontierReply(subjects);

        // Over-cap results are served but not retained: the caller above always gets the freshly computed
        // reply regardless of this check.
        if (_memoOptions.Enabled && subjects.Count <= _memoOptions.MaxMemoSubjects)
            _memo = reply;

        return reply;
    }
}
