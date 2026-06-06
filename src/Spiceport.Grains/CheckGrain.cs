using Orleans.Concurrency;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// Stateless-worker grain that answers permission checks by wrapping <see cref="CheckEngine"/>.
/// </summary>
/// <remarks>
/// Phase 2 topology: ONE stateless-worker grain (integer key 0) fronting a shared,
/// immutable <see cref="CheckEngine"/> singleton. Concurrency comes from
/// <see cref="StatelessWorkerAttribute"/> activating multiple instances on demand.
/// <para>
/// Next optimization (docs/architecture-analysis.md §3.3 option A): grain-per-cache-key,
/// keyed by (resource-type, resource-id, relation, subject, revision, schema-hash), so the
/// grain identity itself caches a verdict. Only the grain wrapping changes; the engine,
/// schema provider and datastore wiring stay the same.
/// </para>
/// </remarks>
[StatelessWorker]
public sealed class CheckGrain(CheckEngine engine, IDatastore datastore) : Grain, ICheckGrain
{
    /// <inheritdoc />
    public async Task<CheckReply> Check(CheckRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        // Evaluate against the freshest committed state. Consistency tokens / quantized
        // revisions are a later phase; HeadRevision is correct (if not yet cacheable) here.
        var head = await datastore.HeadRevision();
        var reader = datastore.SnapshotReader(head.Revision);

        var subject = new ObjectAndRelation(
            request.SubjectType,
            request.SubjectId,
            request.SubjectRelation);

        var result = await engine.Check(
            reader,
            request.ResourceType,
            request.ResourceId,
            request.Permission,
            subject,
            request.Context);

        // CheckVerdict mirrors the Engine's Membership integer values (0/1/2).
        return new CheckReply((CheckVerdict)(int)result.Verdict, result.MissingExprFields);
    }
}
