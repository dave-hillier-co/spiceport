using System.Collections.Immutable;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// The pinning / index-acquisition / caveat-collapse logic shared by <see cref="ReverseOps.ExpandPermissionTree"/>
/// and the streaming <see cref="ReverseOps.StreamLookupSubjects"/> / <see cref="ReverseOps.StreamLookupResources"/>
/// ops. Kept as one copy so the snapshot-pinning and collapse rules cannot drift between the unary and
/// streaming paths.
/// </summary>
internal static class ReverseOpsSupport
{
    /// <summary>Orleans grain code must not <c>ConfigureAwait(false)</c>; keep the captured context.</summary>
    public const ConfigureAwaitOptions ContinueOnCapturedContext = ConfigureAwaitOptions.ContinueOnCapturedContext;

    /// <summary>
    /// Resolves the consistency requirement to the revision actually evaluated and returns a snapshot reader,
    /// the evaluation "now", the read-at token, and the revision. Null consistency (the default) is
    /// MinimizeLatency → the optimized revision, identical to the prior unary behaviour.
    /// </summary>
    public static async Task<(IDatastoreReader Reader, DateTimeOffset Now, string Token, IRevision Revision, string? SchemaHash)> PinReader(
        IDatastore datastore, ConsistencyWire? consistency, CancellationToken cancellationToken)
    {
        var requirement = (consistency ?? ConsistencyWire.MinimizeLatency).ToRequirement();
        var resolved = await RevisionResolver
            .Resolve(datastore, requirement, cancellationToken: cancellationToken)
            .ConfigureAwait(ContinueOnCapturedContext);

        var reader = datastore.SnapshotReader(resolved.Revision);
        var datastoreId = await datastore.GetUniqueId(cancellationToken)
            .ConfigureAwait(ContinueOnCapturedContext);
        var token = ZedTokens.FromRevision(resolved.Revision, resolved.SchemaHash, datastoreId).Token;
        return (reader, DateTimeOffset.UtcNow, token, resolved.Revision, resolved.SchemaHash);
    }

    /// <summary>
    /// Acquires a COMPLETE candidate set for a fresh, unpaged (<paramref name="hasCursorOrLimit"/> false)
    /// <c>LookupResources</c> enumeration of <paramref name="resourceType"/>/<paramref name="permission"/>
    /// via the Leopard membership-walk grain mesh (the addressable replacement for the retired per-silo
    /// <c>MembershipIndexCache</c> replica) — or null when the accelerator is disabled, the request is
    /// paged/resumed, the target shape is not covered, or either walk reports an incomplete result (a
    /// depth-exhausted subtree), in every one of which cases the caller MUST run the live traversal instead.
    /// </summary>
    /// <remarks>
    /// Dispatches TWO root walks — the concrete subject key and its same-type/relation wildcard key (so a
    /// <c>type:*#rel</c> userset edge is followed too, mirroring the retired index's wildcard-seed rule) —
    /// and unions their nodes. The union is then filtered to nodes whose (type, relation) match the target
    /// shape's coverage yields, the reflexive self-membership candidate is added when the subject and
    /// resource types match (mirroring <c>MembershipIndex.TryCoveredResources</c> exactly), and the result is
    /// returned sorted/distinct — the identical contract the retired index's <c>TryCoveredResources</c> gave.
    /// </remarks>
    public static async Task<IReadOnlyList<string>?> AcquireCoveredCandidates(
        IGrainFactory grainFactory,
        MembershipWalkOptions options,
        SchemaSnapshot schema,
        string subjectType,
        string subjectId,
        string subjectRelation,
        string resourceType,
        string permission,
        IRevision revision,
        bool hasCursorOrLimit,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(schema);

        if (!options.Enabled || hasCursorOrLimit)
            return null;

        // A wildcard subject is not a concrete membership query; leave it to the live engine (mirrors the
        // retired index's TryCoveredResources decline rule).
        if (subjectId == CoreConstants.PublicWildcard)
            return null;

        var coverage = schema.MembershipCoverage;
        if (!coverage.TryGetYields(resourceType, permission, out var yieldRelations))
            return null;

        var revisionString = revision.ToString()!;
        var schemaHash = schema.SchemaHash;

        var concreteReply = await WalkRoot(grainFactory, subjectType, subjectId, subjectRelation, revisionString, schemaHash, cancellationToken)
            .ConfigureAwait(ContinueOnCapturedContext);
        var wildcardReply = await WalkRoot(grainFactory, subjectType, CoreConstants.PublicWildcard, subjectRelation, revisionString, schemaHash, cancellationToken)
            .ConfigureAwait(ContinueOnCapturedContext);

        if (concreteReply.Incomplete || wildcardReply.Incomplete)
            return null; // never return a silently short candidate set — fall back to live.

        var nodes = concreteReply.Nodes.Concat(wildcardReply.Nodes)
            .Select(n => new MembershipWalk.ResourceNode(n.Type, n.Id, n.Relation));
        return MembershipWalk.ToCoveredCandidates(nodes, yieldRelations, resourceType, subjectType, subjectId);
    }

    private static async Task<MembershipClosureReply> WalkRoot(
        IGrainFactory grainFactory,
        string subjectType,
        string subjectId,
        string subjectRelation,
        string revision,
        string schemaHash,
        CancellationToken cancellationToken)
    {
        var key = MembershipWalkKey.Build(subjectType, subjectId, subjectRelation, revision, schemaHash);
        var grain = grainFactory.GetGrain<IMembershipWalkGrain>(key);
        var args = new MembershipWalkArgs(Path: [], DepthRemaining: CheckEngine.DefaultMaxDepth);

        // The caller's own token drives the grain call directly: Orleans' native cancellation-token
        // support propagates it to the activation, so there is nothing left to bridge here.
        return await grain.GetContainingSet(args, cancellationToken)
            .ConfigureAwait(ContinueOnCapturedContext);
    }

    /// <summary>
    /// Collapses a verbatim caveat against the request context. Returns false when the subject is definitely
    /// excluded; otherwise sets the collapsed permissionship (member or caveated).
    /// </summary>
    public static bool TryCollapse(
        CaveatExpression? caveat,
        IReadOnlyDictionary<string, object?>? context,
        CaveatEvaluator evaluator,
        out Permissionship permissionship)
    {
        if (caveat is null)
        {
            permissionship = Permissionship.Member;
            return true;
        }

        var result = evaluator.EvaluateExpression(caveat, context);
        switch (result.Outcome)
        {
            case CaveatOutcome.DefinitelyTrue:
                permissionship = Permissionship.Member;
                return true;
            case CaveatOutcome.Caveated:
                permissionship = Permissionship.Caveated(result.MissingFields);
                return true;
            default:
                permissionship = Permissionship.Member;
                return false;
        }
    }
}
