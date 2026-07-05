using System.Collections.Immutable;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// The pinning / index-acquisition / caveat-collapse logic shared by the unary <c>ExpandPermissionTree</c>
/// and the streaming <c>StreamLookupSubjects</c> / <c>StreamLookupResources</c> methods on
/// <see cref="ReverseOpsStreamGrain"/>. Kept as one copy so the snapshot-pinning and collapse rules cannot
/// drift between the unary and streaming paths.
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
    public static async Task<(IDatastoreReader Reader, DateTimeOffset Now, string Token, IRevision Revision)> PinReader(
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
        return (reader, DateTimeOffset.UtcNow, token, resolved.Revision);
    }

    /// <summary>
    /// Acquires the trusted Leopard membership index for this request (built from the current schema at the
    /// resolved revision), or null when the accelerator is disabled — in which case the lookup engine runs its
    /// unchanged live traversal.
    /// </summary>
    public static async Task<MembershipIndex?> AcquireIndex(
        MembershipIndexCache membershipIndex,
        ImmutableList<NamespaceDefinition> namespaces,
        ImmutableList<CaveatDefinition> caveats,
        IDatastoreReader reader,
        IRevision revision,
        CancellationToken cancellationToken)
    {
        var revisionNanos = revision is TimestampRevision t ? t.TimestampNanosSinceEpoch : 0;
        return await membershipIndex
            .TryGet(namespaces, caveats, reader, revisionNanos, cancellationToken)
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
