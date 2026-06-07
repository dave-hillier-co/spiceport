using Spiceport.Core;

namespace Spiceport.Datastore;

/// <summary>
/// How to treat a token that references a different datastore instance, mirroring SpiceDB's
/// <c>MismatchingTokenOption</c>.
/// </summary>
public enum MismatchingTokenOption
{
    /// <summary>Fall back to full consistency (head). SpiceDB's default for at-least-as-fresh.</summary>
    TreatAsFullConsistency = 0,

    /// <summary>Fall back to minimize-latency (optimized).</summary>
    TreatAsMinLatency = 1,

    /// <summary>Raise an error.</summary>
    TreatAsError = 2,
}

/// <summary>
/// Resolves a <see cref="ConsistencyRequirement"/> to a concrete revision and cache mode against a
/// datastore. Mirrors SpiceDB's <c>addRevisionToContextFromConsistency</c> + <c>pickBestRevision</c>.
/// </summary>
public static class RevisionResolver
{
    /// <summary>
    /// Resolves the revision (and schema hash + cache mode) the request should be evaluated at.
    /// </summary>
    /// <exception cref="InvalidConsistencyTokenException">
    /// The token is malformed, references a different datastore where that is an error,
    /// or the exact-snapshot revision is no longer available.
    /// </exception>
    public static async Task<ResolvedRevision> Resolve(
        IDatastore datastore,
        ConsistencyRequirement requirement,
        MismatchingTokenOption mismatchOption = MismatchingTokenOption.TreatAsFullConsistency,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(datastore);
        ArgumentNullException.ThrowIfNull(requirement);

        var parser = await datastore.GetRevisionParser(cancellationToken).ConfigureAwait(false);

        switch (requirement)
        {
            case ConsistencyRequirement.MinimizeLatencyRequirement:
            {
                var opt = await datastore.OptimizedRevision(cancellationToken).ConfigureAwait(false);
                return new ResolvedRevision(opt.Revision, opt.SchemaHash, RevisionMode.Optimized);
            }

            case ConsistencyRequirement.FullyConsistentRequirement:
            {
                var head = await datastore.HeadRevision(cancellationToken).ConfigureAwait(false);
                return new ResolvedRevision(head.Revision, head.SchemaHash, RevisionMode.Exact);
            }

            case ConsistencyRequirement.AtLeastAsFreshRequirement atLeast:
                return await ResolveAtLeastAsFresh(datastore, parser, atLeast.Token, mismatchOption, cancellationToken)
                    .ConfigureAwait(false);

            case ConsistencyRequirement.AtExactSnapshotRequirement atExact:
                return await ResolveAtExactSnapshot(datastore, parser, atExact.Token, cancellationToken)
                    .ConfigureAwait(false);

            default:
                throw new InvalidConsistencyTokenException($"unknown consistency requirement: {requirement.GetType().Name}");
        }
    }

    private static async Task<ResolvedRevision> ResolveAtLeastAsFresh(
        IDatastore datastore,
        IRevisionParser parser,
        ZedToken token,
        MismatchingTokenOption mismatchOption,
        CancellationToken cancellationToken)
    {
        var opt = await datastore.OptimizedRevision(cancellationToken).ConfigureAwait(false);
        var decoded = ZedTokens.DecodeRevision(token, parser);

        switch (decoded.Status)
        {
            case ZedTokenStatus.Unknown:
                throw new InvalidConsistencyTokenException("at_least_as_fresh: malformed ZedToken");

            case ZedTokenStatus.MismatchedDatastoreId:
                switch (mismatchOption)
                {
                    case MismatchingTokenOption.TreatAsFullConsistency:
                        var head = await datastore.HeadRevision(cancellationToken).ConfigureAwait(false);
                        return new ResolvedRevision(head.Revision, head.SchemaHash, RevisionMode.Exact);
                    case MismatchingTokenOption.TreatAsMinLatency:
                        return new ResolvedRevision(opt.Revision, opt.SchemaHash, RevisionMode.Optimized);
                    default:
                        throw new InvalidConsistencyTokenException(
                            "at_least_as_fresh: ZedToken references a different datastore instance");
                }
        }

        // Valid or LegacyEmptyDatastoreId: pick max(optimized, token).
        // If optimized is strictly fresher, use the optimized (cacheable) revision.
        if (opt.Revision.CompareTo(decoded.Revision) > 0)
            return new ResolvedRevision(opt.Revision, opt.SchemaHash, RevisionMode.Optimized);

        // The token is at least as fresh as the optimized bucket (read-your-writes): use it exactly.
        return new ResolvedRevision(decoded.Revision, decoded.SchemaHash, RevisionMode.Exact);
    }

    private static async Task<ResolvedRevision> ResolveAtExactSnapshot(
        IDatastore datastore,
        IRevisionParser parser,
        ZedToken token,
        CancellationToken cancellationToken)
    {
        var decoded = ZedTokens.DecodeRevision(token, parser);

        switch (decoded.Status)
        {
            case ZedTokenStatus.Unknown:
                throw new InvalidConsistencyTokenException("at_exact_snapshot: malformed ZedToken");
            case ZedTokenStatus.MismatchedDatastoreId:
                throw new InvalidConsistencyTokenException(
                    "at_exact_snapshot: ZedToken references a different datastore instance");
        }

        if (!await datastore.CheckRevision(decoded.Revision, cancellationToken).ConfigureAwait(false))
            throw new InvalidConsistencyTokenException(
                $"at_exact_snapshot: revision {decoded.Revision} is no longer available");

        return new ResolvedRevision(decoded.Revision, decoded.SchemaHash, RevisionMode.Exact);
    }
}
