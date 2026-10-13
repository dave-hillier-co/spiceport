using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests.FaultInjection;

/// <summary>
/// Applies a declarative <see cref="CommitRequest"/> to <see cref="ReferenceDatastore"/> with the
/// sequencer's semantics, in the sequencer's order: preconditions against the state before the commit
/// (MustMatch needs a matching row, MustNotMatch none), then the updates, then the delete-by-filter.
/// A rejection applies nothing. Covers what the harness generates: preconditions, relationship updates
/// and delete-by-filter, not schema or counter changes.
/// </summary>
internal static class ReferenceCommit
{
    public static async Task<(CommitReply Reply, IRevision? Revision)> Apply(
        ReferenceDatastore reference, CommitRequest request)
    {
        ulong deleted = 0;
        var reachedLimit = false;
        try
        {
            var revision = await reference.ReadWriteTx(async tx =>
            {
                foreach (var precondition in request.Preconditions)
                {
                    var matched = false;
                    await foreach (var _ in tx.QueryRelationships(WireConvert.ToCoreFilter(precondition.Filter)))
                    {
                        matched = true;
                        break;
                    }

                    if (matched != precondition.MustMatch)
                        throw new Rejected(CommitFailureKind.PreconditionFailed);
                }

                if (request.Updates.Count > 0)
                    await tx.WriteRelationships(request.Updates.Select(WireConvert.ToWriteUpdate).ToList());

                if (request.DeleteByFilter is { } deleteByFilter)
                {
                    (deleted, reachedLimit) = await tx.DeleteRelationships(
                        WireConvert.ToCoreFilter(deleteByFilter.Filter), deleteByFilter.Limit);
                }
            });
            return (new CommitReply(0, null, deleted, reachedLimit), revision);
        }
        catch (Rejected rejected)
        {
            return (Failed(rejected.Kind), null);
        }
        catch (CreateRelationshipExistsException)
        {
            return (Failed(CommitFailureKind.CreateAlreadyExists), null);
        }
    }

    private static CommitReply Failed(CommitFailureKind kind) =>
        new(null, new CommitFailureWire(kind, null), 0, false);

    private sealed class Rejected(CommitFailureKind kind) : Exception
    {
        public CommitFailureKind Kind { get; } = kind;
    }
}
