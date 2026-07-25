using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// The closed-timestamp gate for sequencer-snapshot fetches (the successor of the retired per-silo
/// projection's watermark wait): a reader pinned at revision R must never be built over a fetched state
/// whose head is below R. During cluster membership churn a stale DUPLICATE activation of the singleton
/// can briefly serve an old head (its storage-version CAS keeps it from ever committing, but a pure
/// <see cref="IDatastoreGrain.ReadState"/> against it answers from the stale fold) — so a below-required
/// head is refetched with a bounded retry, and on exhaustion surfaces as
/// <see cref="RevisionNotFoundException"/> rather than a silently-stale snapshot.
/// </summary>
internal static class SequencerStateFetch
{
    /// <summary>Bound on refetch attempts before surfacing the pinned revision as unresolvable.</summary>
    private const int MaxAttempts = 50;

    /// <summary>Delay between refetch attempts (churn settles within the retry budget in practice).</summary>
    private static readonly TimeSpan RetryDelay = TimeSpan.FromMilliseconds(20);

    /// <summary>
    /// Fetches the sequencer's materialized state, requiring its head to cover
    /// <paramref name="requiredRevision"/>. Not a grain method, so <c>ConfigureAwait(false)</c> is correct.
    /// </summary>
    public static async Task<DatastoreGrainState> StateCovering(
        IDatastoreGrain grain, long requiredRevision, CancellationToken ct)
    {
        for (var attempt = 0; ; attempt++)
        {
            var state = await grain.ReadState().ConfigureAwait(false);
            if (state.HeadRevision >= requiredRevision)
                return state;

            if (attempt + 1 >= MaxAttempts)
                throw new RevisionNotFoundException(new TimestampRevision(requiredRevision));
            await Task.Delay(RetryDelay, ct).ConfigureAwait(false);
        }
    }
}
