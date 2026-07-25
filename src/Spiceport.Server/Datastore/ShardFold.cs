using System.Collections.Immutable;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// The per-key restriction of <see cref="LogFold"/>: folds the same committed <see cref="LogEvent"/>
/// sequence, but keeps only the relationship rows of one adjacency slice (<see cref="GraphShardKeyWire"/>).
/// The sharding lemma — <c>fold(log) == merge over keys of fold(log|key)</c> — is what makes this a
/// filter rather than a re-derivation, and it is pinned by <c>ShardFoldLemmaTests</c>: per key, a
/// shard's rows equal the whole fold's rows restricted to that key, and the union over all forward
/// (or all reverse) shards reproduces the whole live set at every readable revision.
/// </summary>
/// <remarks>
/// Within one event the staged Apply/Remove/Commit semantics of <c>MvccReadWriteTransaction</c> are
/// mirrored exactly (repeated touches of one identity collapse to a single appended row; a
/// touch-then-delete leaves no new row), and GC events mirror <c>DatastoreState.CollectBelow</c>'s
/// relationship rules exactly — that is what makes the lemma an equality, not an approximation.
/// SchemaChange/CounterChanges are not shard state and are ignored. A Create is treated identically
/// to a Touch: the write path only ever logs Touch/Delete (creates surface as Touch via
/// <c>DatastoreState.ChangesAt</c>), and the shard fold has no create-conflict to enforce.
/// </remarks>
internal static class ShardFold
{
    /// <summary>
    /// Folds one committed <see cref="LogEvent"/> into the shard state for <paramref name="key"/>.
    /// The watermark (<see cref="GraphShardState.AppliedRevision"/>) advances to the event's revision
    /// even when nothing in the event matches the key.
    /// </summary>
    public static GraphShardState ApplyEvent(GraphShardState state, LogEvent ev, GraphShardKeyWire key)
    {
        if (ev.GcFloor is { } floor)
            return ApplyGc(state, ev.Revision, floor);

        var rows = state.Rows;

        // Stage the event's matching updates with the same per-identity semantics as
        // MvccReadWriteTransaction (Apply/Remove), then commit them the same way, so within-event
        // sequences fold identically to the whole-state replay.
        var baseLive = new HashSet<RowIdentity>();
        foreach (var row in rows)
        {
            if (row.DeletedRevision is null)
                baseLive.Add(RowIdentity.From(row.Relationship));
        }

        var live = new HashSet<RowIdentity>(baseLive);
        var deleted = new HashSet<RowIdentity>();
        var created = new HashSet<RowIdentity>();
        var payloads = new Dictionary<RowIdentity, RelationshipWire>();

        foreach (var update in ev.RelationshipChanges)
        {
            if (!key.Matches(update.Relationship))
                continue;

            // The whole fold stores the wire payload after a round trip through the core Relationship
            // (which normalizes an empty subject relation to the ellipsis and an empty caveat name to
            // "no caveat"); apply the identical normalization so the restricted rows compare equal.
            var rel = WireConvert.ToWire(WireConvert.ToRelationship(update.Relationship));
            var identity = RowIdentity.From(rel);

            if (update.Operation == RelationshipUpdateOpWire.Delete)
            {
                if (!live.Remove(identity))
                    continue;
                created.Remove(identity);
                payloads.Remove(identity);
                if (baseLive.Contains(identity))
                    deleted.Add(identity);
            }
            else // Touch and Create alike: create-or-replace the identity.
            {
                if (!created.Contains(identity) && baseLive.Contains(identity))
                    deleted.Add(identity);
                created.Add(identity);
                live.Add(identity);
                payloads[identity] = rel;
            }
        }

        if (deleted.Count > 0)
        {
            var builder = rows.ToBuilder();
            for (var i = 0; i < builder.Count; i++)
            {
                var row = builder[i];
                if (row.DeletedRevision is null && deleted.Contains(RowIdentity.From(row.Relationship)))
                    builder[i] = row with { DeletedRevision = ev.Revision };
            }
            rows = builder.ToImmutable();
        }

        foreach (var identity in created)
        {
            if (payloads.TryGetValue(identity, out var rel))
                rows = rows.Add(new StoredRelationshipWire(rel, ev.Revision, null));
        }

        return state with { AppliedRevision = ev.Revision, Rows = rows };
    }

    /// <summary>
    /// The rows of the shard visible at <paramref name="revision"/> (half-open MVCC window
    /// <c>[created, deleted)</c>). Callers must reject a revision below the shard's GC floor — see
    /// <see cref="IsReadableAt"/>, mirroring the <c>MvccSnapshotReader</c> constructor guard — because
    /// rows already collected below the floor would be silently missing from the answer.
    /// </summary>
    public static IEnumerable<RelationshipWire> VisibleAt(GraphShardState state, long revision)
    {
        foreach (var row in state.Rows)
        {
            if (row.CreatedRevision <= revision && (row.DeletedRevision is null || row.DeletedRevision > revision))
                yield return row.Relationship;
        }
    }

    /// <summary>True if a read pinned at <paramref name="revision"/> is still exact on this shard.</summary>
    public static bool IsReadableAt(GraphShardState state, long revision) => revision >= state.GcFloor;

    /// <summary>
    /// Collects the shard's rows below <paramref name="floor"/> and stamps the watermark to
    /// <paramref name="revision"/> — the GC branch of the fold, exposed for the thin sequencer's lazy
    /// row GC: stored rows are only compacted when next dirtied and flushed, so every serve path
    /// re-applies the current floor through this method (a floor at or below the shard's own is a pure
    /// relabel). Applying the LATEST floor once is equivalent to applying every intermediate GC event's
    /// floor in sequence: a row is dropped iff it is fully dead or expired at/below the final floor,
    /// which subsumes every lower floor's collection (the same monotonicity
    /// <c>DatastoreState.CollectBelow</c> relies on).
    /// </summary>
    public static GraphShardState CollectBelow(GraphShardState state, long revision, long floor) =>
        ApplyGc(state, revision, floor);

    // Mirrors DatastoreState.CollectBelow's relationship rules exactly: drop a row when it is fully
    // dead below the floor (DeletedRevision <= floor) OR expired at/before the floor (the expiration
    // sweep — see CollectBelow's remarks for why that cannot change any still-servable read). A floor
    // that does not advance the shard's own floor is a no-op on the rows, but the watermark still
    // advances, exactly as LogFold advances the head past a stale GC event.
    private static GraphShardState ApplyGc(GraphShardState state, long revision, long floor)
    {
        if (floor <= state.GcFloor)
            return state with { AppliedRevision = revision };

        var kept = state.Rows.Where(row =>
        {
            if (row.DeletedRevision is { } deletedAt && deletedAt <= floor)
                return false;
            if (row.Relationship.Expiration is { } exp && NanosSinceEpoch(exp) <= floor)
                return false;
            return true;
        }).ToImmutableList();

        return new GraphShardState(revision, floor, kept);
    }

    private static long NanosSinceEpoch(DateTimeOffset dt) => (dt - DateTimeOffset.UnixEpoch).Ticks * 100L;

    /// <summary>
    /// The storage identity of a row — the six-tuple mirroring <c>Spiceport.Datastore.RelationshipKey</c>.
    /// Caveat and expiration are payload, not identity.
    /// </summary>
    private readonly record struct RowIdentity(
        string ResourceType,
        string ResourceId,
        string ResourceRelation,
        string SubjectType,
        string SubjectId,
        string SubjectRelation)
    {
        public static RowIdentity From(RelationshipWire rel) => new(
            rel.ResourceType, rel.ResourceId, rel.ResourceRelation,
            rel.SubjectType, rel.SubjectId, rel.SubjectRelation);
    }
}
