using System.Collections.Immutable;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// The SMALL-STATE restriction of <see cref="LogFold"/>: folds the same committed <see cref="LogEvent"/>
/// sequence, but keeps only what <see cref="DatastoreMetaState"/> carries — head, schema versions,
/// counter versions, GC floor, and the key index. Relationship row content is deliberately NOT folded
/// here; it belongs to the per-key <see cref="ShardFold"/> (the dirty buffer / shard rows). Together,
/// <c>MetaFold + ShardFold-per-touched-key</c> is exactly <see cref="LogFold"/> split along the sharding
/// lemma: <c>fold(log) == merge(fold(log | key) for all keys)</c> for the rows, plus this fold for
/// everything else.
/// </summary>
/// <remarks>
/// Counter versions are appended DIRECTLY from the event's net deltas (never replayed through the
/// guarded register/unregister ops) — the same direct-append stance <see cref="LogFold"/> takes, for the
/// same reason: a same-commit register+unregister nets to a delta whose guard is false in the fold base.
/// Schema versions append the event's self-contained <see cref="LogEvent.SchemaChange"/> as-is (revision
/// + bytes + hash), which is byte-identical to what the whole fold's transaction replay produces. GC
/// events compact the schema/counter histories exactly as <c>DatastoreState.CollectBelow</c> does
/// (latest version at-or-below the floor kept — for counters, per name and only when not a tombstone —
/// plus everything above), so the small state and the whole fold converge on identical histories.
/// </remarks>
internal static class MetaFold
{
    /// <summary>Folds one committed <see cref="LogEvent"/> into the small state.</summary>
    public static DatastoreMetaState ApplyEvent(DatastoreMetaState state, LogEvent ev)
    {
        if (ev.GcFloor is { } floor)
        {
            if (floor <= state.GcFloor)
                return state with { HeadRevision = ev.Revision };
            return state with
            {
                HeadRevision = ev.Revision,
                GcFloor = floor,
                Schemas = CompactSchemas(state.Schemas, floor),
                Counters = CompactCounters(state.Counters, floor),
            };
        }

        var schemas = ev.SchemaChange is { } schema ? state.Schemas.Add(schema) : state.Schemas;

        var counters = state.Counters;
        foreach (var counter in ev.CounterChanges)
            counters = counters.Add(new CounterVersionWire(ev.Revision, counter.Name, counter.Filter));

        // Index every touched key. Add-only here, and VERSION-NEUTRAL: a key not yet indexed is added
        // at NoRowVersion (it has no durable row — its state lives in the dirty buffer until a flush
        // writes one), and an already-indexed key KEEPS its recorded row version. The fold must stay a
        // pure function of the event sequence — it cannot know which storage version a row will land
        // under — so the version bump happens exclusively at the flush, on the meta entry the flush
        // persists (see DatastoreGrain.ApplyUpdatesToStorage); keys whose shard state becomes empty
        // are pruned there too, never mid-fold.
        var forward = state.ForwardKeys;
        var reverse = state.ReverseKeys;
        foreach (var change in ev.RelationshipChanges)
        {
            var forwardKey = ForwardKeyOf(change.Relationship);
            if (!forward.ContainsKey(forwardKey))
                forward = forward.Add(forwardKey, DatastoreMetaState.NoRowVersion);
            var reverseKey = ReverseKeyOf(change.Relationship);
            if (!reverse.ContainsKey(reverseKey))
                reverse = reverse.Add(reverseKey, DatastoreMetaState.NoRowVersion);
        }

        return state with
        {
            HeadRevision = ev.Revision,
            Schemas = schemas,
            Counters = counters,
            ForwardKeys = forward,
            ReverseKeys = reverse,
        };
    }

    /// <summary>
    /// The distinct shard keys (escaped <c>GraphShardGrainKey</c> strings, both directions) an event's
    /// relationship changes touch — the keys whose dirty-buffer entries must be seeded before the event
    /// is folded. A GC event touches no keys (it folds into every already-present entry instead).
    /// Type/id fields need no payload normalization: the wire round-trip only normalizes the subject
    /// relation and caveat, never the four key fields.
    /// </summary>
    public static IReadOnlyCollection<string> TouchedKeys(LogEvent ev)
    {
        if (ev.GcFloor is not null || ev.RelationshipChanges.Count == 0)
            return Array.Empty<string>();

        var keys = new HashSet<string>(StringComparer.Ordinal);
        foreach (var change in ev.RelationshipChanges)
        {
            keys.Add(ForwardKeyOf(change.Relationship));
            keys.Add(ReverseKeyOf(change.Relationship));
        }
        return keys;
    }

    /// <summary>The forward shard key string (<c>f/type/id</c>) of a row's resource.</summary>
    public static string ForwardKeyOf(RelationshipWire rel) =>
        GraphShardGrainKey.Build(GraphShardKeyWire.ForResource(rel.ResourceType, rel.ResourceId));

    /// <summary>The reverse shard key string (<c>r/type/id</c>) of a row's subject.</summary>
    public static string ReverseKeyOf(RelationshipWire rel) =>
        GraphShardGrainKey.Build(GraphShardKeyWire.ForSubject(rel.SubjectType, rel.SubjectId));

    // Mirrors DatastoreState.CollectBelow's SCHEMAS rule: keep the single latest version with
    // Revision <= floor (the version effective at the floor) plus every version above it.
    private static ImmutableList<SchemaVersionWire> CompactSchemas(ImmutableList<SchemaVersionWire> schemas, long floor)
    {
        long? latestAtOrBelow = null;
        foreach (var s in schemas)
            if (s.Revision <= floor)
                latestAtOrBelow = s.Revision;

        return schemas.Where(s => s.Revision > floor || s.Revision == latestAtOrBelow).ToImmutableList();
    }

    // Mirrors DatastoreState.CollectBelow's COUNTERS rule: per name, keep the latest version with
    // Revision <= floor UNLESS it is a tombstone (a dropped tombstone and "no version at all" are
    // indistinguishable to every consumer), plus everything above the floor.
    private static ImmutableList<CounterVersionWire> CompactCounters(ImmutableList<CounterVersionWire> counters, long floor)
    {
        var latestAtOrBelowByName = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (var c in counters)
            if (c.Revision <= floor)
                latestAtOrBelowByName[c.Name] = c.Revision;

        return counters.Where(c =>
        {
            if (c.Revision > floor)
                return true;
            return latestAtOrBelowByName[c.Name] == c.Revision && c.Filter is not null;
        }).ToImmutableList();
    }
}
