using System.Text.Json;
using Spiceport.Grains;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Pins the sharding lemma: <c>fold(log) == merge over keys of fold(log|key)</c>. For seeded random
/// event logs (touches, creates, deletes, caveats, expirations and interleaved GC events), the
/// per-key <see cref="ShardFold"/> must reproduce, key by key, exactly the rows the whole-state
/// <see cref="LogFold"/> produces restricted to that key — and the union over all forward (or all
/// reverse) shards must reproduce the whole live set at every readable revision. This is the gate
/// that makes the graph-sharded datastore's shard state a filter of the one fold rather than a
/// divergent re-derivation (docs/graph-sharded-datastore.md, migration step 2). Pure fold tests —
/// no Orleans cluster.
/// </summary>
public sealed class ShardFoldLemmaTests
{
    // All expirations are generated relative to this fixed instant (never UtcNow), so the generated
    // logs and their GC outcomes are identical on every run. Revisions are nanos-since-epoch of the
    // same instant plus small deltas, so "inside the revision range" expirations are expressible.
    private static readonly DateTimeOffset FixedBase = new(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private const string Ellipsis = "...";

    public static IEnumerable<object[]> Seeds => Enumerable.Range(0, 24).Select(s => new object[] { s });

    [Theory]
    [MemberData(nameof(Seeds))]
    public void ShardFold_SatisfiesTheShardingLemma(int seed)
    {
        var (events, baseRevision) = BuildLog(seed);

        var whole = DatastoreGrainState.Empty(baseRevision);
        foreach (var ev in events)
            whole = LogFold.ApplyEvent(whole, ev);

        Assert.NotEmpty(whole.Relationships); // the lemma must never hold vacuously

        // Every key the log references, plus one forward and one reverse key it never does.
        var forwardKeys = new HashSet<GraphShardKeyWire>();
        var reverseKeys = new HashSet<GraphShardKeyWire>();
        foreach (var ev in events)
        {
            foreach (var update in ev.RelationshipChanges)
            {
                forwardKeys.Add(GraphShardKeyWire.ForResource(update.Relationship.ResourceType, update.Relationship.ResourceId));
                reverseKeys.Add(GraphShardKeyWire.ForSubject(update.Relationship.SubjectType, update.Relationship.SubjectId));
            }
        }
        var neverForward = GraphShardKeyWire.ForResource("document", "never-referenced");
        var neverReverse = GraphShardKeyWire.ForSubject("user", "never-referenced");
        forwardKeys.Add(neverForward);
        reverseKeys.Add(neverReverse);

        var shards = forwardKeys.Concat(reverseKeys).ToDictionary(key => key, key =>
        {
            var shard = GraphShardState.Empty;
            foreach (var ev in events)
                shard = ShardFold.ApplyEvent(shard, ev, key);
            return shard;
        });

        // 1 + 3: per key, the shard's rows are exactly the whole state's rows restricted to that key
        // (full payload + visibility window, order-insensitive), and every shard's watermark and GC
        // floor — matched or not by any event — equal the whole state's.
        foreach (var (key, shard) in shards)
        {
            Assert.Equal(whole.HeadRevision, shard.AppliedRevision);
            Assert.Equal(whole.GcFloor, shard.GcFloor);

            var expected = whole.Relationships
                .Where(row => key.Matches(row.Relationship))
                .Select(CanonRow).OrderBy(s => s, StringComparer.Ordinal).ToList();
            var actual = shard.Rows.Select(CanonRow).OrderBy(s => s, StringComparer.Ordinal).ToList();
            Assert.Equal(expected, actual);
        }

        // 4: never-referenced keys hold no rows but still advanced (checked above for all shards).
        Assert.Empty(shards[neverForward].Rows);
        Assert.Empty(shards[neverReverse].Rows);

        // 2: at sampled readable revisions (the GC floor itself, the first event at/above it, two
        // mid-log points, and the head), the multiset union of VisibleAt over all forward shards —
        // and independently over all reverse shards — equals the whole state's live set.
        Assert.True(whole.GcFloor > 0); // the generated log always contains GC events
        var readable = events.Select(e => e.Revision).Where(r => r >= whole.GcFloor).ToList();
        var samples = new[]
        {
            whole.GcFloor,
            readable[0],
            readable[readable.Count / 3],
            readable[2 * readable.Count / 3],
            whole.HeadRevision,
        }.Distinct();

        foreach (var rev in samples)
        {
            Assert.All(shards.Values, s => Assert.True(ShardFold.IsReadableAt(s, rev)));

            var wholeLive = whole.Relationships
                .Where(row => row.CreatedRevision <= rev && (row.DeletedRevision is null || row.DeletedRevision > rev))
                .Select(row => CanonRel(row.Relationship)).OrderBy(s => s, StringComparer.Ordinal).ToList();

            var forwardUnion = forwardKeys.SelectMany(k => ShardFold.VisibleAt(shards[k], rev))
                .Select(CanonRel).OrderBy(s => s, StringComparer.Ordinal).ToList();
            var reverseUnion = reverseKeys.SelectMany(k => ShardFold.VisibleAt(shards[k], rev))
                .Select(CanonRel).OrderBy(s => s, StringComparer.Ordinal).ToList();

            Assert.Equal(wholeLive, forwardUnion);
            Assert.Equal(wholeLive, reverseUnion);
        }
    }

    [Fact]
    public void TouchThenTouch_SameIdentity_ClosesAndReappends()
    {
        var key = GraphShardKeyWire.ForResource("document", "a");
        var first = Rel("document", "a", "viewer", "user", "alice");
        var second = first with { CaveatName = "is_active", CaveatContext = Context(7) };

        var shard = GraphShardState.Empty;
        shard = ShardFold.ApplyEvent(shard, Ev(10, TouchOf(first)), key);
        shard = ShardFold.ApplyEvent(shard, Ev(20, TouchOf(second)), key);

        Assert.Equal(2, shard.Rows.Count);
        Assert.Contains(shard.Rows, r => r.CreatedRevision == 10 && r.DeletedRevision == 20);
        Assert.Contains(shard.Rows, r => r.CreatedRevision == 20 && r.DeletedRevision is null);
        Assert.Equal(CanonRel(first), CanonRel(Assert.Single(ShardFold.VisibleAt(shard, 15))));
        Assert.Equal(CanonRel(second), CanonRel(Assert.Single(ShardFold.VisibleAt(shard, 20))));
    }

    [Fact]
    public void DeleteOfNeverCreatedIdentity_IsANoOp_ButAdvancesTheWatermark()
    {
        var key = GraphShardKeyWire.ForResource("document", "a");
        var shard = ShardFold.ApplyEvent(
            GraphShardState.Empty,
            Ev(10, DeleteOf(Rel("document", "a", "viewer", "user", "ghost"))),
            key);

        Assert.Empty(shard.Rows);
        Assert.Equal(10, shard.AppliedRevision);
        Assert.Equal(0, shard.GcFloor);
    }

    [Fact]
    public void GcAtExactlyTheDeletedRevision_DropsTheRow()
    {
        var key = GraphShardKeyWire.ForResource("document", "a");
        var dead = Rel("document", "a", "viewer", "user", "alice");
        var survivor = Rel("document", "a", "viewer", "user", "bob");

        var shard = GraphShardState.Empty;
        shard = ShardFold.ApplyEvent(shard, Ev(10, TouchOf(dead), TouchOf(survivor)), key);
        shard = ShardFold.ApplyEvent(shard, Ev(20, DeleteOf(dead)), key);
        shard = ShardFold.ApplyEvent(shard, Ev(25, DeleteOf(survivor)), key);
        shard = ShardFold.ApplyEvent(shard, Gc(30, floor: 20), key);

        // DeletedRevision <= floor is the drop rule (the boundary is inclusive): the row deleted AT
        // the floor is gone, the row deleted just above it survives with its window intact.
        var kept = Assert.Single(shard.Rows);
        Assert.Equal(CanonRel(survivor), CanonRel(kept.Relationship));
        Assert.Equal(25, kept.DeletedRevision);
        Assert.Equal(20, shard.GcFloor);
        Assert.Equal(30, shard.AppliedRevision);
    }

    [Fact]
    public void WireEllipsis_NormalizesTheSameAsWholeFold_SoTheRowClosesInBoth()
    {
        var key = GraphShardKeyWire.ForResource("document", "a");
        // Wire form of ellipsis ("") on the Touch, normalized form ("...") on the later Delete of the
        // same identity: both sides must resolve to the same RowIdentity, or the shard would keep a
        // phantom live row that the whole fold correctly closes.
        var touched = Rel("document", "a", "viewer", "user", "alice", subjectRelation: "");
        var deleted = Rel("document", "a", "viewer", "user", "alice", subjectRelation: Ellipsis);

        var events = new List<LogEvent>
        {
            Ev(10, TouchOf(touched)),
            Ev(20, DeleteOf(deleted)),
        };

        var whole = DatastoreGrainState.Empty(10);
        foreach (var ev in events)
            whole = LogFold.ApplyEvent(whole, ev);

        var shard = GraphShardState.Empty;
        foreach (var ev in events)
            shard = ShardFold.ApplyEvent(shard, ev, key);

        var expected = whole.Relationships.Select(CanonRow).OrderBy(s => s, StringComparer.Ordinal).ToList();
        var actual = shard.Rows.Select(CanonRow).OrderBy(s => s, StringComparer.Ordinal).ToList();
        Assert.Equal(expected, actual);

        // No phantom live row in either fold: the sole row is closed at revision 20.
        var wholeRow = Assert.Single(whole.Relationships);
        Assert.Equal(20, wholeRow.DeletedRevision);
        var shardRow = Assert.Single(shard.Rows);
        Assert.Equal(20, shardRow.DeletedRevision);
    }

    [Fact]
    public void IsReadableAt_IsExactlyAtOrAboveTheGcFloor()
    {
        var key = GraphShardKeyWire.ForResource("document", "a");
        var rel = Rel("document", "a", "viewer", "user", "alice");

        var shard = GraphShardState.Empty;
        shard = ShardFold.ApplyEvent(shard, Ev(10, TouchOf(rel)), key);
        shard = ShardFold.ApplyEvent(shard, Gc(20, floor: 15), key);

        Assert.True(ShardFold.IsReadableAt(shard, 15));
        Assert.False(ShardFold.IsReadableAt(shard, 14));
    }

    [Fact]
    public void StaleOrEqualGcFloor_StillAdvancesTheWatermark_ButLeavesRowsAndFloorUnchanged()
    {
        var key = GraphShardKeyWire.ForResource("document", "a");
        var rel = Rel("document", "a", "viewer", "user", "alice");

        var events = new List<LogEvent>
        {
            Ev(10, TouchOf(rel)),
            Gc(20, floor: 50),
            Gc(30, floor: 50), // equal to the current floor: stale, but the watermark must still move.
        };

        var whole = DatastoreGrainState.Empty(10);
        foreach (var ev in events)
            whole = LogFold.ApplyEvent(whole, ev);

        var shard = GraphShardState.Empty;
        var beforeStale = GraphShardState.Empty;
        foreach (var ev in events)
        {
            if (ev.Revision == 30)
                beforeStale = shard;
            shard = ShardFold.ApplyEvent(shard, ev, key);
        }

        Assert.Equal(30, shard.AppliedRevision);
        Assert.Equal(50, shard.GcFloor);
        Assert.Equal(30, whole.HeadRevision);
        Assert.Equal(50, whole.GcFloor);

        var beforeRows = beforeStale.Rows.Select(CanonRow).OrderBy(s => s, StringComparer.Ordinal).ToList();
        var afterRows = shard.Rows.Select(CanonRow).OrderBy(s => s, StringComparer.Ordinal).ToList();
        Assert.Equal(beforeRows, afterRows);
    }

    [Fact]
    public void ExpirationExactlyEqualToTheFloor_DropsTheRow_InBothFolds()
    {
        // Pick nanos that are exactly tick-aligned (nanos = ticks * 100) so the DateTimeOffset round
        // trip through NanosSinceEpoch is exact, and use that same nanos value as the GC floor: the
        // <= boundary in DatastoreState.CollectBelow (mirrored by ShardFold.ApplyGc) must drop a row
        // whose expiration lands EXACTLY on the floor, not just strictly below it.
        var baseRevision = NanosSinceEpoch(FixedBase);
        var floor = baseRevision + 500 * 100; // tick-aligned offset from the base.
        var expirationInstant = DateTimeOffset.UnixEpoch.AddTicks(floor / 100);
        Assert.Equal(floor, NanosSinceEpoch(expirationInstant)); // sanity: the round trip is exact.

        var key = GraphShardKeyWire.ForResource("document", "a");
        var expiring = Rel("document", "a", "viewer", "user", "alice", exp: expirationInstant);
        var survivor = Rel("document", "a", "viewer", "user", "bob", exp: FixedBase.AddHours(1));

        var events = new List<LogEvent>
        {
            Ev(baseRevision + 10, TouchOf(expiring), TouchOf(survivor)),
            Gc(baseRevision + 1000, floor),
        };

        var whole = DatastoreGrainState.Empty(baseRevision);
        foreach (var ev in events)
            whole = LogFold.ApplyEvent(whole, ev);

        var shard = GraphShardState.Empty;
        foreach (var ev in events)
            shard = ShardFold.ApplyEvent(shard, ev, key);

        var expected = whole.Relationships.Select(CanonRow).OrderBy(s => s, StringComparer.Ordinal).ToList();
        var actual = shard.Rows.Select(CanonRow).OrderBy(s => s, StringComparer.Ordinal).ToList();
        Assert.Equal(expected, actual);

        var kept = Assert.Single(shard.Rows);
        Assert.Equal(CanonRel(survivor), CanonRel(kept.Relationship));
    }

    [Fact]
    public void Gc_SweepsALiveRowExpiredAtOrBeforeTheFloor()
    {
        var baseRevision = NanosSinceEpoch(FixedBase);
        var key = GraphShardKeyWire.ForResource("document", "a");
        var expired = Rel("document", "a", "viewer", "user", "alice", exp: FixedBase.AddTicks(-1));
        var future = Rel("document", "a", "viewer", "user", "bob", exp: FixedBase.AddHours(1));

        var shard = GraphShardState.Empty;
        shard = ShardFold.ApplyEvent(shard, Ev(baseRevision + 10, TouchOf(expired), TouchOf(future)), key);
        shard = ShardFold.ApplyEvent(shard, Gc(baseRevision + 30, floor: baseRevision + 20), key);

        // Both rows are still live (DeletedRevision null), but the one whose expiration nanos fall
        // at/before the floor is swept; the future-dated one is kept.
        var kept = Assert.Single(shard.Rows);
        Assert.Equal(CanonRel(future), CanonRel(kept.Relationship));
        Assert.Null(kept.DeletedRevision);
    }

    // --- Log generator ---

    private static (List<LogEvent> Events, long BaseRevision) BuildLog(int seed)
    {
        var rng = new Random(seed);
        string[] resourceTypes = ["document", "folder", "group"];
        string[] resourceIds = ["d0", "d1", "d2", "d3", "d4", "d5"];
        string[] relations = ["viewer", "editor", "parent", "member"];
        string[] subjectTypes = ["user", "group"];
        string[] subjectIds = ["u0", "u1", "u2", "u3", "u4", "u5"];

        var baseRevision = NanosSinceEpoch(FixedBase);
        var revision = baseRevision;
        var eventCount = 60 + rng.Next(61);
        var events = new List<LogEvent>(eventCount);

        // Sequential liveness tracking mirroring the transaction the whole fold replays through, so a
        // generated Create is never replayed over a live identity (which LogFold would reject).
        var liveByIdentity = new Dictionary<string, RelationshipWire>();
        var liveOrder = new List<string>();

        for (var i = 0; i < eventCount; i++)
        {
            revision += 1 + rng.Next(1000);

            if (i > 0 && i % 25 == 0)
            {
                // Floors are event revisions ~15 events back, so some rows die below the floor and
                // some survive; the cadence (every 25) keeps successive floors strictly increasing.
                var floor = events[events.Count - 15].Revision;
                events.Add(Gc(revision, floor));
                continue;
            }

            var updateCount = 1 + rng.Next(5);
            var updates = new List<RelationshipUpdateWire>(updateCount);
            for (var u = 0; u < updateCount; u++)
            {
                // Bias toward re-targeting a live identity so touches replace and deletes actually close.
                RelationshipWire rel;
                if (liveOrder.Count > 0 && rng.NextDouble() < 0.4)
                {
                    var basis = liveByIdentity[liveOrder[rng.Next(liveOrder.Count)]];
                    rel = Decorate(rng, basis with { CaveatName = null, CaveatContext = null, Expiration = null });
                }
                else
                {
                    var subjectId = rng.NextDouble() < 0.1 ? "*" : Pick(rng, subjectIds);
                    var subjectRelation = subjectId != "*" && rng.NextDouble() < 0.2 ? "member" : Ellipsis;
                    rel = Decorate(rng, new RelationshipWire(
                        Pick(rng, resourceTypes), Pick(rng, resourceIds), Pick(rng, relations),
                        Pick(rng, subjectTypes), subjectId, subjectRelation, null, null, null));
                }

                var identity = IdentityOf(rel);
                var roll = rng.NextDouble();
                var op = roll < 0.6 ? RelationshipUpdateOpWire.Touch
                    : roll < 0.9 ? RelationshipUpdateOpWire.Delete
                    : RelationshipUpdateOpWire.Create;
                if (op == RelationshipUpdateOpWire.Create && liveByIdentity.ContainsKey(identity))
                    op = RelationshipUpdateOpWire.Touch;

                if (op == RelationshipUpdateOpWire.Delete)
                {
                    if (liveByIdentity.Remove(identity))
                        liveOrder.Remove(identity);
                }
                else if (!liveByIdentity.ContainsKey(identity))
                {
                    liveByIdentity[identity] = rel;
                    liveOrder.Add(identity);
                }
                else
                {
                    liveByIdentity[identity] = rel;
                }

                updates.Add(new RelationshipUpdateWire(op, rel));
            }

            events.Add(new LogEvent(revision, updates, null, Array.Empty<CounterDeltaWire>(), null));
        }

        return (events, baseRevision);
    }

    /// <summary>Adds a caveat (~10%) and/or an expiration (~10%) — past, mid-log, or far-future.</summary>
    private static RelationshipWire Decorate(Random rng, RelationshipWire rel)
    {
        if (rng.NextDouble() < 0.1)
            rel = rel with { CaveatName = "is_active", CaveatContext = Context(rng.Next(10)) };
        if (rng.NextDouble() < 0.1)
        {
            rel = rel with
            {
                Expiration = rng.Next(3) switch
                {
                    0 => FixedBase.AddTicks(-(1 + rng.Next(1000))), // below every revision: dies at the first GC
                    1 => FixedBase.AddTicks(rng.Next(700)),         // inside the revision range: dies at some GC
                    _ => FixedBase.AddHours(1 + rng.Next(24)),      // far above the head: survives every GC
                },
            };
        }
        return rel;
    }

    // --- Canonical forms (caveat context compared via its serialized values, matching the
    // GrainBackedDatastoreFidelityTests idiom, since dictionaries compare by reference) ---

    private static string CanonRow(StoredRelationshipWire row) =>
        $"{CanonRel(row.Relationship)}|c={row.CreatedRevision}|d={(row.DeletedRevision is { } d ? d.ToString() : "live")}";

    private static string CanonRel(RelationshipWire r)
    {
        var caveat = r.CaveatName is { Length: > 0 } ? $"[{r.CaveatName}:{ContextString(r.CaveatContext)}]" : "";
        var exp = r.Expiration is { } e ? $"@exp={e:O}" : "";
        return $"{r.ResourceType}:{r.ResourceId}#{r.ResourceRelation}" +
               $"@{r.SubjectType}:{r.SubjectId}#{r.SubjectRelation}{caveat}{exp}";
    }

    private static string ContextString(IReadOnlyDictionary<string, object?>? ctx) =>
        ctx is null || ctx.Count == 0
            ? ""
            : string.Join(",", ctx.OrderBy(kv => kv.Key, StringComparer.Ordinal).Select(kv => $"{kv.Key}={kv.Value}"));

    private static string IdentityOf(RelationshipWire r) =>
        $"{r.ResourceType}:{r.ResourceId}#{r.ResourceRelation}@{r.SubjectType}:{r.SubjectId}#{r.SubjectRelation}";

    // --- Construction helpers ---

    private static LogEvent Ev(long revision, params RelationshipUpdateWire[] updates) =>
        new(revision, updates, null, Array.Empty<CounterDeltaWire>(), null);

    private static LogEvent Gc(long revision, long floor) =>
        new(revision, Array.Empty<RelationshipUpdateWire>(), null, Array.Empty<CounterDeltaWire>(), floor);

    private static RelationshipUpdateWire TouchOf(RelationshipWire r) => new(RelationshipUpdateOpWire.Touch, r);

    private static RelationshipUpdateWire DeleteOf(RelationshipWire r) => new(RelationshipUpdateOpWire.Delete, r);

    private static RelationshipWire Rel(
        string resourceType, string resourceId, string relation,
        string subjectType, string subjectId, string subjectRelation = Ellipsis,
        DateTimeOffset? exp = null) =>
        new(resourceType, resourceId, relation, subjectType, subjectId, subjectRelation, null, null, exp);

    private static IReadOnlyDictionary<string, object?> Context(int level) =>
        JsonSerializer.Deserialize<Dictionary<string, object?>>($$"""{ "level": {{level}} }""")!;

    private static string Pick(Random rng, string[] items) => items[rng.Next(items.Length)];

    private static long NanosSinceEpoch(DateTimeOffset dt) => (dt - DateTimeOffset.UnixEpoch).Ticks * 100L;
}
