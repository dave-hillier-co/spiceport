using System.Collections.Immutable;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Datastore.Memory;
using Spiceport.Engine;
using Spiceport.Schema;

namespace Spiceport.Engine.Tests;

/// <summary>
/// Covers the caching dispatcher and revision quantization composed over the local dispatcher:
/// repeated sub-problems are served from cache, context-varying caveat checks still collapse
/// correctly despite a shared cached branch, cycle-cut results are not cached, and near-in-time
/// revisions bucket together.
/// </summary>
public class CachingDispatcherTests
{
    private const string Schema = """
        definition user {}

        definition group {
            relation member: user | group#member

            permission can_view = member
        }

        definition document {
            relation parent: document
            relation viewer: user | group#member

            permission view = viewer + parent->view
        }
        """;

    /// <summary>Counts every sub-problem that reaches the delegate (i.e. cache misses).</summary>
    private sealed class CountingDispatcher(IDispatcher inner) : IDispatcher
    {
        public int Count { get; private set; }

        public Task<DispatchCheckResult> DispatchCheck(DispatchCheckRequest request, CancellationToken ct)
        {
            Count++;
            return inner.DispatchCheck(request, ct);
        }
    }

    private static async Task<IDatastoreReader> Seed(params Relationship[] rels)
    {
        var store = new InMemoryDatastore();
        var rev = await store.ReadWriteTx(async tx =>
        {
            var updates = rels.Select(r => new RelationshipUpdate(r, UpdateOperation.Create)).ToList();
            await tx.WriteRelationships(updates);
        });
        return store.SnapshotReader(rev);
    }

    private static ObjectAndRelation Onr(string type, string id, string relation = CoreConstants.Ellipsis) =>
        new(type, id, relation);

    private static Relationship Tuple(string resType, string resId, string resRel, ObjectAndRelation subject) =>
        Relationship.Create(new ObjectAndRelation(resType, resId, resRel), subject);

    private static ImmutableDictionary<string, NamespaceDefinition> Compile(string schema) =>
        SchemaCompiler.Compile(schema).ToImmutableDictionary(ns => ns.Name);

    private static DispatchCheckRequest Request(IRevision rev, ObjectAndRelation resource, ObjectAndRelation subject) =>
        new(resource, subject,
            new ResolverMeta(rev, CheckEngine.DefaultMaxDepth, ImmutableHashSet<VisitKey>.Empty));

    private static IRevision Rev(long nanos) => new TimestampRevision(nanos);

    // (a) A repeated identical sub-problem is served from cache: the delegate is invoked once.
    [Fact]
    public async Task RepeatedIdenticalSubProblem_ServedFromCache()
    {
        var reader = await Seed(Tuple("group", "eng", "member", Onr("user", "alice")));
        var namespaces = Compile(Schema);
        var state = new CheckState();
        var rev = Rev(1_000_000_000);

        var local = new LocalDispatcher(namespaces, _ => reader, DateTimeOffset.UtcNow, state);
        var counting = new CountingDispatcher(local);
        var caching = new CachingDispatcher(
            counting,
            new InMemoryDispatchCache(),
            new TimestampRevisionQuantizer(),
            SchemaHash.Compute(namespaces));
        local.Dispatcher = caching;

        var request = Request(rev, Onr("group", "eng", "member"), Onr("user", "alice"));

        var first = await caching.DispatchCheck(request, CancellationToken.None);
        var countAfterFirst = counting.Count;
        var second = await caching.DispatchCheck(request, CancellationToken.None);

        Assert.True(first.Member);
        Assert.True(second.Member);
        Assert.True(countAfterFirst >= 1);
        // The second top-level dispatch hit the cache: no new miss reached the delegate.
        Assert.Equal(countAfterFirst, counting.Count);
    }

    // (b) Two checks with the SAME relationships but DIFFERENT caveat context produce correct
    // (different) verdicts despite sharing a cached pre-context branch.
    [Fact]
    public async Task SameRelationships_DifferentContext_DifferentVerdicts_WithSharedCache()
    {
        const string caveatSchema = """
            caveat over_age(age int, min_age int) {
              age >= min_age
            }

            definition user {}

            definition document {
              relation viewer: user with over_age
              permission view = viewer
            }
            """;

        var reader = await Seed(
            Relationship.Create(
                new ObjectAndRelation("document", "doc1", "viewer"),
                Onr("user", "alice"),
                new ContextualizedCaveat("over_age",
                    new Dictionary<string, object?> { ["min_age"] = 18 })));

        var compiled = SchemaCompiler.CompileSchema(caveatSchema);
        // A shared engine => a shared cache: both checks key the SAME pre-context branch.
        var engine = CheckEngine.WithCaching(compiled.Namespaces, compiled.Caveats);

        var over = await engine.Check(
            reader, "document", "doc1", "view", Onr("user", "alice"),
            caveatContext: new Dictionary<string, object?> { ["age"] = 21 });

        var under = await engine.Check(
            reader, "document", "doc1", "view", Onr("user", "alice"),
            caveatContext: new Dictionary<string, object?> { ["age"] = 16 });

        Assert.Equal(Membership.Member, over.Verdict);
        Assert.Equal(Membership.NotMember, under.Verdict);
    }

    // (c) A cycle-cut result is not cached.
    [Fact]
    public async Task CycleCutResult_IsNotCached()
    {
        // group:a#member -> group:b#member -> group:a#member  (a self-referential cycle)
        var reader = await Seed(
            Tuple("group", "a", "member", Onr("group", "b", "member")),
            Tuple("group", "b", "member", Onr("group", "a", "member")));
        var namespaces = Compile(Schema);
        var state = new CheckState();
        var rev = Rev(2_000_000_000);

        var cache = new InMemoryDispatchCache();
        var local = new LocalDispatcher(namespaces, _ => reader, DateTimeOffset.UtcNow, state);
        var caching = new CachingDispatcher(
            local, cache, new TimestampRevisionQuantizer(), SchemaHash.Compute(namespaces));
        local.Dispatcher = caching;

        var request = Request(rev, Onr("group", "a", "member"), Onr("user", "ghost"));
        var result = await caching.DispatchCheck(request, CancellationToken.None);

        Assert.False(result.Member);
        Assert.True(result.CycleCut);
        // The cycle-cut top-level branch must not have been stored.
        Assert.Equal(0, cache.Count);
    }

    // (d) Quantization buckets two near-in-time revisions together.
    [Fact]
    public void Quantization_BucketsNearRevisionsTogether()
    {
        var quantizer = new TimestampRevisionQuantizer(TimeSpan.FromSeconds(5));
        // 5s window in nanos = 5_000_000_000.
        var baseNanos = 5_000_000_000L * 3; // exactly on a window boundary
        var a = quantizer.Quantize(new TimestampRevision(baseNanos + 1_000_000)); // +1ms
        var b = quantizer.Quantize(new TimestampRevision(baseNanos + 4_000_000_000)); // +4s, same window
        var c = quantizer.Quantize(new TimestampRevision(baseNanos + 6_000_000_000)); // +6s, next window

        Assert.Equal(a, b);
        Assert.NotEqual(a, c);
    }

    // The caching layer must not change verdicts for a plain (non-caveated) repeated check.
    [Fact]
    public async Task CachingEngine_ProducesSameVerdictAsUncached()
    {
        var reader = await Seed(
            Tuple("document", "readme", "parent", Onr("document", "root")),
            Tuple("document", "root", "viewer", Onr("group", "eng", "member")),
            Tuple("group", "eng", "member", Onr("user", "alice")));

        var ns = Compile(Schema).Values.ToList();
        var uncached = new CheckEngine(ns);
        var cached = CheckEngine.WithCaching(ns);

        var u = await uncached.Check(reader, "document", "readme", "view", Onr("user", "alice"));
        var c1 = await cached.Check(reader, "document", "readme", "view", Onr("user", "alice"));
        var c2 = await cached.Check(reader, "document", "readme", "view", Onr("user", "alice"));

        Assert.Equal(u.Verdict, c1.Verdict);
        Assert.Equal(u.Verdict, c2.Verdict);
        Assert.Equal(Membership.Member, c2.Verdict);
    }
}
