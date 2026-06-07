using System.Collections.Immutable;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Datastore.Memory;
using Spiceport.Engine;
using Spiceport.Schema;

namespace Spiceport.Engine.Tests;

/// <summary>
/// Proves the dispatch seam: a counting <see cref="IDispatcher"/> decorator wraps the local
/// dispatcher and confirms every sub-problem of a multi-relation check flows through it.
/// </summary>
public class DispatcherSeamTests
{
    private const string Schema = """
        definition user {}

        definition group {
            relation member: user | group#member
        }

        definition document {
            relation parent: document
            relation viewer: user | group#member

            permission view = viewer + parent->view
        }
        """;

    /// <summary>An <see cref="IDispatcher"/> decorator that counts and records every dispatched sub-problem.</summary>
    private sealed class CountingDispatcher(IDispatcher inner) : IDispatcher
    {
        public int Count { get; private set; }
        public List<(string Resource, string Subject)> Calls { get; } = [];

        public Task<DispatchCheckResult> DispatchCheck(DispatchCheckRequest request, CancellationToken ct)
        {
            Count++;
            Calls.Add((request.Resource.ToString(), request.Subject.ToString()));
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

    [Fact]
    public async Task SubProblems_FlowThroughDecorator_AndVerdictIsUnchanged()
    {
        // doc:readme view -> parent->view -> doc:root viewer -> group:eng#member -> user:alice
        var reader = await Seed(
            Tuple("document", "readme", "parent", Onr("document", "root")),
            Tuple("document", "root", "viewer", Onr("group", "eng", "member")),
            Tuple("group", "eng", "member", Onr("user", "alice")));

        var namespaces = SchemaCompiler.Compile(Schema)
            .ToImmutableDictionary(ns => ns.Name);
        var state = new CheckState();
        var local = new LocalDispatcher(
            namespaces,
            _ => reader,
            DateTimeOffset.UtcNow,
            state);

        var counting = new CountingDispatcher(local);
        // Route every sub-problem the local dispatcher generates back through the decorator.
        local.Dispatcher = counting;

        var meta = new ResolverMeta(
            InProcessRevisionForTest(),
            CheckEngine.DefaultMaxDepth,
            ImmutableHashSet<VisitKey>.Empty);
        var request = new DispatchCheckRequest(
            Onr("document", "readme", "view"),
            Onr("user", "alice"),
            meta);

        var result = await counting.DispatchCheck(request, CancellationToken.None);

        Assert.True(result.Member);
        Assert.Null(result.Caveat);
        Assert.False(result.CycleCut);

        // The top-level call plus every recursive sub-problem went through the decorator. The walk
        // visits more than just the root, proving recursion is delegated rather than self-called.
        Assert.True(counting.Count >= 4, $"expected multiple dispatched sub-problems, got {counting.Count}");
        Assert.Equal(state.DispatchCount, counting.Count);

        // Some specific sub-problems we expect to see routed through the seam.
        Assert.Contains(("document:readme#view", "user:alice"), counting.Calls);
        Assert.Contains(("document:root#viewer", "user:alice"), counting.Calls);
        Assert.Contains(("group:eng#member", "user:alice"), counting.Calls);
    }

    // The engine's in-process revision sentinel is internal; the reader resolver ignores it, so any
    // IRevision works here for driving the dispatcher directly.
    private static IRevision InProcessRevisionForTest()
    {
        var store = new InMemoryDatastore();
        // HeadRevision gives a real, comparable revision identity.
        return store.HeadRevision().GetAwaiter().GetResult().Revision;
    }
}
