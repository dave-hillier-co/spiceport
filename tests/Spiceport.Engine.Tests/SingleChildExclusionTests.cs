using System.Collections.Immutable;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Schema;

namespace Spiceport.Engine.Tests;

/// <summary>
/// An exclusion needs a base and at least one subtracted operand. SpiceDB's <c>difference</c> treats a
/// single-child exclusion as a bug (<c>MustBugf("difference requires more than a single child")</c>,
/// internal/graph/check.go) rather than evaluating it. The schema parser never produces one, so this pins
/// the behaviour for a malformed stored rewrite: it fails loudly instead of granting the base operand.
/// </summary>
public class SingleChildExclusionTests
{
    private const string Schema = """
        definition user {}

        definition document {
            relation viewer: user
            relation banned: user

            permission view = viewer - banned
        }
        """;

    private static ObjectAndRelation Onr(string type, string id, string relation = CoreConstants.Ellipsis) =>
        new(type, id, relation);

    private static async Task<(ReferenceDatastore Store, IDatastoreReader Reader)> Seed(params Relationship[] rels)
    {
        var store = new ReferenceDatastore();
        var rev = await store.ReadWriteTx(async tx =>
        {
            var updates = rels.Select(r => new RelationshipUpdate(r, UpdateOperation.Create)).ToList();
            await tx.WriteRelationships(updates);
        });
        return (store, store.SnapshotReader(rev));
    }

    private static ImmutableDictionary<string, NamespaceDefinition> WithViewRewrite(
        Func<SetOperation, SetOperation> rewrite)
    {
        var namespaces = SchemaCompiler.Compile(Schema).ToImmutableDictionary(ns => ns.Name);
        var document = namespaces["document"];
        var view = document.Relations.Single(r => r.Name == "view");
        var replaced = view with { UsersetRewrite = new UsersetRewrite(rewrite(view.UsersetRewrite!.Operation)) };
        return namespaces.SetItem("document", document with { Relations = document.Relations.Replace(view, replaced) });
    }

    private static async Task<DispatchCheckResult> CheckView(ImmutableDictionary<string, NamespaceDefinition> namespaces)
    {
        var (store, reader) = await Seed(
            Relationship.Create(Onr("document", "readme", "viewer"), Onr("user", "alice")));
        var dispatcher = new LocalDispatcher(namespaces, _ => reader, DateTimeOffset.UtcNow);
        var meta = new ResolverMeta(
            (await store.HeadRevision()).Revision,
            CheckEngine.DefaultMaxDepth,
            ImmutableHashSet<VisitKey>.Empty);
        return await dispatcher.DispatchCheck(
            new DispatchCheckRequest(Onr("document", "readme", "view"), Onr("user", "alice"), meta),
            CancellationToken.None);
    }

    [Fact]
    public async Task A_single_child_exclusion_fails_loudly_instead_of_returning_its_base()
    {
        var namespaces = WithViewRewrite(op => op with { Children = [op.Children[0]] });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => CheckView(namespaces));
        Assert.Contains("more than a single child", ex.Message);
    }

    [Fact]
    public async Task An_empty_exclusion_is_still_no_members()
    {
        var namespaces = WithViewRewrite(op => op with { Children = [] });

        var result = await CheckView(namespaces);

        Assert.False(result.Member);
    }

    [Fact]
    public async Task A_well_formed_exclusion_still_evaluates()
    {
        var result = await CheckView(WithViewRewrite(op => op));

        Assert.True(result.Member);
    }
}
