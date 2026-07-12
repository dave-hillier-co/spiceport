using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Schema;

namespace Spiceport.Engine.Tests;

/// <summary>
/// Gate 2 -- metamorphic invariants that must hold for ANY world <see cref="RandomAuthzWorlds"/> can
/// produce, checked purely against <see cref="CheckEngine"/> (no accelerator involved) so these are
/// engine-semantics gates, not accelerator-completeness gates:
/// <list type="bullet">
/// <item>
/// (a) IRRELEVANT-TUPLE INVARIANCE: writing a relationship on a brand-new, otherwise-unreferenced
/// resource id never changes any previously computed Check verdict at the new revision. This holds for
/// EVERY schema shape (union/intersection/exclusion/arrow) because the new row cannot participate in any
/// existing resource's evaluation -- documents never appear as a subject anywhere in the generated
/// schema, so a fresh document-typed row is provably inert.
/// </item>
/// <item>
/// (b) DELETE MONOTONICITY: deleting one relationship never turns a NotMember verdict into a Member
/// verdict.
/// </item>
/// <item>
/// (c) ADD MONOTONICITY: adding one (possibly already-present) relationship never turns a Member verdict
/// into a NotMember verdict.
/// </item>
/// </list>
/// (b) and (c) only hold for permissions built purely from union (and arrows over union-only
/// permissions) -- removing or adding evidence for an intersection or exclusion operand can flip a
/// verdict in EITHER direction, so those two gates are deliberately checked against
/// <c>document.view_mono</c> (fixed by <see cref="RandomAuthzWorlds"/> as
/// <c>viewer + editor + parent-&gt;view</c>, with <c>folder.view = viewer + parent-&gt;view</c>) which is
/// union/arrow-only on EVERY seed, independent of which template the seed drew for the (possibly
/// non-monotone) `document.view`. (a) has no such restriction and is checked against `document.view`
/// (the seed's varying template) for broader shape coverage.
/// </summary>
/// <remarks>
/// STATED LIMITS (see <see cref="RandomAuthzWorlds"/>): no caveats/expiration, so every verdict is
/// exactly <see cref="Membership.Member"/> or <see cref="Membership.NotMember"/>; a capped sample of
/// query points per seed keeps the whole file well under a minute.
/// </remarks>
public class MetamorphicInvariantTests
{
    public static IEnumerable<object[]> Seeds => RandomAuthzWorlds.Seeds;

    private const int SampleCap = 8;

    [Theory]
    [MemberData(nameof(Seeds))]
    public async Task IrrelevantTuple_DoesNotChangeAnyVerdict(int seed)
    {
        var world = RandomAuthzWorlds.Build(seed);
        var compiled = SchemaCompiler.CompileSchema(world.SchemaText);
        var check = new CheckEngine(compiled.Namespaces, compiled.Caveats);

        var store = new ReferenceDatastore();
        var rev = await store.ReadWriteTx(tx => tx.WriteRelationships(
            world.Relationships.Select(r => new RelationshipUpdate(r, UpdateOperation.Touch)).ToList()));

        var sample = SampleUniverse(world, seed);
        var readerBefore = store.SnapshotReader(rev);
        var before = new Dictionary<(string DocId, string UserId), Membership>();
        foreach (var point in sample)
            before[point] = (await check.Check(readerBefore, "document", point.DocId, "view", Onr("user", point.UserId))).Verdict;

        // A resource id outside the alphabet, on a relation whose object type (document) is never an
        // allowed subject type anywhere in the schema -- so this write cannot participate in any other
        // resource's evaluation, regardless of which document.view template the seed drew.
        var freshDoc = "fresh_" + seed;
        var freshRel = Relationship.Create(
            new ObjectAndRelation("document", freshDoc, "viewer"), Onr("user", world.Users[0]));
        var rev2 = await store.ReadWriteTx(tx => tx.WriteRelationships(
            [new RelationshipUpdate(freshRel, UpdateOperation.Touch)]));
        var readerAfter = store.SnapshotReader(rev2);

        foreach (var point in sample)
        {
            var after = (await check.Check(readerAfter, "document", point.DocId, "view", Onr("user", point.UserId))).Verdict;
            Assert.True(before[point] == after,
                $"seed={seed}: irrelevant-tuple invariance broken for document:{point.DocId}/view, user:{point.UserId}: " +
                $"was {before[point]}, now {after} after writing unrelated {freshRel}");
        }
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public async Task DeleteRelationship_NeverTurnsNotMemberIntoMember(int seed)
    {
        var world = RandomAuthzWorlds.Build(seed);
        if (world.Relationships.Count == 0)
            return;

        var compiled = SchemaCompiler.CompileSchema(world.SchemaText);
        var check = new CheckEngine(compiled.Namespaces, compiled.Caveats);

        var store = new ReferenceDatastore();
        var rev = await store.ReadWriteTx(tx => tx.WriteRelationships(
            world.Relationships.Select(r => new RelationshipUpdate(r, UpdateOperation.Touch)).ToList()));

        var rng = new Random(unchecked(seed * 7919 + 1));
        var toDelete = world.Relationships[rng.Next(world.Relationships.Count)];

        var sample = SampleUniverse(world, seed);
        var readerBefore = store.SnapshotReader(rev);
        var before = new Dictionary<(string DocId, string UserId), Membership>();
        foreach (var point in sample)
            before[point] = (await check.Check(readerBefore, "document", point.DocId, "view_mono", Onr("user", point.UserId))).Verdict;

        var rev2 = await store.ReadWriteTx(tx => tx.WriteRelationships(
            [new RelationshipUpdate(toDelete, UpdateOperation.Delete)]));
        var readerAfter = store.SnapshotReader(rev2);

        foreach (var point in sample)
        {
            var after = (await check.Check(readerAfter, "document", point.DocId, "view_mono", Onr("user", point.UserId))).Verdict;
            Assert.False(before[point] == Membership.NotMember && after == Membership.Member,
                $"seed={seed}: delete monotonicity broken -- deleting {toDelete} turned document:{point.DocId}/view_mono, " +
                $"user:{point.UserId} from NotMember into Member");
        }
    }

    [Theory]
    [MemberData(nameof(Seeds))]
    public async Task AddRelationship_NeverTurnsMemberIntoNotMember(int seed)
    {
        var world = RandomAuthzWorlds.Build(seed);
        var compiled = SchemaCompiler.CompileSchema(world.SchemaText);
        var check = new CheckEngine(compiled.Namespaces, compiled.Caveats);

        var store = new ReferenceDatastore();
        var rev = await store.ReadWriteTx(tx => tx.WriteRelationships(
            world.Relationships.Select(r => new RelationshipUpdate(r, UpdateOperation.Touch)).ToList()));

        var sample = SampleUniverse(world, seed);
        var readerBefore = store.SnapshotReader(rev);
        var before = new Dictionary<(string DocId, string UserId), Membership>();
        foreach (var point in sample)
            before[point] = (await check.Check(readerBefore, "document", point.DocId, "view_mono", Onr("user", point.UserId))).Verdict;

        var rng = new Random(unchecked(seed * 104729 + 1));
        var toAdd = RandomAuthzWorlds.RandomRelationship(rng, world.Users, world.Groups, world.Folders, world.Documents);

        var rev2 = await store.ReadWriteTx(tx => tx.WriteRelationships(
            [new RelationshipUpdate(toAdd, UpdateOperation.Touch)]));
        var readerAfter = store.SnapshotReader(rev2);

        foreach (var point in sample)
        {
            var after = (await check.Check(readerAfter, "document", point.DocId, "view_mono", Onr("user", point.UserId))).Verdict;
            Assert.False(before[point] == Membership.Member && after == Membership.NotMember,
                $"seed={seed}: add monotonicity broken -- adding {toAdd} turned document:{point.DocId}/view_mono, " +
                $"user:{point.UserId} from Member into NotMember");
        }
    }

    private static ObjectAndRelation Onr(string type, string id, string relation = CoreConstants.Ellipsis) =>
        new(type, id, relation);

    private static List<(string DocId, string UserId)> SampleUniverse(RandomAuthzWorlds.World world, int seed)
    {
        var rng = new Random(unchecked(seed * 31 + 17));
        var pairs = new HashSet<(string, string)>();
        while (pairs.Count < SampleCap)
        {
            var doc = world.Documents[rng.Next(world.Documents.Count)];
            var user = world.Users[rng.Next(world.Users.Count)];
            pairs.Add((doc, user));
        }
        return [.. pairs];
    }
}
