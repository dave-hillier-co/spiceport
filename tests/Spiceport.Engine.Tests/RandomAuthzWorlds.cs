using Spiceport.Core;

namespace Spiceport.Engine.Tests;

/// <summary>
/// Hand-rolled deterministic world generator for the property/metamorphic gates
/// (<see cref="WalkEquivalencePropertyTests"/>, <see cref="MetamorphicInvariantTests"/>,
/// <see cref="CrossApiAgreementTests"/>). Given an integer seed, <see cref="Build"/> deterministically
/// produces a compilable schema plus a random relationship set entirely from <c>System.Random(seed)</c> --
/// no wall-clock, no ambient randomness, no external property-testing package.
/// </summary>
/// <remarks>
/// SCHEMA SHAPE: the relation structure (user / group / folder / document, with group nesting via
/// <c>group#member</c>, a <c>user:*</c> wildcard option on group membership, and a folder/document
/// <c>parent</c> arrow) is fixed across every seed so the generated worlds are always compilable and
/// share one query universe shape. What varies per seed is <c>document.view</c>'s permission expression,
/// chosen from <see cref="DocumentViewTemplates"/> -- a set of shapes that between them cover union,
/// intersection, exclusion and arrow composition. <c>document.view_mono</c> and <c>folder.view</c> are
/// deliberately union/arrow-only on EVERY seed (never intersection/exclusion) so the monotonicity gates
/// in <see cref="MetamorphicInvariantTests"/> always have a permission they can legally exercise.
/// <para>
/// GROUP/FOLDER NESTING IS A DAG BY CONSTRUCTION -- a generator design choice, not a schema limitation.
/// A <c>group#member</c> or <c>folder#parent</c> edge is only ever drawn from a higher-indexed alphabet
/// entry to a lower-indexed one, so <see cref="Build"/> can never produce a genuine reachability cycle.
/// This is deliberate: <c>CheckEngine</c>/<c>LocalDispatcher</c> has NO cycle-cut on the verdict path for
/// a real data cycle (see <c>LocalDispatcher.DispatchCheck</c>'s remarks) -- it consumes depth budget
/// until <c>MaxDepthExceededException</c>, exactly matching SpiceDB. That is correct engine behavior, not
/// a completeness bug, but it means a world with a real cycle has no well-defined Check verdict for a
/// metamorphic/cross-API gate to compare against. The membership-WALK accelerator, by contrast, DOES
/// cycle-cut cleanly (see <see cref="MembershipWalk.LocalClosure"/>'s visited set) -- its
/// termination-on-cycles property is exercised by a small, dedicated, hand-built cyclic graph in
/// <see cref="WalkEquivalencePropertyTests"/> instead, which never touches <see cref="CheckEngine"/>.
/// </para>
/// <para>
/// STATED LIMITS: no caveats and no expiration on any generated relationship -- the SpiceDB conformance
/// corpus already covers those axes; this generator is scoped to the plain-relation / userset / set-
/// operation / arrow / wildcard surface. The alphabet is small (5 users, 4 groups, 3 folders, 6
/// documents) by design: dense enough for nesting to have real depth, small enough that a query point
/// enumeration over the full universe (rather than sampling it) stays fast.
/// </para>
/// </remarks>
internal static class RandomAuthzWorlds
{
    /// <summary>Seeds 0..SeedCount-1 are the fixed, deterministic seed set every gate's [Theory] runs over.</summary>
    public const int SeedCount = 24;

    public static IEnumerable<object[]> Seeds => Enumerable.Range(0, SeedCount).Select(s => new object[] { s });

    // document.view templates: [0] union, [1] pure exclusion, [2] pure intersection, [3] union+exclusion,
    // [4] union+arrow, [5] intersection+arrow. Across the seed set every one of these gets exercised
    // (SeedCount is a multiple of the template count), so union / intersection / exclusion / arrow shapes
    // are all covered by the walk-equivalence and cross-API gates.
    private static readonly string[] DocumentViewTemplates =
    [
        "viewer + editor",
        "viewer - banned",
        "viewer & editor",
        "(viewer + editor) - banned",
        "viewer + parent->view",
        "(viewer & editor) + parent->view",
    ];

    public sealed record World(
        string SchemaText,
        IReadOnlyList<Relationship> Relationships,
        IReadOnlyList<string> Users,
        IReadOnlyList<string> Groups,
        IReadOnlyList<string> Folders,
        IReadOnlyList<string> Documents,
        int TemplateIndex);

    public static World Build(int seed)
    {
        var rng = new Random(seed);
        var users = Range("u", 5);
        var groups = Range("g", 4);
        var folders = Range("f", 3);
        var documents = Range("d", 6);

        var templateIndex = rng.Next(DocumentViewTemplates.Length);
        var schema = BuildSchema(DocumentViewTemplates[templateIndex]);

        var rowCount = 30 + rng.Next(91); // 30..120 relationship rows.
        var rels = new List<Relationship>(rowCount);
        for (var i = 0; i < rowCount; i++)
            rels.Add(RandomRelationship(rng, users, groups, folders, documents));

        return new World(schema, rels, users, groups, folders, documents, templateIndex);
    }

    /// <summary>
    /// Draws one random, schema-valid relationship over the given alphabet. Exposed (not just used
    /// internally by <see cref="Build"/>) so <see cref="MetamorphicInvariantTests"/>'s add-monotonicity
    /// gate can draw an additional, independently-random relationship to add to an already-built world.
    /// </summary>
    public static Relationship RandomRelationship(
        Random rng,
        IReadOnlyList<string> users, IReadOnlyList<string> groups,
        IReadOnlyList<string> folders, IReadOnlyList<string> documents)
    {
        var category = rng.Next(11);
        return category switch
        {
            0 => Rel("group", Pick(rng, groups), "member", Onr("user", Pick(rng, users))),
            1 => Rel("group", Pick(rng, groups), "member", Onr("user", CoreConstants.PublicWildcard)),
            // Acyclic by construction: the child (subject) group always has a strictly lower alphabet
            // index than the parent (resource) group -- see the DAG remarks on this class.
            2 => AcyclicNestedEdge(rng, groups, "group", "member", "member", resourceIsHigherIndex: true),
            3 => Rel("folder", Pick(rng, folders), "viewer", Onr("user", Pick(rng, users))),
            4 => Rel("folder", Pick(rng, folders), "viewer", Onr("group", Pick(rng, groups), "member")),
            // folder.parent's RESOURCE is the child folder and its SUBJECT is the parent folder, so the
            // higher alphabet index is the subject here (opposite of group.member's resource-is-container
            // shape) -- both directions strictly increase index from child to container, so both are
            // acyclic; only which side (resource/subject) plays "container" differs.
            5 => AcyclicNestedEdge(rng, folders, "folder", "parent", CoreConstants.Ellipsis, resourceIsHigherIndex: false),
            6 => Rel("document", Pick(rng, documents), "viewer", Onr("user", Pick(rng, users))),
            7 => Rel("document", Pick(rng, documents), "viewer", Onr("group", Pick(rng, groups), "member")),
            8 => Rel("document", Pick(rng, documents), "editor", Onr("user", Pick(rng, users))),
            9 => Rel("document", Pick(rng, documents), "banned", Onr("user", Pick(rng, users))),
            _ => Rel("document", Pick(rng, documents), "parent", Onr("folder", Pick(rng, folders))),
        };
    }

    // Draws a self-referential edge that can never participate in a cycle: one alphabet index is drawn
    // from [1, count-1] (the "container" side, e.g. the containing group or the parent folder) and the
    // other from [0, containerIndex-1] (the "contained" side), so every edge strictly orders container
    // above contained. `resourceIsHigherIndex` picks which side (resource or subject) is the container --
    // group.member's resource is the container; folder.parent's resource is the CHILD, so its subject is
    // the container instead. `subjectRelation` is `resRel` for a userset (group#member) or the ellipsis
    // for a plain-typed relation (folder.parent's subject is just `folder`).
    private static Relationship AcyclicNestedEdge(
        Random rng, IReadOnlyList<string> alphabet, string resType, string resRel, string subjectRelation,
        bool resourceIsHigherIndex)
    {
        var containerIdx = 1 + rng.Next(alphabet.Count - 1);
        var containedIdx = rng.Next(containerIdx);
        var (resIdx, subIdx) = resourceIsHigherIndex ? (containerIdx, containedIdx) : (containedIdx, containerIdx);
        return Rel(resType, alphabet[resIdx], resRel, Onr(resType, alphabet[subIdx], subjectRelation));
    }

    private static string BuildSchema(string documentViewExpr) => $$"""
        definition user {}

        definition group {
            relation member: user | user:* | group#member
        }

        definition folder {
            relation viewer: user | group#member
            relation parent: folder

            permission view = viewer + parent->view
        }

        definition document {
            relation viewer: user | group#member
            relation editor: user
            relation banned: user
            relation parent: folder

            permission view = {{documentViewExpr}}
            permission view_mono = viewer + editor + parent->view
        }
        """;

    private static IReadOnlyList<string> Range(string prefix, int count) =>
        Enumerable.Range(0, count).Select(i => $"{prefix}{i}").ToArray();

    private static string Pick(Random rng, IReadOnlyList<string> items) => items[rng.Next(items.Count)];

    private static ObjectAndRelation Onr(string type, string id, string relation = CoreConstants.Ellipsis) =>
        new(type, id, relation);

    private static Relationship Rel(string resType, string resId, string resRel, ObjectAndRelation subject) =>
        Relationship.Create(new ObjectAndRelation(resType, resId, resRel), subject);
}
