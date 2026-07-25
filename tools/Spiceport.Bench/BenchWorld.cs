using Spiceport.Core;
using Spiceport.Grains.Abstractions;
using Spiceport.Grains.Tests;

namespace Spiceport.Bench;

/// <summary>
/// Size/shape knobs for a generated benchmark world. Every knob feeds a deterministic generator
/// (<see cref="BenchWorld.Generate"/>): the same seed and options always produce the identical
/// relationship set, which is what makes A/B runs (placement on/off, quantization windows)
/// comparable — both sides load byte-identical worlds.
/// </summary>
/// <param name="Users">Regular users <c>u0..</c> (big usersets get their own dedicated users).</param>
/// <param name="Groups">Groups <c>g0..</c>; each gets ~<paramref name="GroupMeanSize"/> user members.</param>
/// <param name="GroupMeanSize">Mean direct members per group (sizes drawn uniformly in [1, 2*mean-1]).</param>
/// <param name="BigUsersetSizes">
/// EXTRA groups of exactly these member counts (default 1000 and 10000), each with a dedicated user
/// alphabet so membership is exact, plus one document (<c>bigdoc{size}</c>) whose viewer is the group —
/// the userset-sweep's point-membership targets.
/// </param>
/// <param name="Folders">Folders <c>f0..</c>, arranged in parent chains of <paramref name="FolderDepth"/>.</param>
/// <param name="FolderDepth">Chain length: <c>f_i</c> has parent <c>f_(i-1)</c> unless i is a chain start.</param>
/// <param name="Documents">Documents <c>d0..</c>; each has one viewer and a random parent folder.</param>
/// <param name="NestingDepth">
/// Group-in-group chain length: groups <c>g1..g_depth</c> each contain the previous group's members
/// (<c>g_i#member ∋ g_(i-1)#member</c>), edges always higher-index-contains-lower so the graph is a DAG
/// (the <c>RandomAuthzWorlds</c> acyclicity stance — the engine has no data-cycle cut on the verdict path).
/// </param>
/// <param name="WildcardDocShare">Fraction of documents whose viewer is the public wildcard <c>user:*</c>.</param>
public sealed record BenchWorldOptions(
    int Users = 200,
    int Groups = 50,
    int GroupMeanSize = 10,
    int[]? BigUsersetSizes = null,
    int Folders = 40,
    int FolderDepth = 4,
    int Documents = 400,
    int NestingDepth = 3,
    double WildcardDocShare = 0.05)
{
    public int[] EffectiveBigUsersetSizes => BigUsersetSizes ?? [1000, 10000];

    /// <summary>Reads the world-shape flags shared by every scenario (each has a small default).</summary>
    public static BenchWorldOptions FromArgs(BenchArgs args) => new(
        Users: args.GetInt("users", 200),
        Groups: args.GetInt("groups", 50),
        GroupMeanSize: args.GetInt("group-mean-size", 10),
        BigUsersetSizes: args.Has("big-userset-sizes") ? args.GetIntArray("big-userset-sizes", [1000, 10000]) : null,
        Folders: args.GetInt("folders", 40),
        FolderDepth: args.GetInt("folder-depth", 4),
        Documents: args.GetInt("documents", 400),
        NestingDepth: args.GetInt("nesting-depth", 3),
        WildcardDocShare: args.GetDouble("wildcard-doc-share", 0.05));

    /// <summary>The world-shape flag names, for scenarios' <see cref="BenchArgs.Validate"/> lists.</summary>
    public static readonly string[] FlagNames =
    [
        "users", "groups", "group-mean-size", "big-userset-sizes",
        "folders", "folder-depth", "documents", "nesting-depth", "wildcard-doc-share",
    ];
}

/// <summary>One big-userset target: the group, its known member/non-member, and its viewer document.</summary>
public sealed record BigUserset(int Size, string GroupId, string DocumentId, string MemberUserId, string NonMemberUserId)
{
    /// <summary>The id of the userset's <paramref name="index"/>-th member (index in [0, Size)) — the
    /// generator's id scheme, kept here so scenarios can probe DISTINCT members without duplicating it.</summary>
    public string MemberId(int index) => $"bigu{Size}_{index}";
}

/// <summary>
/// A deterministic seeded benchmark world over a FIXED schema covering the evaluation features the
/// engine actually exercises: direct relations, userset references (<c>group#member</c>), group
/// nesting, arrows (<c>parent-&gt;view</c>) over folder chains, and the public wildcard. The schema
/// is fixed (unlike <c>RandomAuthzWorlds</c>' per-seed permission templates) because the harness
/// compares CONFIGURATIONS, not schema shapes — every run must evaluate the same expressions.
/// </summary>
public sealed class BenchWorld
{
    /// <summary>The fixed benchmark schema (see class remarks for why it never varies per seed).</summary>
    public const string SchemaText = """
        definition user {}

        definition group {
            relation member: user | group#member

            permission view = member
        }

        definition folder {
            relation parent: folder
            relation viewer: user | group#member

            permission view = viewer + parent->view
        }

        definition document {
            relation parent: folder
            relation viewer: user | user:* | group#member

            permission view = viewer + parent->view
        }
        """;

    /// <summary>A user id that is never written into any relationship (the guaranteed non-member probe).</summary>
    public const string OutsiderUserId = "probe_outsider";

    public required BenchWorldOptions Options { get; init; }
    public required int Seed { get; init; }
    public required IReadOnlyList<string> Users { get; init; }
    public required IReadOnlyList<string> Groups { get; init; }
    public required IReadOnlyList<string> Folders { get; init; }
    public required IReadOnlyList<string> Documents { get; init; }
    public required IReadOnlyList<BigUserset> BigUsersets { get; init; }
    public required IReadOnlyList<RelationshipWire> Relationships { get; init; }

    /// <summary>Builds the deterministic world for (seed, options). Pure — no I/O, no wall clock.</summary>
    public static BenchWorld Generate(int seed, BenchWorldOptions options)
    {
        var rng = new Random(seed);
        var users = Ids("u", options.Users);
        var groups = Ids("g", options.Groups);
        var folders = Ids("f", options.Folders);
        var documents = Ids("d", options.Documents);

        var rels = new List<RelationshipWire>();
        var seen = new HashSet<string>();
        void Add(string resType, string resId, string relation, string subType, string subId, string subRel)
        {
            // Distinct rows only: a duplicate within one BulkImport batch would be a pointless no-op row.
            if (seen.Add($"{resType}:{resId}#{relation}@{subType}:{subId}#{subRel}"))
                rels.Add(new RelationshipWire(resType, resId, relation, subType, subId, subRel, null, null, null));
        }

        // Group direct membership: size uniform in [1, 2*mean-1] so the mean is GroupMeanSize.
        foreach (var group in groups)
        {
            var size = 1 + rng.Next(Math.Max(1, 2 * options.GroupMeanSize - 1));
            for (var i = 0; i < size; i++)
                Add("group", group, "member", "user", users[rng.Next(users.Count)], CoreConstants.Ellipsis);
        }

        // Group-in-group nesting chain g1..g_depth, always higher-index-contains-lower (DAG by construction).
        for (var i = 1; i <= Math.Min(options.NestingDepth, groups.Count - 1); i++)
            Add("group", groups[i], "member", "group", groups[i - 1], "member");

        // Folder parent chains of FolderDepth, plus one viewer per folder (70% user, 30% group#member).
        for (var i = 0; i < folders.Count; i++)
        {
            if (options.FolderDepth > 1 && i % options.FolderDepth != 0)
                Add("folder", folders[i], "parent", "folder", folders[i - 1], CoreConstants.Ellipsis);
            if (rng.NextDouble() < 0.7 || groups.Count == 0)
                Add("folder", folders[i], "viewer", "user", users[rng.Next(users.Count)], CoreConstants.Ellipsis);
            else
                Add("folder", folders[i], "viewer", "group", groups[rng.Next(groups.Count)], "member");
        }

        // Documents: one viewer (wildcard / user / group#member) and a random parent folder.
        foreach (var doc in documents)
        {
            var roll = rng.NextDouble();
            if (roll < options.WildcardDocShare)
                Add("document", doc, "viewer", "user", CoreConstants.PublicWildcard, CoreConstants.Ellipsis);
            else if (roll < options.WildcardDocShare + 0.6 || groups.Count == 0)
                Add("document", doc, "viewer", "user", users[rng.Next(users.Count)], CoreConstants.Ellipsis);
            else
                Add("document", doc, "viewer", "group", groups[rng.Next(groups.Count)], "member");
            if (folders.Count > 0)
                Add("document", doc, "parent", "folder", folders[rng.Next(folders.Count)], CoreConstants.Ellipsis);
        }

        // Big usersets: exact-size groups over dedicated user alphabets, each fronted by one document
        // whose (only) viewer is the group — the point-membership sweep target. The known member is the
        // MIDDLE user so a scan-order accident can never make it artificially cheap.
        var bigs = new List<BigUserset>();
        foreach (var size in options.EffectiveBigUsersetSizes)
        {
            var groupId = $"big{size}";
            var docId = $"bigdoc{size}";
            for (var i = 0; i < size; i++)
                Add("group", groupId, "member", "user", $"bigu{size}_{i}", CoreConstants.Ellipsis);
            Add("document", docId, "viewer", "group", groupId, "member");
            bigs.Add(new BigUserset(size, groupId, docId, $"bigu{size}_{size / 2}", OutsiderUserId));
        }

        return new BenchWorld
        {
            Options = options,
            Seed = seed,
            Users = users,
            Groups = groups,
            Folders = folders,
            Documents = documents,
            BigUsersets = bigs,
            Relationships = rels,
        };
    }

    /// <summary>
    /// Loads the world into a running cluster through the DECLARATIVE write path —
    /// <see cref="IRelationshipsGrain.BulkImportRelationships"/> in fixed-size batches, the fastest
    /// existing surface (create-or-overwrite, no per-row existence check, one commit per batch).
    /// Progress goes to stderr so stdout stays clean for result tables. Returns the last batch's
    /// commit token (the initial freshness floor for at-least-as-fresh checks).
    /// </summary>
    public async Task<string> LoadAsync(MeshTestCluster cluster, int batchSize = 1000)
    {
        var relationships = Relationships;
        var token = "";
        for (var offset = 0; offset < relationships.Count; offset += batchSize)
        {
            var batch = relationships.Skip(offset).Take(batchSize).ToArray();
            var reply = await cluster.Relationships.BulkImportRelationships(new BulkImportRelationshipsArgs(batch));
            token = reply.LoadedAtToken;
            var loaded = Math.Min(offset + batch.Length, relationships.Count);
            if (relationships.Count > 2 * batchSize)
                Console.Error.WriteLine($"  loaded {loaded}/{relationships.Count} relationships");
        }
        return token;
    }

    private static IReadOnlyList<string> Ids(string prefix, int count) =>
        Enumerable.Range(0, count).Select(i => $"{prefix}{i}").ToArray();
}
