using Spiceport.Core;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Bench.Scenarios;

/// <summary>
/// Write-only sweep over precondition breadth x updates-per-commit (pressure points P1/P5): commit
/// p50/p99/p99.9/max (client-observed WriteRelationships latency) plus the flush count from
/// <c>ISequencerMetrics</c>. The p99.9-vs-p50 gap is the every-64th-commit flush sawtooth proxy.
/// Breadths: none, single (single-key MustNotMatch), type (type-wide MustNotMatch over document).
/// </summary>
public static class CommitBreadthScenario
{
    public const string Name = "commit-breadth";

    public const string Help =
        "Commit latency vs precondition breadth and updates-per-commit (P1/P5 sawtooth).\n" +
        "  --breadths=none,single,type   precondition levels (none / single-key MustNotMatch / type-wide MustNotMatch)\n" +
        "  --updates-per-commit=1,8,64   touched keys per commit\n" +
        "  --writers=4                   closed-loop writers (commits serialize on the sequencer regardless)";

    public static async Task RunAsync(BenchArgs args)
    {
        args.Validate([
            .. BenchCommon.SharedFlagNames, .. BenchWorldOptions.FlagNames,
            "breadths", "updates-per-commit", "writers",
        ]);
        var seed = args.GetInt("seed", 42);
        var silos = args.GetInt("silos", 1);
        var duration = args.GetDuration("duration", TimeSpan.FromSeconds(8));
        var warmup = args.GetDuration("warmup", TimeSpan.FromSeconds(2));
        var writers = args.GetInt("writers", 4);
        var breadths = args.GetStringArray("breadths", ["none", "single", "type"]);
        var updatesPerCommit = args.GetIntArray("updates-per-commit", [1, 8, 64]);
        var jsonPath = BenchJson.ResolveOutputPath(args);

        var world = BenchWorld.Generate(seed, BenchWorldOptions.FromArgs(args));
        Console.Error.WriteLine($"booting {silos}-silo cluster; loading {world.Relationships.Count} relationships");
        await using var cluster = await BenchCommon.CreateClusterAsync(silos);
        await world.LoadAsync(cluster);

        var table = new ConsoleTable(
            "breadth", "upd/commit", "commits/s", "errs", "p50 ms", "p99 ms", "p99.9 ms", "max ms", "flushes");
        var cells = new List<object>();
        var warnings = new List<string>();

        foreach (var breadth in breadths)
        {
            var preconditions = BuildPreconditions(breadth);
            foreach (var upc in updatesPerCommit)
            {
                var recorder = new LatencyRecorder();
                // Armed BEFORE the driver starts: both metric seams reset at the warmup boundary so
                // the flush count covers the measured window only, matching the latency recorder.
                var boundaryReset = BenchCommon.ArmWarmupBoundaryResetAsync(cluster, warmup);
                // Each commit TOUCHES `upc` DISTINCT document#viewer rows drawn round-robin from a
                // bounded per-writer key space (512 docs x the user alphabet), so tuples are unique
                // within a commit (a duplicate update in one write is rejected) and the world stays
                // bounded over any run length.
                var result = await BenchDriver.RunClosedLoop(writers, warmup, duration, async (worker, i) =>
                {
                    var updates = new RelationshipUpdateWire[upc];
                    for (var j = 0; j < upc; j++)
                    {
                        var k = i * upc + j;
                        updates[j] = new RelationshipUpdateWire(
                            RelationshipUpdateOpWire.Touch,
                            new RelationshipWire(
                                "document", $"cbdoc_{worker}_{k % 512}", "viewer",
                                "user", world.Users[(int)(k % world.Users.Count)], CoreConstants.Ellipsis,
                                null, null, null));
                    }
                    await cluster.Relationships.WriteRelationships(
                        new WriteRelationshipsArgs(updates, preconditions));
                }, recorder);
                await boundaryReset;

                var lat = recorder.Summarize();
                var seq = cluster.SequencerMetricsSnapshot();
                table.AddRow(
                    breadth, upc, result.MeasuredCompletionsPerSecond.ToString("0.0"), lat.Errors,
                    lat.Ms(lat.P50Micros), lat.Ms(lat.P99Micros), lat.Ms(lat.P999Micros), lat.Ms(lat.MaxMicros),
                    seq.Flush);
                if (lat.FirstError is not null)
                    warnings.Add($"cell breadth={breadth} upd/commit={upc}: first commit error: {lat.FirstError}");
                if (lat.SampleWarning("commit") is { } sampleWarning)
                    warnings.Add($"cell breadth={breadth} upd/commit={upc}: {sampleWarning}");
                cells.Add(new
                {
                    breadth, updatesPerCommit = upc,
                    commitsPerSecond = result.MeasuredCompletionsPerSecond,
                    commit = lat, sequencer = seq,
                });
                Console.Error.WriteLine($"  cell done: breadth={breadth} updatesPerCommit={upc}");
            }
        }

        Console.WriteLine();
        Console.WriteLine($"commit-breadth  seed={seed} silos={silos} duration={duration} warmup={warmup} writers={writers}");
        table.Print(Console.Out);
        Console.WriteLine();
        Console.WriteLine("p99.9 vs p50 gap is the flush-boundary sawtooth proxy (every-64th-commit flush).");
        foreach (var warning in warnings)
            Console.WriteLine(warning);
        if (jsonPath is not null)
            BenchJson.Write(jsonPath, new { scenario = Name, seed, silos, duration, warmup, writers, cells });
    }

    // Plain arrays throughout, not collection expressions: single-element collection expressions
    // lower to a compiler-synthesized list type Orleans has no copier for at the grain boundary.
    private static IReadOnlyList<PreconditionWire>? BuildPreconditions(string breadth) => breadth switch
    {
        "none" => null,
        // Single-key: a fully-specified filter naming one tuple that never exists — the narrowest
        // MustNotMatch a client can express.
        "single" => new[]
        {
            new PreconditionWire(
                PreconditionOpWire.MustNotMatch,
                new RelationshipsFilterWire(
                    ResourceType: "document", ResourceIdPrefix: null, ResourceIds: new[] { "never_doc" },
                    ResourceRelation: "viewer", SubjectType: "user", SubjectIds: new[] { "never_user" },
                    SubjectRelation: null)),
        },
        // Type-wide: only the resource type plus a never-written subject — forces the broad
        // candidate resolution P1 prices (still passes, since the subject never exists).
        "type" => new[]
        {
            new PreconditionWire(
                PreconditionOpWire.MustNotMatch,
                new RelationshipsFilterWire(
                    ResourceType: "document", ResourceIdPrefix: null, ResourceIds: null,
                    ResourceRelation: null, SubjectType: "user", SubjectIds: new[] { "never_user" },
                    SubjectRelation: null)),
        },
        _ => throw new BenchUsageException($"Unknown breadth '{breadth}' (expected none, single, or type)."),
    };
}
