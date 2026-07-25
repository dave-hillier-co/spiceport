using Spiceport.Core;

namespace Spiceport.Bench.Scenarios;

/// <summary>
/// Point-membership checks against documents whose direct viewer is a group of each configured
/// big-userset size, on a single-silo AND a multi-silo cluster (pressure point P3: today the whole
/// visible row set marshals per distinct sub-problem, so latency scaling with userset size — and
/// worsening cross-silo — is the subject-filter-pushdown trigger, docs/scalability-program.md 3.2).
/// Workers alternate a known member (the userset's middle user) and a guaranteed non-member.
/// </summary>
public static class UsersetSweepScenario
{
    public const string Name = "userset-sweep";

    public const string Help =
        "Check latency vs direct-viewer userset size, single-silo vs multi-silo (P3).\n" +
        "  --sizes=1000,10000            big-userset sizes (extra groups of exactly this many members)\n" +
        "  --workers=8                   closed-loop check workers per cell";

    public static async Task RunAsync(BenchArgs args)
    {
        args.Validate([
            .. BenchCommon.SharedFlagNames, .. BenchWorldOptions.FlagNames,
            "sizes", "workers",
        ]);
        var seed = args.GetInt("seed", 42);
        var multiSilos = args.GetInt("silos", 3);
        var duration = args.GetDuration("duration", TimeSpan.FromSeconds(6));
        var warmup = args.GetDuration("warmup", TimeSpan.FromSeconds(2));
        var workers = args.GetInt("workers", 8);
        var jsonPath = BenchJson.ResolveOutputPath(args);

        var baseOptions = BenchWorldOptions.FromArgs(args);
        var sizes = args.Has("sizes") ? args.GetIntArray("sizes", []) : baseOptions.EffectiveBigUsersetSizes;
        var world = BenchWorld.Generate(seed, baseOptions with { BigUsersetSizes = sizes });

        var table = new ConsoleTable("userset size", "silos", "checks/s", "errs", "p50 ms", "p99 ms", "max ms");
        var cells = new List<object>();
        var warnings = new List<string>();

        // Distinct: --silos=1 collapses the single-silo vs multi-silo comparison to one cluster
        // rather than booting an identical cluster twice and printing duplicate cells.
        foreach (var siloCount in new[] { 1, multiSilos }.Distinct())
        {
            Console.Error.WriteLine($"booting {siloCount}-silo cluster; loading {world.Relationships.Count} relationships");
            await using var cluster = await BenchCommon.CreateClusterAsync(siloCount);
            await world.LoadAsync(cluster);

            foreach (var big in world.BigUsersets)
            {
                var recorder = new LatencyRecorder();
                // Armed BEFORE the driver starts: both metric seams reset at the warmup boundary so
                // any post-run metrics inspection covers the measured window only.
                var boundaryReset = BenchCommon.ArmWarmupBoundaryResetAsync(cluster, warmup);
                var result = await BenchDriver.RunClosedLoop(workers, warmup, duration, async (worker, i) =>
                {
                    // Every op probes a DISTINCT subject (deterministic index walk per worker): a distinct
                    // subject is a distinct CheckGrain sub-problem, so each op forces a real shard read
                    // instead of hitting the activation memo — probing the SAME one or two subjects
                    // repeatedly measures the memo, not the userset fetch (the flaw an earlier version of
                    // this cell had, which made latency flat in userset size). Even ops probe a member,
                    // odd ops a synthetic never-written stranger (the exhausted path).
                    var user = (i & 1) == 0
                        ? big.MemberId((int)(((long)worker * 1_000_003 + i) % big.Size))
                        : $"stranger_{worker}_{i}";
                    await cluster.Checker.Check(
                        "document", big.DocumentId, "view",
                        new ObjectAndRelation("user", user, CoreConstants.Ellipsis),
                        caveatContext: null);
                }, recorder);
                await boundaryReset;

                var lat = recorder.Summarize();
                table.AddRow(
                    big.Size, siloCount, result.MeasuredCompletionsPerSecond.ToString("0.0"), lat.Errors,
                    lat.Ms(lat.P50Micros), lat.Ms(lat.P99Micros), lat.Ms(lat.MaxMicros));
                if (lat.FirstError is not null)
                    warnings.Add($"cell size={big.Size} silos={siloCount}: first check error: {lat.FirstError}");
                if (lat.SampleWarning("check") is { } sampleWarning)
                    warnings.Add($"cell size={big.Size} silos={siloCount}: {sampleWarning}");
                cells.Add(new
                {
                    usersetSize = big.Size, silos = siloCount,
                    checksPerSecond = result.MeasuredCompletionsPerSecond, check = lat,
                });
                Console.Error.WriteLine($"  cell done: size={big.Size} silos={siloCount}");
            }
        }

        Console.WriteLine();
        Console.WriteLine($"userset-sweep  seed={seed} duration={duration} warmup={warmup} workers={workers}");
        table.Print(Console.Out);
        foreach (var warning in warnings)
            Console.WriteLine(warning);
        if (jsonPath is not null)
            BenchJson.Write(jsonPath, new { scenario = Name, seed, duration, warmup, workers, cells });
    }
}
