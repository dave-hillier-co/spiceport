namespace Spiceport.Bench.Scenarios;

/// <summary>
/// A/B for <c>GraphPlacementOptions.CoLocateWithShards</c> (pressure point P4, enablement decision
/// 3.5): two multi-silo cluster boots with the SAME seed and identical workload, co-placement off
/// then on. Reports grain hops (the dispatch counter — every real ICheckGrain boundary crossing),
/// memo hit rate, and check p50/p99, plus the off-to-on delta row the decision needs. An untimed
/// global warm pass runs before arm A so tiered JIT and type initialization are not charged to it;
/// <c>--trials=N</c> repeats the full off/on sequence with per-trial rows so an operator can see
/// residual order effects.
/// </summary>
public static class PlacementAbScenario
{
    public const string Name = "placement-ab";

    public const string Help =
        "Identical workload with coLocateWithShards off then on (P4 / decision 3.5).\n" +
        "An untimed global warm pass precedes arm A (tiered-JIT/type-init cost paid up front).\n" +
        "  --check-workers=16            closed-loop check workers\n" +
        "  --trials=1                    repeats of the FULL off/on sequence (per-trial rows expose order effects)";

    public static async Task RunAsync(BenchArgs args)
    {
        args.Validate([
            .. BenchCommon.SharedFlagNames, .. BenchWorldOptions.FlagNames,
            "check-workers", "trials",
        ]);
        var seed = args.GetInt("seed", 42);
        var silos = args.GetInt("silos", 3);
        var duration = args.GetDuration("duration", TimeSpan.FromSeconds(10));
        var warmup = args.GetDuration("warmup", TimeSpan.FromSeconds(3));
        var checkWorkers = args.GetInt("check-workers", 16);
        var trials = args.GetInt("trials", 1);
        var jsonPath = BenchJson.ResolveOutputPath(args);

        var world = BenchWorld.Generate(seed, BenchWorldOptions.FromArgs(args));
        var mix = ConsistencyMix.Parse("100/0/0");

        await BenchCommon.GlobalWarmPassAsync(seed);

        var table = new ConsoleTable(
            "trial", "coLocate", "checks/s", "errs", "p50 ms", "p99 ms", "hops", "hops/check", "memo hit%");
        var cells = new List<object>();
        var warnings = new List<string>();

        for (var trial = 1; trial <= trials; trial++)
        {
            var perSide = new (double CheckRate, LatencySummary Lat, long Hops, double HopsPerCheck)[2];

            for (var side = 0; side < 2; side++)
            {
                var coLocate = side == 1;
                Console.Error.WriteLine(
                    $"trial {trial}/{trials}: booting {silos}-silo cluster (coLocateWithShards={coLocate}); " +
                    $"loading {world.Relationships.Count} relationships");
                await using var cluster = await BenchCommon.CreateClusterAsync(silos, coLocateWithShards: coLocate);
                var loadToken = await world.LoadAsync(cluster);

                var token = new BenchCommon.LastWriteToken(loadToken);
                var docSampler = new ZipfSampler(world.Documents.Count);
                var checkOp = BenchCommon.MakeCheckOp(cluster, world, docSampler, mix, token);
                var rngs = BenchCommon.WorkerRngs(seed, checkWorkers);

                var recorder = new LatencyRecorder();
                // Armed BEFORE the driver starts: the dispatch metrics reset at the warmup boundary
                // so the hop/memo snapshot covers the measured window only, matching the recorder.
                var boundaryReset = BenchCommon.ArmWarmupBoundaryResetAsync(cluster, warmup);
                var result = await BenchDriver.RunClosedLoop(
                    checkWorkers, warmup, duration, (w, _) => checkOp(rngs[w]), recorder);
                await boundaryReset;

                var lat = recorder.Summarize();
                var dispatch = cluster.MetricsSnapshot();
                // Denominator is ATTEMPTS (successes + errors): an errored check still dispatched
                // hops before failing, so excluding it would overstate hops per successful op.
                var attempts = lat.Count + lat.Errors;
                var hopsPerCheck = attempts == 0 ? 0 : (double)dispatch.Dispatch / attempts;
                var memoTotal = dispatch.MemoHit + dispatch.MemoMiss;
                var memoHitRate = memoTotal == 0 ? 0 : 100.0 * dispatch.MemoHit / memoTotal;
                perSide[side] = (result.MeasuredCompletionsPerSecond, lat, dispatch.Dispatch, hopsPerCheck);

                table.AddRow(
                    trial, coLocate ? "on" : "off", result.MeasuredCompletionsPerSecond.ToString("0.0"),
                    lat.Errors, lat.Ms(lat.P50Micros), lat.Ms(lat.P99Micros),
                    dispatch.Dispatch, hopsPerCheck.ToString("0.00"), memoHitRate.ToString("0.0"));
                if (lat.FirstError is not null)
                    warnings.Add($"trial {trial} coLocate={coLocate}: first check error: {lat.FirstError}");
                if (lat.SampleWarning("check") is { } sampleWarning)
                    warnings.Add($"trial {trial} coLocate={coLocate}: {sampleWarning}");
                cells.Add(new
                {
                    trial, coLocateWithShards = coLocate,
                    checksPerSecond = result.MeasuredCompletionsPerSecond,
                    check = lat, dispatch, hopsPerCheck, memoHitRatePercent = memoHitRate,
                });
            }

            var (off, on) = (perSide[0], perSide[1]);
            table.AddRow(
                trial, "delta",
                (on.CheckRate - off.CheckRate).ToString("+0.0;-0.0"),
                on.Lat.Errors - off.Lat.Errors,
                ((on.Lat.P50Micros - off.Lat.P50Micros) / 1000.0).ToString("+0.000;-0.000"),
                ((on.Lat.P99Micros - off.Lat.P99Micros) / 1000.0).ToString("+0.000;-0.000"),
                on.Hops - off.Hops,
                (on.HopsPerCheck - off.HopsPerCheck).ToString("+0.00;-0.00"),
                "");
        }

        Console.WriteLine();
        Console.WriteLine($"placement-ab  seed={seed} silos={silos} duration={duration} warmup={warmup} checkWorkers={checkWorkers} trials={trials}");
        table.Print(Console.Out);
        Console.WriteLine();
        Console.WriteLine("hops/check divides dispatch hops by check ATTEMPTS (successes + errors; errored checks still hop).");
        foreach (var warning in warnings)
            Console.WriteLine(warning);
        if (jsonPath is not null)
            BenchJson.Write(jsonPath, new { scenario = Name, seed, silos, duration, warmup, checkWorkers, trials, cells });
    }
}
