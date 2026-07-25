namespace Spiceport.Bench.Scenarios;

/// <summary>
/// Revision-quantization window sweep (pressure point P4, enablement decision 3.5): the same
/// seeded workload at each window (default 1s/5s/30s, the cluster knob MeshTestCluster threads
/// into GrainBackedDatastore). A low open-loop write rate keeps revisions advancing so the window
/// actually matters — a longer window shares grain keys (and CheckGrain activation memos) across
/// more of the run, at the cost of staler minimize-latency reads. Reports memo hit rate + latency,
/// plus the write side (achieved rate, errors, backlog) so a starved or failing writer arm cannot
/// silently invalidate the sweep. An untimed global warm pass runs before the first arm so tiered
/// JIT and type initialization are paid before arm A; <c>--trials=N</c> repeats the full window
/// sequence and prints per-trial rows so an operator can see residual order effects.
/// </summary>
public static class QuantizationSweepScenario
{
    public const string Name = "quantization-sweep";

    public const string Help =
        "Same workload at each revision-quantization window: memo hit rate + latency (P4 / decision 3.5).\n" +
        "An untimed global warm pass precedes the first arm (tiered-JIT/type-init cost paid up front).\n" +
        "  --windows=1s,5s,30s           quantization windows to sweep\n" +
        "  --check-workers=16            closed-loop check workers\n" +
        "  --write-rate=5                open-loop writes/second (keeps revisions advancing)\n" +
        "  --trials=1                    repeats of the FULL window sequence (per-trial rows expose order effects)";

    public static async Task RunAsync(BenchArgs args)
    {
        args.Validate([
            .. BenchCommon.SharedFlagNames, .. BenchWorldOptions.FlagNames,
            "windows", "check-workers", "write-rate", "trials",
        ]);
        var seed = args.GetInt("seed", 42);
        var silos = args.GetInt("silos", 2);
        var duration = args.GetDuration("duration", TimeSpan.FromSeconds(10));
        var warmup = args.GetDuration("warmup", TimeSpan.FromSeconds(3));
        var checkWorkers = args.GetInt("check-workers", 16);
        var writeRate = args.GetDouble("write-rate", 5);
        var trials = args.GetInt("trials", 1);
        var windows = args.GetDurationArray("windows",
            [TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(30)]);
        var jsonPath = BenchJson.ResolveOutputPath(args);

        var world = BenchWorld.Generate(seed, BenchWorldOptions.FromArgs(args));
        var mix = ConsistencyMix.Parse("100/0/0");

        await BenchCommon.GlobalWarmPassAsync(seed);

        var table = new ConsoleTable(
            "trial", "window", "checks/s", "chk errs", "p50 ms", "p99 ms", "memo hit%", "hops/check",
            "wr/s tgt", "wr/s got", "wr errs", "wr maxIF");
        var cells = new List<object>();
        var warnings = new List<string>();

        for (var trial = 1; trial <= trials; trial++)
        {
            foreach (var window in windows)
            {
                Console.Error.WriteLine(
                    $"trial {trial}/{trials}: booting {silos}-silo cluster (quantization={window}); " +
                    $"loading {world.Relationships.Count} relationships");
                await using var cluster = await BenchCommon.CreateClusterAsync(silos, quantization: window);
                var loadToken = await world.LoadAsync(cluster);

                var token = new BenchCommon.LastWriteToken(loadToken);
                var docSampler = new ZipfSampler(world.Documents.Count);
                var writeOp = BenchCommon.MakeWriteOp(cluster, world, docSampler, new Random(seed), token);
                var checkOp = BenchCommon.MakeCheckOp(cluster, world, docSampler, mix, token);
                var rngs = BenchCommon.WorkerRngs(seed, checkWorkers);

                var checkRecorder = new LatencyRecorder();
                var writeRecorder = new LatencyRecorder();
                // Armed BEFORE the drivers start: both metric seams reset at the warmup boundary so
                // the memo/hop snapshot covers the measured window only, matching the recorders.
                var boundaryReset = BenchCommon.ArmWarmupBoundaryResetAsync(cluster, warmup);
                var writesTask = BenchDriver.RunOpenLoop(writeRate, warmup, duration, writeOp, writeRecorder);
                var checksTask = BenchDriver.RunClosedLoop(
                    checkWorkers, warmup, duration, (w, _) => checkOp(rngs[w]), checkRecorder);
                var writes = await writesTask;
                var checks = await checksTask;
                await boundaryReset;

                var lat = checkRecorder.Summarize();
                var wr = writeRecorder.Summarize();
                var dispatch = cluster.MetricsSnapshot();
                var memoTotal = dispatch.MemoHit + dispatch.MemoMiss;
                var memoHitRate = memoTotal == 0 ? 0 : 100.0 * dispatch.MemoHit / memoTotal;
                // Denominator is ATTEMPTS (successes + errors): an errored check still dispatched
                // hops before failing, so excluding it would overstate hops per successful op.
                var checkAttempts = lat.Count + lat.Errors;
                var hopsPerCheck = checkAttempts == 0 ? 0 : (double)dispatch.Dispatch / checkAttempts;

                table.AddRow(
                    trial, window.TotalSeconds.ToString("0.#") + "s",
                    checks.MeasuredCompletionsPerSecond.ToString("0.0"), lat.Errors,
                    lat.Ms(lat.P50Micros), lat.Ms(lat.P99Micros),
                    memoHitRate.ToString("0.0"), hopsPerCheck.ToString("0.00"),
                    writeRate.ToString("0.0"), writes.MeasuredCompletionsPerSecond.ToString("0.0"),
                    wr.Errors, writes.MaxInFlight);
                if (lat.FirstError is not null)
                    warnings.Add($"trial {trial} window={window}: first check error: {lat.FirstError}");
                if (wr.FirstError is not null)
                    warnings.Add($"trial {trial} window={window}: first write error: {wr.FirstError}");
                foreach (var w in new[] { lat.SampleWarning("check"), wr.SampleWarning("write") })
                {
                    if (w is not null)
                        warnings.Add($"trial {trial} window={window}: {w}");
                }
                cells.Add(new
                {
                    trial, windowSeconds = window.TotalSeconds,
                    checksPerSecond = checks.MeasuredCompletionsPerSecond,
                    check = lat, dispatch, memoHitRatePercent = memoHitRate, hopsPerCheck,
                    writeRateTarget = writeRate,
                    writeRateAchieved = writes.MeasuredCompletionsPerSecond,
                    writeMaxInFlight = writes.MaxInFlight,
                    write = wr,
                });
            }
        }

        Console.WriteLine();
        Console.WriteLine($"quantization-sweep  seed={seed} silos={silos} duration={duration} warmup={warmup} checkWorkers={checkWorkers} writeRate={writeRate} trials={trials}");
        table.Print(Console.Out);
        Console.WriteLine();
        Console.WriteLine("hops/check divides dispatch hops by check ATTEMPTS (successes + errors; errored checks still hop).");
        foreach (var warning in warnings)
            Console.WriteLine(warning);
        if (jsonPath is not null)
            BenchJson.Write(jsonPath, new { scenario = Name, seed, silos, duration, warmup, checkWorkers, writeRate, trials, cells });
    }
}
