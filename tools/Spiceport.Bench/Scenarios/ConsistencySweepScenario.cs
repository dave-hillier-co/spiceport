namespace Spiceport.Bench.Scenarios;

/// <summary>
/// Grid over consistency mixes x write rates (pressure point P2: fully-consistent fan-in shows up
/// as check p99 degrading with write rate). Per cell: closed-loop check workers + an open-loop
/// paced writer, reporting check p50/p99, throughput, and the sequencer inbound-call decomposition
/// (ReadFrom vs GetHead is the fan-in signature). Every cell boots a FRESH cluster and reloads the
/// seeded world, so no cell inherits the previous cell's CheckGrain memos, warm activations, or
/// accumulated log length — that isolation is what makes cells comparable, and it is paid for in
/// wall-clock time (one cluster boot + world load per cell; a full default grid is 12 boots).
/// Correctness of the comparison beats speed in a manual tool.
/// </summary>
public static class ConsistencySweepScenario
{
    public const string Name = "consistency-sweep";

    public const string Help =
        "Check latency vs consistency mix at varying write rates (P2 fan-in).\n" +
        "Each grid cell boots a fresh cluster and reloads the world (isolation over speed).\n" +
        "  --mixes=100/0/0,0/100/0,...   consistency mixes, each min-latency/at-least-as-fresh/fully-consistent\n" +
        "  --write-rates=0,25,100        open-loop write arrival rates per second\n" +
        "  --check-workers=16            closed-loop check workers (tails underestimate under saturation)";

    public static async Task RunAsync(BenchArgs args)
    {
        args.Validate([
            .. BenchCommon.SharedFlagNames, .. BenchWorldOptions.FlagNames,
            "mixes", "write-rates", "check-workers",
        ]);
        var seed = args.GetInt("seed", 42);
        var silos = args.GetInt("silos", 2);
        var duration = args.GetDuration("duration", TimeSpan.FromSeconds(10));
        var warmup = args.GetDuration("warmup", TimeSpan.FromSeconds(3));
        var checkWorkers = args.GetInt("check-workers", 16);
        var writeRates = args.GetIntArray("write-rates", [0, 25, 100]);
        var mixes = args.GetStringArray("mixes", ["100/0/0", "0/100/0", "0/0/100", "70/20/10"])
            .Select(ConsistencyMix.Parse).ToArray();
        var jsonPath = BenchJson.ResolveOutputPath(args);

        var world = BenchWorld.Generate(seed, BenchWorldOptions.FromArgs(args));

        var table = new ConsoleTable(
            "mix", "wr/s tgt", "wr/s got", "wr errs", "wr maxIF", "chk/s", "chk p50 ms", "chk p99 ms", "wr p99 ms",
            "Commit", "ReadFrom", "GetHead", "ReadShard");
        var cells = new List<object>();
        var warnings = new List<string>();

        foreach (var mix in mixes)
        {
            foreach (var writeRate in writeRates)
            {
                // Fresh cluster per cell — see the class remarks: memo/activation/log isolation.
                Console.Error.WriteLine(
                    $"booting {silos}-silo cluster for cell mix={mix} writeRate={writeRate}; " +
                    $"loading {world.Relationships.Count} relationships");
                await using var cluster = await BenchCommon.CreateClusterAsync(silos);
                var loadToken = await world.LoadAsync(cluster);

                var token = new BenchCommon.LastWriteToken(loadToken);
                var docSampler = new ZipfSampler(world.Documents.Count);
                var writeOp = BenchCommon.MakeWriteOp(cluster, world, docSampler, new Random(seed), token);
                var checkOp = BenchCommon.MakeCheckOp(cluster, world, docSampler, mix, token);
                var rngs = BenchCommon.WorkerRngs(seed, checkWorkers);

                var checkRecorder = new LatencyRecorder();
                var writeRecorder = new LatencyRecorder();
                // Armed BEFORE the drivers start so both metric seams reset at the warmup boundary:
                // the snapshots below then cover the measured window only, matching the recorders.
                var boundaryReset = BenchCommon.ArmWarmupBoundaryResetAsync(cluster, warmup);
                var writesTask = BenchDriver.RunOpenLoop(writeRate, warmup, duration, writeOp, writeRecorder);
                var checksTask = BenchDriver.RunClosedLoop(
                    checkWorkers, warmup, duration, (w, _) => checkOp(rngs[w]), checkRecorder);
                var writes = await writesTask;
                var checks = await checksTask;
                await boundaryReset;

                var chk = checkRecorder.Summarize();
                var wr = writeRecorder.Summarize();
                var seq = cluster.SequencerMetricsSnapshot();
                table.AddRow(
                    mix, writeRate, writes.MeasuredCompletionsPerSecond.ToString("0.0"),
                    wr.Errors, writes.MaxInFlight,
                    checks.MeasuredCompletionsPerSecond.ToString("0.0"),
                    chk.Ms(chk.P50Micros), chk.Ms(chk.P99Micros), wr.Ms(wr.P99Micros),
                    seq.Commit, seq.ReadFrom, seq.GetHead, seq.ReadShard);
                if (wr.FirstError is not null)
                    warnings.Add($"cell mix={mix} writeRate={writeRate}: first write error: {wr.FirstError}");
                if (chk.FirstError is not null)
                    warnings.Add($"cell mix={mix} writeRate={writeRate}: first check error: {chk.FirstError}");
                foreach (var opClass in new[] { chk.SampleWarning("check"), wr.SampleWarning("write") })
                {
                    if (opClass is not null)
                        warnings.Add($"cell mix={mix} writeRate={writeRate}: {opClass}");
                }
                cells.Add(new
                {
                    mix = mix.ToString(), writeRateTarget = writeRate,
                    writeRateAchieved = writes.MeasuredCompletionsPerSecond,
                    writeMaxInFlight = writes.MaxInFlight,
                    checksPerSecond = checks.MeasuredCompletionsPerSecond,
                    check = chk, write = wr, sequencer = seq,
                });
                Console.Error.WriteLine($"  cell done: mix={mix} writeRate={writeRate}");
            }
        }

        Console.WriteLine();
        Console.WriteLine($"consistency-sweep  seed={seed} silos={silos} duration={duration} warmup={warmup} checkWorkers={checkWorkers}");
        table.Print(Console.Out);
        foreach (var warning in warnings)
            Console.WriteLine(warning);
        if (jsonPath is not null)
            BenchJson.Write(jsonPath, new { scenario = Name, seed, silos, duration, warmup, checkWorkers, cells });
    }
}
