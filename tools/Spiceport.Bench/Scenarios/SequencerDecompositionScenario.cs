namespace Spiceport.Bench.Scenarios;

/// <summary>
/// Mixed read/write load, then the full <c>ISequencerMetrics</c> snapshot as a table — the direct
/// observable for pressure points P2 (ReadFrom fan-in) and P6 (ReadState admin load), plus the
/// commit-duration and candidate-breadth decomposition for P1/P5. This is the "where do the
/// sequencer's turns go" scenario the other cells reference.
/// </summary>
public static class SequencerDecompositionScenario
{
    public const string Name = "sequencer-decomposition";

    public const string Help =
        "Mixed read/write load; prints the sequencer inbound-call decomposition (P1/P2/P5/P6).\n" +
        "  --check-workers=8             closed-loop check workers\n" +
        "  --write-rate=20               open-loop writes/second\n" +
        "  --mix=70/20/10                check consistency mix (min-latency/at-least-as-fresh/fully-consistent)";

    public static async Task RunAsync(BenchArgs args)
    {
        args.Validate([
            .. BenchCommon.SharedFlagNames, .. BenchWorldOptions.FlagNames,
            "check-workers", "write-rate", "mix",
        ]);
        var seed = args.GetInt("seed", 42);
        var silos = args.GetInt("silos", 2);
        var duration = args.GetDuration("duration", TimeSpan.FromSeconds(10));
        var warmup = args.GetDuration("warmup", TimeSpan.FromSeconds(2));
        var checkWorkers = args.GetInt("check-workers", 8);
        var writeRate = args.GetDouble("write-rate", 20);
        var mix = ConsistencyMix.Parse(args.GetString("mix", "70/20/10")!);
        var jsonPath = BenchJson.ResolveOutputPath(args);

        var world = BenchWorld.Generate(seed, BenchWorldOptions.FromArgs(args));
        Console.Error.WriteLine($"booting {silos}-silo cluster; loading {world.Relationships.Count} relationships");
        await using var cluster = await BenchCommon.CreateClusterAsync(silos);
        var loadToken = await world.LoadAsync(cluster);

        var token = new BenchCommon.LastWriteToken(loadToken);
        var docSampler = new ZipfSampler(world.Documents.Count);
        var writeOp = BenchCommon.MakeWriteOp(cluster, world, docSampler, new Random(seed), token);
        var checkOp = BenchCommon.MakeCheckOp(cluster, world, docSampler, mix, token);
        var rngs = BenchCommon.WorkerRngs(seed, checkWorkers);

        var checkRecorder = new LatencyRecorder();
        var writeRecorder = new LatencyRecorder();
        // Armed BEFORE the drivers start: both metric seams reset at the warmup boundary so the
        // sequencer/dispatch decompositions cover the measured window only, matching the recorders.
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
        var dispatch = cluster.MetricsSnapshot();

        Console.WriteLine();
        Console.WriteLine($"sequencer-decomposition  seed={seed} silos={silos} duration={duration} warmup={warmup} checkWorkers={checkWorkers} writeRate={writeRate} mix={mix}");
        Console.WriteLine();
        // maxIF is the open-loop backlog signal (peak concurrently in-flight ops): the writer arm is
        // the open-loop one, so a maxIF far above writeRate x typical-latency means the pacer was
        // outrunning the system (saturation). The closed-loop check arm's maxIF is its worker count.
        var load = new ConsoleTable("op class", "ops/s", "p50 ms", "p99 ms", "max ms", "errors", "maxIF");
        load.AddRow("check", checks.MeasuredCompletionsPerSecond.ToString("0.0"),
            chk.Ms(chk.P50Micros), chk.Ms(chk.P99Micros), chk.Ms(chk.MaxMicros), chk.Errors, checks.MaxInFlight);
        load.AddRow("write", writes.MeasuredCompletionsPerSecond.ToString("0.0"),
            wr.Ms(wr.P50Micros), wr.Ms(wr.P99Micros), wr.Ms(wr.MaxMicros), wr.Errors, writes.MaxInFlight);
        load.Print(Console.Out);
        if (chk.FirstError is not null)
            Console.WriteLine($"first check error: {chk.FirstError}");
        if (wr.FirstError is not null)
            Console.WriteLine($"first write error: {wr.FirstError}");
        if (chk.SampleWarning("check") is { } chkWarning)
            Console.WriteLine(chkWarning);
        if (wr.SampleWarning("write") is { } wrWarning)
            Console.WriteLine(wrWarning);

        Console.WriteLine();
        var table = new ConsoleTable("sequencer counter", "count", "note");
        table.AddRow("Commit", seq.Commit, "inbound commits (accepted, rejected, or thrown)");
        table.AddRow("CommitShed", seq.CommitShed, "commits shed by the per-silo admission gate (RESOURCE_EXHAUSTED)");
        table.AddRow("ReadFrom", seq.ReadFrom, "log-tail catch-up calls - the P2 fan-in observable");
        table.AddRow("ReadShard", seq.ReadShard, "cold-shard hydrations");
        table.AddRow("GetHead", seq.GetHead, "head reads (fresh-pinned consistency resolution)");
        table.AddRow("ReadState", seq.ReadState, "admin-plane whole-state assemblies - the P6 observable");
        table.AddRow("ReadSchemaAt", seq.ReadSchemaAt, "schema-at-revision reads");
        table.AddRow("Flush", seq.Flush, "flush boundaries (dirty rows + meta durable) - the P5 sawtooth");
        table.AddRow("CommitMicrosTotal", seq.CommitMicrosTotal,
            seq.Commit == 0 ? "" : $"mean {(seq.CommitMicrosTotal / (double)seq.Commit / 1000.0):0.000} ms/commit");
        table.AddRow("CommitMicrosMax", seq.CommitMicrosMax, $"longest single commit turn ({seq.CommitMicrosMax / 1000.0:0.000} ms)");
        table.AddRow("CommitCandidates<=1", seq.CommitCandidates1, "commits by candidate-key breadth (P1)");
        table.AddRow("CommitCandidates2-8", seq.CommitCandidates2To8, "");
        table.AddRow("CommitCandidates9-64", seq.CommitCandidates9To64, "");
        table.AddRow("CommitCandidates65+", seq.CommitCandidates65Plus, "");
        table.Print(Console.Out);

        Console.WriteLine();
        var disp = new ConsoleTable("dispatch counter", "count");
        disp.AddRow("Dispatch (check-grain hops)", dispatch.Dispatch);
        disp.AddRow("MemoHit", dispatch.MemoHit);
        disp.AddRow("MemoMiss", dispatch.MemoMiss);
        disp.AddRow("LoopBypass", dispatch.LoopBypass);
        disp.Print(Console.Out);

        if (jsonPath is not null)
        {
            BenchJson.Write(jsonPath, new
            {
                scenario = Name, seed, silos, duration, warmup, checkWorkers, writeRate, mix = mix.ToString(),
                checksPerSecond = checks.MeasuredCompletionsPerSecond,
                writesPerSecond = writes.MeasuredCompletionsPerSecond,
                checkMaxInFlight = checks.MaxInFlight,
                writeMaxInFlight = writes.MaxInFlight,
                check = chk, write = wr, sequencer = seq, dispatch,
            });
        }
    }
}
