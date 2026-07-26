namespace Spiceport.Bench.Scenarios;

/// <summary>
/// The remote-network counterpart to the in-process check scenarios (docs/scalability-program.md
/// section 2): closed-loop checks against a REAL <c>RigSilo</c> cluster over real gRPC, priced in
/// actual per-call round trips and protobuf serialization rather than in-process method calls.
/// <c>tools/rig/rig.sh</c> boots the silos this scenario talks to.
/// </summary>
public static class RemoteCheckScenario
{
    public const string Name = "remote-check";

    public const string Help =
        "Closed-loop checks against a REAL RigSilo cluster over gRPC (tools/rig/rig.sh boots the silos).\n" +
        "Numbers are still RELATIVE (A/B deltas), but now priced in real per-call RTTs and\n" +
        "serialization, not in-process method calls.\n" +
        "  --endpoints=host:port,...      gRPC addresses, one per silo (required)\n" +
        "  --rig=host:port,...            rig HTTP addresses, same order as --endpoints (required)\n" +
        "  --check-workers=16             closed-loop check workers, round-robined across channels\n" +
        "  --mix=100/0/0                  check consistency mix (min-latency/at-least-as-fresh/fully-consistent)";

    public static async Task RunAsync(BenchArgs args)
    {
        args.Validate([
            .. BenchCommon.SharedFlagNames, .. BenchWorldOptions.FlagNames,
            "endpoints", "rig", "check-workers", "mix",
        ]);
        var seed = args.GetInt("seed", 42);
        var duration = args.GetDuration("duration", TimeSpan.FromSeconds(10));
        var warmup = args.GetDuration("warmup", TimeSpan.FromSeconds(3));
        var checkWorkers = args.GetInt("check-workers", 16);
        var mix = ConsistencyMix.Parse(args.GetString("mix", "100/0/0")!);
        var jsonPath = BenchJson.ResolveOutputPath(args);

        var world = BenchWorld.Generate(seed, BenchWorldOptions.FromArgs(args));

        using var rig = new RemoteRig(
            args.GetStringArray("endpoints", []), args.GetStringArray("rig", []));

        Console.Error.WriteLine($"waiting for {rig.SiloCount} rig silo(s) to report healthy...");
        await rig.WaitUntilReadyAsync(TimeSpan.FromSeconds(30));

        Console.Error.WriteLine("writing schema...");
        await rig.WriteSchemaAsync(BenchWorld.SchemaText);
        Console.Error.WriteLine($"loading {world.Relationships.Count} relationships...");
        var loadToken = await rig.LoadWorldAsync(world);

        var token = new BenchCommon.LastWriteToken(loadToken);
        var docSampler = new ZipfSampler(world.Documents.Count);
        var rngs = BenchCommon.WorkerRngs(seed, checkWorkers);

        Func<int, long, Task> checkOp = async (worker, _) =>
        {
            var rng = rngs[worker];
            var doc = world.Documents[docSampler.Next(rng)];
            var user = world.Users[rng.Next(world.Users.Count)];
            var consistency = mix.Pick(rng, token.Get);
            await rig.CheckAsync(rig.InvokerForWorker(worker), doc, user, consistency);
        };

        var recorder = new LatencyRecorder();
        // Armed BEFORE the driver starts: the remote analog of ArmWarmupBoundaryResetAsync, POSTing
        // /rig/reset to EVERY silo so the summed metric snapshot covers the measured window only.
        var boundaryReset = ArmRemoteWarmupBoundaryResetAsync(rig, warmup);
        var result = await BenchDriver.RunClosedLoop(checkWorkers, warmup, duration, checkOp, recorder);
        await boundaryReset;

        var lat = recorder.Summarize();
        var (dispatch, sequencer) = await rig.SummedMetricsAsync();
        var attempts = lat.Count + lat.Errors;
        var hopsPerCheck = attempts == 0 ? 0 : (double)dispatch.Dispatch / attempts;
        var memoTotal = dispatch.MemoHit + dispatch.MemoMiss;
        var memoHitRate = memoTotal == 0 ? 0 : 100.0 * dispatch.MemoHit / memoTotal;

        Console.WriteLine();
        Console.WriteLine(
            $"remote-check  seed={seed} silos={rig.SiloCount} duration={duration} warmup={warmup} " +
            $"checkWorkers={checkWorkers} mix={mix}");
        var table = new ConsoleTable("checks/s", "errs", "p50 ms", "p99 ms", "hops", "hops/check", "memo hit%");
        table.AddRow(
            result.MeasuredCompletionsPerSecond.ToString("0.0"), lat.Errors,
            lat.Ms(lat.P50Micros), lat.Ms(lat.P99Micros),
            dispatch.Dispatch, hopsPerCheck.ToString("0.00"), memoHitRate.ToString("0.0"));
        table.Print(Console.Out);
        if (lat.FirstError is not null)
            Console.WriteLine($"first check error: {lat.FirstError}");
        if (lat.SampleWarning("check") is { } sampleWarning)
            Console.WriteLine(sampleWarning);

        Console.WriteLine();
        var seqTable = new ConsoleTable("sequencer counter", "count");
        seqTable.AddRow("Commit", sequencer.Commit);
        seqTable.AddRow("ReadFrom", sequencer.ReadFrom);
        seqTable.AddRow("ReadShard", sequencer.ReadShard);
        seqTable.AddRow("GetHead", sequencer.GetHead);
        seqTable.AddRow("ReadState", sequencer.ReadState);
        seqTable.AddRow("ReadSchemaAt", sequencer.ReadSchemaAt);
        seqTable.AddRow("Flush", sequencer.Flush);
        seqTable.Print(Console.Out);

        if (jsonPath is not null)
        {
            BenchJson.Write(jsonPath, new
            {
                scenario = Name, seed, silos = rig.SiloCount, duration, warmup, checkWorkers, mix = mix.ToString(),
                checksPerSecond = result.MeasuredCompletionsPerSecond,
                check = lat, dispatch, sequencer, hopsPerCheck, memoHitRatePercent = memoHitRate,
            });
        }
    }

    /// <summary>
    /// The remote analog of <c>BenchCommon.ArmWarmupBoundaryResetAsync</c>: waits out the warmup
    /// window then POSTs <c>/rig/reset</c> to every silo, so the metrics snapshot taken after the run
    /// covers (approximately) the measured window only.
    /// </summary>
    internal static async Task ArmRemoteWarmupBoundaryResetAsync(RemoteRig rig, TimeSpan warmup)
    {
        if (warmup > TimeSpan.Zero)
            await Task.Delay(warmup);
        await rig.ResetAllAsync();
    }
}
