namespace Spiceport.Bench.Scenarios;

/// <summary>
/// The remote-network counterpart to <c>sequencer-decomposition</c>: closed-loop checks plus an
/// OPEN-LOOP paced writer against a REAL <c>RigSilo</c> cluster over gRPC, reporting the sequencer
/// inbound-call decomposition summed across silos — the direct observable for the 3.1 tail-cache
/// trigger under real network/serialization cost. <c>tools/rig/rig.sh</c> boots the silos.
/// </summary>
public static class RemoteDecompositionScenario
{
    public const string Name = "remote-decomposition";

    public const string Help =
        "Closed-loop checks + open-loop paced writes against a REAL RigSilo cluster over gRPC\n" +
        "(tools/rig/rig.sh boots the silos); prints the summed sequencer inbound-call decomposition\n" +
        "(the 3.1 tail-cache trigger observable). Numbers are RELATIVE (A/B deltas) but now priced in\n" +
        "real per-call RTTs and serialization, not in-process method calls.\n" +
        "  --endpoints=host:port,...      gRPC addresses, one per silo (required)\n" +
        "  --rig=host:port,...            rig HTTP addresses, same order as --endpoints (required)\n" +
        "  --check-workers=8              closed-loop check workers, round-robined across channels\n" +
        "  --write-rate=20                open-loop writes/second (coordinated-omission-safe pacing)\n" +
        "  --mix=70/20/10                 check consistency mix (min-latency/at-least-as-fresh/fully-consistent)\n" +
        "  --updates-per-commit=1         relationship updates per paced WriteRelationships call (commit shape)\n" +
        "  --breadth=none|single          precondition breadth per write (none / single-key MUST_NOT_MATCH);\n" +
        "                                 'type' is deliberately unsupported here (see docs/scalability-program.md P1)";

    public static async Task RunAsync(BenchArgs args)
    {
        args.Validate([
            .. BenchCommon.SharedFlagNames, .. BenchWorldOptions.FlagNames,
            "endpoints", "rig", "check-workers", "write-rate", "mix", "updates-per-commit", "breadth",
        ]);
        var seed = args.GetInt("seed", 42);
        var duration = args.GetDuration("duration", TimeSpan.FromSeconds(10));
        var warmup = args.GetDuration("warmup", TimeSpan.FromSeconds(2));
        var checkWorkers = args.GetInt("check-workers", 8);
        var writeRate = args.GetDouble("write-rate", 20);
        var mix = ConsistencyMix.Parse(args.GetString("mix", "70/20/10")!);
        var updatesPerCommit = args.GetInt("updates-per-commit", 1);
        if (updatesPerCommit < 1)
            throw new BenchUsageException($"--updates-per-commit must be >= 1, got {updatesPerCommit}.");
        var breadth = args.GetString("breadth", "none")!;
        // 'type' (a type-wide MUST_NOT_MATCH scan) is a documented pathological path
        // (docs/scalability-program.md P1) that the durable-backend campaign does not need;
        // rejected explicitly rather than silently degrading to 'none'.
        if (breadth == "type")
            throw new BenchUsageException(
                "--breadth=type is not supported by remote-decomposition (the type-wide scan is a documented " +
                "pathological path; use commit-breadth for that measurement). Use none or single.");
        if (breadth is not ("none" or "single"))
            throw new BenchUsageException($"Unknown --breadth '{breadth}' (expected none or single).");
        var singleKeyPrecondition = breadth == "single";
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
        var checkRngs = BenchCommon.WorkerRngs(seed, checkWorkers);
        // The single open-loop writer's own RNG: seeded one worker-index past the check workers so
        // its draw stream never collides with any check worker's, keeping the whole run reproducible
        // for a given --seed.
        var writeRng = BenchCommon.WorkerRngs(seed, checkWorkers + 1)[checkWorkers];
        var writeInvoker = rig.InvokerForWorker(checkWorkers);

        long writeCounter = 0;
        Func<Task> writeOp = async () =>
        {
            IReadOnlyList<(string Doc, string User)> updates;
            if (updatesPerCommit == 1)
            {
                string doc, user;
                lock (writeRng)
                {
                    doc = world.Documents[docSampler.Next(writeRng)];
                    user = world.Users[writeRng.Next(world.Users.Count)];
                }
                updates = new[] { (doc, user) };
            }
            else
            {
                // Mirrors CommitBreadthScenario's multi-update commits: each write TOUCHES
                // `updatesPerCommit` DISTINCT document#viewer rows drawn round-robin from a bounded
                // key space, so tuples are unique within a commit (a duplicate update in one write
                // is rejected) and the world stays bounded over any run length.
                var i = Interlocked.Increment(ref writeCounter);
                var batch = new (string, string)[updatesPerCommit];
                for (var j = 0; j < updatesPerCommit; j++)
                {
                    var k = i * updatesPerCommit + j;
                    batch[j] = (
                        world.Documents[(int)(k % world.Documents.Count)],
                        world.Users[(int)(k % world.Users.Count)]);
                }
                updates = batch;
            }
            var written = await rig.WriteBatchAsync(writeInvoker, updates, singleKeyPrecondition);
            token.Set(written);
        };

        Func<int, long, Task> checkOp = async (worker, _) =>
        {
            var rng = checkRngs[worker];
            var doc = world.Documents[docSampler.Next(rng)];
            var user = world.Users[rng.Next(world.Users.Count)];
            var consistency = mix.Pick(rng, token.Get);
            await rig.CheckAsync(rig.InvokerForWorker(worker), doc, user, consistency);
        };

        var checkRecorder = new LatencyRecorder();
        var writeRecorder = new LatencyRecorder();
        var boundaryReset = RemoteCheckScenario.ArmRemoteWarmupBoundaryResetAsync(rig, warmup);
        var writesTask = BenchDriver.RunOpenLoop(writeRate, warmup, duration, writeOp, writeRecorder);
        var checksTask = BenchDriver.RunClosedLoop(checkWorkers, warmup, duration, checkOp, checkRecorder);
        var writes = await writesTask;
        var checks = await checksTask;
        await boundaryReset;

        var chk = checkRecorder.Summarize();
        var wr = writeRecorder.Summarize();
        var (dispatch, sequencer) = await rig.SummedMetricsAsync();

        Console.WriteLine();
        Console.WriteLine(
            $"remote-decomposition  seed={seed} silos={rig.SiloCount} duration={duration} warmup={warmup} " +
            $"checkWorkers={checkWorkers} writeRate={writeRate} mix={mix} " +
            $"updatesPerCommit={updatesPerCommit} breadth={breadth}");
        Console.WriteLine();
        var load = new ConsoleTable("op class", "ops/s", "p50 ms", "p99 ms", "max ms", "errors", "maxIF");
        load.AddRow("check", checks.MeasuredCompletionsPerSecond.ToString("0.0"),
            chk.Ms(chk.P50Micros), chk.Ms(chk.P99Micros), chk.Ms(chk.MaxMicros), chk.Errors, checks.MaxInFlight);
        // The write op-class label carries the commit shape (upd/commit, precondition breadth) inline
        // since this table has no separate column for it — the shape is what the durable-backend
        // campaign is varying, so it must be visible next to the numbers it produced.
        load.AddRow($"write (upd/commit={updatesPerCommit} breadth={breadth})",
            writes.MeasuredCompletionsPerSecond.ToString("0.0"),
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
        table.AddRow("Commit", sequencer.Commit, "inbound commits (accepted, rejected, or thrown)");
        table.AddRow("ReadFrom", sequencer.ReadFrom, "log-tail catch-up calls - the P2 fan-in observable");
        table.AddRow("ReadShard", sequencer.ReadShard, "cold-shard hydrations");
        table.AddRow("GetHead", sequencer.GetHead, "head reads (fresh-pinned consistency resolution)");
        table.AddRow("ReadState", sequencer.ReadState, "admin-plane whole-state assemblies - the P6 observable");
        table.AddRow("ReadSchemaAt", sequencer.ReadSchemaAt, "schema-at-revision reads");
        table.AddRow("Flush", sequencer.Flush, "flush boundaries (dirty rows + meta durable) - the P5 sawtooth");
        table.AddRow("CommitMicrosTotal", sequencer.CommitMicrosTotal,
            sequencer.Commit == 0 ? "" : $"mean {(sequencer.CommitMicrosTotal / (double)sequencer.Commit / 1000.0):0.000} ms/commit");
        table.AddRow("CommitMicrosMax", sequencer.CommitMicrosMax, $"longest single commit turn ({sequencer.CommitMicrosMax / 1000.0:0.000} ms)");
        table.AddRow("CommitCandidates<=1", sequencer.CommitCandidates1, "commits by candidate-key breadth (P1)");
        table.AddRow("CommitCandidates2-8", sequencer.CommitCandidates2To8, "");
        table.AddRow("CommitCandidates9-64", sequencer.CommitCandidates9To64, "");
        table.AddRow("CommitCandidates65+", sequencer.CommitCandidates65Plus, "");
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
                scenario = Name, seed, silos = rig.SiloCount, duration, warmup, checkWorkers, writeRate, mix = mix.ToString(),
                updatesPerCommit, breadth,
                checksPerSecond = checks.MeasuredCompletionsPerSecond,
                writesPerSecond = writes.MeasuredCompletionsPerSecond,
                checkMaxInFlight = checks.MaxInFlight,
                writeMaxInFlight = writes.MaxInFlight,
                check = chk, write = wr, sequencer, dispatch,
            });
        }
    }
}
