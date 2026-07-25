using Spiceport.Bench;
using Spiceport.Bench.Scenarios;

// The Phase-0 measurement harness (docs/scalability-program.md section 2): a manual, attended
// console tool driving an in-process multi-silo TestCluster with the production silo wiring.
// Never part of the automated suite; results are relative (A/B deltas), never absolute capacity,
// and are never committed.

var scenarios = new (string Name, string Help, Func<BenchArgs, Task> Run)[]
{
    (ConsistencySweepScenario.Name, ConsistencySweepScenario.Help, ConsistencySweepScenario.RunAsync),
    (CommitBreadthScenario.Name, CommitBreadthScenario.Help, CommitBreadthScenario.RunAsync),
    (UsersetSweepScenario.Name, UsersetSweepScenario.Help, UsersetSweepScenario.RunAsync),
    (PlacementAbScenario.Name, PlacementAbScenario.Help, PlacementAbScenario.RunAsync),
    (QuantizationSweepScenario.Name, QuantizationSweepScenario.Help, QuantizationSweepScenario.RunAsync),
    (SequencerDecompositionScenario.Name, SequencerDecompositionScenario.Help, SequencerDecompositionScenario.RunAsync),
};

if (args.Length == 0 || args[0] is "--help" or "-h" or "help")
{
    PrintHelp();
    return 0;
}

var name = args[0];
var scenario = scenarios.FirstOrDefault(s => s.Name == name);
if (scenario.Run is null)
{
    Console.Error.WriteLine($"Unknown scenario '{name}'.");
    Console.Error.WriteLine();
    PrintHelp();
    return 2;
}

try
{
    var parsed = BenchArgs.Parse(args.Skip(1));
    await scenario.Run(parsed);
    return 0;
}
catch (BenchUsageException ex)
{
    Console.Error.WriteLine(ex.Message);
    return 2;
}

void PrintHelp()
{
    Console.WriteLine("Spiceport.Bench - the Phase-0 measurement harness (docs/scalability-program.md section 2).");
    Console.WriteLine();
    Console.WriteLine("Usage: dotnet run --project tools/Spiceport.Bench -- <scenario> [--key=value ...]");
    Console.WriteLine();
    Console.WriteLine("Shared flags (every scenario):");
    Console.WriteLine("  --seed=42            world/workload seed (identical seed => byte-identical world)");
    Console.WriteLine("  --silos=N            silo count (scenario-specific default)");
    Console.WriteLine("  --duration=10s       measured window per cell (5s/2m/500ms forms accepted)");
    Console.WriteLine("  --warmup=3s          warmup window, excluded from all statistics");
    Console.WriteLine("  --json=/path/out.json  machine-readable results; paths inside the repo are refused");
    Console.WriteLine("                         (benchmark results are never committed)");
    Console.WriteLine();
    Console.WriteLine("World-shape flags (every scenario):");
    Console.WriteLine("  --users --groups --group-mean-size --big-userset-sizes --folders --folder-depth");
    Console.WriteLine("  --documents --nesting-depth --wildcard-doc-share");
    Console.WriteLine();
    Console.WriteLine("Load modes: writers are OPEN-LOOP (paced arrivals that never wait for the previous op,");
    Console.WriteLine("so a slow op cannot suppress subsequent samples - the coordinated-omission defense).");
    Console.WriteLine("Checks are CLOSED-LOOP (N workers back-to-back) or open-loop where a scenario says so;");
    Console.WriteLine("closed-loop tail latencies UNDERESTIMATE under saturation, because a stuck worker stops");
    Console.WriteLine("generating the arrivals that would have observed the stall. Read tails accordingly.");
    Console.WriteLine();
    Console.WriteLine("All numbers are RELATIVE: the in-process cluster shares one thread pool and skips real");
    Console.WriteLine("networking, so compare A/B deltas between configurations, never absolute capacity.");
    Console.WriteLine();
    Console.WriteLine("Scenarios:");
    foreach (var (scenarioName, help, _) in scenarios)
    {
        Console.WriteLine();
        Console.WriteLine($"  {scenarioName}");
        foreach (var line in help.Split('\n'))
            Console.WriteLine($"    {line}");
    }
}
