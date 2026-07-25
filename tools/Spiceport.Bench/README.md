# Spiceport.Bench

The Phase-0 measurement harness of the scalability program
(`docs/scalability-program.md` §2): a manual, attended console tool that boots an in-process
Orleans `TestCluster` with the production silo wiring (the shared `MeshTestCluster`
configurators, compile-linked from `tests/Spiceport.Grains.Tests`), loads a deterministic
seeded world through the declarative write path, drives paced workloads, and reads the two
permanent metric seams (`IDispatchMetrics`, `ISequencerMetrics`).

It is never part of the automated suite. CI measures nothing.

## The relative-numbers caveat

The cluster is in-process: every silo shares one thread pool and no call crosses a real
network. Absolute figures from this harness are therefore meaningless as capacity numbers.
What it produces is **A/B deltas between configurations** — placement on vs off, one
quantization window vs another, one consistency mix vs another — under identical seeded
worlds. Absolute capacity, when it matters, is an attended run against real booted silos,
out of scope here.

## Results are never committed

Benchmark results are status, not method; the program records method, knobs, and triggers
only (`docs/scalability-program.md`, invariants). Accordingly `--json` refuses any path
inside the repository tree — write results to `/tmp` or similar.

## Workload model

Deterministic per seed: a fixed schema (user / group with nesting / folder chains with
`parent->view` arrows / document with wildcard viewers) whose shape covers the evaluated
features, sized by world flags (`--users --groups --documents ...` — see `--help`). Writers
run **open-loop** (paced arrivals that never wait for the previous op, the
coordinated-omission defense); checks run closed-loop with N workers unless a scenario says
otherwise — closed-loop tails underestimate under saturation. Latencies are raw microsecond
samples in a preallocated ring, percentiles by sort after the run, warmup excluded.

## Scenarios

```
dotnet run -c Release --project tools/Spiceport.Bench -- consistency-sweep --mixes=100/0/0,0/0/100 --write-rates=0,25,100
dotnet run -c Release --project tools/Spiceport.Bench -- commit-breadth --breadths=none,single,type --updates-per-commit=1,8,64
dotnet run -c Release --project tools/Spiceport.Bench -- userset-sweep --sizes=1000,10000 --silos=3
dotnet run -c Release --project tools/Spiceport.Bench -- placement-ab --silos=3
dotnet run -c Release --project tools/Spiceport.Bench -- quantization-sweep --windows=1s,5s,30s
dotnet run -c Release --project tools/Spiceport.Bench -- sequencer-decomposition --write-rate=20 --mix=70/20/10
```

Each maps to a pressure point in `docs/scalability-program.md` §1: consistency-sweep (P2),
commit-breadth (P1/P5), userset-sweep (P3), placement-ab and quantization-sweep (P4, the
§3.5 enablement decisions), sequencer-decomposition (P2/P6). `--help` documents every flag.
