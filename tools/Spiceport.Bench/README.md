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

## Remote mode: driving the real-network rig

Every scenario above shares one thread pool and skips real networking — deltas only, never
absolute capacity, and structurally blind to per-call RTT and serialization cost. `remote-check`
and `remote-decomposition` are the real-network counterparts: the same workload/consistency-mix
model, but issuing genuine `authzed.api.v1` gRPC calls against a cluster of separate
`tools/Spiceport.RigSilo` processes.

Boot the cluster with the orchestrator, then point Bench at it:

```
tools/rig/rig.sh up 3
dotnet run -c Release --project tools/Spiceport.Bench -- remote-check \
  --endpoints=127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502 \
  --rig=127.0.0.1:8580,127.0.0.1:8581,127.0.0.1:8582 \
  --mix=100/0/0
dotnet run -c Release --project tools/Spiceport.Bench -- remote-decomposition \
  --endpoints=127.0.0.1:8500,127.0.0.1:8501,127.0.0.1:8502 \
  --rig=127.0.0.1:8580,127.0.0.1:8581,127.0.0.1:8582 \
  --write-rate=20 --mix=70/20/10
tools/rig/rig.sh down
```

`rig.sh up N` prints the exact `--endpoints`/`--rig` values for the cluster it just started, so
they can be pasted straight into the commands above rather than derived from the port scheme by
hand. `rig.sh ab [--silos=N] [--trials=T] [bench flags...]` automates the co-placement A/B
procedure itself: fresh cluster per arm (co-placement off, then on) per trial, running
`remote-check --json` into each cell, with results left under `$HOME/.spiceport-rig/results/`
(overridable via `SPICEPORT_RIG_HOME`) — never inside this repository, per the results-are-never-
committed rule above. See `tools/rig/rig.sh`'s header comment for the port-allocation scheme and
state-directory layout, and `docs/scalability-program.md` §2 for what the rig answers that the
in-process harness cannot.

Once a real network and real serialization are in the loop, `remote-*` numbers are still
*relative* — compare A/B deltas between configurations under the same rig, same seed, same
world — but they are no longer relative in the same forgiving way the in-process numbers are:
per-call RTT and (de)serialization cost, which the in-process cluster hides entirely, now shows
up directly in the latencies and in cross-silo vs same-silo comparisons.
