# Scalability program: measure, then earn each optimization

The graph-sharded datastore (`graph-sharded-datastore.md`) removed the state ceilings with
structural arguments and equivalence gates. What it did **not** do is measure anything: the
wins are asymptotic (silo memory O(hot shards), sequencer memory O(tail + metadata), recovery
O(tail)), while every potential regression lives in constants — grain-hop cost per adjacency
read, per-commit candidate I/O on the serialization point, catch-up fan-in, marshalling of
large usersets. Constants are where actor systems die in practice.

This document is the program for closing that gap. Its governing rule, from the maintainer's
standing stance: **measurement precedes optimization, and every mitigation is built only when
its trigger fires** — a trigger being a measured condition, not a date. Until a trigger fires,
the corresponding mitigation is a design sketch here, not a work item. Complexity must be
earned; deleting or avoiding a subsystem beats tuning one.

Two kinds of item live here:

1. **Phase 0** — the measurement harness and observability seams. Unconditional: everything
   else depends on it.
2. **Triggered mitigations** — each with the pressure point it answers, a design sketch, its
   correctness gate, its complexity cost, and the measured condition that would justify
   building it.

## Invariants that bound the program

- **Semantics never move.** Every mitigation lands behind the same discipline the migration
  used: row-level equivalence gates where data is served, verdict gates where verdicts are,
  the conformance corpus green two ways, the differential suite as the external check.
- **No whole-graph creep.** No mitigation may reintroduce a component whose memory is
  O(graph). Caches are bounded and their bounds are stated; the tail cache below is O(tail)
  by construction and gets the same structural tripwire treatment the sequencer's slim state
  has.
- **Benchmark results are never committed.** Results are status; this document records
  method, knobs, and triggers only. Decisions taken from measurements are recorded as
  maintainer decisions where they land (options defaults, this file's trigger notes).
- **The single-writer total order stays.** Nothing here touches it; write *throughput* work
  means shortening the serialized turn, never adding writers. The full argument for why —
  what the consistency contract minimally requires, why weaker orders (vector clocks, HLC
  without commit-wait) cannot carry it, and the two triggers that would re-open the question
  — is the standing analysis in `future-work.md` Part 3, with the trigger-gated
  decomposition ladder (group commit → Corfu ticket split → Calvin execution) recorded as
  its §1.15.

---

## 1. The pressure points, honestly stated

| # | Pressure point | Where it lives |
|---|---|---|
| P1 | Per-commit candidate I/O on the write path: `Commit` loads clean shard states from storage inside the single-threaded turn; broad precondition filters resolve to O(type) key loads; head-of-line blocking for all writes | `DatastoreGrain.Commit` candidate assembly |
| P2 | Catch-up fan-in: after any commit, every active shard whose watermark lags must `ReadFrom` the sequencer before serving a fresh-pinned read — O(active shards × write rate) calls to one activation; Watch streams tail the same way | `GraphShardGrain` catch-up; `GrainBackedDatastore.Watch` |
| P3 | Hot keys and large usersets: one shard activation per key; a point-membership Check against a million-member userset currently fetches the whole visible row set per distinct sub-problem, serialized cross-silo | `ShardedGraphReader.QueryRelationships` → `RowsAt` |
| P4 | Unmeasured constants: adjacency reads are grain hops where they were dictionary hits; co-placement is off; the revision-quantization window has never been tuned under load; flush adds a p99 sawtooth every 64th commit | everywhere; the stance itself |
| P5 | Key-index write amplification: the full forward/reverse index maps serialize into every `meta/{v}` row at each flush — O(distinct objects) bytes per 64 commits, and resident in sequencer memory | `DatastoreMetaState`, the flush |
| P6 | Admin plane pays O(graph) per scan through the sequencer, interleaving on the activation writes serialize through | `ISnapshotScanner`, `ReadState` assembly |

P1–P3 are the structural risks; P4 is the epistemic one; P5–P6 are known residuals with
documented trades.

---

## 2. Phase 0 — the measurement harness (unconditional)

A manual, attended console harness — committed, runnable, never part of the automated suite
(the same standing as the `zed` smoke test; CI measures nothing).

**Where.** The harness lives at `tools/Spiceport.Bench`: a console project driving an
in-process multi-silo `TestCluster` with the production silo wiring (the `MeshTestCluster`
configurators, compile-linked). Scenarios: `consistency-sweep` (Check latency vs consistency
mix at varying write rates), `commit-breadth` (commit latency vs precondition breadth and
updates per commit), `userset-sweep` (Check latency vs direct-viewer userset size, single- vs
multi-silo), `placement-ab` (identical workload with co-placement off then on),
`quantization-sweep` (memo hit rate and latency per revision-quantization window), and
`sequencer-decomposition` (mixed load with the sequencer inbound-call breakdown). In-process
clusters share a thread pool and skip real networking, so the harness reports **relative**
numbers — A/B deltas between configurations — not absolute capacity. Absolute capacity, when
it matters, is an attended run against real booted silos, out of scope for the harness itself.

**Workload model.** Deterministic seeded worlds, parameterized by: object count, relation
fan-out and nesting depth (the `RandomAuthzWorlds` shape, scaled), a big-userset tail (groups
of 10^3..10^6 members), Zipf skew for read traffic, write mix (touch/delete ratios,
precondition breadth from single-key to type-wide), and consistency mix
(minimize-latency / at-least-as-fresh / fully-consistent proportions).

**What it must answer** (each maps to a pressure point):

- Check latency/throughput (p50/p99) vs consistency mix at varying write rates — P2's
  fan-in shows up as fully-consistent p99 degrading with write rate.
- Commit latency vs precondition breadth and touched-key count; the flush-boundary sawtooth
  isolated (every-64th-commit p99 vs baseline) — P1, P5.
- Hot-key skew and userset size sweeps, same-silo vs cross-silo — P3.
- Co-placement on/off and quantization-window sweeps (1s/5s/30s): hop counts and memo hit
  rates via the existing `IDispatchMetrics` snapshots, latency deltas — P4.
- Sequencer inbound call decomposition (`Commit` / `ReadFrom` / `ReadShard` / `GetHead` /
  `ReadState` counts) under mixed load — the direct observable for P2 and P6.

**Observability seams to add** (small, unconditional): a sequencer-side counter snapshot in
the `IDispatchMetrics` idiom (plain counters + snapshot type; the `System.Diagnostics.Metrics`
route was assessed and rejected for the dispatch metrics — same reasoning applies), counting
inbound calls by method and commits by candidate-key count bucket. Cheap enough to keep on
permanently; the harness and any future profiling read the same seam.

**Hygiene rider** (unconditional, small): log-row writes still use fresh storage wrappers; a
crashed-then-retried append can ETag-clash on an orphan log row. Convert to the ETag-tolerant
read-then-write the shard rows use.

**The regression set** (a standing practice, not a CI job): after every change this program
lands — and before declaring any goal met — run the four standard scenarios
(`userset-sweep`, `sequencer-decomposition`, `commit-breadth`, `consistency-sweep`) at the
standard world and durations, with `--json` output kept OUTSIDE the repo, and compare
against the previous run's output. A change is judged on its goal metric AND on the absence
of regression in the scenarios it did not target; "shouldn't affect the read path" is a
hypothesis the set exists to test, not a reason to skip it. Results are still never
committed — the baseline is whatever the previous run produced, and a missing baseline
means running the set twice, before and after.

**The real-network rig.** An attended run against genuinely separate silo processes on a real
network — the follow-up instrument for the questions the in-process cluster structurally
cannot answer, because they are priced in per-call RTTs and serialization the shared-process
cluster skips. Two items wait on it: the co-placement default (3.5) and the tail-cache
verdict (3.1).

It lives in three pieces. `tools/Spiceport.RigSilo` is the silo process: the same production
grain/gRPC wiring as `src/Spiceport.Api`, parameterized per-process (ports, cluster id,
co-placement on/off) and exposing `/healthz` plus a plain-JSON `/rig/metrics` /
`/rig/reset` pair over the same `IDispatchMetrics`/`ISequencerMetrics` seams the in-process
harness reads. `tools/Spiceport.Bench`'s `remote-check` and `remote-decomposition` scenarios
are the driver: real `authzed.api.v1` gRPC calls against a `--endpoints` list, fanned out
across workers, with the same latency/metrics reporting as their in-process counterparts.
`tools/rig/rig.sh` is the orchestrator: boots and tears down an N-silo cluster with
deterministic port allocation, refuses to start over a live or port-colliding previous run,
and drives the co-placement A/B procedure (`rig.sh ab`) end to end — fresh cluster per arm
per trial, `off` then `on`, `remote-check --json` per cell.

The rig also carries the **durable arm** (`up N --durable [--fresh-data]`): a Postgres
container under the same teardown discipline, the vendored Orleans DDL applied once per
data volume, and `ConnectionStrings__OrleansStorage` passed to every silo so
`AddDatastoreGrainStorage` runs its production AdoNet path. The data volume deliberately
survives `down` — recovery runs reboot over it (`zed` reading a recovered store with
nothing reloaded is the proof pattern). Two deployment facts the durable arm established,
recorded here because any real deployment re-derives them the hard way: the connection
budget must satisfy silos × per-silo pool < server `max_connections` (Npgsql defaults to a
100-connection pool PER PROCESS; the failure mode is a cluster-wide write outage of opaque
`53300` errors), and a loopback container prices durability I/O but not database-network
RTT, so durable-arm numbers stay relative like everything else here.

Like every measurement here it is attended and manual, with explicit teardown (`rig.sh down`,
also trap-driven on interruption), never part of the automated suite, and results are written
outside the repository tree — `rig.sh` never writes into it.

---

## 3. Triggered mitigations

### 3.1 Per-silo log-tail cache (answers P2)

**Sketch.** A per-silo `LogTailCache` DI singleton: a bounded ring of recent `LogEvent`s plus
a head watermark, advanced by a single-flight `ReadFrom` per observer pulse — piggybacking on
the `LogWatchHub` push that already exists. `GraphShardGrain` catch-up consults its host
silo's cache first and falls back to the sequencer only when the cache window does not cover
its watermark (cold shards hydrate via `ReadShard` regardless). Watch streams can consume the
same cache, collapsing their per-stream `ReadFrom` tailing too. This restores exactly the
projection era's per-silo pull amortization — one sequencer call per silo per commit instead
of one per active shard — at O(tail) memory, holding raw events, never folded state.

**Gates.** The existing shard equivalence and Watch-liveness suites re-run cache-on; a
structural tripwire pins that the cache type holds `LogEvent`s only (no row collections).

**Cost.** One small class, one DI registration, a shard fallback branch.

**Trigger.** The harness shows sequencer `ReadFrom` call rate scaling with active-shard count
(rather than silo count) under fresh-read load, or sequencer turn saturation attributable to
tail serving before commit throughput is otherwise exhausted.

**Trigger status: NOT FIRED — measured on the real-network rig.** In-process runs confirmed
the call-volume shape (an order of magnitude more `ReadFrom` under fresh-read mixes) but the
calls are cheap without a network, so the verdict waited for the rig (§2). The rig's
decomposition runs (3 real silo processes, mixed load at realistic write rates) price the
calls with real RTTs and still show no symptom: `ReadFrom` dominates sequencer inbound calls
by count exactly as projected, yet check p99 and mean commit latency hold steady as the write
rate grows fivefold — and `ReadFrom` volume grows markedly *sublinearly* with write rate,
because a shard's catch-up call covers every commit since its watermark, an amortization the
per-commit fan-in model undercounts. The cache stays sketched, not built. Re-measure on the
rig at larger silo and hot-shard counts, or when sequencer turn saturation appears with
`ReadFrom` as the dominant inbound class.

### 3.2 Subject-filter pushdown on shard reads (answers P3, the Check half)

**Sketch.** The dominant large-userset cost is point-membership: `CheckDirect` asks "is
subject S in this userset" and today receives the entire visible row set to find one row.
Push the subject constraint into the shard call — `RowsAt(revision, subjectSelector?)` with
the existing `SubjectsSelectorWire` shape — and the reply shrinks to the matching rows.
Server-side filtering over in-memory rows is trivial; `Expand`/`LookupSubjects` legitimately
enumerate and keep the unfiltered call. This is strictly less data movement with no new
component — the rare optimization that also simplifies.

**Gates.** Pushdown-on vs pushdown-off row equality across the equivalence suite; the corpus
through the mesh.

**Cost.** One optional parameter through `IGraphShardGrain`/`ShardedGraphReader`; small.

**Trigger.** The userset-size sweep shows Check latency scaling with userset cardinality, or
cross-silo `RowsAt` payload sizes dominating dispatch cost in the skewed workload.

**Trigger status: FIRED, then BUILT — goal met.** Harness runs (distinct-subject probes, so
the activation memo cannot mask the fetch) showed point-membership latency scaling linearly
with userset cardinality and multiplying severalfold cross-silo. The build took two rounds,
both measurement-driven: the pushdown alone removed the wire cost (latency became
silo-count-independent) but left an O(userset) serve-side scan serialized on the shard
activation, so the shard gained a lazily-built per-state subject index (multi-version
buckets by subject + a non-terminals list; index-served candidates run the identical
visibility/Matches pipeline, so answers are byte-identical to the scan). Post-build the
sweep is flat within ~1.5× across three decades of userset size on both silo configs.
Two notes for reproducers: the sweep must probe DISTINCT subjects per op (a fixed subject
measures the memo, not the fetch), and the engine-side narrowing must stay a SUPERSET of
what CheckDirect consumes — exact subject, type-scoped wildcard, and every non-terminal
subject — because a bare subject-equality pushdown silently breaks recursion. Residual,
off the steady path: one-time O(userset) hydration payload and first index build per
activation (visible as max-latency spikes at extreme cardinality); the chunked-`RowsAt`
fallback in the sketch remains the answer if that ever matters.

**Fallback if enumeration paths hit message-size limits:** chunked/streaming `RowsAt` for the
enumerate-everything callers; a `[StatelessWorker]` read facade over hot shards stays the
last resort, priced as a real replication protocol.

### 3.3 Commit-turn shortening (answers P1)

**Sketch**, in escalating order — take only what the numbers demand:
1. **Parallelize candidate loads**: the per-key storage reads are independent;
   `Task.WhenAll` them before the serialized apply. Shortens the turn, changes nothing else.
2. **Bounded clean-state LRU** in the sequencer: the thin sequencer is one end of a dial
   whose other end was the old whole-fold-in-memory design; an LRU of clean shard states is
   a chosen point between them. The bound is a stated option; the no-whole-graph invariant
   caps it. Takes storage reads off the common path for hot keys.
3. **Flush write batching/parallelism**: dirty rows are independent write-once rows; fan them
   out. Trims the every-64th-commit sawtooth.
Broad preconditions (type-wide filters) stay honestly slow — that cost is inherent to
serializable breadth and is the documented trade.

**Gates.** `CommitContractTests`, the crash-window/durability suites (write-order discipline
must survive batching — the head row remains the sole commit point), `ThinSequencerTests`.

**Trigger.** Commit p99 attributable to candidate loads (1, 2) or flush-boundary sawtooth
p99 materially above steady-state p99 (3), at write rates the deployment actually needs.

**Trigger status: exact-key diagnosis RESOLVED; items 1–2 re-gated on the re-baseline.**
The suspected bug-shaped inefficiency was real: candidate resolution scanned the whole key
index (with a string parse per key) even for filters naming explicit ids, making an
exact-key precondition's cost linear in graph cardinality. Fixed: explicit-id filters now
construct their keys and probe the index maps directly — O(#ids) — on both the forward and
reverse sides; the scan survives only for the shapes that cannot name their keys (type-only,
prefix). Post-fix, the cardinality sweep is flat and an exact-key precondition sits within
~1.3× of a bare commit (the honest residual: one candidate load + filter evaluation).
The re-baseline answered the items-1–2 question: NOT triggered in-process. Realistic
commits resolve one-to-few candidates (nothing to parallelize) and candidate loads against
in-memory storage are far below the turn's other costs; an exact-key precondition's
steady-state residual is sub-millisecond. The flush sawtooth is the write path's dominant
remaining cost (see 3.4) and item 3 rides with 3.4 when that lands.

**Durable-backend verdict (rig, Postgres arm): items 1–2 stay NOT fired.** The rig's
durable campaign (loopback Postgres container, per-silo pool caps, fresh store per cell)
prices the commit turn with real I/O: the turn is dominated by the per-commit durable log
append plus the flush-boundary writes, not by candidate loads — a single-update commit's
mean turn lands around an fsync (~10 ms class on the test hardware), with single-key
preconditions indistinguishable from bare commits. Nothing here is a candidate-load cost to
parallelize or LRU away. What the durable numbers DO establish is the single-writer
ceiling itself: offered write load above roughly 1/mean-turn collapses into open-loop
queueing (latencies ramp until Orleans' response timeout), which is the write-demand
number the `future-work.md` §1.15 ladder trigger asks for, and the graceful-overload gap
is tracked as its own issue. Checks kept serving throughout write saturation (interleaved
reads), with degraded p99 while the activation queue was deep.

### 3.4 Key-index chunking (answers P5)

**Sketch.** Split the index maps into per-type rows (`index/{v}/f/{type}`,
`index/{v}/r/{type}`), version-qualified and committed by the head row exactly like shard
rows; `meta/{v}` shrinks to the type list plus per-type row versions. A flush writes only the
types it dirtied. Recovery reads the referenced index rows. The crash-window analysis extends
by the same argument the shard rows use — old meta references only old index versions.

**Gates.** Durability + migration suites extended for the layout change (a second in-place
migration, meta-v1 to meta-v2).

**Cost.** Moderate — a second versioned-row family and a migration.

**Trigger.** Meta-row serialization visible in flush latency, or meta row size beyond
storage-row comfort at the deployment's object cardinality.

**Trigger status: FIRED, then BUILT — goal substantially met.** The re-baseline's
cardinality sweep showed bare-commit p50 flat but throughput collapsing severalfold and p99
growing linearly with object cardinality, the entire excess being the flush turn serializing
the whole index. As built, the durable index is delta rows (O(dirty) per flush, dropped keys
as explicit tombstones) plus a round-robin bucket rotation (one bucket per direction per
flush; bucket assignment = stable hash % BucketCount is part of the durable contract), a
cardinality-independent slim meta, and batched parallel flush writes (3.3 item 3). Post-build
the sweep's throughput degradation across a 30x cardinality range collapsed from severalfold
to a modest residual — the documented O(N/BucketCount) bucket-rewrite term plus a CPU-only
bucket-selection scan — and absolute throughput ROSE at every cardinality (the write
batching pays everywhere). Recovery is bounded (all buckets + one rotation of deltas) with a
fail-loud ascending-order guard; migrations from both legacy layouts are in-place and
one-way (rollback across the index migration is forbidden — see
`graph-sharded-datastore.md`); the crash-window analysis was adversarially reviewed with no
blockers, and the pruned-delta and tombstone-then-recreate paths are test-reachable via a
test-injectable bucket count. Tighten the residual (larger BucketCount, per-bucket dirty
tracking) only if a deployment's cardinality demands it.

### 3.5 Enablement decisions (answers P4 — decisions, not code)

- **Co-placement** (`GraphPlacementOptions.CoLocateWithShards`): flip on when the A/B shows
  a real hop/latency win; the locality hash is stable across quantization-window keyspace
  rotation, so the measurement should confirm the win persists across windows.
  *Status: DECIDED — default ON.* The real-network rig's A/B (`rig.sh ab`, fresh 3-silo
  cluster per cell, three trials per arm) was decisive with non-overlapping ranges: on real
  sockets the ON arm carries substantially higher check throughput with p50 and p99 both
  well under the OFF arm's, in every trial. Hops per check are essentially unchanged between
  arms — the mechanism is hop *locality*, not hop count: co-placement converts cross-silo
  grain calls into local ones, which is precisely the cost the in-process cluster could not
  price and why the in-process delta alone was modest. Opting out remains a deployment
  override via `AddGraphLocalityPlacement`.
- **Revision quantization**: the window is a shard/memo reuse dial; sweep it and record the
  chosen default as a maintainer decision where the option lives.
  *Status: DECIDED — the 5s default stays.* Sweeps show the window is a weak dial for
  realistic workload shapes: memo hit rates are dominated by sub-problem cardinality, not
  window length, and longer windows buy single-digit-percent throughput for real staleness.
  Revisit only if a workload with materially lower sub-problem cardinality appears.

### 3.6 Storage-direct scans without the sequencer (answers P6)

**Sketch.** The durable layout is already a committed contract (head → meta → versioned
rows, all immutable once the head row references them). A scanner that reads grain storage
directly — head row, then meta, then the referenced row versions — assembles a consistent
snapshot with **zero sequencer involvement**: admin scans stop contending with writes
entirely. Read-only, no coordination, safe by the same argument that makes recovery safe.
Couples the scanner to the storage layout, which the durability tests already pin.

**Gates.** Scanner-vs-`ReadState` equivalence; layout-contract tests already exist.

**Cost.** Moderate — a storage-layout reader parallel to the recovery path.

**Trigger.** Scan/export traffic measurably interfering with write latency, or `ReadState`
assembly dominating sequencer load in the call decomposition.

---

## 4. The attempt order, as decided from measurement

The harness has run realistic sweeps; the trigger-status notes above record what fired. The
order below is the decided program — cheapest diagnosis first, fired triggers next,
re-measurement between fixes so attribution stays clean, the one structural change after the
cheap wins, and a better instrument before the two judgment calls:

1. **Diagnose the exact-key precondition cost** (3.3's status note) — RESOLVED: it was the
   index scan; explicit-id filters now probe the index maps by constructed key, and the
   cardinality sweep is flat.
2. **Subject-filter pushdown (3.2)** — BUILT, goal met: point-membership is flat in userset
   cardinality (pushdown + shard-side subject index; see 3.2's status note).
3. **Re-baseline** — DONE: steps 1–2 hold and compose in the standard configuration
   (userset flat, seed-window schema hops zero); 3.3 items 1–2 are not triggered in-process
   (re-gated on durable-backend/real-network evidence); the flush is confirmed as the
   dominant remaining write-path cost and 3.4's goal is quantified in its status note.
4. **Key-index chunking (3.4)**, with flush write batching (3.3 item 3) adjacent — BUILT,
   goal substantially met (see 3.4's status note): flush cost is O(dirty + N/BucketCount),
   the cardinality sweep flattened, and absolute commit throughput rose at every point.
5. **The real-network rig (§2)** — BUILT, and both decisions it existed for are taken:
   the co-placement default is ON (3.5's status note) and the tail cache did not fire
   (3.1's status note). The rig also immediately earned its keep as a correctness
   instrument: its first boots exposed two real-multi-process bugs the in-process cluster
   structurally hides (cross-silo schema propagation; a WriteSchema validator divergence
   from SpiceDB), both fixed with regression tests.
6. **Only on evidence:** the tail cache (3.1) if rig runs at larger silo/hot-shard counts
   fire it; storage-direct scans (3.6) when admin-scan interference is observed. The
   commit-turn items (3.3.1/3.3.2) are now CLOSED on durable-backend evidence — the durable
   commit turn is I/O-dominated, not candidate-load-dominated (3.3's status note) — leaving
   the write path's next lever the `future-work.md` §1.15 ladder, behind its write-demand
   trigger, and graceful overload behavior at the ceiling tracked as an issue.
