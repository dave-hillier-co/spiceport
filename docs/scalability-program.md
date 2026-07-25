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
  means shortening the serialized turn, never adding writers.

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

**The real-network rig** (built when either decision below needs it, not before): an
attended run against genuinely separate silo processes on a real network — the follow-up
instrument for the questions the in-process cluster structurally cannot answer, because they
are priced in per-call RTTs and serialization the shared-process cluster skips. Two items
wait on it: the co-placement default (3.5) and the tail-cache verdict (3.1). It reuses the
same scenarios and seams; only the cluster bootstrap differs. Like every measurement here it
is attended and manual, with explicit teardown, never part of the automated suite.

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

**Trigger status: not fired in-process.** Harness runs confirm the call-volume shape (an
order of magnitude more `ReadFrom` under fresh-read mixes) but show only marginal throughput
cost — the calls are cheap without a network. The cost this mitigation answers lives in
per-call RTTs the in-process cluster cannot express, so the verdict is deferred to the
real-network rig (§2); do not build on in-process evidence alone.

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

**Trigger status: FIRED.** Harness runs (distinct-subject probes, so the activation memo
cannot mask the fetch) show point-membership latency scaling linearly with userset
cardinality and multiplying severalfold cross-silo. This is the first mitigation to build.
Measurement note for reproducers: the userset sweep must probe DISTINCT subjects per op —
probing a fixed subject measures the activation memo, not the fetch.

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

**Trigger status: diagnose before building.** The commit-breadth sweep shows an EXACT-KEY
precondition costing several times a bare commit — far above one storage read, which points
at candidate resolution itself (suspect: the filter-to-candidate step scans the key index
even when the filter names explicit ids and a direct lookup would do). That diagnosis comes
FIRST: fixing a bug-shaped inefficiency changes the baseline every 3.3 item would be
measured against, so none of 1–3 is built until the exact-key path is understood and the
sweep re-run. The flush sawtooth is observed (see 3.4, the shared cause); item 3 rides with
3.4 when that lands.

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

**Trigger status: fires on growth.** Harness runs at modest object cardinality (tens of
thousands of keys) already show the flush boundary spiking commit latency well above the
steady-state mean, consistent with whole-index serialization per flush. The cost scales with
object cardinality, so this is scheduled after the cheap fixes rather than awaiting further
evidence; flush write batching (3.3 item 3) lands adjacent to it.

### 3.5 Enablement decisions (answers P4 — decisions, not code)

- **Co-placement** (`GraphPlacementOptions.CoLocateWithShards`): flip on when the A/B shows
  a real hop/latency win; the locality hash is stable across quantization-window keyspace
  rotation, so the measurement should confirm the win persists across windows.
  *Status: pending the real-network rig.* In-process A/B trials are consistently positive
  (throughput up, p50 down, never negative), and the same-silo effect can only grow once a
  real network prices the cross-silo hop — but the flip waits for that confirmation, since
  the in-process delta alone is modest.
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

1. **Diagnose the exact-key precondition cost** (3.3's status note) — likely a small fix
   with a large effect on every precondition-carrying write, and a prerequisite for honestly
   costing anything else in 3.3.
2. **Subject-filter pushdown (3.2)** — the fired trigger; small change, strictly less data
   movement, shifts the read-side baseline.
3. **Re-baseline** — re-run the userset, commit-breadth, and decomposition sweeps. Fixes
   demonstrably dominate each other's signal; nothing further is decided on stale numbers.
4. **Key-index chunking (3.4)**, with flush write batching (3.3 item 3) adjacent — the one
   structural storage change, scheduled because its cost scales with object cardinality.
5. **The real-network rig (§2)** — the prerequisite instrument for the two open decisions:
   the co-placement default (3.5) and whether the tail cache (3.1) fires at all.
6. **Only on evidence:** the tail cache (3.1) if the real-network numbers fire it; the
   remaining commit-turn items (3.3.1/3.3.2) if the re-baseline still shows candidate-load
   cost; storage-direct scans (3.6) when admin-scan interference is observed.
