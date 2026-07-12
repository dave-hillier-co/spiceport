# SpiceDB → Orleans/.NET: Architecture Analysis

A port of [SpiceDB](https://github.com/authzed/spicedb) (a Zanzibar implementation) to
.NET (latest) and Microsoft Orleans (virtual actors). This document analyses the source
architecture, then maps it onto an actor model and proposes a porting workflow.

> Scope note: this is an evergreen design document. It describes *what the systems are*
> and *how the mapping works*, not project status.

---

## 1. What Zanzibar/SpiceDB actually is

A relationship-based authorization engine. Three primitives:

- **Schema** — object *definitions*, each with *relations* (direct edges) and *permissions*
  (computed via set operations over relations). Compiled from a DSL.
- **Relationships (tuples)** — `resource:id#relation@subject:id[#subrelation]`. Billions of
  rows. The data plane.
- **Queries** — the engine answers:
  - `Check` — can `subject` do `relation` on `resource`? (forward, boolean)
  - `Expand` — the tree of subjects for a relation
  - `LookupResources` — which resources can `subject` access? (reverse index)
  - `LookupSubjects` — which subjects can access `resource`? (reverse)

The hard parts are all in **evaluation**: a `Check` recursively decomposes a permission into
unions / intersections / exclusions of sub-relations and *arrows* (tupleset-to-userset, e.g.
`folder->view`), fanning out across the graph until it hits direct tuples. This recursion is
the workload, and it is where the actor model becomes interesting.

### Consistency model (the non-negotiable constraint)

Zanzibar is built on **MVCC snapshot reads**. Every read happens *at a revision*. Clients
carry a **ZedToken** (zookie) — an opaque consistency token encoding a revision — to get
"at least as fresh as my last write" semantics and avoid the "new enemy" problem. This shapes
every storage decision below: **evaluation is a pure function of `(schema@rev, tuples@rev,
request)`**. There is no mutable per-object runtime state during a check.

---

## 2. Source architecture (verified against the Go tree)

| Subsystem | Path | Responsibility |
|---|---|---|
| **Public API** | `internal/services/v1` | gRPC v1: Permissions, Schema, Relationships services |
| **Dispatch** | `internal/dispatch` | Routes & caches recursive sub-problems; local + remote |
| **Graph engine** | `internal/graph` | The actual Check/Expand/Lookup* graph walk |
| **Schema/DSL** | `pkg/schemadsl` (lexer→parser→compiler), `pkg/schema` | Compile DSL → `NamespaceDefinition`; build reachability graph |
| **Datastore** | `pkg/datastore`, `internal/datastore/*` | Tuple storage + revisions. Backends: memdb, postgres, crdb, mysql, spanner |
| **Caveats** | `internal/caveats`, `pkg/caveats` | ABAC via **CEL** expressions evaluated during check |
| **Caching** | `internal/dispatch/caching` + `internal/dispatch/keys` | Caches dispatch responses, keyed by canonicalized request |
| **Query planner** | `pkg/query` | Iterator-based plans (Union/Intersection/Alias/Recursive…) |

### The dispatch chain (key to the port)

```
gRPC v1 request
  → CachingDispatcher        (Ristretto in-process cache, key = canonical request)
  → RemoteDispatcher         (routes to a cluster node via CONSISTENT HASH on requestKey)
  → localDispatcher / graph  (concurrent graph walk; spawns sub-DispatchChecks)
        ↘ recurses back into the chain for every sub-problem
```

Three hand-rolled distributed-systems mechanisms live in this chain — and **all three are
things Orleans provides natively**:

1. **Consistent-hash routing** (`internal/dispatch/remote` + `github.com/authzed/consistent`):
   a sub-problem's cache key is hashed so the *same* sub-problem always lands on the *same*
   node → cache locality across the cluster.
2. **Singleflight / in-flight coalescing**: identical concurrent sub-problems are deduplicated.
3. **Cluster membership / hashring rebalancing** as nodes come and go.

Cycle/termination control travels *in the request metadata*, not in state:
`ResolverMeta { at_revision, depth_remaining, visited, schema_hash }`. An exact visited set
detects revisited `(definition, relation, resource, subject)` tuples; `depth_remaining`
decrements per hop.

---

## 3. The actor mapping — the central design decision

### 3.1 What a grain should *not* be

The tempting mapping — **one stateful grain per object** (`document:doc1` holds its tuples) —
is wrong for Zanzibar:

- Data is too large and too cold (billions of tuples, most touched rarely) to economically
  activate per-object.
- Entity grains hold *current* state; Zanzibar needs **point-in-time reads at arbitrary
  revisions**. Zookie consistency is incompatible with "the grain's latest value".
- Writes need a **global monotonic revision** and cross-object snapshot consistency — that is
  a datastore's job (MVCC), not per-grain consistency.

**Conclusion: relationship storage stays in a real MVCC datastore, read at a revision. It is not
*dispatch*-grain state.** Orleans then realizes that datastore *itself* natively, as a single
event-sourced grain with per-silo read projections — see §3.5.

### 3.2 What a grain *should* be — dispatch as virtual actors

SpiceDB's dispatch unit is a **pure, cacheable sub-problem**. That is an exact fit for an
Orleans grain **keyed by the sub-problem itself**, and it lets Orleans absorb the three
hand-rolled mechanisms above:

| SpiceDB mechanism | Orleans equivalent |
|---|---|
| Consistent-hash dispatch routing (`authzed/consistent`) | **Grain directory** — routes to the activation for a given sub-problem key |
| Singleflight (in-flight dedup) | **Single activation per grain key** — concurrent callers coalesce onto one activation |
| Hashring membership/rebalancing | **Orleans cluster membership** (silos) |
| Remote dispatch gRPC (`internal/dispatch/remote`) | **Inter-silo grain calls** — the whole `remote` + `cluster` package disappears |
| Ristretto per-node cache | **CheckGrain activation state** — memoizes the pre-context `Branch` with idle-collection eviction |

The elegant result: **`internal/dispatch/remote`, `cluster`, the consistent-hash balancer,
and the singleflight machinery collapse into the Orleans runtime.** We port the *graph
engine* (the interesting part) and delete the *distribution plumbing* (the tedious part).

### 3.3 The dispatcher seam is the core mechanism

The core mechanism that makes the domain *actor-addressable* is the **dispatcher seam**, not
the grain class. The engine never recurses into itself directly; every sub-problem flows
through a single `IDispatcher.DispatchCheck(request)` interface. Because recursion is mediated
by an interface, the same engine runs unchanged whether a sub-problem resolves in-process or
hops to another silo — the seam is the only thing that decides. The grain is simply one
implementation behind that seam, and **its identity is the sub-problem**: the domain concept
that has identity, is cacheable, and is the unit of distribution.

Two pieces sit on the seam, and they are *not* peers in a chain:

- `OrleansDispatcher` turns **every** sub-problem into a grain call. The grain key *is* the
  canonical sub-problem (`resourceType, resourceId, relation, subject, quantizedRevision,
  schemaHash`, escaped and joined). It resolves the `CheckGrain` for that key and invokes it; the
  Orleans **grain directory** finds or creates the single activation, so identical concurrent
  sub-problems coalesce there. There is no in-process shortcut for "locally owned" work and
  therefore no hand-rolled ownership computation — placement is the directory's job, and the
  consistent-hash ring the port once carried has been deleted.
- `LocalDispatcher` is the **one expansion step run *inside* a `CheckGrain`**. It walks the
  permission's set-operations for exactly one level, then calls *back through the seam* — i.e.
  through `OrleansDispatcher` again — for each child sub-problem. So recursion crosses a grain
  boundary at every level and the mesh is real.

**The activation is the cache.** `CheckGrain` activation state memoizes the pre-context `Branch`
(membership + caveat *expression*), never the collapsed verdict — caveat context is applied
per-request at the caller. The grain key already carries the quantized revision and schema hash,
so the keyspace rotates on its own each quantization window; the activation's idle-collection age
*is* the eviction policy. This is the Zanzibar paper's *delegate-side* cache expressed by the
runtime: the grain that owns a sub-problem is the server that caches it, and single-activation is
the lock table that dedupes concurrent misses. Cycle-cut results are served but not retained.
There is no separate caller-side cache layer.

The traversal state that is *not* part of the identity — `depthRemaining` and the exact
visited set — rides ambient in the Orleans `RequestContext` (via `DispatchContext`), so a remote grain
continues the same cycle guard while the wire contract carries nothing but the key and the
cancellation token. Cross-cutting concerns are **grain call filters**, not hand-threaded
plumbing: an outgoing filter maps cross-silo dispatch exceptions into the dispatch-error taxonomy
(transient → `Unavailable`, cancellation → `Cancelled`, domain exceptions pass through), and an
incoming filter enforces the depth ceiling at the grain boundary and counts hops.

The grain identity being the sub-problem is verified by an Orleans `TestCluster` running the
conformance corpus (set-ops, arrow, wildcard, nested-group, recursive, caveats) **through the
grain mesh**, with results identical to the in-process engine over the same datastore.

**Cycle / termination control.** The exact visited set (an `ImmutableHashSet<VisitKey>`, bounded
by the max recursion depth — at most 50 entries) is a loop hint, *not* on the correctness path.
Termination rests solely on `depthRemaining`: a genuine cycle consumes depth until
`MaxDepthExceededException` (gRPC `FailedPrecondition`), exactly as SpiceDB does. A visited-set
hit at a grain boundary forces the normal (reentrant) grain call with the returned result tagged
`CycleCut` at the caller, so the memo never stores a path-dependent branch; the hit is exact, so
it can only force a correct recompute, never change a verdict.

**Revision quantization (tuning, not correctness).** Every write mints a fresh revision, so an
un-quantized grain key would never be reused. A quantizer snaps an *optimized* (minimize-latency)
revision to a coarse bucket so concurrent requests share one activation and its memo; an exact
revision is pinned as-is. Consistency does not depend on the key carrying a mode tag: a read is a
pure function of the pinned revision *value*, so two sub-problems with the same revision string
compute the identical answer and share the activation exactly. Whether a revision was chosen as
optimized or exact is decided once, at resolution time (§3.5's closed-timestamp gate makes an
exact or fresh read block until its revision is visible); nothing downstream of the key needs to
know.

### 3.4 Other components, by actor role

- **Schema** → a per-silo `ISchemaProvider` (a DI singleton holding an immutable, versioned
  `SchemaSnapshot` that is swapped atomically on a schema write), not a grain. It holds the
  compiled `NamespaceDefinition`s + the precomputed **reachability graph** (used to prune
  `LookupResources`). It is small and versioned, so its hash scopes the dispatch keyspace: a
  schema change yields a fresh hash and therefore a fresh keyspace, and stale activations age out.
- **Streaming queries** (`LookupResources` / `LookupSubjects`) → **native `IAsyncEnumerable`
  grain methods** with runtime backpressure, the opaque resume cursor carried on each item so a
  client-facing limited stream still resumes byte-for-byte (matching SpiceDB's cursored LR3).
  Because a live enumerator pins to one activation — which a stateless worker cannot guarantee —
  these run on dedicated **guid-keyed stream grains** under default placement: a fresh Guid per
  stream gives each enumeration its own private activation, reclaimed by idle collection. `Expand`
  returns a whole tree (no cursor) and stays a unary call on the same grain family. Orleans
  Streams are *not* needed for the request path. `LookupResources` prunes with the reachability
  graph and is further accelerated by a log-derived membership index (the Leopard projection — §3.5).
- **Watch / changefeed** → a consumer of the datastore's own event log (§3.5): a per-silo
  notifier registers a grain observer on the datastore grain and fans out `RevisionChange`s to
  subscribers, rather than a separate replication tap.
- **Per-silo components** (the read projection and the Watch notifier) → owned by an Orleans
  **`GrainService`**, which is silo-lifecycle-managed: it bootstraps the projection *before* the
  silo accepts traffic (so the first request never pays the bootstrap) and tears the Watch
  observer down cleanly on shutdown. Their identity lives in a plain DI singleton so the per-Check
  read path reaches them in-process, with no grain hop.
- **Fan-out concurrency** → `Task.WhenAll` over sub-problem grain calls, bounded by a semaphore
  mirroring SpiceDB's `ConcurrencyLimits`. Orleans turn-based execution is not a bottleneck: each
  distinct sub-problem is its own grain, so a fan-out spreads across activations (and silos), and
  `CheckGrain` is `[Reentrant]` so a same-key re-entry on a genuine cycle is accepted rather than
  blocked.
- **Cycle/depth control** → termination rests on `depthRemaining` (a genuine cycle errors at the
  depth limit), with the exact visited set carried in `RequestContext` only as a loop-bypass hint,
  exactly as SpiceDB does. No actor state required.

### 3.5 Storage as an event-sourced grain (the log is the storage/compute seam)

§3.1 ruled out per-object grains and concluded storage is an MVCC datastore read at a revision.
Orleans realizes that datastore natively — as one event-sourced grain — without an external SQL
schema of its own:

- **One event-sourced cluster-singleton `DatastoreGrain`** owns all relationship/schema/counter
  state. It is a **journaled grain whose append-only log of `LogEvent`s is the source of truth**;
  the materialized MVCC state is the *fold* over that log. A commit is a single version-checked
  **append** (the compare-and-swap serialization point), never a whole-state rewrite. Persistence
  is the grain's own responsibility through a custom-storage interface over an Orleans grain-storage
  provider (in-memory in dev, AdoNet/Postgres in production) — **no application SQL**: each event is
  a per-version entry, with periodic snapshots plus log compaction. Garbage collection is driven by
  an Orleans **Reminder** that periodically appends a `GcApplied(floor)` event to the log; because GC
  is itself a log event, every fold applies it identically. The collect drops relationship rows fully
  dead below the floor, sweeps expired tuples, and compacts old schema and counter versions. The GC
  floor defaults to a 24-hour window, bounding state growth and aligning with Zanzibar's use of
  zookie staleness as a retention boundary. Reads pinned below the floor throw
  `RevisionNotFoundException`, so consumers re-bootstrap.
  The single non-reentrant activation makes the head-compare-and-append atomic, so the revision it
  mints is the **cluster-wide global order**. *This single ordered log is the total order that
  defeats Zanzibar's "new enemy" problem — the global-order point Spanner provides in the original.*

- **The log is the seam between storage and compute.** Reads do not fetch the whole state per Check.
  Each silo keeps a **materialized projection** folded incrementally from the log: it bootstraps once
  from a snapshot, then advances by pulling only the log tail (`ReadFrom(afterRevision)`). A Check
  reads its silo's local projection in-process — no grain hop, and no per-Check full-state fetch.

- **Closed-timestamp consistency.** The projection carries an *applied watermark* (the highest
  revision folded). A read pinned at revision `rev` blocks until `watermark ≥ rev` (catch-up-on-demand
  over the log) before serving. Because the log is a single total order, once the watermark reaches
  `rev` every commit `≤ rev` is present — read-your-writes / no new enemy — and `rev ≤ head`
  guarantees the wait terminates. This gate sits *below* the reader seam and combines with the
  pinned-revision grain key (§3.3): a sub-problem is identified and served at exactly its pinned
  revision, so an exact or fresh read blocks until that revision is visible and the activation memo
  keyed to it is never derived from a stale prefix.

- **Single-writer ceiling is intentional.** All writes serialize through the one ordered log (the
  global-order point). The design scales *reads* (per-silo projections) and cheapens *writes*
  (appends, not whole-blob rewrites), but does not raise the single-writer ceiling. Sharded
  per-namespace logs are out of scope — they would reintroduce the cross-shard global-order problem.

The evaluation contract is unchanged: a Check is still a pure function of `(schema@rev, tuples@rev,
request)`. The grain never holds *dispatch* state; it holds the *log*, and the projections, Watch
feed, and Leopard index below are all pure folds of that one log.

**Two consumers ride the same log feed:**

- **Watch (the changefeed)** consumes the `LogEvent` feed directly. A per-silo notifier registers a
  grain observer on the datastore grain, so a commit anywhere pushes the new head to it (and a local
  commit pulses it immediately, zero-hop); each Watch stream tails `ReadFrom` from its own cursor,
  maps each event to a `RevisionChange`, and parks on the shared signal. Observer delivery is
  best-effort, so a slow per-silo heartbeat doubles as registration refresh and missed-push backstop —
  one heartbeat per silo, not a poller per stream; checkpoints ride the revision the feed has
  progressed through, so a consumer filtering to a content subset still observes liveness.

- **A Leopard-style membership accelerator** (on by default, opt-out) is a mesh of per-subject
  walk grains (`IMembershipWalkGrain`, keyed by subject key + exact revision + schema hash): each
  activation computes one reverse-adjacency hop over a reader pinned at the key's revision and
  recurses through the sibling grains for its parents' own containers, memoizing the containing-set
  closure. A walk over a pinned MVCC snapshot is revision-exact by construction — no fold/catch-up
  machinery, deletes are the trivial case — and cold subject keys simply never activate. It yields
  **candidates, never verdicts**: the trusted `CheckEngine` confirms every candidate (soundness — an
  over-broad walk can only cost an extra Check), and the walk-on==walk-off equivalence gates pin
  completeness (an *incomplete* candidate set would silently drop results, which confirmation cannot
  detect). It engages only for shapes the schema coverage analysis can fully flatten (no
  tuple-to-userset arrows) and only for fresh, unpaged enumerations, leaving the cursored live
  traversal untouched; a depth-exhausted walk reports itself incomplete and the caller falls back to
  the live traversal.

---

## 4. The parts with no actor angle (straight ports / risks)

These are pure CPU and must be ported faithfully; they carry the real porting risk:

- **Schema DSL compiler** — `lexer → parser → compiler` producing the namespace protos and
  reachability graph. Sizeable, but self-contained and highly testable.
- **Graph evaluation semantics** — union/intersection/exclusion, arrows
  (tupleset-to-userset), wildcards (`user:*`), aliasing, expiration, and **caveat** short-
  circuiting. This is where subtle correctness bugs hide.
- **Caveats (CEL)** — SpiceDB embeds `cel-go`. **Decision (made via spike): adopt the
  `Cel` NuGet package (`rayokota/cel.net`, a CEL-Java port) as the evaluator rather than port
  a CEL parser.** Verified it supports full eval, the `.all()` macro, `in`, ternary,
  short-circuit (`false && missing` → `false` without error), and custom-function registration
  (so `in_cidr`/`isSubtreeOf` and IP/map types can be wired in). Its one gap vs `cel-go` is
  **partial evaluation** (a genuinely-needed missing var throws instead of yielding a residual
  AST). We bridge that with a thin shim: try full eval; if a needed var is missing, return a
  `Caveated` verdict listing the missing fields (short-circuit handled natively by the library).
  Residual-AST fidelity (re-evaluating a partial later) is deferred. This was the flagged
  dependency risk; it is now retired.
- **Datastore + revision model** — the in-memory MVCC mechanics (visibility at a revision, the
  per-revision diff, ZedToken encode/decode) are a straight port and stay the reusable core: the
  same `MvccSnapshotReader` fold serves both the `ReferenceDatastore` reference model and the
  silo projections.
  Durability is *not* a hand-rolled SQL datastore but the event-sourced grain's own storage (§3.5) —
  the log + snapshots persist via an Orleans grain-storage provider (AdoNet/Postgres), so there is no
  bespoke `xid8`/tuple SQL schema to maintain.
- **Protobuf API** — keep the **v1 gRPC API byte-compatible** so existing clients and the
  `zed` CLI work unchanged. .NET has first-class gRPC. The *internal* dispatch proto becomes
  Orleans grain interfaces (no gRPC between silos).

---

## 5. Testing strategy (drives the workflow)

SpiceDB ships a **YAML conformance corpus** at
`internal/services/integrationtesting/testconfigs/*.yaml` (e.g. `indirectgroups`,
`caveatarrow`, `nestedwildcardexclusions`, `relexpiration`…) plus `consistency_test.go`. Each
file is a schema + relationships + expected Check/Expand/Lookup assertions. The
`pkg/validationfile` loader defines the format.

**Port the loader and run this corpus as the .NET conformance suite.** It gives an executable,
classic-TDD definition of "correct" that is independent of our implementation, and it lets us
build the engine red→green file by file. This aligns with a sociable-tests TDD approach: test
through the public Check/Lookup surface against real schema+data, not mocks.

---

## 6. Proposed porting workflow (phased, each phase shippable & tested)

**Phase 0 — Foundations (no actors).**
v1 proto compiled to .NET; core data types (`ObjectAndRelation`, `RelationReference`,
`NamespaceDefinition`, `UsersetRewrite`); ZedToken encode/decode; the `validationfile` YAML
loader. Exit: corpus files parse into in-memory objects.

**Phase 1 — Single-node correctness (no Orleans yet).**
Schema DSL compiler + reachability graph; in-memory MVCC datastore; the `Check` engine
(unions/intersections/exclusions/arrows/wildcards) with visited-set+depth termination. Drive entirely
by the YAML conformance corpus. Exit: all non-caveat, non-reverse Check tests green.

**Phase 2 — Introduce Orleans.**
Make every sub-problem a `CheckGrain` call behind the dispatcher seam, replacing direct
recursion with grain-to-grain dispatch, and let the grain directory own placement. Move the
dispatch cache into activation state. Exit: corpus still green, now running on a multi-silo
cluster; hop and memo metrics visible.

**Phase 3 — Reverse & expand.**
`Expand`, `LookupResources`, `LookupSubjects` as `IAsyncEnumerable` grains with cursors, using
the reachability graph to prune. Exit: reverse-index corpus files green.

**Phase 4 — ABAC & freshness.**
Caveats (CEL decision per §4), expiration, Watch/changefeed via a per-silo grain-observer
notifier over the datastore log, BatchCheck. Exit: `caveat*` and `relexpiration*` corpus files
green.

**Phase 5 — Storage & scale.**
Durable persistence for the event-sourced datastore grain via the AdoNet (Postgres)
grain-storage provider — no application SQL; consistency/perf benchmarks; tune the placement
policy and concurrency limits. Exit: durable-storage tests + load test.

---

## 7. One-paragraph summary

Storage and consistency stay exactly as Zanzibar designed them: MVCC, revision-scoped reads —
*not* dispatch-grain state. But Orleans realizes the datastore itself too, as a single
**event-sourced grain** whose append-only log is the source of truth: the log offset is the global
revision (the total order that defeats "new enemy"), commits are cheap appends, per-silo projections
fold the log for local reads, and the same feed powers Watch and an optional Leopard index — with no
bespoke SQL datastore (durability rides Orleans grain storage). The other win from Orleans is in the
**dispatch layer**:
SpiceDB's recursive Check decomposes into pure, cacheable sub-problems that it routes by
consistent hashing, deduplicates with singleflight, and rebalances across a hashring — and
those three mechanisms are precisely what Orleans virtual actors give for free via placement,
single-activation, and cluster membership. So we port the *graph evaluation engine* and the
*schema compiler* (the genuinely hard, actor-agnostic code), keep the v1 gRPC API byte-
compatible, reuse SpiceDB's YAML conformance corpus as the compatibility anchor (a finite regression suite), and let the Orleans
runtime replace the entire `remote`/`cluster`/hashring distribution layer.

Candidate directions beyond this design — further Orleans-native consolidation and deliberate
relaxations of Google-contingent Zanzibar details — are analyzed in [`future-work.md`](future-work.md).
