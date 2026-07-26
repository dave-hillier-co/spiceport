# Future work: candidate architectural directions

This document records directions that have been analyzed and judged promising but are **not
committed work**. It complements `architecture-analysis.md` (which explains the architecture as
built); this file explains where the architecture *could* go, why, and what each move would cost.
Two lenses organize it:

1. **Lean further into Orleans** — replace remaining hand-rolled mechanisms with runtime
   primitives, continuing the port's founding thesis (port the graph engine, delete the
   distribution plumbing).
2. **Relax Google-contingent Zanzibar details** — the paper's implementation choices encode
   Google's constraints (Spanner's cost model, ~10⁷ QPS, one globally-interlinked graph). Where a
   constraint does not apply here, relaxing it buys capabilities the paper could not offer.

## Invariants that bound all of this

Whatever is taken from this list, these do not move:

- **The conformance corpus stays green, unweakened.** Every direction below is additive
  (stronger defaults, more atomicity, more history, more scale) — never a semantic change to
  Check/Expand/Lookup verdicts.
- **New-enemy protection can be strengthened, never weakened.** Zookies remain a first-class API
  contract: they must not be removed, deprecated, or semantically weakened, even if they stop
  being the primary consistency mechanism.
- **The candidates-never-verdicts discipline generalizes.** Any accelerating index or materialized
  view has its candidates confirmed by `CheckEngine` (soundness) with completeness pinned by an
  on==off equivalence gate (the Leopard pattern); it may serve verdicts directly only after a
  fold-correctness equivalence gate proves it verdict-identical to the live evaluator.
- **The `authzed.api.v1` surface** stays `zed`-compatible.

---

## Part 1 — Leaning further into Orleans

### 1.1 Leopard as addressable sibling-recursion walk grains (implemented)

The per-silo `MembershipIndexCache`/`MembershipIndex` replica (a flattened reverse-adjacency
snapshot, folded incrementally from the log tail) is retired. In its place, `IMembershipWalkGrain`
is a grain keyed by "the membership-walk closure rooted at subject key `type:id#relation` at
(revision, schemaHash)": each activation computes ONE reverse-adjacency hop
(`MembershipWalk.DirectParents`, a single `ReverseQueryRelationships` post-filtered to the schema's
coverage scan set — see `MembershipCoverage`, the pure schema analysis extracted from the retired
index's `Build`) and dispatches every parent onward to the SIBLING grain keyed by that parent — the
same cross-grain-recursion idiom `CheckGrain` uses for `DispatchCheck`, with an exact ancestor path
(not a probabilistic bloom) as the cycle guard, since a false-positive skip here would silently drop
a candidate subtree. Because a walk runs over a reader pinned to one exact MVCC revision, it is
revision-exact by construction — there is no fold/catch-up machinery to keep correct as the log
advances, and a delete excludes its detached subtree immediately at the post-delete revision (the
old replica's weak spot). Cold subject keys simply never activate; warm ones deactivate on ordinary
idle collection like any other grain, sharding the working set instead of replicating it whole on
every silo. The accelerator yields candidates, never verdicts: `CheckEngine` confirmation
guarantees soundness, and the walk-on==walk-off equivalence gates pin completeness (a silently
missing candidate is the failure confirmation cannot see).

### 1.2 Reminder-driven MVCC garbage collection (implemented)

An Orleans **Reminder** on the singleton `DatastoreGrain` periodically appends a `GcApplied(floor)`
event to the log. Because GC is itself a log event, every fold — grain state, graph-shard folds, the
membership index — applies it identically. The collect drops relationship rows fully dead below the
floor, sweeps expired tuples, and compacts old schema and counter versions. Reads pinned below the
floor throw `RevisionNotFoundException`, so consumers re-bootstrap; stale zookies map to
`InvalidArgument`. The floor defaults to a 24-hour window, bounding state growth and aligning with
Zanzibar/Spanner's use of zookie staleness as a retention boundary.

### 1.3 Activation-as-cache (the dispatch cache dissolves into the runtime) (implemented)

In the paper, subproblem results are cached at the *delegate* — the server that owns the
subproblem on the hash ring — and a lock table dedupes concurrent misses. Orleans expresses both
natively: the `CheckGrain` key is the cache key, single-activation is the lock table. The
activation holds the computed pre-context `Branch`; eviction is activation collection age tuned
near the revision-quantization window.

The invariants transfer structurally:
- The activation memoizes the pre-context `Branch`; `Collapse` stays per-request.
- Cycle-cut results are served but not retained.
- Exact-revision requests mint a different (unquantized) grain key — a naturally separate
  keyspace.

**Stages (a)/(b)/(c) all implemented.** Stage (a) established the activation memoization layer.
Stages (b) and (c) — deletion of the caller-side `CachingDispatcher` and the elimination of the
locally-owned subproblem bypass — were resolved by **MAINTAINER DECISION for simplicity over
performance** (the benchmark gate was deliberately skipped). Every sub-problem now flows through
a grain call, and an exact visited-set hit forces the normal (reentrant) grain call with the result
force-tagged `CycleCut` at the caller.

### 1.4 Directory-owned location: delete the hash ring (implemented)

Single-activation dedupe is enforced by the Orleans **grain directory**, not by placement —
placement only chooses where the *first* activation lands. With every subproblem now a grain
call, ownership never needs pre-computation: just call, and the directory finds or creates the
activation. The hand-rolled `ConsistentHashPlacement`, `HashRing`, and `ISiloOwnership`
subsystems have been deleted. Placement becomes a pluggable policy, including potential future
use of `ResourceOptimizedPlacement` and Orleans activation rebalancing.

### 1.5 Native cancellation propagation (implemented)

Every unary grain method (`ICheckGrain.DispatchCheck`, `ISubjectFrontierGrain.GetFrontier`,
`IMembershipWalkGrain.GetContainingSet`) takes a plain `CancellationToken` directly — Orleans
10.1 propagates it natively across the grain boundary, so there is no hand-rolled bridging type
and no per-call-site cancellation-source plumbing. `OrleansDispatcher` passes the caller's own
token straight into `ICheckGrain.DispatchCheck`; `CheckGrain` passes its own incoming token into
the local engine and into every recursive child dispatch, so cancellation continues through the
whole call tree instead of stopping at the first grain boundary. A caller-cancelled call faults
the underlying grain call itself with an `OperationCanceledException`, which
`CheckDispatchOutgoingCallFilter` classifies exactly like any other dispatch failure (→
`Cancelled`) — no separate caller-side race is needed. Delivery to the remote activation is
fire-and-forget by default (Orleans' `WaitForCancellationAcknowledgement` messaging option, off
by default): the local await unwinds as soon as the cancellation signal is sent, without waiting
for the remote activation to acknowledge it.

### 1.6 Grain call filters for the cross-cutting layer (implemented)

`CheckDispatchOutgoingCallFilter` maps cross-silo `DispatchCheck` exceptions into the
dispatch-error taxonomy (transient → `Unavailable`, cancellation → `Cancelled`, domain
exceptions pass through unchanged). `CheckDispatchIncomingCallFilter` increments the hop
counter and enforces the boundary-depth ceiling via the `DepthRemaining` RequestContext value.
The dispatchers now carry only dispatch logic (key building, cancellation bridging, the exact
visited-set loop-bypass tag), with error mapping and hop counting delegated to the native
interceptor seam.

### 1.7 `RequestContext` for traversal state (implemented)

`DepthRemaining` and the exact visited set now ride in the Orleans `RequestContext` via the
scoped `DispatchContext` helper, not in the request DTOs. The wire contract for `DispatchCheck`
is now exactly the canonical sub-problem (the grain key) plus the cancellation token.
**Trade, stated honestly:** implicit context is harder to see in tests and debuggers — the
repo accepts this with an explicit test helper (`SetDispatchContext`) to inject context for
unit verification.

### 1.8 Lifecycle ownership of the per-silo components (implemented, then simplified by 1.13)

The remaining per-silo component is the `LogWatchHub` Watch notifier, a plain DI singleton whose
lifetime belongs to the container, which disposes it (bounded observer unsubscribe) on silo
teardown. A `GrainService` (`DatastoreProjectionService`) originally existed to bootstrap the
per-silo `SiloProjection` before the silo accepted traffic; the graph-sharded datastore (1.13)
retired the projection and with it the service — there is nothing to bootstrap pre-traffic,
because `GraphShardGrain`s hydrate their own key's slice on first touch. The Leopard accelerator
(§1.1) likewise needs no per-silo lifecycle-managed component: `IMembershipWalkGrain` activations
resolve on demand via `IGrainFactory`, keyed by (subject, revision, schemaHash), so there is no
shared singleton to bootstrap.

### 1.9 `[Immutable]` wire types (implemented)

The pre-context branch reply (`DispatchCheckReply`) and folded `LogEvent` are marked `[Immutable]`,
eliminating their defensive deep copy on same-silo grain calls. (The check request `DispatchCheckArgs`
was deleted in 1.7; the wire contract is now just the grain key + cancellation token.) This supports
the "always call the grain, even locally" pattern (1.3(c)/1.4) by reducing the cost of intra-silo
grain calls.

### 1.10 In-process reverse-ops / data-plane reads (implemented)

`LookupSubjects`, `LookupResources`, `ExpandPermissionTree`, `ReadRelationships`, and
`BulkExportRelationships` run IN-PROCESS in the gRPC service layer — the same pattern
`AuthzedWatchV1Service` already uses for Watch (see its remarks). Two
Server-layer helper classes, `ReverseOps` (over the `IGraphReaderSource` shard mesh) and
`RelationshipReads` (over the `ISnapshotScanner` scan seam), hold the pinning /
schema-at-revision resolution / caveat-collapse
logic and expose the same `IAsyncEnumerable`/`Task` shapes an earlier Guid-keyed grain layer used to; they
are registered as DI singletons alongside the rest of the grain mesh services
(`AddSpiceportGrainServices`) and injected straight into the gRPC services. A dedicated per-stream
grain hop bought only compute placement, not a consistency or caching
benefit — unlike `CheckGrain`, `SubjectFrontierGrain`, and `MembershipWalkGrain`, which memoize state
across calls and so stay real grains, dispatched to from these in-process walks exactly as before.

Client-facing cursors are unchanged API contract. Every streamed item still carries its own opaque resume
cursor (the byte-identical token from the existing codecs — the subject-id cursor, the multi-section
lookup-resources keyset cursor, the revision-pinned bulk-export cursor), so a client-supplied cursor still
resumes mid-stream and the token formats are unchanged. The limited RPCs take at most the requested items
and echo the last item's cursor exactly as before.

Being plain in-process C# rather than grain code, the streaming ops take the caller's plain
`CancellationToken` directly with no Orleans grain-method plumbing to bridge — simpler than the retired
Guid-per-stream grain scheme, with identical per-item cancellation behavior. The Leopard membership index
still accelerates an unlimited, cursorless `LookupResources` (its fast path yields a complete candidate set
confirmed by Check, dispatched via `IMembershipWalkGrain` across the mesh); a *limited* walk runs the
cursor-bearing live traversal so every item carries a resume cursor.

A prior iteration of this design routed these ops through Guid-keyed grains under default placement,
because native Orleans `IAsyncEnumerable` grain streaming is not safe on a `[StatelessWorker]` grain (the
grain-side extension pins the live enumerator to one activation keyed by a client-minted request id, and a
stateless worker's activations are not individually addressable). Since every read those grains performed
was already served from local memory, the grain hop bought no consistency or caching benefit — only a real
but pointless per-stream activation cost — so that layer was deleted in favor of running the same logic
directly in the gRPC service process.

### 1.11 Broadcast channel for the log fan-out (deliberately deferred)

A per-silo implicit subscription could unify shard catch-up freshness and the Watch signal into one
push feed. Deferred because the observer + slow-heartbeat design is simpler than adding a stream
provider, and the closed-timestamp gate needs a pull path for exactness anyway. Revisit only if
shard catch-up latency appears in profiles.

### 1.12 `ISnapshotSegmentGrain` for the bootstrap read path (dissolved by 1.13)

`ISnapshotSegmentGrain` keyed by snapshot log version was the deferred answer to multi-silo
cold-start contention on the whole-snapshot bootstrap. The graph-sharded datastore (1.13)
dissolved the problem rather than optimizing it: there is no whole-snapshot silo bootstrap — a
cold `GraphShardGrain` hydrates its own key's slice on first touch via a per-key `ReadShard` read.

### 1.13 Graph-sharded datastore: dissolve `IDatastore` into grains (realized)

The deepest lean-into-Orleans move, realized in full: the per-silo `SiloProjection`
whole-graph replica is retired; engine reads resolve to per-key `GraphShardGrain`s (forward by
object, reverse by subject), each the per-key restriction of the same log fold (`ShardFold`),
activation-as-hot-set-cache with a per-shard closed-timestamp watermark; writes are declarative
`DatastoreGrain.Commit` requests executed at the serialization point with typed `CommitReply`
failures; broad scans and schema-at-revision go storage-direct (`ISnapshotScanner` /
`ISchemaSource`); the **thin-sequencer flush protocol** removed the fold from the sequencer
grain itself (slim meta state + dirty-buffer flush to version-qualified per-key rows, the head
row as the sole commit point, O(tail) recovery, in-place migration from the whole-state
layout); and the **co-placement director** (`GraphLocalityPlacement`) is built as a
default-off locality hint whose enablement is gated on measurement. No component holds the
whole graph: silo memory is O(hot shards), sequencer memory O(tail + dirty keys + metadata).
The single-writer total order is deliberately retained. Design, interfaces, the §3.1
objections, and the staging are in [`graph-sharded-datastore.md`](graph-sharded-datastore.md).

### 1.14 Scalability program: measure, then earn each optimization

With the state ceilings removed structurally, the remaining scalability questions live in
constants — grain-hop cost, per-commit candidate I/O, shard catch-up fan-in, large-userset
marshalling, key-index write amplification — none of which have been measured, by deliberate
stance. [`scalability-program.md`](scalability-program.md) is the standing program: an
unconditional measurement harness plus a set of trigger-gated mitigations (per-silo log-tail
cache, subject-filter pushdown on shard reads, commit-turn shortening, key-index chunking,
storage-direct scans) each with a design sketch, an equivalence gate, and the measured
condition that would justify building it. Nothing in it is scheduled; triggers decide.

---

## Part 2 — Relaxing Google-contingent Zanzibar details

### 2.1 Per-tenant datastore grains: linear write scaling

**The contingent detail.** The single-writer log ceiling was accepted because sharding the log
reintroduces cross-shard ordering — the problem Spanner exists to solve. That objection holds only
while tuples can reference each other across the whole graph, which at Google they genuinely do
(one Groups namespace serves every product).

**The relaxation.** A multi-tenant deployment has no cross-tenant edges. Key the `DatastoreGrain`
by tenant: each tenant gets its own log, its own total order, its own GC window and schema — and
no cross-shard ordering problem, because there are no cross-shard edges. Virtual actors add what
Google's design cannot: cold tenants deactivate to zero cost; hot tenants get their own
serialization point. The single-writer ceiling becomes per-tenant — effectively gone.

**Discounted (maintainer decision).** The load-bearing cost is not the grain — Orleans makes the
per-tenant `DatastoreGrain` key nearly free — but that "tenant" has no home in the
`authzed.api.v1` wire protocol, which carries no tenant field. This project supports *only* that
protocol (the same constraint that keeps the surface `zed`-compatible), so a tenant could enter
only out-of-band (a metadata header threaded through every RPC), turning tenant isolation into a
security boundary — a permanent tax on every future change — to buy write scaling this project
does not need. Multi-customer deployments are already served the Zanzibar-native way: model a
`tenant` object in the schema and hang everything off it, on the one existing graph, with no code.
So per-tenant sharding is discounted, not scheduled. Were it ever revisited, the first artifact is
a threat-model of the isolation guarantees, not a grain-key change (the grain-key change is the
easy thing that makes the hard thing look done).

### 2.2 Incrementally-materialized reachability: O(1) checks

**The contingent detail.** The paper computes every check on demand and confines denormalization
to Leopard because materializing views at Spanner write volumes was infeasible — a cost argument,
not a correctness one.

**The relaxation.** With the event log, a materialized "who-can-see-what" reachability set is
*just another fold*, incrementally maintained like the graph shards. For non-caveated paths, Check
becomes a hash lookup; the dispatch mesh remains the evaluator for caveated/complex paths and the
verifier for the view. The candidates-never-verdicts discipline applies until an equivalence gate
(the Leopard on==off pattern) proves the view verdict-identical and lets it serve verdicts directly.
**Sequencing.** The performance moonshot; wait until 1.1 proves the incremental-fold pattern.

### 2.3 Read-your-writes by default without weakening zookies

**The contingent detail.** Zanzibar defaults to bounded staleness because cache shareability at
~10⁷ QPS demands quantization, pushing the freshness burden onto clients via zookie plumbing.

**The relaxation.** The closed-timestamp gate makes freshness a cheap per-shard watermark wait.
Flip the default: every read is read-your-writes unless the caller opts into staleness
for latency. This changes only the default for callers that do not supply a zookie.

**Non-negotiable constraint.** Zookies remain fully supported and first-class for explicit
revision chaining and new-enemy protection. This direction must not remove, deprecate, hide, or
weaken them. "Optional" means a caller can obtain a safe default without supplying one; it does
not make the zookie mechanism optional for the server to implement.

### 2.4 Transactional schema+data migrations

**The contingent detail.** Spanner gives Zanzibar per-write atomicity only; namespace-config
changes are a staged-rollout dance because config and data cannot move together.

**The relaxation.** Everything here already serializes through one CAS append. That serialization
point can carry more: schema change + tuple rewrites as *one atomic log event* — "rename this
relation and rewrite its tuples, atomically, at a single revision." A capability neither Zanzibar
nor SpiceDB offers, nearly free in this architecture.

### 2.5 Time-travel and audit as product features

**The contingent detail.** The paper's changelog is internal plumbing for Watch and Leopard;
retention is an ops knob.

**The relaxation.** Here the log is the source of truth and point-in-time evaluation already
works (checks are pure functions of `state@rev`). Expose it: "did this subject have access at
time T", "diff the access set between two revisions", "explain this verdict at the revision it
was granted". The GC window becomes a *product* knob (audit retention) rather than a cache bound.
Pairs with, and constrains, 1.2 (GC floor = audit horizon).

### 2.6 Revision-exact downstream consumers

**The contingent detail.** In the paper, Watch consumers (e.g. search ACL filters) are eventually
consistent with checks.

**The relaxation.** Watch and Check consume the same total order here, so a downstream projection
— an ACL-aware search index, a permission cache in another service — can be *provably* consistent
at a named revision, using the checkpoint semantics the feed already emits. The
transactional-outbox guarantee, structurally.

---

### 1.15 Decomposing the order's work: batch → ticket → Calvin (trigger-gated ladder)

The total order stays (see Part 3's standing analysis for why), but "the order" and "the
order's WORK" are different things — the sequencer activation currently performs four jobs of
which only one (issuing the order) is irreducibly singular. A ladder of decompositions, each
strictly more actor-shaped, none weakening the guarantee, each gated on the scalability
program's measurement discipline (`scalability-program.md`) with the trigger being measured
write demand, not taste:

1. **Group commit.** Batch concurrent `Commit` requests into one log append — classic WAL
   amortization; the actor stays, its throughput multiplies by batch depth. The first rung if
   the write ceiling ever fires.
2. **The Corfu split: order without data.** The sequencer becomes a ticket dispenser —
   assigns positions, touches no payload; log storage moves to sharded segment rows/actors.
   The thin sequencer is already halfway there (the flush protocol removed the fold); this
   removes the data path. A near-stateless counter actor can ticket orders of magnitude more
   commits than it can execute.
3. **Calvin-style deterministic execution.** Deterministic databases showed that if a
   transaction's read/write set is declared before execution, a cheap global SEQUENCE plus
   deterministic per-shard execution yields cross-shard serializability without two-phase
   commit: single-shard transactions (the common case) never coordinate beyond the ticket;
   multi-shard ones do one deterministic agreement round. The enabling asset already exists:
   the declarative `CommitRequest` with candidate-key resolution IS the read/write-set
   declaration Calvin requires. This moves execution — the sequencer's heaviest job — into
   the shard actors. Note what it keeps: a total order of REQUESTS, thinned to near-nothing,
   not removed.
4. **HLC + commit-wait per shard** — the fully-decentralized rung, external consistency with
   no ordering actor at all, priced in clock discipline (commit-wait at NTP error is WORSE
   than a sequencer hop; PTP makes it competitive at real operational burden) and
   k-dimensional token plumbing. Ranked below Calvin on complexity-per-benefit; recorded so
   the option is priced, not forgotten.

## Part 3 — Deliberate non-goals

Recorded so they are not relitigated by accident:

### The standing analysis: is total order truly required?

Asked directly (and worth re-asking): the graph has many seemingly unrelated parts, and
causal machinery such as vector clocks solves ordering elsewhere — so is the total order a
requirement or a habit? The analysis, recorded so the next round starts here:

**What the contract minimally requires** — four obligations, none of which literally says
"total order":

- **R1 (order):** commit order must extend *(in-system causality ∪ real-time order of
  non-overlapping commits)*. Truly concurrent writes may be left unordered or ordered
  arbitrarily — no observer can ever hold evidence of either ordering.
- **R2 (snapshots):** every read is a consistent CUT of that order, and cuts exposed along an
  observer chain must be monotone.
- **R3 (tokens):** zookies encode cuts with a computable dominance test (at-least-as-fresh =
  cut dominance).
- **R4 (the rest of the API):** preconditions and multi-key writes need cross-shard
  serializability; Watch needs one stable resumable linearization; fully-consistent reads
  need a cut dominating everything committed.

**Why R1 includes real time — the load-bearing subtlety.** The paper's canonical new enemy
(remove the viewer, then have someone ELSE add the sensitive content) carries its causal
chain through channels the system cannot see — a conversation, an RPC between services that
never touch a zookie. Vector clocks capture in-system causality only; out-of-band causality
is invisible to them but not to real time, because every physical channel has latency —
which is precisely what Spanner's TrueTime exploits. Without any time authority, two
causally-unlinked writes on different shards stay incomparable FOREVER: a "consistent"
vector cut can mix one shard's morning with another's evening, serving a state that never
existed at any instant — an unbounded misordering, not a race window. The graded ladder:

| Mechanism | R1 coverage | Residual anomaly |
|---|---|---|
| Single sequencer | full | none |
| TrueTime / commit-wait | full | none (paid as +ε write latency) |
| HLC, no commit-wait | causal + real time up to clock skew | out-of-band channels faster than skew (CockroachDB documents this exact gap as its "causal reverse" anomaly — the one thing Spanner prevents that it does not) |
| Vector clocks alone | in-system causality only | **unbounded** |

Zanzibar's own default STALENESS is not a precedent for relaxing this: stale reads are old
but prefix-consistent — real past states of the one commit order. A non-prefix cut serves a
fiction. The whole SpiceDB-compatible behavioral surface (at-exact-snapshot replay included)
assumes prefix semantics.

**The re-derivation problem.** Build on R1–R4 without the total order and the API
regenerates small total orders anyway: Watch's resumable checkpointed stream is a
constructed linearization (a total order moved to read time and frozen forever); Calvin-style
precondition serializability is a total order of requests; fully-consistent reads need a
global-frontier authority; tokens become k-dimensional vectors with k-fold dominance gates
on every chained read. **The total order is not the requirement — it is the FIXED POINT: the
closed form from which all four obligations fall out as corollaries.** Dismantling it does
not delete the concept; it shatters it into pieces that must each be maintained.

**The cost model.** Removing the singleton buys write throughput beyond one actor and write
availability beyond one activation — headroom the project's recorded scale stance does not
currently need — against commit-wait or anomaly windows, k-dimensional tokens, a multi-shard
transaction protocol, and a materialized Watch linearization. The one case needing NO
cross-ordering — provably disjoint subgraphs — is exactly the tenant partition (2.1),
blocked by the wire protocol, not by ordering theory: within one `authzed.api.v1` graph,
unrelatedness is one write away from ending.

**Re-open triggers** (either suffices to revisit this analysis rather than the conclusion
being permanent): measured write demand exceeding what a batched ticket actor can issue
(see 1.15 rungs 1–2), or the wire-protocol constraint relaxing enough to express tenancy —
at which point per-tenant total orders (and vector-of-tenant-heads zookies) become the
natural design rather than a compromise. The rig's durable campaign put the first real
number behind the demand side of that trigger: with a durable backend the unbatched
single-writer ceiling is roughly one commit per durable-append turn (an fsync-class mean —
on the order of a hundred single-update commits per second on modest hardware,
proportionally fewer as updates per commit grow), so rung 1 (group commit)
becomes worth building as soon as a durable deployment's sustained write demand approaches
its measured ceiling; the rig's `remote-decomposition --durable` cells are the standing
instrument for measuring it.

- **Orleans distributed transactions.** The single ordered log is the serialization point;
  transactions would blur the global-order story that defeats the new-enemy problem.
- **Sharded / per-namespace logs within a tenant.** Reintroduces cross-shard ordering — see
  the standing analysis above for the full argument (what the contract requires, why vector
  clocks cannot carry it, and the two triggers that would re-open the question). Note the
  1.15 ladder is NOT an exception: Corfu and Calvin shard log storage and execution, never
  the order. (2.1 is not an exception either: tenant isolation removes cross-shard *edges*,
  which is what makes per-tenant logs sound.)
- **Per-object *current-state* entity grains.** Ruled out in `architecture-analysis.md` §3.1:
  too large/cold to activate economically; zookie point-in-time reads are incompatible with
  "the grain's latest value". The ruling does not extend to per-key grains holding *versioned
  slices of the MVCC fold* — that is the built graph-shard shape, analyzed in
  [`graph-sharded-datastore.md`](graph-sharded-datastore.md) (see 1.13).
- **Built-in multi-tenancy / per-tenant log sharding.** Discounted (see 2.1). The
  `authzed.api.v1` surface has no tenant field and this project supports only that protocol, so a
  tenant could enter only out-of-band and isolation would become a permanent security boundary —
  for write scaling the project does not need. Multi-customer deployments are served the
  Zanzibar-native way: a `tenant` object modeled in the schema, on the one graph.
- **Any feature requiring a wire-protocol extension.** `authzed.api.v1` compatibility is a hard
  boundary, not just a default. A direction that cannot be expressed within the existing surface
  (or purely server-side beneath it) is out of scope by construction.
- **Removing or weakening zookies.** They remain a first-class compatibility and consistency
  contract. Any future read-your-writes default is additive and applies only when the caller does
  not supply one.
- **Per-revision state grains ("versions as actors").** Revisions are *identities* (dispatch keys,
  cache keys, watermarks), not state-bearing actors. Version-state grains would lose structural
  sharing across the grain boundary (each activation holds isolated memory, so consecutive
  revisions could not share structure), reintroduce per-Check hops or full fetches, and make the
  hot quantized revision a cluster-wide single-activation bottleneck — while the write side still
  needs the one serialization point regardless. The narrow variant this ruling anticipated —
  immutable snapshot grains for bootstrap distribution — was dissolved along with the whole-snapshot
  silo bootstrap itself (see 1.12/1.13).

## Suggested ordering, if taken as a program

1. **1.1–1.7** — all completed. Cross-cutting infrastructure (filters, RequestContext) is now
   in place and the dispatcher seam is clean: error mapping and depth enforcement are native,
   and the wire contract is minimal (sub-problem + cancellation).
2. **1.13 graph-sharded datastore** — realized in full (per-key shard grains, declarative
   `Commit`, storage-direct scans, the thin-sequencer flush protocol, the default-off
   co-placement director), dissolving 1.12 along the way.
3. **1.14 scalability program** — the standing next step: the measurement harness is
   unconditional; every optimization behind it is trigger-gated
   (`scalability-program.md`).
4. **1.11** — the broadcast channel remains a deferred refinement; note the scalability
   program's per-silo log-tail cache (its §3.1) would deliver most of what 1.11 promised,
   via the existing observer push rather than a stream provider.
5. **1.15** — the order's-work ladder (batch → ticket → Calvin) sits behind the write-demand
   trigger; nothing is built until the scalability program's evidence fires it.
6. **2.3 / 2.4 / 2.5** — cheap, immediately differentiating product capabilities.
7. **2.1 per-tenant** and **2.2 materialized reachability** — each behind its own design document.
