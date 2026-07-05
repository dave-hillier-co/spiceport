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
- **The never-an-oracle discipline generalizes.** Any accelerating index or materialized view
  either has its candidates confirmed by `CheckEngine`, or earns oracle status only through a
  fold-correctness equivalence gate (the Leopard on==off pattern).
- **The `authzed.api.v1` surface** stays `zed`-compatible.

---

## Part 1 — Leaning further into Orleans

### 1.1 Leopard as an incremental log projection (implemented)

`MembershipIndexCache` now bootstraps once from a snapshot and folds incrementally from the log
tail, following the `SiloProjection` pattern. It applies each `LogEvent`'s relationship deltas
to the reverse-adjacency sets incrementally, with full rebuild only on schema change or when log
compaction advances past the cache watermark. The index remains a complete candidate superset
confirmed by `CheckEngine`, never an oracle — it cannot change a verdict.

### 1.2 Reminder-driven MVCC garbage collection (implemented)

An Orleans **Reminder** on the singleton `DatastoreGrain` periodically appends a `GcApplied(floor)`
event to the log. Because GC is itself a log event, every fold — grain state, silo projections, the
membership index — applies it identically. The collect drops relationship rows fully dead below the
floor, sweeps expired tuples, and compacts old schema and counter versions. Reads pinned below the
floor throw `RevisionNotFoundException`, so consumers re-bootstrap; stale zookies map to
`InvalidArgument`. The floor defaults to a 24-hour window, bounding state growth and aligning with
Zanzibar/Spanner's use of zookie staleness as a retention boundary.

### 1.3 Activation-as-cache (the dispatch cache dissolves into the runtime)

**Current state.** Caching is a separate layer (`CachingDispatcher`) beside the actor runtime,
with its own keying, TTL, and the most intricate wiring in the codebase (the silo-wide root /
`ISiloDispatcher` holder cycle).

**Direction.** In the paper, subproblem results are cached at the *delegate* — the server that
owns the subproblem on the hash ring — and a lock table dedupes concurrent misses. Orleans
expresses both natively: the `CheckGrain` key already **is** the cache key, single-activation
**is** the lock table. Let the activation hold the computed pre-context `Branch`; express eviction
as activation collection age tuned near the revision-quantization window (the quantized revision +
schema hash in the key make the keyspace rotate every window, so idle-collection is the TTL).

The invariants transfer structurally rather than by convention:

- *Branch, not verdict*: the activation memoizes the pre-context `Branch`; `Collapse` stays
  per-request.
- *Cycle-cut results not cached*: a `Branch` computed under a traversal-bloom cut is served but
  not retained.
- *Exact vs optimized reads*: exact-revision requests mint a different (unquantized) grain key —
  a naturally separate keyspace, replacing the `RevisionMode`-in-cache-key convention.

**Known tensions.**
1. *Hot-key serial turns*: a memoized activation processes turns serially; serving a memoized
   `Branch` is pure, so the serve path wants `[AlwaysInterleave]` (compute once under the implicit
   lock table, serve concurrently).
2. *Remote re-hop*: callers stop caching remote results locally and re-hop to the warm owner —
   the paper's exact trade. Empirical question, not architectural.
3. *The local-recurse hole*: locally-owned subproblems bypass grains, so they would bypass the
   activation cache. The pure answer — stop bypassing — is the step most likely to cost latency
   and must be benchmark-gated (see 1.4 and 1.9).

**Staging.** (a) Move the `Branch` memo into `CheckGrain` activation state with `[AlwaysInterleave]`
serving and tuned collection age, keeping everything else; (b) measure hit rates and latency
against the mesh benchmarks; (c) only then decide whether the caller-side cache and the
local-recurse hybrid earn their complexity or get deleted. Each step independently shippable and
reversible; `CachingDispatcherTests` and the corpus gate every move.

### 1.4 Directory-owned location: delete the hash ring

**Current state.** `ConsistentHashPlacement` + `HashRing` + `ISiloOwnership` exist for one reason:
the local-recurse shortcut must compute a subproblem's owner silo *without activating the grain*.

**Direction.** Single-activation dedupe is enforced by the Orleans **grain directory**, not by
placement — placement only chooses where the *first* activation lands. If every subproblem is a
grain call (the endpoint of 1.3), ownership never needs to be computed: just call, and the
directory finds or creates the activation. The hand-rolled ring becomes deletable and placement
becomes a pluggable policy — including `ResourceOptimizedPlacement` and Orleans activation
rebalancing, which self-balances hot silos in a way a static ring cannot.

**Wins.** Deletes an entire subsystem; unlocks runtime-managed load balancing.
**Dependency.** Only coherent as the completion of 1.3(c).

### 1.5 Native cancellation propagation (implemented)

`OrleansDispatcher` now bridges each engine `CancellationToken` to an Orleans
`GrainCancellationToken`, and `CheckGrain` passes its underlying token into the local engine.
Cancellation therefore continues through every recursive child dispatch instead of stopping at
the first grain boundary. Delivery to a remote activation is awaited before the cancelled hop
unwinds, so gRPC deadline expiry prunes the mesh-wide computation.

### 1.6 Grain call filters for the cross-cutting layer

**Current state.** `DispatchErrorMapper`, `IDispatchMetrics`, and depth-budget enforcement are
hand-threaded through the dispatch path.

**Direction.** `IIncomingGrainCallFilter` / `IOutgoingGrainCallFilter` are the native interceptor
seam: exception mapping, hop counters, and depth enforcement become one filter each, and the
dispatchers stop carrying plumbing that is not dispatch logic.

### 1.7 `RequestContext` for traversal state

`depthRemaining` and the traversal bloom are call-chain context, not part of the subproblem
identity, yet they ride in the request DTOs. Orleans `RequestContext` flows implicitly across
grain calls; moving them there makes the wire contract exactly the canonical subproblem.
**Trade, stated honestly:** implicit context is harder to see in tests and debuggers — this is
taste as much as simplification, and the lowest-priority item here.

### 1.8 `GrainService` for the per-silo components

`SiloProjection`, `LogWatchHub`, and `MembershipIndexCache` are the "one per silo" pattern
hand-built as DI singletons with manual lifecycle. The native primitive for that shape is the
**GrainService**: silo-lifecycle-managed (the projection could bootstrap *before* the silo accepts
traffic) and addressable from grains. Moderate win; the DI versions are workable.

### 1.9 `[Immutable]` wire types (implemented)

The check request (`DispatchCheckArgs`), pre-context branch reply (`DispatchCheckReply`), and
folded `LogEvent` are marked `[Immutable]`, eliminating their defensive deep copy on same-silo
grain calls. This is a small win alone, but helps make 1.3(c)/1.4's "always call the grain, even
locally" path cheap enough to benchmark fairly.

### 1.10 Native `IAsyncEnumerable` grain streaming (internal paths)

Client-facing cursors are API contract and must stay (they survive reconnects). The *internal*
page-loop plumbing between service, grain, and engine for reverse ops / bulk export can become
plain `IAsyncEnumerable` grain calls with native backpressure, deleting cursor round-trips where
nothing needs resumability.

### 1.11 Broadcast channel for the log fan-out (deliberately deferred)

A per-silo implicit subscription could unify projection freshness and the Watch signal into one
push feed. Deferred because the observer + slow-heartbeat design is simpler than adding a stream
provider, and the closed-timestamp gate needs a pull path for exactness anyway. Revisit only if
projection catch-up latency appears in profiles.

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

**Cost.** "Tenant" enters every grain key, every projection, and the API surface — an
architectural fork that deserves its own design document before any code. Highest leverage on
this list: it changes what the system *is* (single-tenant engine → multi-tenant platform).

### 2.2 Incrementally-materialized reachability: O(1) checks

**The contingent detail.** The paper computes every check on demand and confines denormalization
to Leopard because materializing views at Spanner write volumes was infeasible — a cost argument,
not a correctness one.

**The relaxation.** With the event log, a materialized "who-can-see-what" reachability set is
*just another fold*, incrementally maintained like the projection. For non-caveated paths, Check
becomes a hash lookup; the dispatch mesh remains the evaluator for caveated/complex paths and the
verifier for the view. The never-an-oracle discipline applies until a corpus equivalence gate
(the Leopard on==off pattern) earns the view oracle status.
**Sequencing.** The performance moonshot; wait until 1.1 proves the incremental-fold pattern.

### 2.3 Read-your-writes by default without weakening zookies

**The contingent detail.** Zanzibar defaults to bounded staleness because cache shareability at
~10⁷ QPS demands quantization, pushing the freshness burden onto clients via zookie plumbing.

**The relaxation.** The closed-timestamp gate makes freshness a cheap watermark wait on a local
projection. Flip the default: every read is read-your-writes unless the caller opts into staleness
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

## Part 3 — Deliberate non-goals

Recorded so they are not relitigated by accident:

- **Orleans distributed transactions.** The single ordered log is the serialization point;
  transactions would blur the global-order story that defeats the new-enemy problem.
- **Sharded / per-namespace logs within a tenant.** Reintroduces cross-shard ordering. (2.1 is
  not an exception: tenant isolation removes cross-shard *edges*, which is what makes per-tenant
  logs sound.)
- **Per-object state grains.** Ruled out in `architecture-analysis.md` §3.1: too large/cold to
  activate economically; zookie point-in-time reads are incompatible with "the grain's latest
  value".
- **Removing or weakening zookies.** They remain a first-class compatibility and consistency
  contract. Any future read-your-writes default is additive and applies only when the caller does
  not supply one.
- **Per-revision state grains ("versions as actors").** Revisions are *identities* (dispatch keys,
  cache keys, watermarks), not state-bearing actors. Version-state grains would lose structural
  sharing across the grain boundary (each activation holds isolated memory, so consecutive
  revisions could not share structure), reintroduce per-Check hops or full fetches, and make the
  hot quantized revision a cluster-wide single-activation bottleneck — while the write side still
  needs the one serialization point regardless. The narrow variant that may someday earn a place:
  immutable snapshot/log-segment grains for *bootstrap distribution only* (new silos hydrating
  from a peer instead of the singleton).

## Suggested ordering, if taken as a program

1. **1.3 activation-as-cache**, staged and benchmark-gated, and — only if stage (c) wins its
   benchmark — **1.4**.
2. **2.3 / 2.4 / 2.5** — cheap, immediately differentiating product capabilities.
3. **2.1 per-tenant** and **2.2 materialized reachability** — each behind its own design document.
