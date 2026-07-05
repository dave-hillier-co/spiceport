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
a grain call, and a traversal-bloom hit forces the normal (reentrant) grain call with the result
force-tagged `CycleCut` at the caller.

### 1.4 Directory-owned location: delete the hash ring (implemented)

Single-activation dedupe is enforced by the Orleans **grain directory**, not by placement —
placement only chooses where the *first* activation lands. With every subproblem now a grain
call, ownership never needs pre-computation: just call, and the directory finds or creates the
activation. The hand-rolled `ConsistentHashPlacement`, `HashRing`, and `ISiloOwnership`
subsystems have been deleted. Placement becomes a pluggable policy, including potential future
use of `ResourceOptimizedPlacement` and Orleans activation rebalancing.

### 1.5 Native cancellation propagation (implemented)

`OrleansDispatcher` now bridges each engine `CancellationToken` to an Orleans
`GrainCancellationToken`, and `CheckGrain` passes its underlying token into the local engine.
Cancellation therefore continues through every recursive child dispatch instead of stopping at
the first grain boundary. Delivery to a remote activation is awaited before the cancelled hop
unwinds, so gRPC deadline expiry prunes the mesh-wide computation.

### 1.6 Grain call filters for the cross-cutting layer (implemented)

`CheckDispatchOutgoingCallFilter` maps cross-silo `DispatchCheck` exceptions into the
dispatch-error taxonomy (transient → `Unavailable`, cancellation → `Cancelled`, domain
exceptions pass through unchanged). `CheckDispatchIncomingCallFilter` increments the hop
counter and enforces the boundary-depth ceiling via the `DepthRemaining` RequestContext value.
The dispatchers now carry only dispatch logic (key building, cancellation bridging, the bloom
loop-bypass tag), with error mapping and hop counting delegated to the native interceptor seam.

### 1.7 `RequestContext` for traversal state (implemented)

`DepthRemaining` and the traversal bloom now ride in the Orleans `RequestContext` via the
scoped `DispatchContext` helper, not in the request DTOs. The wire contract for `DispatchCheck`
is now exactly the canonical sub-problem (the grain key) plus the cancellation token.
**Trade, stated honestly:** implicit context is harder to see in tests and debuggers — the
repo accepts this with an explicit test helper (`SetDispatchContext`) to inject context for
unit verification.

### 1.8 `GrainService` for the per-silo components (implemented)

`DatastoreProjectionService` (a `GrainService`) owns the lifecycle of the per-silo
`SiloProjection`/`LogWatchHub` pair: it bootstraps the projection and starts the hub at the
`RuntimeGrainServices` lifecycle stage — strictly before the silo accepts traffic, so the first
request never pays the bootstrap — and disposes the hub (bounded observer unsubscribe) on silo
shutdown. Identity lives in the `IDatastoreProjectionHost` DI singleton rather than the
`GrainService` itself: the only supported DI-reachable client of a live `GrainService` is the
message-passing `GrainServiceClient<T>`, which would put a hop back on the per-Check read path the
projection exists to eliminate. `MembershipIndexCache` stays a plain DI singleton — its build is
request-schema-driven, so lifecycle management adds nothing. The private-instance
`GrainBackedDatastore` constructor survives as the test seam the push-Watch proofs need.

### 1.9 `[Immutable]` wire types (implemented)

The pre-context branch reply (`DispatchCheckReply`) and folded `LogEvent` are marked `[Immutable]`,
eliminating their defensive deep copy on same-silo grain calls. (The check request `DispatchCheckArgs`
was deleted in 1.7; the wire contract is now just the grain key + cancellation token.) This supports
the "always call the grain, even locally" pattern (1.3(c)/1.4) by reducing the cost of intra-silo
grain calls.

### 1.10 Native `IAsyncEnumerable` grain streaming (internal paths) (implemented)

The internal page-loop plumbing between the gRPC services and the reverse-ops / data-plane grains is
now native Orleans `IAsyncEnumerable` grain streaming. `LookupSubjects`, `LookupResources`,
`ReadRelationships`, and `BulkExportRelationships` are single grain calls whose results stream back one
item at a time with runtime backpressure — the per-page service→grain round-trip is deleted. Each gRPC
front door drives one `await foreach` and stops enumerating when it has enough; stopping the client-side
loop stops the upstream engine walk, because the engine ops are themselves `IAsyncEnumerable`.

Client-facing cursors are unchanged API contract. Every streamed item still carries its own opaque resume
cursor (the byte-identical token from the existing codecs — the subject-id cursor, the multi-section
lookup-resources keyset cursor, the revision-pinned bulk-export cursor), so a client-supplied cursor still
resumes mid-stream and the token formats are unchanged. The limited RPCs take at most the requested items
and echo the last item's cursor exactly as before.

**The `[StatelessWorker]` incompatibility and the Guid-keyed adaptation.** Native `IAsyncEnumerable`
streaming is not safe on a stateless-worker grain: the grain-side extension that backs it holds the live
enumerator in memory on one specific activation keyed by a client-minted request id, and a stateless
worker's activations are not individually addressable — a follow-up `MoveNext` can land on an activation
that never started the stream. The reverse-ops and data-plane grains that page today are stateless workers,
so the streaming ops moved to NEW `IGrainWithGuidKey` grains (`IReverseOpsStreamGrain`,
`IRelationshipsStreamGrain`) under default placement. Each service mints a fresh `Guid` per RPC, so
`StartEnumeration` and every `MoveNext` for that stream target the same brand-new, never-shared activation.
These per-stream activations are reclaimed by ordinary idle collection, like any other grain — a real but
minor cost against the deleted per-page hops. The shared pinning / index / caveat-collapse logic lives in
one place (`ReverseOpsSupport`) so the unary and streaming paths cannot drift.

`Expand` stays unary — folded onto `IReverseOpsStreamGrain` alongside the streaming lookups, since it was
the sole remaining member of the now-deleted stateless-worker `IReverseOpsGrain`. Callers target it via a
stable well-known Guid (`IReverseOpsStreamGrain.ExpandKey`, `Guid.Empty`) rather than minting a fresh one
per call, since Expand has no follow-up `MoveNext` and needs no per-stream activation affinity — it returns
a whole tree, has no cursor, and needs no streaming. The Leopard membership index still accelerates an
unlimited, cursorless
`LookupResources` (its fast path yields a complete candidate set confirmed by Check); a *limited* walk runs
the cursor-bearing live traversal so every item carries a resume cursor. Native streaming also gives
per-item cancellation for free — the trailing `CancellationToken` threads into the engine walk, which the
prior unary methods ignored. `Orleans.Runtime.AsyncEnumerableExtensions.WithBatchSize(n)` is available on
the caller side as the native backpressure/batching knob that replaces the deleted manual page-size
plumbing.

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

1. **1.1–1.7** — all completed. Cross-cutting infrastructure (filters, RequestContext) is now
   in place and the dispatcher seam is clean: error mapping and depth enforcement are native,
   and the wire contract is minimal (sub-problem + cancellation).
2. **1.8 / 1.11** — the remaining optional Orleans-native refinements (GrainService, broadcast channel)
   — low leverage, deferred. (1.9 and 1.10 are done.)
3. **2.3 / 2.4 / 2.5** — cheap, immediately differentiating product capabilities.
4. **2.1 per-tenant** and **2.2 materialized reachability** — each behind its own design document.
