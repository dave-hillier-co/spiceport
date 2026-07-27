# Graph-sharded datastore: dissolving `IDatastore` into grains

The design for the built storage shape (`future-work.md` §1.13 links here). It replaces the
whole-graph read shape — the per-silo `SiloProjection` replica, now retired — with a commit
sequencer and grain-sharded adjacency shards: engine reads resolve to per-key
`GraphShardGrain`s, writes are declarative `DatastoreGrain.Commit` requests, and broad scans
go storage-direct. The realized steps are in the present tense below; §7 records the staging,
including the one piece that is built but off by default (the co-placement director, whose
enablement is gated on measurement).

> Relationship to `architecture-analysis.md` §3.1: that section rules out **per-object
> *entity* grains** — grains whose state is an object's *current* tuples — and that ruling
> stands. This design builds something §3.1 does not address: per-key grains holding
> **versioned slices of the MVCC fold**, activated on demand. The distinction is load-bearing
> and is argued in §4 below.

---

## 1. Diagnosis: `IDatastore` was SpiceDB's shape, not the domain's

`IDatastoreReader` promises a snapshot of *everything* at a revision. No consumer wants
that. The engines (`CheckEngine`, the Lookup engines, `ExpandEngine`) consume exactly two
call shapes — `QueryRelationships` pinned to a resource (forward adjacency) and
`ReverseQueryRelationships` pinned to a subject (reverse adjacency) — plus
schema-at-revision. Four genuinely distinct roles were fused into one database-shaped
abstraction because that is the shape SpiceDB's `pkg/datastore` handed over:

| Role | Carrier before the reshape | Actual access pattern |
|---|---|---|
| Graph reads (Check/Expand/Lookup) | `IDatastoreReader` over the whole-graph `SiloProjection` | Point reads keyed by **object** (forward) or **subject** (reverse) |
| Commit + revision minting | `ReadWriteTx` → CAS append on the singleton | Serialized, singular by necessity (the total order) |
| Bulk scans (ReadRelationships, export, counters, preconditions) | The same reader, filter-scanning the whole fold | Enumeration — the anti-actor workload |
| Changefeed | `Watch` tailing the log | Log tail |

The whole-graph reader was the leaked abstraction: it forced every silo to materialize the
entire fold so a point read could be a dictionary hit, which is what capped state at
"the graph fits in RAM on every silo". The graph's two natural keys — object and subject —
are actor keys. The mesh already half-conceded this: `MembershipWalkGrain` *is* the reverse
graph modelled as on-demand grains, and `CheckGrain`'s key begins with the resource.

**The reshape, as built: `IDatastore`'s production roles split across a sequencer, two
grain-sharded adjacency families, and a storage-direct scan seam.**

---

## 2. Architecture as realized

```
                DatastoreGrain (grain, cluster singleton — the THIN sequencer)
                 - Commit: declarative wire contract; mints revisions (the one total order)
                 - holds only the small state (head/schemas/counters/floor/key index)
                   plus a dirty buffer of keys touched since their rows last flushed
                 - Watch feed (unchanged: grain observer + heartbeat backstop)
                 - ReadShard: per-key snapshot for shard hydration; ReadFrom: log tail
                        |
        durable log + per-key shard rows + meta rows (stock Orleans grain storage)
                        |
        +---------------+----------------------+
        |                                      |
  GraphShardGrain (forward key)         GraphShardGrain (reverse key)
  key: (objectType, objectId)           key: (subjectType, subjectId)
  versioned usersets of ONE object      versioned back-references of ONE subject
  serves: RowsAt(rev)                   serves: RowsAt(rev)
  consumers: CheckGrain, Expand,        consumers: MembershipWalkGrain,
  LookupSubjects                        LookupResources, SubjectFrontierGrain
```

This is a distributed LSM expressed in grains: the sequencer is the WAL owner, shard grains
are the sorted runs (each holds its key's MVCC history within the GC window), and "flush" is
shards advancing their watermarks by folding the tail. The fold is reused, not rewritten: a
shard's state is the existing `LogFold`/`MvccSnapshotReader` fold **restricted to one key**
(`ShardFold`). Because the fold is already a pure function of the log, sharding it is a
filter — this is the deep reason the refactor was tractable, and it yields the checkable
sharding lemma (§7 step 2): `fold(log) == merge(fold(log | key) for all keys)`.

**Reads.** A read pinned at `rev` goes to the shard (`ShardedGraphReader` resolves the grain;
`IGraphReaderSource` hands the engines the pinned reader). If the shard's watermark ≥ `rev`
it serves from local state — the closed-timestamp gate, per-shard instead of per-silo. If
not, the shard pulls the log tail (catch-up-on-demand); a cold shard always hydrates via the
sequencer's per-key `ReadShard` snapshot first, never a from-zero log replay (the retained
tail starts at the compaction floor). The evaluation contract is untouched: a Check remains a
pure function of `(schema@rev, tuples@rev, request)`; only *who holds the tuples* changed.

**Writes.** The sequencer is the sole serialization point; the single ordered log and the
new-enemy defence are unchanged. `DatastoreGrain.Commit` is the declarative wire contract:
preconditions, updates, delete-by-filter, schema writes (guarded by `ExpectedSchemaHash`),
and counter changes are evaluated and executed inside the single-threaded, non-reentrant
activation. The single-writer activation makes head-moved conflicts near-impossible — it
cannot lose its own conditional append — but the client still carries a bounded `HeadMoved`
retry, because a stale duplicate activation during cluster-membership churn can briefly race
the storage version check. Every rejection returns as a typed `CommitReply` failure with
nothing applied. The fold has left the grain: a commit assembles a PARTIAL base from exactly
the shard keys its updates/preconditions/delete filter can read (candidate-key resolution
over the key index — sound by the sharding lemma; the argument is inline at the assembly
site in `DatastoreGrain.Commit`), evaluates against it, and pre-seeds the dirty buffer for
every key the resulting event touches before the append. `ReadWriteTx`
survives only as the `ExpectedHead` compatibility path — the caller-evaluated CAS shape —
over the same `Commit` contract.

**Activation state = the hot set.** Cold keys' shards deactivate under Orleans idle
collection; cold keys never activate at all until touched. Silo memory is O(hot working
set), not O(graph).

---

## 3. The seams that replace the whole-graph reader

- **`IGraphReader`** (in `Spiceport.Datastore`) is the engine seam — the two pinned call
  shapes the engines actually produce: `QueryRelationships` with explicit resource ids and
  `ReverseQueryRelationships` with explicit subject ids. `ShardedGraphReader` implements it
  by resolving the matching `GraphShardGrain` per key; anything scan-shaped throws
  `NotSupportedException` — broad scans belong on the scan seam, not the shard mesh. Replies
  are `[Immutable]` so same-silo calls do not copy. The engines receive their pinned reader
  through `IGraphReaderSource`.
- **`DatastoreGrain.Commit` is the write wire contract.** The `ReadWriteTx` lambda shape
  (`Func<IReadWriteTransaction, Task>`) pretended the caller interactively read-and-wrote
  inside a transaction; in reality the client staged mutations and CAS-appended once.
  `Commit(preconditions, updates, deleteByFilter, schema + ExpectedSchemaHash, counters)` as
  a plain declarative request is honest, executes on the sequencer's serialization point
  (no caller-evaluated CAS loop; only the bounded duplicate-activation `HeadMoved` retry
  remains client-side), and reports every rejection as a typed `CommitReply` failure with
  nothing applied. `ReadWriteTx` remains as the `ExpectedHead` compatibility path over the
  same contract.
- **`ISnapshotScanner`** is the scan seam: bulk export, loose-filter ReadRelationships, and
  counter evaluation fetch a sequencer snapshot per scan and serve it through the same
  `MvccSnapshotReader` the reference model uses — scan semantics cannot drift from the
  reference reader. Scans are the workload actors are worst at; routing them around the
  shard mesh keeps shard activations for graph work. Client-facing cursors (keyset,
  revision-pinned bulk export) are unchanged API contract.
- **`ISchemaSource`** is the schema-at-revision seam: one sequencer read per schema hash per
  silo (the compiled snapshot is cached by hash), never per check.

`ReferenceDatastore` and the `IDatastore` reader family survive as the
implementation-independent reference model the conformance corpus and equivalence gates run
against.

---

## 4. The §3.1 objections, answered head-on

**"Data is too large and too cold to economically activate per-object."** That objection
assumes grains hold *current entity state* and must be resident to be useful. Shard grains
hold *versioned slices* and activate on demand — cold means "on disk in your snapshot row",
the same economics as a database page outside the buffer pool. The genuinely new cost is the
**cold negative lookup**: a Check against an object with no tuples activates a grain to
answer "empty". Two-stage answer: (a) accept it initially — it is one point read by key, the
same cost class as the SQL read SpiceDB pays on every uncached check, and the activation
idle-collects; (b) if profiles demand, add a **versioned existence index** — a fold of
`(key, createdRev, tombstoneRev)` intervals, metadata-sized, held per-silo — answering
"definitely empty at rev" without activation. It must be exact (a wrong "empty" is a wrong
verdict — the same reason the probabilistic visited-set bloom was replaced with an exact
set), and as a fold of the same log it is gated on==off like everything else.

**"Entity grains hold current state; zookies need point-in-time reads."** Answered by
construction: each shard holds its own MVCC history within the GC window and serves at any
covered revision. `GcApplied(floor)` remains a log event; every shard applies it in its fold
identically — that invariant survives untouched.

**"Writes need a global monotonic revision and cross-object snapshot consistency."**
Unchanged: the sequencer — the `DatastoreGrain` itself — is that global-order point. Nothing
in this design shards the *log* — only the *fold*.

**The hot-object problem** (a group with a million members read by thousands of concurrent
checks): one shard activation serializes reads. Mitigations, in order: reads over the
immutable folded structure are `[AlwaysInterleave]`-safe (the same lesson already applied to
the `DatastoreGrain`'s pure reads — `[ReadOnly]` alone does not interleave past writes); the
reply for a large userset is an immutable snapshot reference, cheap to hand out repeatedly;
and if a single activation still saturates, a `[StatelessWorker]` read-replica face hydrated
from the shard is the escape hatch. The retired projection design had the same data hot — it
was just pre-replicated to every silo. Shards replicate *on demand* instead of *always,
everywhere*.

---

## 5. The alignment prize: compute lands on its data

This part is built and ON by default: the enablement was gated on measurement per the
simplicity-over-performance stance, and the real-network rig's A/B decided it
(`scalability-program.md` §3.5) — on real sockets the hint converts cross-silo grain calls
into local ones for a decisive latency/throughput win. `GraphLocalityPlacement` (a pluggable
placement director, the extension `future-work.md` §1.4 anticipated when the hash ring was
deleted) biases the FIRST activation of the four graph grain families onto the silo a stable
hash of their locality key names — a hint only; the grain directory remains the sole authority
for identity and dedup, and on membership change existing activations stay put while locality
degrades gracefully. The toggle is `GraphPlacementOptions.CoLocateWithShards` (opting out is a deployment override
via `AddGraphLocalityPlacement`). It is where the design stops being a storage refactor and becomes
the actor-native completion of the rearchitecture's founding thesis:

- `CheckGrain`'s key begins with `(resourceType, resourceId, …)`; the forward-keyed
  `GraphShardGrain`'s key *is* `(resourceType, resourceId)`. A custom placement director —
  pluggable, the extension `future-work.md` §1.4 anticipated when the hash ring was deleted —
  places check grains on the silo of their object's shard. The first data read is then a
  same-silo call against an `[Immutable]` reply: function shipping and data shipping become
  the same ship.
- `MembershipWalkGrain` and `SubjectFrontierGrain` are keyed by subject key — the same key as
  the reverse-keyed `GraphShardGrain`. They co-place the same way, or eventually merge: the
  walk grain is naturally the compute face of the reverse shard.

The end state is symmetrical and legible: **the graph is stored as two grain-sharded
adjacency indexes — forward by object, reverse by subject — each co-located with the compute
family that consumes it, both folds of one sequencer log.** The only non-graph-shaped
component left is the sequencer, and it is thin.

---

## 6. What dissolved, and which ceilings moved

Deleted or dissolved:

- `SiloProjection` as a whole-graph structure, and with it the per-silo bootstrap. The
  bootstrap-then-tail-fold pattern it carried survives, restricted to one key, inside
  `GraphShardGrain`.
- `DatastoreProjectionService` / `IDatastoreProjectionHost` — nothing to bootstrap
  pre-traffic; shards self-hydrate on activation.
- The multi-silo cold-start problem `future-work.md` §1.12 deferred (`ISnapshotSegmentGrain`)
  — dissolved rather than optimized: no silo ever fetches the whole snapshot pre-traffic.
- The engines' dependency on the wide reader: per-Check graph reads go through
  `IGraphReaderSource`/`ShardedGraphReader`, never `IDatastoreReader`.

Deliberately retained:

- `GrainBackedDatastore` as a narrow facade: revision resolution, token minting, Watch, and
  the `ReadWriteTx` compatibility write path (tests/BulkImport/SeedData) — submitted over
  the same `Commit` wire contract with `ExpectedHead`.
- `ReferenceDatastore` and the `IDatastore` reader family as the reference model for the
  conformance corpus and the fold-equivalence gates.

Ceilings, honestly stated:

- **Per-silo read-fleet state ceiling — gone.** The graph no longer needs to fit in RAM on
  every silo; silo memory is O(hot shards). This is the scalability win, and the one that
  mattered.
- **Silo cold-start warmup — gone.** Silo start hydrates nothing; shards hydrate on first
  touch.
- **Sequencer state ceiling — gone.** The thin-sequencer flush protocol (§7 step 6) removed
  the materialized fold from the `DatastoreGrain`: its memory is the small state plus a
  dirty buffer bounded by write rate x the flush interval, and its recovery is O(log tail +
  touched keys), never O(graph). The one deliberately O(graph) surface left is the
  admin-plane `ReadState` assembly (scan seam, compatibility writes, equivalence gates).
- **Write ceiling — deliberately retained.** One sequencer, one total order: the new-enemy
  invariant and the recorded non-goal. The ceiling is a design stance, not a casualty. What
  happens AT the ceiling is governed: a per-silo admission gate (`SequencerAdmission`,
  entered by the production declarative write path in `RelationshipsGrain`) bounds each
  silo's in-flight commits, shedding the excess as `SequencerOverloadedException` → gRPC
  `RESOURCE_EXHAUSTED` — a deliberate, retryable overload signal instead of an unbounded
  activation queue collapsing into Orleans response timeouts (issue #36). The bound is
  `SequencerAdmissionOptions.MaxInFlightCommits`; shed commits are counted by
  `ISequencerMetrics.RecordCommitShed`. Raising the ceiling itself remains the separate,
  demand-triggered batch/ticket/Calvin ladder (`docs/future-work.md` §1.15).

Honest costs:

- More moving parts holding the same invariant: per-shard watermarks and hydration where
  there was one projection.
- Per-check data reads become grain calls where they were dictionary hits — bounded by
  co-placement (§5), the activation memo (repeat sub-problems never re-read), and the fact
  that `CheckGrain` already hops per sub-problem; measure before assuming it hurts.
- Watch checkpoint semantics must still ride the sequencer feed so a filtered consumer
  observes liveness — the mechanism is unchanged, but it deserves an explicit test.
- The existence index, if ever needed, is one more fold to keep honest.

---

## 7. The staging, in the repository's own discipline: what is realized, what remains

Every step lands with the conformance corpus green two ways and the differential suite
against real SpiceDB as the external gate. Shards serve *data*, not candidates, so the
applicable instrument is the **fold-correctness equivalence gate** (the stronger gate the
`future-work.md` invariants reserve for anything that would serve verdicts), not
candidates-plus-Check-confirmation.

Realized (the built system):

1. **The engine seam is narrow.** `IGraphReader` carries the two pinned call shapes, and
   `CheckEngine`/Expand/Lookup read only through it — the engines never needed the wide
   reader.
2. **The keyed fold is extracted.** `ShardFold` is the log fold restricted to one key; the
   sharding lemma — `fold(log) == merge(fold(log | key) for all keys)` — is property-tested
   against `ReferenceDatastore`.
3. **The shard grain families serve `IGraphReader`.** `GraphShardGrain` (forward by object,
   reverse by subject) behind `ShardedGraphReader`, with the fold-equivalence gate asserting
   shard reads agree row-for-row with a sequencer-snapshot reader.
4. **`Commit` is the explicit wire contract.** Declarative
   preconditions/updates/deleteByFilter/schema/counters execute on the sequencer with typed
   `CommitReply` failures; `ReadWriteTx` survives as the `ExpectedHead` compatibility path.
5. **Scans go through `ISnapshotScanner`**, schema-at-revision through `ISchemaSource`, and
   the per-silo whole-graph projection is deleted — sharded reads are the only engine path.
6. **The thin-sequencer flush protocol.** The fold has left the grain. The sequencer's
   journaled state is only the small `DatastoreMetaState` (head, schemas, counters, GC
   floor, and the key index of populated forward/reverse keys); rows persist per adjacency
   key as `shard/f|r/{type}/{id}` `GraphShardState` rows in the same stock grain storage,
   rewritten from an in-memory dirty buffer at the same 64-event cadence the whole-state
   snapshots used (write-once `meta/{version}` rows version the flushes; the `head` row
   stays the commit point; clears stay best-effort post-commit). The load-bearing fact is
   the sharding lemma's corollary: a clean key's stored row content is complete — only its
   `AppliedRevision` label is stale and is relabeled to head on serve — so clean keys need
   NO per-key tail replay, and recovery is O(tail + touched keys). Row GC is lazy (rows
   compact when next dirtied+flushed; every serve path re-applies the floor, and reads
   below the floor are rejected anyway). `Commit` evaluates over a partial base assembled
   by candidate-key resolution; `ReadState` survives as the admin plane's on-demand
   whole-state assembly. A legacy whole-state `snapshot/{version}` store migrates in place
   on first activation.

   The key index's own durable form is chunked (per-direction bucket rows rewritten one per
   flush in rotation, plus per-flush delta rows; the meta row carries only the layout
   descriptor, never the maps inline), and stores written by the earlier inline-map layout
   migrate to it in place on first activation. **That migration is ONE-WAY — rollback across
   it is forbidden.** A binary predating the chunked index, activated against a migrated
   store, deserializes the meta row without the layout field it does not know, sees the
   (deliberately empty) inline maps, and silently reads the store as EMPTY — every
   relationship unreachable — and its first flush persists that emptiness durably. Roll
   forward only; recover a wrongly rolled-back store from backup, not by re-upgrading.

7. **The co-placement director** (§5) — built as `GraphLocalityPlacement`, default ON:
   the locality hint is proven by a multi-silo co-location gate, and the default was decided
   by the real-network rig's A/B (`scalability-program.md` §3.5), per the measurement-gated
   stance. Opting out remains a deployment override.

---

## 8. Invariants preserved

- **One total order.** The sequencer's CAS append mints revisions exactly as the
  `DatastoreGrain` does; the log is never sharded. New-enemy protection and zookie semantics
  are untouched.
- **Purity.** Evaluation remains `(schema@rev, tuples@rev, request)`; readers are
  revision-pinned; the closed-timestamp gate survives as a per-shard watermark wait.
- **GC as a log event.** Every shard applies `GcApplied(floor)` in its fold identically.
- **The corpus stays green, unweakened**, and `authzed.api.v1` compatibility (cursors and
  token formats included) is unchanged.
- **No application SQL.** The log, the per-key shard rows and the meta rows all persist
  through stock Orleans grain storage.

This reshape also strengthens rather than spends the other analyzed directions:
incrementally-materialized reachability (`future-work.md` §2.2) becomes another shard family
folded from the same log; time-travel audit (§2.5) is per-shard history already stored. The
sequencer-plus-folds spine is the part of the current design worth keeping — this design
keeps exactly that, and makes everything around it grain-shaped.
