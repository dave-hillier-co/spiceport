# Graph-sharded datastore: dissolving `IDatastore` into grains

A candidate architectural direction, analyzed but not committed (the same status as the
directions in `future-work.md`, which links here). It proposes replacing the whole-graph
storage shape — the singleton `DatastoreGrain` fold plus per-silo `SiloProjection` — with a
thin commit sequencer and two grain-sharded adjacency families, and retiring `IDatastore`
from the production path.

> Relationship to `architecture-analysis.md` §3.1: that section rules out **per-object
> *entity* grains** — grains whose state is an object's *current* tuples — and that ruling
> stands. This document proposes something §3.1 does not address: per-key grains holding
> **versioned slices of the MVCC fold**, activated on demand. The distinction is load-bearing
> and is argued in §4 below.

---

## 1. Diagnosis: `IDatastore` is SpiceDB's shape, not the domain's

`IDatastoreReader` promises a snapshot of *everything* at a revision. No consumer wants
that. The engines (`CheckEngine`, the Lookup engines, `ExpandEngine`) consume exactly two
call shapes — `QueryRelationships` pinned to a resource (forward adjacency) and
`ReverseQueryRelationships` pinned to a subject (reverse adjacency) — plus
schema-at-revision. Four genuinely distinct roles are fused into one database-shaped
abstraction because that is the shape SpiceDB's `pkg/datastore` handed over:

| Role | Carrier today | Actual access pattern |
|---|---|---|
| Graph reads (Check/Expand/Lookup) | `IDatastoreReader` over the whole-graph `SiloProjection` | Point reads keyed by **object** (forward) or **subject** (reverse) |
| Commit + revision minting | `ReadWriteTx` → CAS append on the singleton | Serialized, singular by necessity (the total order) |
| Bulk scans (ReadRelationships, export, counters, preconditions) | The same reader, filter-scanning the whole fold | Enumeration — the anti-actor workload |
| Changefeed | `Watch` tailing the log | Log tail |

The whole-graph reader is the leaked abstraction: it forces every silo to materialize the
entire fold so a point read can be a dictionary hit, which is what caps state at
"the graph fits in RAM on every silo". The graph's two natural keys — object and subject —
are actor keys. The mesh already half-concedes this: `MembershipWalkGrain` *is* the reverse
graph modelled as on-demand grains, and `CheckGrain`'s key begins with the resource.

**The reshape: `IDatastore` dissolves into a thin sequencer plus two grain-sharded adjacency
families, with scans served from storage rather than grains.**

---

## 2. Target architecture

```
                ICommitSequencer (grain, cluster singleton, THIN)
                 - CAS append; mints revisions (the one total order)
                 - holds only the unflushed log tail + head
                 - Watch feed (unchanged: grain observer + heartbeat backstop)
                        |
          durable log + per-shard snapshots (stock Orleans grain storage)
                        |
        +---------------+----------------------+
        |                                      |
  ObjectShardGrain                      SubjectShardGrain
  key: (objectType, objectId)           key: subject key (type:id[#rel])
  versioned usersets of ONE object      versioned back-references of ONE subject
  serves: ReadUserset(relation, rev)    serves: ReverseEdges(rev)
  consumers: CheckGrain, Expand,        consumers: MembershipWalkGrain,
  LookupSubjects                        LookupResources, SubjectFrontierGrain
```

This is a distributed LSM expressed in grains: the sequencer is the memtable + WAL (bounded —
only the tail not yet folded into shards), shard grains are the sorted runs (each holds its
key's MVCC history within the GC window), and "flush" is shards advancing their watermarks by
folding the tail. The fold is reused, not rewritten: a shard's state is the existing
`LogFold`/`MvccSnapshotReader` fold **restricted to one key**. Because the fold is already a
pure function of the log, sharding it is a filter — this is the deep reason the refactor is
tractable, and it yields a checkable lemma (§7 step 2).

**Reads.** A read pinned at `rev` goes to the shard. If the shard's watermark ≥ `rev` it
serves from local state — the same closed-timestamp gate as today, per-shard instead of
per-silo. If not, the shard pulls the filtered tail (catch-up-on-demand, the existing
mechanism). The evaluation contract is untouched: a Check remains a pure function of
`(schema@rev, tuples@rev, request)`; only *who holds the tuples* changes.

**Writes.** The sequencer stays the sole serialization point; the single ordered log and the
new-enemy defence are unchanged. Preconditions are the subtle part — they need a read at
head, and the sequencer no longer holds the fold. The LSM shape solves it:
state-at-head = shard-state-at-watermark + tail replay, and the sequencer *owns the tail*.
During a commit (single-threaded, non-reentrant — nothing else can move head) it evaluates
preconditions by querying the affected shards at their watermarks and overlaying its
in-memory tail delta. Exact, no locks, no two-phase commit. A broad precondition filter means
a scatter and therefore a slow write — the correct place to pay.

**Activation state = the hot set.** Cold objects' shards deactivate; their history sits in
their snapshot row until touched. Silo memory becomes O(hot working set), not O(graph).

---

## 3. The three interfaces that replace `IDatastore`

```csharp
// The engine seam — grain-shaped; replaces IDatastoreReader for Check/Expand/Lookup.
// Constructed pinned at a revision, as the reader is today.
public interface IGraphReader
{
    IAsyncEnumerable<Relationship> ReadUserset(
        ObjectRef resource, string? relation, CancellationToken ct);
    IAsyncEnumerable<Relationship> ReadBackReferences(
        SubjectRef subject, ReverseQueryOptions? options, CancellationToken ct);
}

// The write/consistency seam — the sequencer grain's face.
public interface ICommitSequencer
{
    Task<RevisionWithSchemaHash> HeadRevision(CancellationToken ct);
    Task<RevisionWithSchemaHash> OptimizedRevision(CancellationToken ct);
    Task<IRevision> Commit(CommitRequest request, CancellationToken ct);
    IAsyncEnumerable<RevisionChange> Watch(
        IRevision after, WatchOptions options, CancellationToken ct);
}

// The scan seam — storage-direct, no grains.
public interface ISnapshotScanner
{
    IAsyncEnumerable<Relationship> Scan(
        RelationshipsFilter filter, IRevision rev, CancellationToken ct);
}
```

- **`IGraphReader`** is what the engines already use in practice: every engine call site is
  one of these two shapes (`RelationshipsFilter` with a pinned resource, or a
  `SubjectsFilter`). The implementation resolves the shard grain and calls it; replies are
  `[Immutable]` so same-silo calls do not copy. Schema-at-revision moves fully to
  `ISchemaProvider`, where it already effectively lives.
- **`ReadWriteTx`'s lambda shape dies.** `Func<IReadWriteTransaction, Task>` pretends the
  caller interactively reads-and-writes inside a transaction; in reality
  `GrainBackedDatastore` stages mutations and CAS-appends once. `Commit(mutations,
  preconditions)` as a plain request is honest and makes the wire contract to the sequencer
  explicit. The transaction *object* was another piece of SpiceDB's shape that never fit.
- **`ISnapshotScanner`** takes bulk export, loose-filter ReadRelationships, counter
  evaluation, and the precondition scatter off the actor mesh: it reads the durable shard
  snapshots + tail replay directly. Scans are the workload actors are worst at; routing them
  through storage keeps shard activations for graph work. Client-facing cursors (keyset,
  revision-pinned bulk export) are unchanged API contract.

`ReferenceDatastore` and the `IDatastore` family survive **in tests only**, as the
implementation-independent reference model the conformance corpus runs against.

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
Unchanged: the sequencer is that global-order point, exactly as the `DatastoreGrain` is
today. Nothing in this design shards the *log* — only the *fold*.

**The hot-object problem** (a group with a million members read by thousands of concurrent
checks): one shard activation serializes reads. Mitigations, in order: reads over the
immutable folded structure are `[AlwaysInterleave]`-safe (the same lesson already applied to
the `DatastoreGrain`'s pure reads — `[ReadOnly]` alone does not interleave past writes); the
reply for a large userset is an immutable snapshot reference, cheap to hand out repeatedly;
and if a single activation still saturates, a `[StatelessWorker]` read-replica face hydrated
from the shard is the escape hatch. The current design has the same data hot — it is just
pre-replicated to every silo. Shards replicate *on demand* instead of *always, everywhere*.

---

## 5. The alignment prize: compute lands on its data

This is where the design stops being a storage refactor and becomes the actor-native
completion of the rearchitecture's founding thesis:

- `CheckGrain`'s key begins with `(resourceType, resourceId, …)`; `ObjectShardGrain`'s key
  *is* `(resourceType, resourceId)`. A custom placement director — pluggable, the extension
  `future-work.md` §1.4 anticipated when the hash ring was deleted — places check grains on
  the silo of their object's shard. The first data read is then a same-silo call against an
  `[Immutable]` reply: function shipping and data shipping become the same ship.
- `MembershipWalkGrain` and `SubjectFrontierGrain` are keyed by subject key — the same key as
  `SubjectShardGrain`. They co-place the same way, or eventually merge: the walk grain is
  naturally the compute face of the reverse shard.

The end state is symmetrical and legible: **the graph is stored as two grain-sharded
adjacency indexes — forward by object, reverse by subject — each co-located with the compute
family that consumes it, both folds of one sequencer log.** The only non-graph-shaped
component left is the sequencer, and it is thin.

---

## 6. What dissolves, and which ceilings move

Deleted or dissolved:

- `SiloProjection` as a whole-graph structure, and with it the per-silo bootstrap.
- `DatastoreProjectionService` / `IDatastoreProjectionHost` — nothing to bootstrap
  pre-traffic; shards self-hydrate on activation.
- The multi-silo cold-start problem `future-work.md` §1.12 defers (`ISnapshotSegmentGrain`)
  — dissolved rather than optimized: no silo ever fetches the whole snapshot.
- `GrainBackedDatastore` — its write path becomes the sequencer client, its read path
  becomes `IGraphReader`.
- `IDatastore` / `IDatastoreReader` / `IReadWriteTransaction` from the production path
  (they remain under `tests/` with the reference model).
- `DatastoreGrain` slims to the sequencer: append + tail-serve + Watch feed, no fold
  maintenance.

Ceilings:

- **State ceiling — eliminated.** The graph no longer needs to fit in RAM on every silo.
  This is the scalability win, and the one that mattered.
- **Bootstrap/failover ceiling — eliminated.** Sequencer recovery = read head + bounded
  tail; silo start = nothing.
- **Write ceiling — deliberately retained.** One sequencer, one total order: the new-enemy
  invariant and the recorded non-goal. Thinning the sequencer raises its practical
  throughput anyway (it no longer maintains the fold), but the ceiling is a design stance,
  not a casualty.

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

## 7. Migration, in the repository's own discipline

Every step lands with the conformance corpus green two ways and the differential suite
against real SpiceDB as the external gate. Shards serve *data*, not candidates, so the
applicable instrument is the **fold-correctness equivalence gate** (the stronger gate the
`future-work.md` invariants reserve for anything that would serve verdicts), not
candidates-plus-Check-confirmation.

1. **Narrow the engine seam first, with no behavior change.** Introduce `IGraphReader`,
   implement it trivially over the existing `SiloProjection`, and move
   `CheckEngine`/Expand/Lookup onto it. A pure refactor that proves the engines never needed
   the wide reader.
2. **Extract the keyed fold.** Parameterize `LogFold` by a key predicate and property-test
   the sharding lemma: `fold(log) == merge(fold(log | key) for all keys)`, checked in-memory
   against `ReferenceDatastore`.
3. **Stand up the shard grain families behind `IGraphReader`** under a shadow flag, running
   the fold-equivalence gate: every read answered by both projection and shards must agree
   exactly.
4. **Slim the sequencer.** Move precondition evaluation to tail-overlay + shard-base, make
   `Commit` the explicit wire contract, stop maintaining the whole fold in the grain.
5. **Move scans to `ISnapshotScanner`**, flip the default, delete the projection, and retire
   `IDatastore` from `src/`.
6. **Co-placement director last** — pure performance, gated by measurement, per the
   simplicity-over-performance stance.

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
- **No application SQL.** Shard snapshots and the log persist through stock Orleans grain
  storage, as today.

This reshape also strengthens rather than spends the other analyzed directions:
incrementally-materialized reachability (`future-work.md` §2.2) becomes another shard family
folded from the same log; time-travel audit (§2.5) is per-shard history already stored. The
sequencer-plus-folds spine is the part of the current design worth keeping — this design
keeps exactly that, and makes everything around it grain-shaped.
