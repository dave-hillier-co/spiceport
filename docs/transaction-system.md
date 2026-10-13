# Transaction system: from one sequencer to FoundationDB's roles

The plan for scaling writes past the single sequencer (`future-work.md` §1.15 links here). It
takes FoundationDB's transaction-system decomposition as the blueprint, because Spiceport's
storage already has FoundationDB's shape with four roles fused into one grain. Nothing here
weakens the guarantee: the total order and its revisions stay, as do strict serializability
and the new-enemy defence. What moves is the order's WORK. Every step past group commit is
trigger-gated on measured write demand (`scalability-program.md`).

Reading: Zhou et al., *FoundationDB: A Distributed Unbundled Transactional Key Value Store*,
SIGMOD 2021, for the roles and recovery; Corbett et al., *Spanner*, OSDI 2012, for what the
Zanzibar paper assumes of storage; Thomson et al., *Calvin*, SIGMOD 2012, for the
deterministic alternative at rung 3 of §1.15.

---

## 1. What already has FoundationDB's shape

| FoundationDB role | Spiceport |
|---|---|
| Storage servers: apply their slice of the log, keep a version window, serve reads at a version, readers wait on a version watermark | `GraphShardGrain`: `ShardFold`, the `AppliedRevision` gate, the persisted projection |
| Data distribution | `ShardIndexGrain` buckets and the grain directory |
| The log as the changelog | The journal of `LogEvent`s, feeding Watch and the membership walk |
| Versions as consistency tokens | Revisions as zedtokens |
| Ratekeeper (admission control) | The per-silo sequencer admission gate (`RESOURCE_EXHAUSTED` past its bound) |

The difference is that `DatastoreGrain` is four FoundationDB roles at once:

- the **sequencer** (it mints revisions);
- the **commit proxy** (it batches commits and drives each one);
- the **resolver** (it evaluates preconditions and `deleteByFilter` itself);
- the **transaction log** (its journal is the durable log).

The plan takes those apart, cheapest step first.

## 2. Steps inside the one sequencer

These need no new roles. Each removes work from the serialization point or makes a later split
a move rather than a rewrite.

1. **Group commit (built).** `Commit` interleaves and queues. One writer, an Orleans
   `BatchWorker`, drains the queue in arrival order: each run of commits is a batch, each
   request evaluated against the state the requests before it left, appended with one
   conditional raise and one confirm. GC rounds and projection flushes are items in the same
   queue, so writes never interleave and none waits in the activation's own request queue,
   where it would hold up the non-interleaving calls behind it. Every reply, rejections included, waits for the
   batch to be durable, because a rejection may rest on an earlier member's event. A batch
   costs one storage write however many commits it carries. In BeneDB, Thresh's journal
   adaptor writes a confirm's pending events in one append, which is what makes the batch one
   write there too.
2. **Read versions without a hot spot.** Every request's head read is a `GetHead` call on the
   singleton. A per-silo batcher, FoundationDB's GRV proxies, lets concurrent requests on one
   silo share one fetch. Sharing is safe: a head fetched after a request began is never older
   than one fetched at its start.
3. **A deterministic fault-injection harness (built for storage faults and deactivations).**
   FoundationDB's simulation testing in TestCluster form. `FaultHarness`
   (`tests/Spiceport.Grains.Tests/FaultInjection`) runs a seeded workload of declarative
   commits (touches, creates, deletes, preconditions, delete-by-filter) against the real
   sequencer and shard mesh, one at a time and in concurrent groups, and injects sequencer and
   shard-projection write failures, projection flushes and whole-cluster deactivations at
   seeded points. Every commit is replayed against `ReferenceDatastore` in the order the
   sequencer assigned and must reach the same outcome; reads through the shard mesh at every
   committed revision must match the reference at the same point. The seed fixes the workload
   and the fault schedule, not the silo's thread interleaving: commits in one concurrent group
   touch disjoint resources, so their outcomes do not depend on the order the sequencer picks,
   and a seed's trace reproduces exactly. The harness gates itself with a planted lost-write
   fault it must report. Silo kills and GC rounds are not yet injected. The interleaved-read
   and failed-projection-write gates are hand-written instances of the same idea. It comes
   before step 4 and before any split, not after.
4. **Conflict checking in the sequencer, evaluation by the callers.** `GrainBackedDatastore`
   evaluates a request against the shard grains at a read version and sends concrete
   mutations plus its read set. The sequencer keeps an in-memory window of recent writes and
   accepts the request if nothing in the read set was written since the read version. The
   caller retries a conflict internally. The sequencer's per-commit work shrinks to an
   in-memory check and an append. §4 has the mapping.
5. **Pipelining.** Evaluate batch N+1 while batch N's append is in flight, confirming in
   order. Worth it only once the append is the only wait left.

## 3. The target decomposition

| Role | Grain | State |
|---|---|---|
| Sequencer | One singleton. `GetCommitVersion(prev) -> (prev, v)`, and the latest committed version for reads | Its epoch only; no I/O per commit |
| Commit proxy | `[StatelessWorker]`. Batches commits, gets a version, resolves, logs, replies once durable | None |
| Resolver | Partitioned by key range. Checks read sets against recent writes | In memory only; rebuilt empty on recovery |
| Transaction log | Append-only segment grains. Mutations tagged with the shard keys they touch | Durable, through the storage provider |
| Storage | `GraphShardGrain`, unchanged in role. Pulls its tagged mutations from the logs instead of calling `ReadShardTail` on the sequencer | Its projection, persisted as a checkpoint |

With the logs out of the sequencer, cold shard activations and the projection fan-out no longer
touch the serialization point, and broad scans assemble from the shard index alone.

**Empty versions.** A version carrying no mutation for a shard must still advance that shard's
watermark, or a read pinned at that version waits forever. The logs tell every tag "nothing for
you up to v", as FoundationDB's do. The existing per-silo `LogWatchHub` head push is the
starting point.

## 4. The semantic change: optimistic commits

The declarative `CommitRequest` survives at the API. What changes is where it is evaluated. In
the target, the caller evaluates at a read version and declares what it read:

| Commit feature | Becomes |
|---|---|
| Precondition (must / must-not match) | A read at the read version, plus a read-conflict range over the filter's keys. A filter naming only a resource type conflicts on the whole type range. |
| `deleteByFilter` | Read the matches, emit concrete deletes, plus a read-conflict range over the filter |
| CREATE (`AlreadyExists`) | A point read and a read-conflict on that key |
| `ExpectedSchemaHash` | A read-conflict on the schema key |
| Counters | Writes to counter keys |
| `ExpectedHead` (the `ReadWriteTx` compatibility path) | The same idea at whole-head granularity, generalised to key ranges |

Two rules hold throughout:

- **A conflict never reaches the wire.** The caller retries internally, as SpiceDB retries
  serialization failures. Error codes stay exactly as they are; a new code would make `zed`
  retry or crash.
- **Read-version age is bounded by the resolver's window.** A read older than the window
  cannot be checked and is retried at a fresh version (FoundationDB's "transaction too old").
  The window sits inside the MVCC GC window, so the read is always servable.

## 5. Recovery

FoundationDB moves the whole transaction system to a new generation when any of its roles
fails: lock the old logs, take the highest version durable on all of them as the recovery
version, start fresh roles above it. Orleans reactivates grains one at a time, so this has to
be built:

- The sequencer persists an **epoch** and bumps it on activation.
- Proxies, resolvers and log appends carry the epoch. A log grain refuses an append from an
  older epoch; the storage provider's conditional write (Thresh's `tryAppend`) is the fencing
  primitive.
- A reactivated sequencer fences the old generation, reads the recovery version from the logs,
  and mints versions above it. Commits in flight under the old epoch fail and are retried by
  their callers.

A minimal form is needed from the version-only sequencer onward; with partitioned resolvers it
must cover every role.

## 6. Stages and gates

| Stage | Change | Gate to start | Proof before merge |
|---|---|---|---|
| 0 | Group commit | Built | Batched commits share one write; batch members see earlier members; CAS exact under concurrency |
| 1 | Read-version batching | Head reads visible in sequencer call rates | One `GetHead` per silo per batch window; consistency suites unchanged |
| 2 | Fault-injection harness | Built for storage faults and deactivations; silo kills and GC rounds next | Seeded runs reproduce; equivalence to `ReferenceDatastore` under injected faults |
| 3 | Logs out of the sequencer | Cold-activation or projection load on the sequencer measured | Shards pull from logs; empty-version advance; durability suites across reactivation |
| 4 | Version-only sequencer and commit proxies, one resolver | Write demand past group commit's ceiling | Epoch fencing; recovery under the harness |
| 5 | Optimistic commits, partitioned resolvers | Resolver CPU is the bound | §4 mapping exact under the conformance corpus, the differential suite and the harness |

Every stage keeps the conformance corpus green twice (reference datastore and grain mesh) and
the differential suite agreeing with real SpiceDB.

## 7. Where the new system lives

Below the sequencer, nothing is SpiceDB-specific: versioned transactions over an ordered key
space are general. Spiceport builds it as its own project (`Spiceport.Transactions`), with the
SpiceDB layer (MVCC rows, filter-to-conflict-range mapping) kept separate. When BeneDB ports
it, the generic half belongs in Thresh, which already owns storage, and BeneDB keeps only the
SpiceDB layer.
