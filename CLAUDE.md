# CLAUDE.md

Guidance for working in this repository. See `README.md` for the project overview,
`docs/architecture-analysis.md` for the design rationale, and `docs/future-work.md` for
candidate directions that are analyzed but not committed.

## What this is

A port of [SpiceDB](https://github.com/authzed/spicedb) (Google Zanzibar) to .NET 10 +
Microsoft Orleans. The recursive permission-check dispatch runs on Orleans virtual actors; the
gRPC surface is `authzed.api.v1`-compatible (the real `zed` CLI works against it).

## Build & test

```bash
dotnet build                                   # whole solution
dotnet test                                    # all tests
dotnet test tests/Spiceport.Conformance.Tests  # the SpiceDB conformance corpus (fast)
```

- Target framework is `net10.0`; nullable + implicit usings are ON (see `Directory.Build.props`).
- **Use the dotnet CLI for package/reference/project changes** (`dotnet add package`,
  `dotnet add reference`, `dotnet sln add`). Do not hand-edit `<PackageReference>`/
  `<ProjectReference>` items. Editing build items like `<Protobuf>` is fine.
- The grain-storage **durability tests** (`tests/Spiceport.Grains.Tests/Durability`) use
  Testcontainers and require Docker running; they spin up and dispose their own `postgres`
  container (and skip when Docker is unavailable).
- The solution file is `Spiceport.slnx` (the .NET 10 XML solution format).

## Architecture (the load-bearing ideas)

- **Storage is not *dispatch*-grain state.** Evaluation is a pure function of
  `(schema@revision, tuples@revision, request)`; dispatch grains never hold relationship data.
  The MVCC mechanics (visibility at a revision, the per-revision diff) live in `Spiceport.Datastore`
  and are reused everywhere — the `ReferenceDatastore` conformance oracle and the silo projections
  share one fold.
- **Storage is an event-sourced grain (the log is the storage/compute seam).** All
  relationship/schema/counter state lives behind a single cluster-singleton `DatastoreGrain`, a
  **journaled grain whose append-only `LogEvent` log is the source of truth**; the materialized state
  is the fold. A commit is a version-checked **append** (the CAS serialization point), not a
  whole-state rewrite; the single non-reentrant activation makes the minted revision the cluster-wide
  global order. Persistence is the grain's own via `ICustomStorageInterface` over an Orleans
  grain-storage provider — **no application SQL** (per-version log entries + periodic snapshots +
  compaction). Each silo reads from a **`SiloProjection`** folded incrementally from the log
  (`ReadFrom` tail, bootstrap-once) — no per-Check full fetch; exact/at-least-as-fresh reads block
  until the projection watermark covers the pinned revision (closed-timestamp gate). The same log feed
  drives **Watch** (one per-silo `LogWatchHub` notifier, no per-stream polling) and an on-by-default
  (opt-out via `MembershipIndexOptions`) **Leopard `MembershipIndex`** for `LookupResources` (a complete candidate superset confirmed by
  `CheckEngine`, never an oracle — it cannot change a verdict). See `docs/architecture-analysis.md` §3.5.
- **The dispatcher seam is the core mechanism.** `Spiceport.Engine`'s `CheckEngine` never
  recurses into itself directly — every sub-problem flows through `IDispatcher.DispatchCheck`.
  Implementations are `OrleansDispatcher` (resolves a grain call via the Orleans directory) and
  `LocalDispatcher` (one expansion step). The grain identity *is* the canonical sub-problem
  `(resourceType, resourceId, relation, subject, quantizedRevision, schemaHash)`.
- **Dispatch via grain calls.** Every sub-problem is a grain call; the Orleans grain directory
  owns location. `CheckGrain` activation state is the only dispatch cache, memoizing the
  pre-context `Branch` with idle-collection eviction. A bounded **traversal bloom** carries the
  cycle guard across grain boundaries.
- **Caching subtleties (do not regress):** the `CheckGrain` activation memo stores the
  *pre-context* `Branch` (membership + caveat expression), never the collapsed verdict — caveat
  context is applied per-request at the caller. The grain key excludes the traversal bloom,
  depth, and caveat context. Cycle-cut results are served but not retained. Revisions are
  quantized so grain keys are shared within a window; `schemaHash` is in the key so a schema
  change yields a fresh keyspace.
- **Consistency.** Reads honor a `ConsistencyRequirement`; `RevisionMode` (Optimized vs Exact)
  threads into the cache key so fresh/at-exact/fully-consistent reads never serve stale data.

## Conventions

- Idiomatic modern C#: records for immutable data, file-scoped namespaces, `IAsyncEnumerable`
  for streaming, `[GenerateSerializer]` for any type crossing a grain boundary.
- Keep engine logic out of the API layer. gRPC service classes are pure translation over the
  grains; engine/graph logic lives in `Spiceport.Engine`/`Spiceport.Grains`.
- Map errors to gRPC status codes deliberately (e.g. CREATE-conflict -> `AlreadyExists`,
  precondition/schema-validation failure -> `FailedPrecondition`, bad consistency token ->
  `InvalidArgument`). A wrong code makes `zed` retry or crash.
- Cross-grain exceptions must be `[GenerateSerializer]` to round-trip the Orleans boundary.

## Testing discipline

- **The SpiceDB conformance corpus is the correctness oracle.** `tests/.../TestData/*.yaml`
  (schema + relationships + Check/Lookup assertions) must stay green; the same corpus runs
  through the engine over the `ReferenceDatastore` oracle and the Orleans grain mesh, and both
  must agree. Never weaken/skip a corpus case to make something pass.
- **Verify grains via the Orleans `TestingHost`** (in-process `TestCluster`), not by booting a
  host. For server/client-streaming gRPC, drive the service in-process with a fake
  `IServerStreamWriter`/`IAsyncStreamReader` + a fake `ServerCallContext`. Do **not** start a
  Kestrel host (`dotnet run`) inside tests/CI — a backgrounded host can orphan and run forever.
- A real `zed`/`grpcurl` smoke test against a booted host is valuable but is an attended,
  manual step with explicit host teardown — keep it out of the automated suite.
- Cluster-using tests share a non-parallel xUnit collection (the test cluster passes schema via
  a process-wide static).

## Protos

`authzed.api.v1` is vendored under `src/Spiceport.Protos/Protos/authzed/` via
`buf export buf.build/authzed/api`. Grpc.Tools generates `Authzed.Api.V1` server bases; the
`<Protobuf>` items set `ProtoRoot`/`AdditionalImportDirs` so the `buf/validate` + `google/api`
import deps resolve (compiled message-only). To add an RPC, override the generated base in the
relevant `Authzed*V1Service` and map it onto an existing grain.

## House style (from the maintainer)

- No emojis. Prefer semantic HTML in any web UI. Classic (sociable) TDD over mockist.
- Don't write status updates (test counts, dates, "currently…") into committed docs — keep
  `README.md`/`CLAUDE.md`/`docs/` evergreen.
