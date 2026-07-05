# Spiceport

A port of [SpiceDB](https://github.com/authzed/spicedb) — the open-source implementation of
Google's [Zanzibar](https://authzed.com/zanzibar) authorization system — to **.NET 10** and
**Microsoft Orleans** (virtual actors).

Spiceport answers the Zanzibar question — *"can subject X perform action Y on resource Z?"* —
from a relationship graph defined by a schema, and it does so by running the recursive
permission-check dispatch on Orleans grains. It speaks the `authzed.api.v1` gRPC protocol, so
the official [`zed`](https://github.com/authzed/zed) CLI and SpiceDB clients work against it.

## Why Orleans

SpiceDB hand-rolls three distributed-systems mechanisms in its dispatch layer — consistent-hash
request routing (for cache locality), in-flight request coalescing (singleflight), and cluster
membership/rebalancing. Those are exactly what a virtual-actor runtime provides natively. So
Spiceport keeps the part that is genuinely hard — the graph-evaluation engine and the schema
compiler — and lets Orleans replace the distribution plumbing:

- The recursive Check decomposes into pure, cacheable **sub-problems**. Each sub-problem is a
  grain identity. Grain placement gives consistent-hash routing; single activation gives
  singleflight; inter-silo calls replace SpiceDB's remote-dispatch RPC layer.
- A **dispatcher seam** (`IDispatcher`) mediates every sub-problem, so the same engine runs
  unchanged whether a sub-problem resolves in-process or hops to another silo.
- A consistent-hash **placement director** plus a **local-recurse** shortcut means a sub-problem
  owned by the local silo is computed in-process and only cross-shard sub-problems hop a grain —
  mirroring how SpiceDB only RPCs across nodes.

The full design rationale is in [`docs/architecture-analysis.md`](docs/architecture-analysis.md).

## What works

- **Permission queries**: Check, BatchCheck/CheckBulkPermissions, ExpandPermissionTree,
  LookupResources, LookupSubjects.
- **Schema & data**: WriteSchema/ReadSchema (dynamic, versioned), Write/Read/Delete
  relationships, bulk import/export, Watch (changefeed).
- **Schema introspection**: ReflectSchema, DiffSchema, ComputablePermissions, DependentRelations.
- **ReBAC + ABAC**: unions / intersections / exclusions, arrows (tupleset-to-userset),
  wildcards, nested & recursive groups, **caveats** (CEL) and **relationship expiration**.
- **Consistency**: ZedTokens with minimize-latency / at-least-as-fresh / fully-consistent /
  at-exact-snapshot semantics (read-your-writes).
- **Write safety**: preconditions (must-match / must-not-match) and schema-change validation.
- **Storage**: the datastore is an **event-sourced cluster-singleton Orleans grain** — an
  append-only log of changes is the source of truth, each silo reads from a projection folded from
  that log, and the same feed drives Watch. Durable via Orleans grain storage (in-memory for dev,
  **PostgreSQL** via AdoNet) with no application SQL schema. SpiceDB's consistency conformance
  corpus passes against both the reference datastore oracle and the grain mesh.
- **Relationship counters** (ExperimentalService): register/unregister a named counter over a
  filter and count matching relationships at a revision (computed on demand).
- **`authzed.api.v1`** gRPC surface — verified end to end with the real `zed` CLI (schema,
  relationships, check, lookups, backup/restore).

## Project layout

```
src/
  Spiceport.Core              core model: ObjectAndRelation, Relationship, schema model,
                              Revision/ZedToken, tuple string parsing
  Spiceport.Datastore         datastore abstraction + the MVCC state model (fold, snapshot reader,
                              transaction) + the ReferenceDatastore conformance oracle
  Spiceport.Server            the engine and the mesh, in one project:
                              Schema/  schema DSL compiler (lexer -> parser -> compiler) + reachability graph
                              Engine/  Check/Expand/Lookup engine + the IDispatcher seam + caching dispatcher
                              Grains/  dispatch mesh, placement, schema/relationships, the event-sourced
                                       datastore grain + per-silo projection, Watch, Leopard index
                              Grains.Abstractions/  grain interfaces + serializable DTOs
  Spiceport.Silo              standalone Orleans silo host
  Spiceport.Api               co-hosted silo + ASP.NET Core gRPC (authzed.api.v1 + internal)
  Spiceport.Protos            protobuf contracts (vendored authzed.api.v1 + internal)
tests/                        xUnit; engine/schema/datastore unit tests, the SpiceDB conformance
                              corpus, and Orleans TestingHost mesh tests
docs/                         architecture analysis + the Zanzibar paper
```

## Build & test

Requires the .NET 10 SDK. Docker is required only for the grain-storage durability tests
(Testcontainers PostgreSQL).

```bash
dotnet build                 # build the solution
dotnet test                  # run all tests (durability tests spin up a Postgres container)
```

## Run

The API host co-hosts an Orleans silo and the gRPC services in one process:

```bash
dotnet run --project src/Spiceport.Api --launch-profile https   # serves https://localhost:7022 (HTTP/2)
```

Then drive it with the real `zed` CLI (uses the dev certificate; any token, the server is
auth-agnostic):

```bash
zed schema write schema.zed --endpoint localhost:7022 --token any --no-verify-ca
zed relationship create document:readme viewer user:alice --endpoint localhost:7022 --token any --no-verify-ca
zed permission check document:readme view user:alice --endpoint localhost:7022 --token any --no-verify-ca   # => true
```

gRPC requires the HTTP/2 (https) endpoint.

### Durable datastore storage

The whole datastore (relationships, schema, counters) lives behind a single cluster-singleton Orleans
grain. It is **event-sourced**: an append-only log of changes is the source of truth and the
materialized state is the fold, so a write is a cheap version-checked append rather than a whole-state
rewrite, the log offset is the global revision, and each silo reads from a projection folded from the
log (no per-Check full fetch). By default the grain uses in-memory grain storage, so the localhost dev
host needs no external dependency but the state is lost on restart. To make it durable, point the silo
at a Postgres database via the `ConnectionStrings:OrleansStorage` configuration key (env form
`ConnectionStrings__OrleansStorage`, fallback key `Storage:ConnectionString`):

```bash
export ConnectionStrings__OrleansStorage="Host=localhost;Port=5432;Database=spiceport;Username=postgres;Password=postgres"
```

When that key is set the silo persists the grain via Orleans AdoNet grain storage and the state
survives silo restart and activation migration. The target database must first have the Orleans
AdoNet schema applied — run `PostgreSQL-Main.sql` then `PostgreSQL-Persistence.sql` (shipped with the
`Microsoft.Orleans.Persistence.AdoNet` package / the Orleans repo). The provider is configured to use
the binary grain-storage serializer so relationship caveat context (JSON values) round-trips intact.

## Attribution & license

Spiceport is a derivative work of [SpiceDB](https://github.com/authzed/spicedb) by
[AuthZed](https://authzed.com), which is licensed under the Apache License 2.0. The vendored
`authzed.api.v1` protobuf definitions are AuthZed's. This repository is intended to be
distributed under the Apache License 2.0; see `LICENSE`/`NOTICE` for the full terms and
attribution.
