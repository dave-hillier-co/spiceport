using System.Runtime.CompilerServices;

// The grain-backed datastore (Spiceport.Server) reuses these MVCC mechanics — the same
// (DatastoreState fold, MvccReadWriteTransaction staging/commit, MvccSnapshotReader queries) that
// back the ReferenceDatastore oracle — by converting the Orleans wire state into DatastoreState
// rather than re-deriving that logic. These types are deliberately internal MVCC encapsulation,
// so expose them to Server (only) as friends.
[assembly: InternalsVisibleTo("Spiceport.Server")]

// Grain-side gate tests fold/compare the MVCC state directly (event-log equivalence,
// journaled-replay reconstruction), so the test assembly is also a friend.
[assembly: InternalsVisibleTo("Spiceport.Grains.Tests")]

// Datastore-level GC gates (DatastoreState.CollectBelow) unit-test the fold primitive directly, since
// ReferenceDatastore never calls it itself (only the event-sourced DatastoreGrain's janitor does).
[assembly: InternalsVisibleTo("Spiceport.Datastore.Tests")]
