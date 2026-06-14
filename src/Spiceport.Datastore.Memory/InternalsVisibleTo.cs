using System.Runtime.CompilerServices;

// The grain-backed datastore (Spiceport.Server) reuses the in-memory MVCC mechanics
// (DatastoreState fold, InMemoryReadWriteTransaction staging/commit, InMemoryDatastoreReader queries)
// by converting the Orleans wire state into DatastoreState rather than re-deriving that logic. These
// types are deliberately internal MVCC encapsulation, so expose them to Server (only) as friends.
[assembly: InternalsVisibleTo("Spiceport.Server")]
