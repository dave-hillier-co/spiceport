using System.Security.Cryptography;
using Spiceport.Core;
using Spiceport.Datastore.Memory;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// The event-log fold and its inverse (the proposal diff). This is the single definition of "apply a
/// committed <see cref="LogEvent"/> to the datastore state", used by the event-sourced
/// <c>DatastoreGrain</c>'s <c>TransitionState</c> (replay + live append) and by the storage replay on
/// reactivation. It is deliberately implemented by REUSING the in-memory MVCC
/// <see cref="InMemoryReadWriteTransaction"/> (the same mechanics the write path runs through), so the
/// fold is provably equal to the in-memory <c>Commit()</c> rather than a divergent re-derivation of the
/// MVCC visibility rules.
/// </summary>
internal static class LogFold
{
    /// <summary>
    /// Folds one committed <see cref="LogEvent"/> into the datastore state. Each relationship change is a
    /// Touch (create-or-replace the identity, closing any prior live row) or a Delete (close the live row);
    /// a schema change appends the new schema version; each counter change appends a counter version
    /// (null filter = tombstone). The resulting head is the event's revision.
    /// </summary>
    public static DatastoreGrainState ApplyEvent(DatastoreGrainState state, LogEvent ev)
    {
        var baseState = DatastoreStateConverters.ToMemory(state);

        // Replay the resolved changes through a fresh in-memory transaction pinned at the event revision,
        // then commit — exactly the path the write produced this event from, so the fold equals that Commit.
        var tx = new InMemoryReadWriteTransaction(baseState, ev.Revision);

        if (ev.RelationshipChanges.Count > 0)
        {
            var updates = ev.RelationshipChanges
                .Select(u => new RelationshipUpdate(
                    WireConvert.ToRelationship(u.Relationship),
                    u.Operation == RelationshipUpdateOpWire.Delete ? UpdateOperation.Delete : UpdateOperation.Touch))
                .ToList();
            // Synchronous completion: the in-memory tx stages immediately.
            tx.WriteRelationships(updates).GetAwaiter().GetResult();
        }

        if (ev.SchemaChange is { } schema)
            tx.WriteStoredSchema(schema.Bytes).GetAwaiter().GetResult();

        var committed = tx.Commit();

        // Counters are folded by appending the event's NET counter versions DIRECTLY, matching what
        // InMemoryReadWriteTransaction.Commit appends (a raw CounterVersion for whatever net op survived) —
        // NOT by replaying through the guarded tx.WriteCounter/DeleteCounter, whose register/unregister
        // preconditions can be false in the fold base for a same-commit register+unregister (or the inverse),
        // which would throw and poison replay even though the original Commit succeeded.
        var counters = committed.Counters;
        foreach (var counter in ev.CounterChanges)
        {
            var filter = counter.Filter is { } f ? WireConvert.ToCoreFilter(f) : null;
            counters = counters.Add(new CounterVersion(ev.Revision, counter.Name, filter));
        }

        return DatastoreStateConverters.ToGrain(committed with { Counters = counters });
    }

    /// <summary>
    /// Builds the canonical <see cref="LogEvent"/> from a <see cref="ProposedWrite"/> by stamping the
    /// grain-minted <paramref name="revision"/>: the proposal already carries the resolved relationship
    /// Touch/Delete changes and the counter deltas, so the only derivation is the schema version (revision +
    /// bytes + hash) for the optional schema bytes. This is the single point that turns a revision-less
    /// proposal into a self-contained, foldable event.
    /// </summary>
    public static LogEvent EventFromProposal(ProposedWrite write, long revision)
    {
        var schemaChange = write.SchemaBytes is { } bytes
            ? new SchemaVersionWire(revision, bytes, ComputeHash(bytes))
            : null;
        return new LogEvent(revision, write.RelationshipChanges, schemaChange, write.CounterChanges);
    }

    private static string ComputeHash(byte[] bytes) =>
        Convert.ToHexStringLower(SHA256.HashData(bytes));
}
