using System.Collections.Immutable;
using Spiceport.Core;

namespace Spiceport.Datastore;

/// <summary>A relationship row with the revision range over which it is live.</summary>
/// <param name="Relationship">The stored relationship (payload + identity).</param>
/// <param name="CreatedRevision">The revision at which the row became live.</param>
/// <param name="DeletedRevision">The revision at which the row was deleted, or null if still live.</param>
internal sealed record StoredRelationship(
    Relationship Relationship,
    long CreatedRevision,
    long? DeletedRevision)
{
    /// <summary>True if this row is visible when reading at <paramref name="atRevision"/>.</summary>
    public bool IsVisibleAt(long atRevision) =>
        CreatedRevision <= atRevision && (DeletedRevision is null || DeletedRevision > atRevision);
}

/// <summary>A schema bytes version stamped with the revision at which it was written.</summary>
internal sealed record SchemaVersion(long Revision, byte[] Bytes, string Hash);

/// <summary>
/// An MVCC version of a registered counter, stamped with the revision at which it was written. A null
/// <paramref name="Filter"/> marks a tombstone (the counter was unregistered at this revision).
/// </summary>
internal sealed record CounterVersion(long Revision, string Name, RelationshipsFilter? Filter);

/// <summary>
/// An immutable point-in-time state of the datastore. Each committed transaction produces a new
/// instance; snapshot readers capture a reference and remain correct regardless of later writes.
/// </summary>
/// <param name="GcFloor">
/// The revision below which MVCC history has been garbage-collected (0 = nothing collected yet). Reads
/// pinned strictly below this floor are rejected (see the <c>MvccSnapshotReader</c> constructor guard) —
/// their result could otherwise silently omit rows <see cref="CollectBelow"/> has already dropped.
/// </param>
internal sealed record DatastoreState(
    long HeadRevision,
    ImmutableList<StoredRelationship> Relationships,
    ImmutableList<SchemaVersion> Schemas,
    ImmutableList<CounterVersion> Counters,
    long GcFloor = 0)
{
    public static DatastoreState Empty(long initialRevision) =>
        new(initialRevision, ImmutableList<StoredRelationship>.Empty, ImmutableList<SchemaVersion>.Empty, ImmutableList<CounterVersion>.Empty);

    /// <summary>Returns the relationships live at the given revision.</summary>
    public IEnumerable<Relationship> LiveAt(long atRevision)
    {
        foreach (var row in Relationships)
        {
            if (row.IsVisibleAt(atRevision))
                yield return row.Relationship;
        }
    }

    /// <summary>
    /// Returns the relationship changes committed AT the given revision, reconstructed from the row
    /// created/deleted stamps. A row created at the revision surfaces as a touch (carrying the payload);
    /// a row deleted at the revision surfaces as a delete (carrying the removed relationship). A touch
    /// over an existing key produces both a delete of the old payload and a touch of the new — we emit
    /// only the touch (the live result), matching SpiceDB's per-revision update semantics.
    /// </summary>
    public IReadOnlyList<RelationshipUpdate> ChangesAt(long atRevision)
    {
        var changes = new List<RelationshipUpdate>();
        var touchedKeys = new HashSet<RelationshipKey>();

        foreach (var row in Relationships)
        {
            if (row.CreatedRevision == atRevision)
            {
                changes.Add(new RelationshipUpdate(row.Relationship, UpdateOperation.Touch));
                touchedKeys.Add(RelationshipKey.From(row.Relationship));
            }
        }

        foreach (var row in Relationships)
        {
            if (row.DeletedRevision == atRevision && !touchedKeys.Contains(RelationshipKey.From(row.Relationship)))
                changes.Add(new RelationshipUpdate(row.Relationship, UpdateOperation.Delete));
        }

        return changes;
    }

    /// <summary>True if the unified schema was (re)written exactly at the given revision.</summary>
    public bool SchemaChangedAt(long atRevision)
    {
        foreach (var schema in Schemas)
        {
            if (schema.Revision == atRevision)
                return true;
        }
        return false;
    }

    /// <summary>Returns the schema bytes effective at the given revision, or null if none.</summary>
    public byte[]? SchemaAt(long atRevision)
    {
        byte[]? result = null;
        foreach (var schema in Schemas)
        {
            if (schema.Revision <= atRevision)
                result = schema.Bytes;
            else
                break;
        }
        return result;
    }

    /// <summary>Returns the schema hash effective at the given revision, or null if none.</summary>
    public string? SchemaHashAt(long atRevision)
    {
        string? result = null;
        foreach (var schema in Schemas)
        {
            if (schema.Revision <= atRevision)
                result = schema.Hash;
            else
                break;
        }
        return result;
    }

    /// <summary>
    /// Returns the filter registered for the named counter live at the given revision (last-wins fold over
    /// versions with <c>Revision &lt;= atRevision</c>), or null if the last version is a tombstone / none.
    /// </summary>
    public RelationshipsFilter? CounterFilterAt(string name, long atRevision)
    {
        RelationshipsFilter? result = null;
        var found = false;
        foreach (var version in Counters)
        {
            if (version.Name == name && version.Revision <= atRevision)
            {
                result = version.Filter;
                found = true;
            }
        }
        return found ? result : null;
    }

    /// <summary>Returns the counters live at the given revision.</summary>
    public IEnumerable<RegisteredCounter> LiveCountersAt(long atRevision)
    {
        var latest = new Dictionary<string, RelationshipsFilter?>();
        foreach (var version in Counters)
        {
            if (version.Revision <= atRevision)
                latest[version.Name] = version.Filter;
        }
        foreach (var (name, filter) in latest)
        {
            if (filter is not null)
                yield return new RegisteredCounter(name, filter);
        }
    }

    /// <summary>
    /// Collects MVCC history strictly below <paramref name="floor"/>: this is the ONE fold definition GC
    /// events apply (see <c>LogFold.ApplyEvent</c> in Spiceport.Server), so the grain state and any
    /// other fold of the same log all converge on the identical collected state.
    /// A no-op (returns <see langword="this"/>) when <paramref name="floor"/> does not advance the current
    /// <see cref="GcFloor"/> — GC only ever moves forward. <see cref="HeadRevision"/> is untouched: the
    /// head advances through the normal per-revision fold, not through collection.
    /// </summary>
    /// <remarks>
    /// <para>
    /// RELATIONSHIPS: a row is dropped when it is fully dead below the floor (<c>DeletedRevision &lt;=
    /// floor</c> — invisible to every reader at or above the floor, and readers below the floor are
    /// rejected outright) OR when it is expired at/before the floor (see the expiration-sweep
    /// justification below). Every other row — in particular one created at/below the floor that is
    /// still live, or deleted only after the floor — is kept unchanged, so every read at a revision
    /// &gt;= floor is byte-identical before and after collection.
    /// </para>
    /// <para>
    /// EXPIRATION SWEEP — justification: the only read paths that consult <c>Relationship.OptionalExpiration</c>
    /// (<c>MvccSnapshotReader</c>/<c>MvccReadWriteTransaction</c>'s <c>IsExpired</c>) compare it against
    /// <see cref="DateTimeOffset.UtcNow"/> AT QUERY TIME — never against the pinned MVCC revision. Revisions
    /// and expirations both live on the same nanos-since-Unix-epoch clock, and <paramref name="floor"/> is
    /// always a timestamp that has ALREADY elapsed by the time collection runs (the caller derives it as
    /// <c>min(head, NowNanos() - window)</c>). Because wall-clock time only moves forward, every future
    /// query's "now" is &gt;= floor. So any row whose expiration is &lt;= floor is GUARANTEED to satisfy
    /// <c>exp &lt;= now</c> for every future query regardless of which revision is pinned — it would never
    /// be yielded again even if kept. Dropping it therefore cannot change any still-servable read. (Rows
    /// reconstructed via <c>DatastoreState.ChangesAt</c> for Watch-style replay are a different path — but
    /// the grain-backed Watch feed replays raw <c>LogEvent.RelationshipChanges</c> from the log, never
    /// <c>ChangesAt</c> over the folded/collected state, so that path is unaffected by this sweep.)
    /// </para>
    /// <para>
    /// SCHEMAS: kept are the single LATEST version with <c>Revision &lt;= floor</c> (the version effective
    /// at the floor, needed by any read/fold at a revision &gt;= floor with no later schema write) plus
    /// every version above the floor.
    /// </para>
    /// <para>
    /// COUNTERS: compacted PER NAME using the same rule as schemas — the latest version with
    /// <c>Revision &lt;= floor</c> per counter name, plus everything above the floor — EXCEPT that a kept
    /// latest-at-or-below version that is itself a tombstone (<c>Filter is null</c>) is dropped entirely:
    /// nothing above the floor can need to consult it (a tombstone and "no version at all" both resolve to
    /// "not registered" for every consumer of <c>CounterFilterAt</c>/<c>LiveCountersAt</c>).
    /// </para>
    /// </remarks>
    public DatastoreState CollectBelow(long floor)
    {
        if (floor <= GcFloor)
            return this;

        var keptRelationships = Relationships.Where(row =>
        {
            if (row.DeletedRevision is { } deleted && deleted <= floor)
                return false; // fully dead below the floor
            if (row.Relationship.OptionalExpiration is { } exp && NanosSinceEpoch(exp) <= floor)
                return false; // expired at/before the floor: see the justification above
            return true;
        }).ToImmutableList();

        var keptSchemas = CompactLatestAtOrBelow(Schemas, s => s.Revision, floor);
        var keptCounters = CompactCountersPerName(floor);

        return this with
        {
            Relationships = keptRelationships,
            Schemas = keptSchemas,
            Counters = keptCounters,
            GcFloor = floor,
        };
    }

    private ImmutableList<CounterVersion> CompactCountersPerName(long floor)
    {
        // The latest revision <= floor per counter name (Counters is append-ordered, so the last match
        // per name IS its latest revision <= floor).
        var latestAtOrBelowByName = new Dictionary<string, long>();
        foreach (var c in Counters)
            if (c.Revision <= floor)
                latestAtOrBelowByName[c.Name] = c.Revision;

        return Counters.Where(c =>
        {
            if (c.Revision > floor)
                return true; // everything above the floor is kept unconditionally
            // The unique latest-at-or-below version for this name: keep it UNLESS it is a tombstone (a
            // dropped tombstone and "no version at all" are indistinguishable to every consumer).
            return latestAtOrBelowByName[c.Name] == c.Revision && c.Filter is not null;
        }).ToImmutableList();
    }

    private static ImmutableList<T> CompactLatestAtOrBelow<T>(
        ImmutableList<T> versions, Func<T, long> revisionOf, long floor)
    {
        long? latestAtOrBelow = null;
        foreach (var v in versions)
            if (revisionOf(v) <= floor)
                latestAtOrBelow = revisionOf(v);

        return versions.Where(v => revisionOf(v) > floor || revisionOf(v) == latestAtOrBelow).ToImmutableList();
    }

    private static long NanosSinceEpoch(DateTimeOffset dt) => (dt - DateTimeOffset.UnixEpoch).Ticks * 100L;
}
