using System.Collections.Immutable;
using Spiceport.Core;

namespace Spiceport.Datastore.Memory;

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
/// An immutable point-in-time state of the datastore. Each committed transaction produces a new
/// instance; snapshot readers capture a reference and remain correct regardless of later writes.
/// </summary>
internal sealed record DatastoreState(
    long HeadRevision,
    ImmutableList<StoredRelationship> Relationships,
    ImmutableList<SchemaVersion> Schemas)
{
    public static DatastoreState Empty(long initialRevision) =>
        new(initialRevision, ImmutableList<StoredRelationship>.Empty, ImmutableList<SchemaVersion>.Empty);

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
}
