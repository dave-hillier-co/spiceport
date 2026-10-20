using Spiceport.Core;

namespace Spiceport.Datastore;

/// <summary>What a <see cref="IDatastore.Watch"/> stream should carry.</summary>
[Flags]
public enum WatchContent
{
    /// <summary>Relationship create/touch/delete updates committed at the revision.</summary>
    Relationships = 1,

    /// <summary>A signal that the schema changed at the revision (no detailed diff for this slice).</summary>
    Schema = 2,

    /// <summary>
    /// Emit checkpoint markers that carry only a revision (no content). Mirrors SpiceDB's
    /// <c>WatchCheckpoints</c>: a consumer watching a subset of content still sees revision progress
    /// (a liveness signal) even when nothing matching its filter changed.
    /// </summary>
    Checkpoints = 4,

    /// <summary>Both relationship updates and schema-change signals.</summary>
    All = Relationships | Schema,
}

/// <summary>Options controlling a <see cref="IDatastore.Watch"/> stream.</summary>
/// <param name="Content">Which kinds of change to emit (default: relationships only).</param>
public sealed record WatchOptions(WatchContent Content = WatchContent.Relationships);

/// <summary>
/// The set of changes committed at a single revision. Emitted in strictly increasing revision order.
/// Mirrors SpiceDB's <c>datastore.RevisionChanges</c>: a revision plus the relationship updates (and an
/// optional schema-changed flag) that became visible at it.
/// </summary>
/// <param name="Revision">The revision at which these changes committed.</param>
/// <param name="RelationshipChanges">
/// Relationship mutations committed at <paramref name="Revision"/>. A create/touch surfaces as
/// <see cref="UpdateOperation.Touch"/> carrying the new payload; a delete surfaces as
/// <see cref="UpdateOperation.Delete"/> carrying the removed relationship.
/// </param>
/// <param name="SchemaChanged">True if the unified schema was (re)written at this revision.</param>
/// <param name="IsCheckpoint">
/// True if this is a checkpoint marker (no content): it signals that the changefeed has progressed
/// through <paramref name="Revision"/> so a consumer filtering to a subset of content still observes
/// revision progress. Mirrors SpiceDB's <c>RevisionChanges.IsCheckpoint</c>.
/// </param>
/// <param name="TransactionMetadata">
/// The metadata the transaction that committed at <paramref name="Revision"/> was given (see
/// <see cref="IDatastore.ReadWriteTx"/>), or null when it was given none. Opaque to the datastore. SpiceDB's
/// <c>RevisionChanges.Metadatas</c> is a list because some of its datastores merge concurrent transactions into
/// one revision; every revision here is exactly one transaction, so it holds at most one. A checkpoint
/// never carries it.
/// </param>
public sealed record RevisionChange(
    IRevision Revision,
    IReadOnlyList<RelationshipUpdate> RelationshipChanges,
    bool SchemaChanged = false,
    bool IsCheckpoint = false,
    byte[]? TransactionMetadata = null);
