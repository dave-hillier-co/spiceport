using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// The schema-at-revision seam of the graph-sharded design (<c>docs/graph-sharded-datastore.md</c> §3):
/// reads the schema bytes persisted at a pinned revision straight from the sequencer's fold, so schema
/// resolution needs no per-silo whole-graph projection. Consumed by <see cref="SchemaResolver"/> ONLY on
/// a schema-hash cache miss — the hash is the identity and the compiled snapshot is cached per silo, so
/// the read behind this seam happens once per hash per silo, never per check.
/// </summary>
public interface ISchemaSource
{
    /// <summary>
    /// Returns the schema bytes effective at <paramref name="revision"/> (the last version persisted at
    /// or before it), or null when none exists (the seed-only window).
    /// </summary>
    Task<byte[]?> ReadSchemaAt(IRevision revision, CancellationToken ct);
}

/// <summary>
/// The grain-backed source: one <see cref="IDatastoreGrain.ReadSchemaAt"/> call against the
/// cluster-singleton sequencer, whose confirmed fold serves any resolvable pinned revision (every
/// resolvable revision is at or below the head the grain minted it under).
/// </summary>
internal sealed class GrainSchemaSource(IGrainFactory grainFactory) : ISchemaSource
{
    /// <inheritdoc />
    public Task<byte[]?> ReadSchemaAt(IRevision revision, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(revision);

        // Mirrors ReferenceDatastore.ToNanos: the timestamp form is the only revision identity the
        // datastore mints, so anything else is a caller bug, not a fallback case.
        var nanos = revision switch
        {
            TimestampRevision t => t.TimestampNanosSinceEpoch,
            _ => throw new InvalidRevisionException($"unsupported revision type: {revision.GetType().Name}"),
        };
        return grainFactory.GetGrain<IDatastoreGrain>(IDatastoreGrain.Key).ReadSchemaAt(nanos);
    }
}
