using System.Collections.Immutable;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;

namespace Spiceport.Grains;

/// <summary>Toggle for the Leopard <see cref="MembershipIndex"/> accelerator. Default ON (opt-out).</summary>
public sealed class MembershipIndexOptions
{
    /// <summary>When false the index is never built or consulted; lookups run the live traversal.</summary>
    public bool Enabled { get; init; } = true;
}

/// <summary>
/// A per-silo cache of the flattened <see cref="MembershipIndex"/>, keyed by the silo schema's hash and the
/// revision it was built at. It recomputes the index (a full scan) when the schema changes or a request needs
/// a fresher revision than the cached build, and reuses it otherwise. The index is built from the SAME
/// namespaces the lookup engine's confirming Check uses (the silo's current schema), so the candidates it
/// seeds and the verdicts that confirm them are always schema-consistent.
/// </summary>
/// <remarks>
/// Freshness rule: an index built at revision <c>R</c> is a complete candidate superset for any request at a
/// revision <c>&lt;= R</c> (it only ever contains MORE relationships, and Check — run against the request's
/// own reader — trims any that are not yet visible). So a cached index is reused when it was built at a
/// revision at least as fresh as the request; an older index is rebuilt rather than risk a missed member.
/// When disabled, <see cref="TryGet"/> always returns null and the caller runs the unchanged live traversal.
/// </remarks>
public sealed class MembershipIndexCache(MembershipIndexOptions options)
{
    private readonly bool _enabled = options.Enabled;
    private readonly object _lock = new();

    private MembershipIndex? _index;
    private long _builtRevision;

    /// <summary>
    /// Returns a schema-consistent index usable to seed candidates for a request at <paramref name="revision"/>,
    /// or null when the accelerator is disabled (the caller then runs the live traversal). Builds/refreshes
    /// the index as needed from <paramref name="reader"/> (pinned at <paramref name="revision"/>).
    /// </summary>
    public async Task<MembershipIndex?> TryGet(
        ImmutableList<NamespaceDefinition> namespaces,
        ImmutableList<CaveatDefinition> caveats,
        IDatastoreReader reader,
        long revision,
        CancellationToken cancellationToken = default)
    {
        if (!_enabled)
            return null;

        var hash = SchemaHash.Compute(namespaces, caveats);

        lock (_lock)
        {
            if (_index is { } cached && cached.SchemaHash == hash && _builtRevision >= revision)
                return cached;
        }

        var built = await MembershipIndex.Build(namespaces, reader, hash, cancellationToken).ConfigureAwait(false);

        lock (_lock)
        {
            // Keep the freshest build for this schema; concurrent builders simply converge on the latest.
            if (_index is null || _index.SchemaHash != hash || _builtRevision < revision)
            {
                _index = built;
                _builtRevision = revision;
            }
            return _index.SchemaHash == hash && _builtRevision >= revision ? _index : built;
        }
    }
}
