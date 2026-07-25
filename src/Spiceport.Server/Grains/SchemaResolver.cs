using System.Collections.Concurrent;
using System.Text;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Schema;

namespace Spiceport.Grains;

/// <summary>
/// A per-silo cache of compiled <see cref="SchemaSnapshot"/>s keyed by schema hash. It turns the schema
/// bytes that are folded into the datastore log (and therefore materialized identically on every silo)
/// into the compiled model the check engine evaluates against, so a <see cref="CheckGrain"/> can evaluate
/// under the schema its grain key names — a pure function of the key's pinned revision — instead of the
/// silo-local <see cref="ISchemaProvider.Current"/>, which only reflects a <c>WriteSchema</c> that landed
/// on this silo.
/// </summary>
/// <remarks>
/// The hash IS the identity: a given hash always compiles to the same model, so the first caller to see a
/// hash compiles it and every later caller (on any check) reuses the cached snapshot — including its lazily
/// built reachability graphs. Schemas are few and small, so the cache is left unbounded (a schema hash
/// rotates the keyspace on every schema change, but stale entries are harmless and cheap; if bounding is
/// ever wanted it is a drop-in LRU). Compilation reproduces exactly what <see cref="MutableSchemaProvider"/>
/// does for a live write, so a resolved snapshot and a write-path snapshot for the same bytes are
/// interchangeable.
/// </remarks>
public sealed class SchemaResolver
{
    private readonly ConcurrentDictionary<string, SchemaSnapshot> _byHash = new();

    /// <summary>Returns the cached compiled snapshot for the given hash, if it has already been compiled.</summary>
    public bool TryGet(string schemaHash, out SchemaSnapshot snapshot) =>
        _byHash.TryGetValue(schemaHash, out snapshot!);

    /// <summary>
    /// Resolves the compiled schema named by <paramref name="schemaHash"/> at the revision
    /// <paramref name="reader"/> is pinned to: a cache hit is a pure lookup; a miss reads the schema bytes
    /// persisted at the revision (via <paramref name="reader"/>) and compiles-and-caches them. Returns <paramref name="fallback"/> when the revision pins no
    /// schema hash, or holds no persisted schema (the seed-only window) — the seed is the identical embedded
    /// constant on every silo, so the fallback does not diverge across the cluster.
    /// </summary>
    public async Task<SchemaSnapshot> ResolveAsync(
        string? schemaHash, IDatastoreReader reader, SchemaSnapshot fallback, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(fallback);

        if (string.IsNullOrEmpty(schemaHash))
            return fallback;
        if (_byHash.TryGetValue(schemaHash, out var cached))
            return cached;

        var bytes = await reader.ReadStoredSchema(ct).ConfigureAwait(false);
        return bytes is null ? fallback : CompileFetched(bytes);
    }

    /// <summary>
    /// Resolves the compiled schema named by <paramref name="schemaHash"/> at the pinned
    /// <paramref name="revision"/> through the <see cref="ISchemaSource"/> seam: a cache hit is a pure
    /// lookup; a miss reads the schema bytes persisted at the revision straight from the sequencer's fold
    /// (so the hop happens once per hash per silo) and compiles-and-caches them. Returns
    /// <paramref name="fallback"/> when the revision pins no schema hash, or holds no persisted schema
    /// (the seed-only window) — the seed is the identical embedded constant on every silo, so the fallback
    /// does not diverge across the cluster.
    /// </summary>
    public async Task<SchemaSnapshot> ResolveAsync(
        string? schemaHash, ISchemaSource source, IRevision revision, SchemaSnapshot fallback, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(revision);
        ArgumentNullException.ThrowIfNull(fallback);

        if (string.IsNullOrEmpty(schemaHash))
            return fallback;
        if (_byHash.TryGetValue(schemaHash, out var cached))
            return cached;

        var bytes = await source.ReadSchemaAt(revision, ct).ConfigureAwait(false);
        return bytes is null ? fallback : CompileFetched(bytes);
    }

    /// <summary>
    /// Compiles-and-caches bytes fetched on a cache miss under their ACTUAL computed hash — never under
    /// the requested hash. A token can pair a fresh revision with a stale silo-local hash; caching the
    /// fetched bytes under that mismatched requested hash would poison the cache PERMANENTLY (every later
    /// lookup of the stale hash would serve the wrong compiled schema, and the fresh hash would never get
    /// an entry). Returning the compiled schema-at-revision regardless of the requested label is the
    /// correct MVCC behavior: evaluation is a pure function of schema@rev, and the requested hash was
    /// only a (possibly stale) label for it.
    /// </summary>
    private SchemaSnapshot CompileFetched(byte[] bytes) =>
        GetOrCompile(StoredSchemaHash.Compute(bytes), bytes);

    /// <summary>
    /// Returns the compiled snapshot for <paramref name="schemaHash"/>, compiling <paramref name="schemaBytes"/>
    /// on first sight and caching the result. <paramref name="schemaBytes"/> is the UTF-8 schema DSL persisted
    /// at the revision (the exact bytes a <c>WriteSchema</c> stored); it is consulted only on a cache miss.
    /// </summary>
    public SchemaSnapshot GetOrCompile(string schemaHash, byte[] schemaBytes)
    {
        ArgumentNullException.ThrowIfNull(schemaHash);
        ArgumentNullException.ThrowIfNull(schemaBytes);
        return _byHash.GetOrAdd(schemaHash, static (hash, bytes) => Compile(hash, bytes), schemaBytes);
    }

    private static SchemaSnapshot Compile(string schemaHash, byte[] schemaBytes)
    {
        var text = Encoding.UTF8.GetString(schemaBytes);
        var compiled = SchemaCompiler.CompileSchema(text);
        // The persisted hash is authoritative (computed and validated at write time); reuse it verbatim so a
        // resolved snapshot's hash matches the grain key that selected it, rather than recomputing.
        return new SchemaSnapshot(compiled, schemaHash, text, Version: 0);
    }
}
