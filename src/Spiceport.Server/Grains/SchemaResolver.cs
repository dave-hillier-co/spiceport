using System.Collections.Concurrent;
using System.Text;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
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
    /// Pre-populates the cache with an already-compiled snapshot under its own hash. Used at registration
    /// to seed the embedded startup schema: until a <c>WriteSchema</c> persists bytes into the log, every
    /// grain key carries the SEED schema's hash, and without this entry each dispatch would miss the
    /// cache, pay a sequencer <c>ReadSchemaAt</c> hop, read null (no persisted schema yet), and fall back —
    /// an uncacheable per-dispatch singleton call the measurement harness exposed at ~16k/s.
    /// </summary>
    public void Seed(SchemaSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        _byHash.TryAdd(snapshot.SchemaHash, snapshot);
    }

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
        return bytes is null ? FallbackFor(schemaHash, fallback) : CompileFetched(bytes);
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
        return bytes is null ? FallbackFor(schemaHash, fallback) : CompileFetched(bytes);
    }

    /// <summary>
    /// The no-persisted-schema (seed-window) fallback. When the requested hash IS the fallback's own hash,
    /// the label provably names the fallback's bytes, so the fallback is cached under it — turning the
    /// per-dispatch miss-fetch-null-fall-back cycle into a one-time event per silo. A requested hash that
    /// differs from the fallback's is an ambiguous label with no bytes to verify it against; it stays
    /// uncached and serves the fallback for this call only.
    /// </summary>
    private SchemaSnapshot FallbackFor(string schemaHash, SchemaSnapshot fallback)
    {
        if (schemaHash == fallback.SchemaHash)
            _byHash.TryAdd(schemaHash, fallback);
        return fallback;
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
    /// <remarks>
    /// Dual-key rationale: the bytes are indexed under BOTH their stored-bytes hash
    /// (<see cref="StoredSchemaHash.Compute(byte[])"/>, a raw SHA-256 over the persisted UTF-8 — what the
    /// log folds a schema write's hash as) AND their structural hash
    /// (<see cref="SchemaHash.Compute(System.Collections.Generic.IEnumerable{Spiceport.Core.NamespaceDefinition},System.Collections.Generic.IEnumerable{Spiceport.Core.CaveatDefinition}?)"/>,
    /// what <see cref="MutableSchemaProvider.CurrentSchemaHash"/> — and therefore every dispatch grain
    /// key — pins). The two hash spaces are different functions of the same bytes, so a lookup keyed by
    /// one alone would never hit an entry compiled and cached only under the other: a structural-hash
    /// request would miss the cache on EVERY call after a fetch (never just once), paying a sequencer
    /// <c>ReadSchemaAt</c> hop forever. Caching under both is sound (not the poisoning this method's
    /// primary doc guards against): unlike the REQUESTED hash, which may be stale or plain wrong, both
    /// hashes cached here are pure, always-correct functions of the compiled model actually produced —
    /// there is no way for either entry to name the wrong schema.
    /// </remarks>
    private SchemaSnapshot CompileFetched(byte[] bytes)
    {
        var snapshot = GetOrCompile(StoredSchemaHash.Compute(bytes), bytes);
        _byHash.TryAdd(SchemaHash.Compute(snapshot.Namespaces, snapshot.Caveats), snapshot);
        return snapshot;
    }

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
