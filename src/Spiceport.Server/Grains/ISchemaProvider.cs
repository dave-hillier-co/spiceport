using System.Collections.Immutable;
using Spiceport.Core;
using Spiceport.Engine;
using Spiceport.Schema;

namespace Spiceport.Grains;

/// <summary>
/// An immutable snapshot of the live schema: the compiled model, its stable hash, the source text it
/// was compiled from, and a monotonic version. Because it is a reference-typed record, a whole snapshot
/// can be swapped atomically by a single reference assignment, so any request that has captured a
/// snapshot keeps observing a consistent schema for its whole lifetime.
/// </summary>
/// <param name="Schema">The compiled schema (namespace + caveat definitions).</param>
/// <param name="SchemaHash">A stable hash of the compiled schema, used to scope dispatch cache and grain keys.</param>
/// <param name="SourceText">The verbatim DSL the schema was compiled from (CompiledSchema is lossy; this round-trips ReadSchema).</param>
/// <param name="Version">A monotonic version, incremented on each successful update.</param>
public sealed record SchemaSnapshot(
    CompiledSchema Schema,
    string SchemaHash,
    string SourceText,
    long Version)
{
    // Built at most once per snapshot (Lazy, not eager): the first reader of ReachabilityFull/ReachabilityFirst
    // triggers the build and every subsequent reader of THIS snapshot observes the same cached instance; a
    // schema Update() constructs a brand-new SchemaSnapshot (and hence fresh Lazy<> cells), so the graph is
    // dropped for GC when the snapshot is swapped rather than accumulating in a process-wide cache.
    private readonly Lazy<ReachabilityGraph> _reachabilityFull =
        new(() => ReachabilityGraph.Build(Schema.Namespaces.ToImmutableDictionary(ns => ns.Name), ReachabilityMode.Full),
            LazyThreadSafetyMode.ExecutionAndPublication);

    private readonly Lazy<ReachabilityGraph> _reachabilityFirst =
        new(() => ReachabilityGraph.Build(Schema.Namespaces.ToImmutableDictionary(ns => ns.Name), ReachabilityMode.First),
            LazyThreadSafetyMode.ExecutionAndPublication);

    /// <summary>The compiled namespace definitions.</summary>
    public ImmutableList<NamespaceDefinition> Namespaces => Schema.Namespaces;

    /// <summary>The compiled caveat definitions.</summary>
    public ImmutableList<CaveatDefinition> Caveats => Schema.Caveats;

    /// <summary>The full-mode reachability graph for this snapshot's schema, built lazily on first use.</summary>
    public ReachabilityGraph ReachabilityFull => _reachabilityFull.Value;

    /// <summary>The first-mode reachability graph for this snapshot's schema, built lazily on first use.</summary>
    public ReachabilityGraph ReachabilityFirst => _reachabilityFirst.Value;
}

/// <summary>
/// Supplies the process-wide compiled schema that the check engine evaluates against, as a mutable,
/// versioned, thread-safe snapshot that can be swapped at runtime via <see cref="Update"/>.
/// </summary>
/// <remarks>
/// Reads observe a whole consistent <see cref="SchemaSnapshot"/> via <see cref="Current"/>; updates
/// compile-then-swap atomically. The dispatch mesh reads <see cref="SchemaSnapshot.SchemaHash"/> per
/// request (through <see cref="Spiceport.Engine.ISchemaHashSource"/>), so a swap is reflected in every
/// new cache and grain key and pre-change cache entries are never reused.
/// </remarks>
public interface ISchemaProvider
{
    /// <summary>Atomically reads the whole current schema snapshot.</summary>
    SchemaSnapshot Current { get; }

    /// <summary>
    /// Compiles the given schema DSL text and atomically swaps it in as the current snapshot.
    /// Throws before any swap if the text fails to compile, so the live schema is never left torn.
    /// </summary>
    /// <param name="schemaText">The schema DSL text to compile and install.</param>
    /// <returns>The newly installed snapshot.</returns>
    SchemaSnapshot Update(string schemaText);
}

/// <summary>
/// Default <see cref="ISchemaProvider"/>: holds a single <see cref="SchemaSnapshot"/> that is swapped
/// atomically on <see cref="Update"/>. Also exposes the current hash via
/// <see cref="Spiceport.Engine.ISchemaHashSource"/> for the dispatch mesh.
/// </summary>
public sealed class MutableSchemaProvider : ISchemaProvider, ISchemaHashSource
{
    private volatile SchemaSnapshot _current;

    /// <summary>Creates a provider seeded with the given schema text (compiled immediately).</summary>
    /// <param name="schemaText">The initial schema DSL text.</param>
    public MutableSchemaProvider(string schemaText)
    {
        ArgumentNullException.ThrowIfNull(schemaText);
        _current = Compile(schemaText, version: 0);
    }

    /// <inheritdoc />
    public SchemaSnapshot Current => _current;

    /// <inheritdoc />
    public string CurrentSchemaHash => _current.SchemaHash;

    /// <inheritdoc />
    public SchemaSnapshot Update(string schemaText)
    {
        ArgumentNullException.ThrowIfNull(schemaText);

        // Compile first: a compile failure throws here, BEFORE any swap, so _current is never torn.
        var next = Compile(schemaText, _current.Version + 1);

        // A single reference assignment to a volatile field is atomic: in-flight readers keep their
        // captured snapshot; new readers see the new one.
        _current = next;
        return next;
    }

    private static SchemaSnapshot Compile(string schemaText, long version)
    {
        var compiled = SchemaCompiler.CompileSchema(schemaText);
        var hash = SchemaHash.Compute(compiled.Namespaces, compiled.Caveats);
        return new SchemaSnapshot(compiled, hash, schemaText, version);
    }
}
