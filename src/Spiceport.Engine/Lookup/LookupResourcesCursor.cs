using System.Collections.Immutable;

namespace Spiceport.Engine;

/// <summary>
/// An opaque resume token for <see cref="LookupResourcesEngine.LookupResources"/>, made of one
/// ordered section per nesting level. Port of SpiceDB's cursored response sections, simplified to
/// deterministic-ordering resume (no chunk cache / parallel cursored iterators).
/// </summary>
/// <remarks>
/// Iteration is deterministic: entrypoints are processed in a fixed order and resource ids are sorted,
/// so a section's <c>(EntrypointIndex, LastResourceId)</c> uniquely positions resumption. Passing a
/// cursor skips entrypoints before <see cref="LookupResourcesCursorSection.EntrypointIndex"/> and
/// resource ids at or before <see cref="LookupResourcesCursorSection.LastResourceId"/> within the
/// resumed entrypoint, then recurses with the remaining nested sections.
/// </remarks>
/// <param name="Sections">One section per nesting level, outermost first.</param>
public sealed record LookupResourcesCursor(ImmutableList<LookupResourcesCursorSection> Sections)
{
    /// <summary>An empty cursor (start from the beginning).</summary>
    public static LookupResourcesCursor Empty { get; } = new([]);
}

/// <summary>One nesting level of a <see cref="LookupResourcesCursor"/>.</summary>
/// <param name="EntrypointIndex">The index of the entrypoint being resumed at this level.</param>
/// <param name="LastResourceId">The last resource id already yielded within that entrypoint.</param>
public sealed record LookupResourcesCursorSection(int EntrypointIndex, string LastResourceId);
