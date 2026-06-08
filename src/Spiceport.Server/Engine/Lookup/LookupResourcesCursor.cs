using System.Collections.Immutable;
using Spiceport.Core;

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

/// <summary>
/// One nesting level of a <see cref="LookupResourcesCursor"/>. Exactly one resume mechanism applies per
/// section, selected by the entrypoint kind at this level:
/// <list type="bullet">
///   <item><b>Portion-1 leaf</b> (<see cref="EntrypointIndex"/> == -1): resume the self-match by skipping
///   sorted resource ids at or before <see cref="LastResourceId"/>.</item>
///   <item><b>Query entrypoint</b> (Relation / arrow): resume the reverse-query stream strictly after
///   <see cref="AfterKeyset"/> (the last fully-consumed chunk's final relationship). Null means the first
///   chunk (resume from the start of this entrypoint).</item>
///   <item><b>Structural rewrite</b> (Self / ComputedUserset): no within-level position; resumption is
///   carried entirely by the deeper sections. Both <see cref="LastResourceId"/> and
///   <see cref="AfterKeyset"/> are null.</item>
/// </list>
/// </summary>
/// <param name="EntrypointIndex">The entrypoint index being resumed (-1 marks the Portion-1 self-match leaf).</param>
/// <param name="LastResourceId">For the Portion-1 leaf: the last sorted resource id already yielded.</param>
/// <param name="AfterKeyset">For a query entrypoint: the exclusive datastore keyset to resume the scan after.</param>
public sealed record LookupResourcesCursorSection(
    int EntrypointIndex,
    string? LastResourceId = null,
    RelationshipReference? AfterKeyset = null);
