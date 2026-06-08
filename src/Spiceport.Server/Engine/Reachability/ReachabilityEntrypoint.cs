namespace Spiceport.Engine;

/// <summary>The kind of structural edge an entrypoint represents. Port of SpiceDB's entrypoint kinds.</summary>
public enum ReachabilityEntrypointKind
{
    /// <summary>A directly-written base relation (RELATION_ENTRYPOINT).</summary>
    Relation,

    /// <summary>A computed userset rewrite (COMPUTED_USERSET_ENTRYPOINT).</summary>
    ComputedUserset,

    /// <summary>A tuple-to-userset arrow (TUPLESET_TO_USERSET_ENTRYPOINT).</summary>
    TupleToUserset,

    /// <summary>The resource itself treated as a subject (SELF_ENTRYPOINT).</summary>
    Self,
}

/// <summary>
/// Whether an entrypoint produces a direct result or one that must be re-validated.
/// Port of <c>DIRECT_OPERATION_RESULT</c> vs <c>REACHABLE_CONDITIONAL_RESULT</c>.
/// </summary>
public enum EntrypointResultStatus
{
    /// <summary>Under a pure union: a reached resource is genuinely a member along this edge.</summary>
    DirectResult,

    /// <summary>Under an intersection/exclusion/<c>.all()</c>: reachability must be confirmed via Check.</summary>
    ConditionalResult,
}

/// <summary>
/// A productive edge in the reachability graph: a way a subject of some type can structurally reach
/// a containing relation. Port of SpiceDB's <c>core.ReachabilityEntrypoint</c> paired with its
/// <c>parentRelation</c>.
/// </summary>
/// <param name="Kind">The kind of structural edge.</param>
/// <param name="TargetRelation">
/// The relation this entrypoint lives on (the containing relation the edge reaches), i.e. SpiceDB's
/// <c>parentRelation</c>.
/// </param>
/// <param name="ContainingRelation">
/// The containing relation; equal to <paramref name="TargetRelation"/> in this two-level model.
/// </param>
/// <param name="ComputedUsersetRelation">For ComputedUserset and TupleToUserset edges, the computed relation.</param>
/// <param name="TuplesetRelation">For TupleToUserset edges, the tupleset relation that is traversed.</param>
/// <param name="ResultStatus">Whether this is a direct or conditional result.</param>
public sealed record ReachabilityEntrypoint(
    ReachabilityEntrypointKind Kind,
    RelationReference TargetRelation,
    RelationReference ContainingRelation,
    string? ComputedUsersetRelation = null,
    string? TuplesetRelation = null,
    EntrypointResultStatus ResultStatus = EntrypointResultStatus.ConditionalResult)
{
    /// <summary>
    /// True when a resource reached via this entrypoint is genuinely a member (the edge is under a
    /// pure union), so no confirming Check is required.
    /// </summary>
    public bool IsDirectResult => ResultStatus == EntrypointResultStatus.DirectResult;
}
