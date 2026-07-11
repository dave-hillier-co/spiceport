namespace Spiceport.Grains.Abstractions;

/// <summary>
/// One node of a walked membership closure: the (type, id, relation) of a resource a subject was found
/// directly named on.
/// </summary>
[GenerateSerializer, Immutable]
public sealed record ResourceNodeWire(
    [property: Id(0)] string Type,
    [property: Id(1)] string Id,
    [property: Id(2)] string Relation);

/// <summary>
/// The argument to <see cref="IMembershipWalkGrain.GetContainingSet"/>: the exact ancestor path this call
/// is arriving on (canonical subject-key strings, root first) and the remaining recursion budget.
/// </summary>
/// <param name="Path">
/// The canonical subject-key strings (<c>type:id#relation</c>) of every ancestor on the call stack that led
/// to this grain, root first. Unlike <c>ICheckGrain</c>'s probabilistic traversal bloom, this is an EXACT
/// list: a false-positive skip here would silently drop a whole subtree of candidates (an incomplete
/// result), which the bounded traversal bloom's rare false positive cannot risk for a completeness-critical
/// candidate walk.
/// </param>
/// <param name="DepthRemaining">The recursion budget remaining, decremented by one per hop.</param>
[GenerateSerializer]
public sealed record MembershipWalkArgs(
    [property: Id(0)] IReadOnlyList<string> Path,
    [property: Id(1)] int DepthRemaining);

/// <summary>
/// The reply from <see cref="IMembershipWalkGrain.GetContainingSet"/>: every resource node reachable from
/// this grain's subject key (its own direct parents plus everything its children's replies contributed),
/// and whether the result is trustworthy as a COMPLETE candidate set.
/// </summary>
/// <param name="Nodes">The union of this grain's direct parents and every child's reported nodes.</param>
/// <param name="CycleCut">
/// True if a direct parent's subject-key-as-child was already on the caller's <see cref="MembershipWalkArgs.Path"/>
/// (a genuine back-edge) and was therefore skipped rather than recursed into. A path-hit cut is still
/// COMPLETE for reachability (the ancestor is already accounted for via the path that reached it first), so,
/// unlike Check's cycle-cut, this does not itself force <see cref="Incomplete"/>.
/// </param>
/// <param name="Incomplete">
/// True if <see cref="MembershipWalkArgs.DepthRemaining"/> was exhausted before the walk could recurse into
/// every parent — the reply is a PARTIAL candidate set and the caller MUST fall back to the live traversal
/// rather than trust it as complete.
/// </param>
[GenerateSerializer, Immutable]
public sealed record MembershipClosureReply(
    [property: Id(0)] IReadOnlyList<ResourceNodeWire> Nodes,
    [property: Id(1)] bool CycleCut,
    [property: Id(2)] bool Incomplete);

/// <summary>
/// A grain keyed by "the addressable membership-walk closure rooted at subject key
/// <c>subjType:subjId#subjRelation</c> at <c>(revision, schemaHash)</c>" — the sharded, addressable
/// replacement for the retired per-silo <c>MembershipIndexCache</c>/<c>MembershipIndex</c> replica. The
/// grain's STRING KEY is, in order: <c>subjType/subjId/subjRelation/revision/schemaHash</c> (see
/// <see cref="Spiceport.Grains.MembershipWalkKey"/>).
/// </summary>
/// <remarks>
/// Recursion crosses grain boundaries exactly like <see cref="ICheckGrain"/>: computing one subject's
/// containing set dispatches to the SIBLING <see cref="IMembershipWalkGrain"/> keyed by each direct parent
/// (same revision/schemaHash), so the walk is genuinely sharded rather than replicated. Because a walk runs
/// over a reader pinned to the key's exact revision, it is revision-exact by construction — there is no
/// fold/catch-up machinery to keep it correct as the log advances, unlike the retired cache.
/// </remarks>
public interface IMembershipWalkGrain : IGrainWithStringKey
{
    /// <summary>
    /// Returns the containing set reachable from this grain's subject key: its own direct parents plus,
    /// for each parent not already on <paramref name="args"/>'s path, the recursively-walked sibling
    /// grain's contribution. See <see cref="MembershipClosureReply"/> for the completeness contract.
    /// </summary>
    Task<MembershipClosureReply> GetContainingSet(MembershipWalkArgs args, GrainCancellationToken cancellationToken);
}
