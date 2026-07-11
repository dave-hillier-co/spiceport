using Spiceport.Core;

namespace Spiceport.Grains.Abstractions;

/// <summary>
/// The serializable reply from a dispatched sub-problem: the engine's pre-context branch
/// (tri-state membership plus an optional gating caveat) augmented with the cycle-cut flag.
/// </summary>
/// <remarks>
/// This is the PRE-CONTEXT branch, never the collapsed verdict: the caveat is returned as its stable
/// serialized form so the caller can collapse it against request-time context. Mirrors the engine's
/// <c>DispatchCheckResult</c>.
/// </remarks>
/// <param name="Member">True if the subject is a (possibly caveated) member.</param>
/// <param name="Caveat">
/// The serialized gating caveat expression, or null for unconditional membership / non-membership.
/// </param>
/// <param name="CycleCut">
/// True if this subtree was depth- or loop-affected and must not be cached. There is no visited-set
/// verdict cut: this flag is force-set on the RETURNED reply by the Orleans dispatcher when the
/// exact visited set reports a genuine repeat on this path, purely so the result is excluded from
/// the grain's activation memo, not because the verdict itself was altered.
/// </param>
/// <param name="DepthRequired">
/// The recursion depth this sub-problem actually consumed below itself (leaf = 1). Travels back across
/// the grain boundary so the silo-wide caching dispatcher can gate reuse on
/// <c>DepthRemaining &gt;= DepthRequired</c> — mirroring SpiceDB's <c>ResponseMeta.DepthRequired</c>.
/// </param>
[GenerateSerializer, Immutable]
public sealed record DispatchCheckReply(
    [property: Id(0)] bool Member,
    [property: Id(1)] SerializedCaveat? Caveat,
    [property: Id(2)] bool CycleCut,
    [property: Id(3)] int DepthRequired = 1);

/// <summary>
/// A grain keyed by the canonical sub-problem identity. The grain's STRING KEY is, in order:
/// <c>resourceType/resourceId/relation/subjectType/subjectId/subjectRelation/quantizedRevision/schemaHash</c>
/// — so the grain identity itself is the cache key for the sub-problem.
/// </summary>
/// <remarks>
/// Recursion crosses grain boundaries: computing one sub-problem dispatches its children back through
/// the Orleans dispatcher, which addresses a different grain per child key. The cross-cutting depth
/// budget and exact visited-set cycle guard are NOT part of that identity — they ride ambiently in the
/// Orleans <see cref="Orleans.Runtime.RequestContext"/> via
/// <see cref="Spiceport.Grains.Abstractions.DispatchContext"/> rather than as a method argument, so this
/// method's wire contract is exactly the canonical sub-problem (the grain key) plus the cancellation
/// token. See <see cref="Spiceport.Grains.Abstractions.DispatchContext"/> for the scoping guarantee this
/// relies on.
/// </remarks>
public interface ICheckGrain : IGrainWithStringKey
{
    /// <summary>
    /// Evaluates the one sub-problem this grain is keyed to, dispatching children onward. The depth
    /// budget and exact visited-set cycle guard are read from the ambient
    /// <see cref="Spiceport.Grains.Abstractions.DispatchContext"/>, which the caller must have set before
    /// making this call. The Orleans cancellation token propagates caller cancellation across the grain
    /// boundary and through every recursive child dispatch.
    /// </summary>
    Task<DispatchCheckReply> DispatchCheck(GrainCancellationToken cancellationToken);
}
