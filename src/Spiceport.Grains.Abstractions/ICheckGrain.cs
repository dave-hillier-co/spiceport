namespace Spiceport.Grains.Abstractions;

/// <summary>
/// The parts of a dispatched sub-problem that are NOT encoded in the grain's string key and so must
/// travel in the call: the remaining recursion budget and the cycle-guard visited set.
/// </summary>
/// <remarks>
/// The grain identity (its string key) already pins the canonical sub-problem coordinates
/// (resource, subject, quantized revision, schema hash). Two callers asking the same sub-problem on
/// different recursion paths address the SAME grain but may carry a different depth budget / visited
/// set, so those cross-cutting fields ride in <see cref="DispatchCheckArgs"/> rather than the key.
/// <para>
/// <see cref="Visited"/> is sent as an explicit serializable set for correctness in this slice.
/// FUTURE OPTIMIZATION: SpiceDB carries a bounded-size traversal bloom filter instead of an exact
/// set, trading a small false-positive rate for a fixed wire/CPU cost. That is the intended
/// replacement once the mesh is load-bearing.
/// </para>
/// </remarks>
/// <param name="DepthRemaining">The remaining recursion depth budget for this sub-problem.</param>
/// <param name="Visited">
/// The set of (resource, subject) visit keys currently in-flight on this path.
/// </param>
[GenerateSerializer]
public sealed record DispatchCheckArgs(
    [property: Id(0)] int DepthRemaining,
    [property: Id(1)] IReadOnlySet<VisitKeyParts> Visited);

/// <summary>
/// The six coordinates of a cycle-guard visit key (resource type/id/relation and subject
/// type/id/relation), made serializable so the in-flight visited set can cross the grain boundary.
/// </summary>
[GenerateSerializer]
public sealed record VisitKeyParts(
    [property: Id(0)] string ResourceType,
    [property: Id(1)] string ResourceId,
    [property: Id(2)] string ResourceRelation,
    [property: Id(3)] string SubjectType,
    [property: Id(4)] string SubjectId,
    [property: Id(5)] string SubjectRelation);

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
/// <param name="CycleCut">True if a visited-set cutoff was hit anywhere in this subtree.</param>
[GenerateSerializer]
public sealed record DispatchCheckReply(
    [property: Id(0)] bool Member,
    [property: Id(1)] SerializedCaveat? Caveat,
    [property: Id(2)] bool CycleCut);

/// <summary>
/// A grain keyed by the canonical sub-problem identity. The grain's STRING KEY is, in order:
/// <c>resourceType/resourceId/relation/subjectType/subjectId/subjectRelation/quantizedRevision/schemaHash</c>
/// — so the grain identity itself is the cache key for the sub-problem.
/// </summary>
/// <remarks>
/// Recursion crosses grain boundaries: computing one sub-problem dispatches its children back through
/// the Orleans dispatcher, which addresses a different grain per child key. The cross-cutting depth /
/// visited fields that are not part of the identity travel in <see cref="DispatchCheckArgs"/>.
/// </remarks>
public interface ICheckGrain : IGrainWithStringKey
{
    /// <summary>Evaluates the one sub-problem this grain is keyed to, dispatching children onward.</summary>
    Task<DispatchCheckReply> DispatchCheck(DispatchCheckArgs args);
}
