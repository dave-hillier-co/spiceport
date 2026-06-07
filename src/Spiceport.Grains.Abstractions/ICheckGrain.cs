using Spiceport.Core;

namespace Spiceport.Grains.Abstractions;

/// <summary>
/// The parts of a dispatched sub-problem that are NOT encoded in the grain's string key and so must
/// travel in the call: the remaining recursion budget and the cycle-guard visited set.
/// </summary>
/// <remarks>
/// The grain identity (its string key) already pins the canonical sub-problem coordinates
/// (resource, subject, quantized revision, schema hash). Two callers asking the same sub-problem on
/// different recursion paths address the SAME grain but may carry a different depth budget / cycle
/// guard, so those cross-cutting fields ride in <see cref="DispatchCheckArgs"/> rather than the key.
/// <para>
/// The cycle guard is a bounded traversal Bloom filter (SpiceDB-style): <see cref="BloomBits"/> is the
/// fixed-width bit array (≤1KB) and <see cref="BloomK"/> the number of hash functions, so the wire/CPU
/// cost is constant regardless of recursion depth. It replaces the prior exact visited set. The guard
/// is conservative — a Bloom false-positive can only cut a branch (never grant) and cut results are
/// never cached, so it is sound.
/// </para>
/// </remarks>
/// <param name="DepthRemaining">The remaining recursion depth budget for this sub-problem.</param>
/// <param name="BloomBits">The serialized traversal-Bloom bit array (the cycle guard for this path).</param>
/// <param name="BloomK">The number of hash functions the Bloom was built with.</param>
/// <param name="Mode">
/// Whether the revision pinned in the grain key is an optimized (quantizable) bucket revision or an
/// exact revision that must be keyed exactly in the branch cache (never quantized). Defaults to
/// <see cref="RevisionMode.Optimized"/> so existing callers are unchanged.
/// </param>
[GenerateSerializer]
public sealed record DispatchCheckArgs(
    [property: Id(0)] int DepthRemaining,
    [property: Id(1)] byte[] BloomBits,
    [property: Id(2)] int BloomK,
    [property: Id(3)] RevisionMode Mode = RevisionMode.Optimized);

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

    /// <summary>
    /// Returns the <c>ToParsableString()</c> of the silo this activation is hosted on. Used to prove
    /// that consistent-hash placement put the grain on the silo the hash ring predicts; it activates
    /// the grain (an empty step) and reports where it landed.
    /// </summary>
    Task<string> GetHostSilo();
}
