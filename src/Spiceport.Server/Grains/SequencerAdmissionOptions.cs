namespace Spiceport.Grains;

/// <summary>
/// Configuration for the per-silo sequencer write admission gate (<see cref="SequencerAdmission"/>).
/// Registered by <c>AddSpiceportGrainServices</c> with these defaults; a host opts out or retunes by
/// registering an override (the same last-registration-wins pattern as
/// <see cref="MembershipWalkOptions"/> / <see cref="ActivationMemoOptions"/>).
/// </summary>
public sealed class SequencerAdmissionOptions
{
    /// <summary>
    /// The maximum number of commits this silo may have in flight to the cluster-singleton sequencer
    /// grain at once; a commit arriving beyond it is shed with
    /// <c>SequencerOverloadedException</c> (gRPC <c>RESOURCE_EXHAUSTED</c>) instead of queueing without
    /// bound on the sequencer's single non-reentrant activation. The bound is the silo's worst-case
    /// sequencer-queue contribution, so its product with the per-commit turn time (times the silo
    /// count) must stay well under the Orleans response timeout — the failure mode this gate exists to
    /// prevent (issue #36). Zero or negative disables the gate (unbounded, the pre-gate behavior).
    /// </summary>
    public int MaxInFlightCommits { get; init; } = 128;
}
