namespace Spiceport.Grains.Abstractions;

/// <summary>
/// A grain keyed by "the pre-context subject frontier of resource#relation for subjectType(#subjectRelation)
/// at (quantizedRevision, schemaHash)" — the whole result of a <c>LookupSubjectsEngine.LookupSubjects</c>
/// walk from that root, memoized per activation exactly as <see cref="ICheckGrain"/> memoizes a Check
/// sub-problem's pre-context branch (stage (a) of "Activation-as-cache", <c>docs/future-work.md</c> item
/// 1.3). The grain's STRING KEY is, in order:
/// <c>resourceType/resourceId/relation/subjectType/subjectRelation/quantizedRevision/schemaHash</c>.
/// </summary>
/// <remarks>
/// Unlike <see cref="ICheckGrain"/> there is NO dispatcher seam here: the engine walk it wraps
/// (<c>LookupSubjectsEngine</c>) is consumed unchanged and computes the whole frontier in-process behind
/// a single grain call, not a recursive dispatch tree. See <see cref="Spiceport.Grains.SubjectFrontierGrain"/>
/// for the memo contract this grain implements.
/// </remarks>
public interface ISubjectFrontierGrain : IGrainWithStringKey
{
    /// <summary>Returns the pre-context frontier this grain is keyed to, computing it on a cold activation.</summary>
    Task<SubjectFrontierReply> GetFrontier(GrainCancellationToken cancellationToken);
}
