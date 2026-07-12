using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// Maps between the engine's in-process <c>FoundSubject</c> tree and the Orleans-serializable
/// <see cref="FrontierSubjectWire"/> wire form carried across the <see cref="ISubjectFrontierGrain"/>
/// boundary, delegating caveat (de)serialization to <see cref="CaveatWire"/>. Both the live engine-walk
/// path and the memoized path in <see cref="ReverseOps"/> consume the SAME
/// <c>FoundSubject</c> shape (the memoized path reconstructs it via <see cref="FromWire"/>), so the
/// caveat-collapse / cursor-skip post-processing loop is written once and shared by both.
/// </summary>
internal static class FrontierWire
{
    public static FrontierSubjectWire ToWire(FoundSubject subject) => new(
        subject.SubjectId,
        CaveatWire.ToWire(subject.Caveat),
        subject.IsWildcard,
        subject.ExcludedSubjects?.Select(ToWire).ToList());

    public static FoundSubject FromWire(FrontierSubjectWire wire) => new(
        wire.SubjectId,
        CaveatWire.FromWire(wire.Caveat),
        wire.IsWildcard,
        wire.ExcludedSubjects?.Select(FromWire).ToList());
}
