using Spiceport.Core;
using Spiceport.Engine;
using Spiceport.Grains;

namespace Spiceport.Grains.Tests;

/// <summary>
/// <see cref="FrontierWire"/> round-trips the engine's <c>FoundSubject</c> tree (SubjectId, verbatim
/// caveat expression, wildcard flag, and exclusions) through the Orleans-serializable
/// <see cref="FrontierSubjectWire"/> shape unchanged, exactly as <see cref="CaveatWire"/> round-trips a
/// bare <see cref="CaveatExpression"/>.
/// </summary>
public class FrontierWireRoundTripTests
{
    [Fact]
    public void RoundTrips_a_plain_concrete_subject_with_no_caveat()
    {
        var subject = new FoundSubject("alice");

        var wire = FrontierWire.ToWire(subject);
        var back = FrontierWire.FromWire(wire);

        Assert.Equal(subject.SubjectId, back.SubjectId);
        Assert.Null(back.Caveat);
        Assert.False(back.IsWildcard);
        Assert.Null(back.ExcludedSubjects);
    }

    [Fact]
    public void RoundTrips_a_caveated_wildcard_with_caveated_exclusions_and_a_nested_expression_tree()
    {
        // A nested And/Or/Not tree gating the wildcard itself.
        var wildcardCaveat = new CaveatExpression.Or([
            new CaveatExpression.And([
                CaveatExpression.FromCaveat(new ContextualizedCaveat(
                    "over_age", new Dictionary<string, object?> { ["min_age"] = 18 })),
                new CaveatExpression.Not(
                    CaveatExpression.FromCaveat(new ContextualizedCaveat("banned"))),
            ]),
            CaveatExpression.FromCaveat(new ContextualizedCaveat(
                "is_admin", new Dictionary<string, object?> { ["level"] = 3L })),
        ]);

        var excluded = new List<FoundSubject>
        {
            new("frank", CaveatExpression.FromCaveat(new ContextualizedCaveat("blocked"))),
            new("james"), // unconditionally excluded.
        };

        var subject = new FoundSubject(
            CoreConstants.PublicWildcard, wildcardCaveat, IsWildcard: true, ExcludedSubjects: excluded);

        var wire = FrontierWire.ToWire(subject);
        var back = FrontierWire.FromWire(wire);

        Assert.Equal(CoreConstants.PublicWildcard, back.SubjectId);
        Assert.True(back.IsWildcard);
        Assert.NotNull(back.ExcludedSubjects);
        Assert.Equal(2, back.ExcludedSubjects!.Count);

        Assert.Equal("frank", back.ExcludedSubjects[0].SubjectId);
        Assert.IsType<CaveatExpression.Leaf>(back.ExcludedSubjects[0].Caveat);
        Assert.Equal("james", back.ExcludedSubjects[1].SubjectId);
        Assert.Null(back.ExcludedSubjects[1].Caveat);

        var roundTrippedOr = Assert.IsType<CaveatExpression.Or>(back.Caveat);
        Assert.Equal(2, roundTrippedOr.Children.Count);

        var and = Assert.IsType<CaveatExpression.And>(roundTrippedOr.Children[0]);
        var leaf1 = Assert.IsType<CaveatExpression.Leaf>(and.Children[0]);
        Assert.Equal("over_age", leaf1.Caveat.CaveatName);
        Assert.Equal(18, leaf1.Caveat.Context!["min_age"]);

        var not = Assert.IsType<CaveatExpression.Not>(and.Children[1]);
        var leaf2 = Assert.IsType<CaveatExpression.Leaf>(not.Child);
        Assert.Equal("banned", leaf2.Caveat.CaveatName);

        var leaf3 = Assert.IsType<CaveatExpression.Leaf>(roundTrippedOr.Children[1]);
        Assert.Equal("is_admin", leaf3.Caveat.CaveatName);
        Assert.Equal(3L, leaf3.Caveat.Context!["level"]);
    }
}
