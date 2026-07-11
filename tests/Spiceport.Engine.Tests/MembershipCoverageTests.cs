using Spiceport.Schema;

namespace Spiceport.Engine.Tests;

/// <summary>
/// Coverage-analysis gates for <see cref="MembershipCoverage"/>: which <c>(resourceType, nameOrPermission)</c>
/// targets flatten to stored base-relation edges, ported from the retired <c>MembershipIndex</c>'s inline
/// coverage assertions (<c>Stage4MembershipIndexTests</c>, now <c>Stage4MembershipWalkEquivalenceTests</c>).
/// Pure schema analysis — no datastore, no revision.
/// </summary>
public class MembershipCoverageTests
{
    private const string NestedSchema = """
        definition user {}

        definition group {
            relation member: user | group#member
        }

        definition document {
            relation viewer: user | group#member
            relation editor: user
            relation banned: user

            permission view = viewer + editor
            permission allowed_view = viewer - banned
            permission edit = editor & viewer
            permission via_arrow = viewer + parent_view
            permission parent_view = editor
            permission truly_arrowed = viewer->member
        }
        """;

    private static MembershipCoverage Build(string schemaText) =>
        MembershipCoverage.Build(SchemaCompiler.CompileSchema(schemaText).Namespaces);

    [Fact]
    public void BaseRelation_IsCovered_YieldsItself()
    {
        var coverage = Build(NestedSchema);
        Assert.True(coverage.TryGetYields("group", "member", out var yields));
        Assert.Contains("member", yields);
        Assert.Contains(("group", "member"), coverage.ScanSet);
    }

    [Fact]
    public void UnionPermission_IsCovered_YieldsAllOperands()
    {
        var coverage = Build(NestedSchema);
        Assert.True(coverage.TryGetYields("document", "view", out var yields));
        Assert.Contains("viewer", yields);
        Assert.Contains("editor", yields);
    }

    [Fact]
    public void ExclusionPermission_IsCovered_YieldsOnlyFirstOperand()
    {
        var coverage = Build(NestedSchema);
        Assert.True(coverage.TryGetYields("document", "allowed_view", out var yields));
        Assert.Contains("viewer", yields);
        Assert.DoesNotContain("banned", yields); // the negative operand never seeds candidates
    }

    [Fact]
    public void IntersectionPermission_IsCovered_YieldsOnlyFirstOperand()
    {
        var coverage = Build(NestedSchema);
        Assert.True(coverage.TryGetYields("document", "edit", out var yields));
        Assert.Contains("editor", yields); // `editor & viewer` — first operand only
        Assert.DoesNotContain("viewer", yields);
    }

    [Fact]
    public void UnionOfComputedUsersets_WithNoArrow_IsCovered()
    {
        var coverage = Build(NestedSchema);
        // `via_arrow = viewer + parent_view`; `parent_view = editor` has no arrow, so the whole union covers.
        Assert.True(coverage.TryGetYields("document", "via_arrow", out var yields));
        Assert.Contains("viewer", yields);
        Assert.Contains("editor", yields);
    }

    [Fact]
    public void TupleToUsersetArrow_AbortsCoverage()
    {
        var coverage = Build(NestedSchema);
        Assert.False(coverage.TryGetYields("document", "truly_arrowed", out _));
    }

    [Fact]
    public void UnknownRelation_IsNotCovered()
    {
        var coverage = Build(NestedSchema);
        Assert.False(coverage.TryGetYields("document", "no_such_relation", out _));
        Assert.False(coverage.TryGetYields("no_such_type", "view", out _));
    }

    [Fact]
    public void WildcardSubjectSchema_BaseRelationIsCovered()
    {
        const string wildcardSchema = """
            definition user {}

            definition group {
                relation member: user:* | user | group#member
            }
            """;
        var coverage = Build(wildcardSchema);
        Assert.True(coverage.TryGetYields("group", "member", out var yields));
        Assert.Contains("member", yields);
    }

    [Fact]
    public void EmptySchema_HasEmptyCoverage()
    {
        var coverage = Build("definition user {}");
        Assert.True(coverage.IsEmpty);
    }
}
