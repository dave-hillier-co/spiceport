using Spiceport.Engine;

namespace Spiceport.Engine.Tests;

/// <summary>
/// Unit tests for <see cref="VisitKey"/>'s canonical wire string: round-trip stability, injectivity
/// across adjacent-field boundaries, and fail-loud parsing of a malformed string.
/// </summary>
public class VisitKeyTests
{
    [Fact]
    public void ToCanonicalString_then_FromCanonicalString_round_trips()
    {
        var key = new VisitKey("document", "doc1", "view", "user", "alice", "member");

        var canonical = key.ToCanonicalString();
        var parsed = VisitKey.FromCanonicalString(canonical);

        Assert.Equal(key, parsed);
    }

    [Fact]
    public void Canonical_strings_are_injective_across_adjacent_field_boundaries()
    {
        // ("a", "bc", ...) and ("ab", "c", ...) must not collide on the rendered string: each part is
        // length-prefixed, so no concatenation ambiguity survives.
        var first = new VisitKey("a", "bc", "rel", "user", "u", "...");
        var second = new VisitKey("ab", "c", "rel", "user", "u", "...");

        Assert.NotEqual(first.ToCanonicalString(), second.ToCanonicalString());
    }

    [Fact]
    public void Ids_containing_the_unit_separator_round_trip()
    {
        // Nothing on the request path enforces the SpiceDB object-id grammar, so an id carrying the
        // separator character reaches the dispatcher and must survive the wire rendering intact.
        var key = new VisitKey("document", "a\u001Fb", "view", "user", "alice", "...");

        var parsed = VisitKey.FromCanonicalString(key.ToCanonicalString());

        Assert.Equal(key, parsed);
    }

    [Fact]
    public void Keys_differing_only_in_separator_placement_render_distinctly()
    {
        var first = new VisitKey("document", "a\u001Fb", "c", "user", "alice", "...");
        var second = new VisitKey("document", "a", "b\u001Fc", "user", "alice", "...");

        Assert.NotEqual(first.ToCanonicalString(), second.ToCanonicalString());
    }

    [Theory]
    [InlineData("")]
    [InlineData("x")]
    [InlineData("8\u001Fdocument")]
    [InlineData("-1\u001F")]
    [InlineData("08\u001Fdocument4\u001Fdoc14\u001Fview4\u001Fuser5\u001Falice3\u001F...")]
    [InlineData("8\u001Fdocument4\u001Fdoc14\u001Fview4\u001Fuser5\u001Falice03\u001F...")]
    [InlineData("+8\u001Fdocument4\u001Fdoc14\u001Fview4\u001Fuser5\u001Falice3\u001F...")]
    [InlineData("99\u001Fdocument4\u001Fdoc14\u001Fview4\u001Fuser5\u001Falice3\u001F...")]
    [InlineData("8\u001Fdocument4\u001Fdoc14\u001Fview4\u001Fuser5\u001Falice")]
    [InlineData("8\u001Fdocument4\u001Fdoc14\u001Fview4\u001Fuser5\u001Falice9\u001F...")]
    public void FromCanonicalString_throws_FormatException_on_malformed_length_prefix(string malformed)
    {
        Assert.Throws<FormatException>(() => VisitKey.FromCanonicalString(malformed));
    }

    [Fact]
    public void FromCanonicalString_throws_FormatException_on_trailing_data()
    {
        var canonical = new VisitKey("document", "doc1", "view", "user", "alice", "member").ToCanonicalString();

        Assert.Throws<FormatException>(() => VisitKey.FromCanonicalString(canonical + "x"));
        Assert.Throws<FormatException>(() => VisitKey.FromCanonicalString(canonical + "0\u001F"));
    }
}
