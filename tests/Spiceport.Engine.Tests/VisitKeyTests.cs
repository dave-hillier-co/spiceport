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
        // ("a", "bc", ...) and ("ab", "c", ...) must not collide on the joined string: the separator
        // (U+001F) cannot appear in an ONR field, so no naive concatenation ambiguity survives.
        var first = new VisitKey("a", "bc", "rel", "user", "u", "...");
        var second = new VisitKey("ab", "c", "rel", "user", "u", "...");

        Assert.NotEqual(first.ToCanonicalString(), second.ToCanonicalString());
    }

    [Fact]
    public void FromCanonicalString_throws_FormatException_on_wrong_part_count()
    {
        var sep = (char)0x1F;
        var tooFew = string.Join(sep, "document", "doc1", "view", "user", "alice");

        Assert.Throws<FormatException>(() => VisitKey.FromCanonicalString(tooFew));
    }

    [Fact]
    public void FromCanonicalString_throws_FormatException_on_too_many_parts()
    {
        var sep = (char)0x1F;
        var tooMany = string.Join(sep, "document", "doc1", "view", "user", "alice", "member", "extra");

        Assert.Throws<FormatException>(() => VisitKey.FromCanonicalString(tooMany));
    }
}
