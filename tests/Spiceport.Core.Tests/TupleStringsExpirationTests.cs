namespace Spiceport.Core.Tests;

/// <summary>
/// The tuple-string expiration suffix accepts RFC3339 only, as upstream SpiceDB's parser does
/// (<c>time.Parse(time.RFC3339, ...)</c>), so a tuple string means the same relationship
/// whenever it is parsed.
/// </summary>
public class TupleStringsExpirationTests
{
    private const string Prefix = "doc:1#viewer@user:alice";

    [Theory]
    [InlineData("01:02:03")]
    [InlineData("2025-1-2")]
    [InlineData("2025.01.02")]
    [InlineData("2025-01-02")]
    [InlineData("2025-01-02T03:04:05")]
    [InlineData("2025-01-02T03:04Z")]
    public void Non_rfc3339_expiration_is_rejected(string expiration)
    {
        var tuple = $"{Prefix}[expiration:{expiration}]";

        Assert.False(TupleStrings.TryParseRelationship(tuple, out _));
        Assert.Throws<FormatException>(() => TupleStrings.ParseRelationship(tuple));
    }

    [Theory]
    [InlineData("2025-01-02T03:04:05Z", 0L)]
    [InlineData("2025-01-02T03:04:05.123Z", 1_230_000L)]
    [InlineData("2025-01-02T03:04:05.123456789Z", 1_234_567L)]
    public void Rfc3339_utc_expiration_is_accepted(string expiration, long extraTicks)
    {
        var rel = TupleStrings.ParseRelationship($"{Prefix}[expiration:{expiration}]");

        var expected = new DateTimeOffset(2025, 1, 2, 3, 4, 5, TimeSpan.Zero).AddTicks(extraTicks);
        Assert.Equal(expected, rel.OptionalExpiration);
        Assert.Equal(TimeSpan.Zero, rel.OptionalExpiration!.Value.Offset);
    }

    [Fact]
    public void Rfc3339_offset_expiration_is_converted_to_utc()
    {
        var rel = TupleStrings.ParseRelationship($"{Prefix}[expiration:2025-01-02T03:04:05-01:00]");

        Assert.Equal(new DateTimeOffset(2025, 1, 2, 4, 4, 5, TimeSpan.Zero), rel.OptionalExpiration);
        Assert.Equal(TimeSpan.Zero, rel.OptionalExpiration!.Value.Offset);
    }

    [Fact]
    public void Formatted_expiration_round_trips()
    {
        var original = Relationship.Create(
            new ObjectAndRelation("doc", "1", "viewer"),
            new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis),
            expiration: new DateTimeOffset(2025, 1, 2, 3, 4, 5, TimeSpan.Zero).AddTicks(1_234_567));

        var formatted = TupleStrings.FormatRelationship(original);
        var reparsed = TupleStrings.ParseRelationship(formatted);

        Assert.Equal(original, reparsed);
    }
}
