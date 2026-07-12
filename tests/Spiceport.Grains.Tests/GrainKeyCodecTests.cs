using Spiceport.Grains;

namespace Spiceport.Grains.Tests;

/// <summary>
/// <see cref="GrainKeyCodec"/> is the shared mechanics behind <see cref="GrainKey"/>,
/// <see cref="SubjectFrontierKey"/>, and <see cref="MembershipWalkKey"/>: escape-join on <c>Join</c>,
/// strict segment-count unescape-split on <c>Split</c>.
/// </summary>
public class GrainKeyCodecTests
{
    [Fact]
    public void Join_then_Split_round_trips_segments_containing_separators_and_percent_signs()
    {
        var segments = new[] { "doc/ument", "id#with/slash%percent", "rel%ation", "rev/ision#1", "hash%with/slash" };

        var key = GrainKeyCodec.Join(segments);
        var parsed = GrainKeyCodec.Split(key, segments.Length);

        Assert.Equal(segments, parsed);
    }

    [Theory]
    [InlineData("too/few")]
    [InlineData("way/too/many/segments/than/expected")]
    public void Split_with_wrong_segment_count_throws_naming_expected_count_and_key(string malformed)
    {
        var ex = Assert.Throws<FormatException>(() => GrainKeyCodec.Split(malformed, 3));

        Assert.Contains("3", ex.Message);
        Assert.Contains(malformed, ex.Message);
    }
}
