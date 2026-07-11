using Spiceport.Core;
using Spiceport.Grains;

namespace Spiceport.Grains.Tests;

/// <summary>
/// <see cref="SubjectFrontierKey"/> build/parse round-trip, mirroring <c>GrainKeyTests</c>' coverage of
/// <see cref="GrainKey"/>: escaping of separator/reserved characters and a strict segment-count parse.
/// </summary>
public class SubjectFrontierKeyTests
{
    private static ObjectAndRelation Resource(string type, string id, string relation) =>
        new(type, id, relation);

    [Fact]
    public void Build_then_Parse_round_trips_every_component()
    {
        var resource = Resource("document", "readme", "view");
        var key = SubjectFrontierKey.Build(resource, "user", CoreConstants.Ellipsis, "12345", "hash-abc");

        var parts = SubjectFrontierKey.Parse(key);

        Assert.Equal(resource, parts.Resource);
        Assert.Equal("user", parts.SubjectType);
        Assert.Equal(CoreConstants.Ellipsis, parts.SubjectRelation);
        Assert.Equal("12345", parts.Revision);
        Assert.Equal("hash-abc", parts.SchemaHash);
    }

    [Fact]
    public void Ids_containing_separator_hash_and_percent_round_trip_unmangled()
    {
        var resource = Resource("doc/ument", "id#with/slash%percent", "vi/ew");
        var key = SubjectFrontierKey.Build(
            resource, "us/er#type", "rel%ation", "rev/ision#1", "hash%with/slash");

        var parts = SubjectFrontierKey.Parse(key);

        Assert.Equal(resource, parts.Resource);
        Assert.Equal("us/er#type", parts.SubjectType);
        Assert.Equal("rel%ation", parts.SubjectRelation);
        Assert.Equal("rev/ision#1", parts.Revision);
        Assert.Equal("hash%with/slash", parts.SchemaHash);
    }

    [Theory]
    [InlineData("too/few/segments")]
    [InlineData("way/too/many/segments/than/the/seven/expected/here")]
    public void Wrong_segment_count_throws(string malformed)
    {
        Assert.Throws<FormatException>(() => SubjectFrontierKey.Parse(malformed));
    }
}
