using Spiceport.Api;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Go's <c>base64.StdEncoding.DecodeString</c> failure offsets (<c>CorruptInputError</c>), each observed from
/// Go's own decoder, which SpiceDB quotes when a ZedToken is not base64.
/// </summary>
public class GoBase64Tests
{
    [Theory]
    [InlineData("garbage", 4)]
    [InlineData("Zm9v YmFy", 4)]
    [InlineData("Z===", 1)]
    [InlineData("Zm=x", 2)]
    [InlineData("Zm=", 3)]
    [InlineData("Zm9v====", 4)]
    [InlineData("Zm9vYg", 4)]
    [InlineData("é", 0)]
    [InlineData("Zm9vYg=", 7)]
    [InlineData("Zm9vYg==x", 8)]
    [InlineData("Zm9vYmFyZm9vYmFy!", 16)]
    public void Reports_the_offset_Go_reports(string input, int offset) =>
        Assert.Equal(offset, GoBase64.CorruptInputOffset(input));

    [Theory]
    [InlineData("")]
    [InlineData("Zm9vYmFy")]
    [InlineData("Zm9v\nYmFy")]
    [InlineData("Zm9vYg==")]
    [InlineData("Zm9vYmE=\n")]
    [InlineData("Zm9\r\nv")]
    [InlineData("Zm9vYm\n=\n=")]
    public void Accepts_what_Go_accepts(string input) => Assert.Null(GoBase64.CorruptInputOffset(input));
}
