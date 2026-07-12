namespace Spiceport.Grains;

/// <summary>
/// Shared mechanics for the grain-key codecs (<see cref="GrainKey"/>, <see cref="SubjectFrontierKey"/>,
/// <see cref="MembershipWalkKey"/>): URL-style per-segment escaping joined on <c>/</c>, and a strict
/// segment-count parse. Each grain-key type keeps its own <c>Build</c>/<c>Parse</c> signature and its own
/// parts record — this only factors out the identical escape/join/split/unescape mechanics they all
/// hand-rolled the same way.
/// </summary>
internal static class GrainKeyCodec
{
    private const char Separator = '/';

    public static string Join(params string[] segments) =>
        string.Join(Separator, Array.ConvertAll(segments, Uri.EscapeDataString));

    public static string[] Split(string key, int expectedCount)
    {
        var parts = key.Split(Separator);
        if (parts.Length != expectedCount)
        {
            throw new FormatException(
                $"Malformed grain key (expected {expectedCount} segments): '{key}'.");
        }

        return Array.ConvertAll(parts, Uri.UnescapeDataString);
    }
}
