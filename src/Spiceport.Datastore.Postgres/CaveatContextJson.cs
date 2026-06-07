using System.Text.Json;

namespace Spiceport.Datastore.Postgres;

/// <summary>
/// Serializes a caveat context map (<c>IReadOnlyDictionary&lt;string, object?&gt;</c>) to/from the
/// jsonb column. Values are scalars, lists, or nested maps. On read, JSON values are materialized into
/// CLR primitives (string, bool, long/double, list, dictionary) so the round-tripped relationship is
/// value-equal to what was written for the conformance corpus.
/// </summary>
internal static class CaveatContextJson
{
    public static string Serialize(IReadOnlyDictionary<string, object?> context) =>
        JsonSerializer.Serialize(context);

    public static IReadOnlyDictionary<string, object?>? Deserialize(string? json)
    {
        if (string.IsNullOrEmpty(json))
            return null;

        using var doc = JsonDocument.Parse(json);
        if (doc.RootElement.ValueKind != JsonValueKind.Object)
            return null;

        var result = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var prop in doc.RootElement.EnumerateObject())
            result[prop.Name] = Convert(prop.Value);
        return result;
    }

    private static object? Convert(JsonElement el) => el.ValueKind switch
    {
        JsonValueKind.String => el.GetString(),
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.Null => null,
        JsonValueKind.Number => el.TryGetInt64(out var l) ? l : el.GetDouble(),
        JsonValueKind.Array => el.EnumerateArray().Select(Convert).ToList(),
        JsonValueKind.Object => el.EnumerateObject()
            .ToDictionary(p => p.Name, p => Convert(p.Value), StringComparer.Ordinal),
        _ => null,
    };
}
