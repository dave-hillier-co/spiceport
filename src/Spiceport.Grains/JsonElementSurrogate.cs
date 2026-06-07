using System.Text.Json;
using Orleans.Serialization.Cloning;

namespace Spiceport.Grains;

/// <summary>
/// Orleans serialization surrogate for <see cref="JsonElement"/>.
/// </summary>
/// <remarks>
/// Relationship caveat context is parsed with <c>JsonSerializer.Deserialize&lt;Dictionary&lt;string, object?&gt;&gt;</c>,
/// so its values are boxed <see cref="JsonElement"/> instances. Those values ride along inside the
/// pre-context caveat leaf carried across the grain boundary in <c>DispatchCheckReply</c>, and Orleans
/// has no built-in copier/codec for <see cref="JsonElement"/>. This surrogate captures the element's
/// raw JSON text on the wire and re-parses it on the way back, giving Orleans a stable round-trip.
/// </remarks>
[GenerateSerializer]
public struct JsonElementSurrogate
{
    /// <summary>The raw JSON text of the captured element.</summary>
    [Id(0)]
    public string RawJson;
}

/// <summary>Converts <see cref="JsonElement"/> to and from its serializable surrogate.</summary>
[RegisterConverter]
public sealed class JsonElementSurrogateConverter
    : IConverter<JsonElement, JsonElementSurrogate>
{
    /// <inheritdoc />
    public JsonElement ConvertFromSurrogate(in JsonElementSurrogate surrogate)
    {
        using var doc = JsonDocument.Parse(surrogate.RawJson);
        return doc.RootElement.Clone();
    }

    /// <inheritdoc />
    public JsonElementSurrogate ConvertToSurrogate(in JsonElement value) =>
        new() { RawJson = value.GetRawText() };
}

/// <summary>
/// Deep-copier for <see cref="JsonElement"/>. A <see cref="JsonElement"/> backed by a live
/// <see cref="JsonDocument"/> is only a view; <see cref="JsonElement.Clone"/> detaches it so it remains
/// valid after the source document is disposed.
/// </summary>
[RegisterCopier]
public sealed class JsonElementCopier : IDeepCopier<JsonElement>
{
    /// <inheritdoc />
    public JsonElement DeepCopy(JsonElement input, CopyContext context) => input.Clone();
}
