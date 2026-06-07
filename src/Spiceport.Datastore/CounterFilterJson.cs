using System.Text.Json;
using System.Text.Json.Serialization;

namespace Spiceport.Datastore;

/// <summary>
/// A flat, JSON-serializable mirror of the proto-shaped subset of <see cref="RelationshipsFilter"/> that a
/// registered counter carries (resource type/id/prefix/relation plus an optional single subject filter).
/// This is exactly the shape SpiceDB persists for a counter (its <c>core.RelationshipFilter</c> bytes), so
/// it round-trips the registered filter without depending on the proto assembly. Residual filters
/// (caveat/expiration/multi-selector) are not part of a registered counter and are intentionally omitted.
/// </summary>
public sealed class CounterFilterJson
{
    /// <summary>Resource type constraint.</summary>
    [JsonPropertyName("rt")] public string? ResourceType { get; set; }

    /// <summary>Resource id constraint.</summary>
    [JsonPropertyName("rid")] public string? ResourceId { get; set; }

    /// <summary>Resource id prefix constraint.</summary>
    [JsonPropertyName("rpfx")] public string? ResourceIdPrefix { get; set; }

    /// <summary>Resource relation constraint.</summary>
    [JsonPropertyName("rrel")] public string? ResourceRelation { get; set; }

    /// <summary>Subject type constraint (single subject filter).</summary>
    [JsonPropertyName("st")] public string? SubjectType { get; set; }

    /// <summary>Subject id constraint.</summary>
    [JsonPropertyName("sid")] public string? SubjectId { get; set; }

    /// <summary>Subject relation constraint.</summary>
    [JsonPropertyName("srel")] public string? SubjectRelation { get; set; }

    private static readonly JsonSerializerOptions Options = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    /// <summary>Serializes a <see cref="RelationshipsFilter"/> to its persisted UTF-8 JSON bytes.</summary>
    public static byte[] Serialize(RelationshipsFilter filter)
    {
        var poco = new CounterFilterJson
        {
            ResourceType = filter.OptionalResourceType,
            ResourceIdPrefix = filter.OptionalResourceIdPrefix,
            ResourceRelation = filter.OptionalResourceRelation,
        };

        if (filter.OptionalResourceIds is { Count: > 0 } ids)
            poco.ResourceId = ids[0];

        if (filter.OptionalSubjectsSelectors is { Count: > 0 } selectors)
        {
            var s = selectors[0];
            poco.SubjectType = s.OptionalSubjectType;
            if (s.OptionalSubjectIds is { Count: > 0 } sids)
                poco.SubjectId = sids[0];
            poco.SubjectRelation = s.RelationFilter?.NonEllipsisRelation;
        }

        return JsonSerializer.SerializeToUtf8Bytes(poco, Options);
    }

    /// <summary>Deserializes persisted UTF-8 JSON bytes back into a <see cref="RelationshipsFilter"/>.</summary>
    public static RelationshipsFilter Deserialize(byte[] bytes)
    {
        var poco = JsonSerializer.Deserialize<CounterFilterJson>(bytes, Options)
            ?? new CounterFilterJson();

        IReadOnlyList<SubjectsSelector>? selectors = null;
        if (poco.SubjectType is not null || poco.SubjectId is not null || poco.SubjectRelation is not null)
        {
            selectors =
            [
                new SubjectsSelector(
                    OptionalSubjectType: poco.SubjectType,
                    OptionalSubjectIds: poco.SubjectId is { } sid ? [sid] : null,
                    RelationFilter: poco.SubjectRelation is { } srel
                        ? new SubjectRelationFilter(NonEllipsisRelation: srel)
                        : null),
            ];
        }

        return new RelationshipsFilter
        {
            OptionalResourceType = poco.ResourceType,
            OptionalResourceIds = poco.ResourceId is { } rid ? [rid] : null,
            OptionalResourceIdPrefix = poco.ResourceIdPrefix,
            OptionalResourceRelation = poco.ResourceRelation,
            OptionalSubjectsSelectors = selectors,
        };
    }
}
