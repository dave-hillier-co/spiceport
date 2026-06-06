using System.Collections.Immutable;
using System.Text.Json;
using Spiceport.Core;
using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace Spiceport.Conformance.Tests;

/// <summary>
/// Loads and parses SpiceDB validation/consistency YAML files into a typed
/// <see cref="ValidationFile"/>. Handles the schema block, the newline-separated
/// relationships block, and the assertTrue/assertFalse assertion blocks.
/// </summary>
public static class ValidationFileLoader
{
    private static readonly IDeserializer Deserializer = new DeserializerBuilder()
        .WithNamingConvention(CamelCaseNamingConvention.Instance)
        .IgnoreUnmatchedProperties()
        .Build();

    /// <summary>Loads and parses a validation file from disk.</summary>
    public static ValidationFile LoadFromFile(string path)
    {
        var yaml = File.ReadAllText(path);
        return Parse(yaml);
    }

    /// <summary>Parses validation file YAML content.</summary>
    public static ValidationFile Parse(string yaml)
    {
        var raw = Deserializer.Deserialize<RawValidationFile>(yaml)
                  ?? throw new FormatException("Validation file is empty.");

        var schemaText = (raw.Schema ?? string.Empty).TrimEnd();

        var relationships = ParseRelationships(raw.Relationships ?? string.Empty);
        var assertions = ParseAssertions(raw.Assertions);

        return new ValidationFile(schemaText, relationships, assertions);
    }

    private static ImmutableList<Relationship> ParseRelationships(string block)
    {
        var builder = ImmutableList.CreateBuilder<Relationship>();
        foreach (var rawLine in block.Split('\n'))
        {
            var line = rawLine.Trim();
            if (line.Length == 0 || line.StartsWith("//", StringComparison.Ordinal))
            {
                continue;
            }

            builder.Add(TupleStrings.ParseRelationship(line));
        }

        return builder.ToImmutable();
    }

    private static ImmutableList<ParsedAssertion> ParseAssertions(RawAssertions? raw)
    {
        var builder = ImmutableList.CreateBuilder<ParsedAssertion>();
        if (raw is null)
        {
            return builder.ToImmutable();
        }

        foreach (var s in raw.AssertTrue ?? [])
        {
            builder.Add(ParseAssertion(s, AssertionExpectation.True));
        }

        foreach (var s in raw.AssertCaveated ?? [])
        {
            builder.Add(ParseAssertion(s, AssertionExpectation.Caveated));
        }

        foreach (var s in raw.AssertFalse ?? [])
        {
            builder.Add(ParseAssertion(s, AssertionExpectation.False));
        }

        return builder.ToImmutable();
    }

    private static ParsedAssertion ParseAssertion(string source, AssertionExpectation expectation)
    {
        var (tupleString, context) = SplitWithContext(source);

        // An assertion's tuple part is a full relationship string (resource@subject),
        // so reuse the relationship parser: it handles #... ellipsis, wildcards and
        // ellipsis normalisation for the subject relation.
        var relationship = TupleStrings.ParseRelationship(tupleString);

        // If the assertion did not carry a `with {json}` context, fall back to any
        // caveat context embedded directly in the relationship tuple.
        context ??= relationship.OptionalCaveat?.Context;

        return new ParsedAssertion(
            relationship.Resource,
            relationship.Subject,
            expectation,
            context,
            source);
    }

    private static (string Tuple, IReadOnlyDictionary<string, object?>? Context) SplitWithContext(string source)
    {
        var trimmed = source.Trim();
        var withIndex = trimmed.IndexOf(" with ", StringComparison.Ordinal);
        if (withIndex < 0)
        {
            return (trimmed, null);
        }

        var tuplePart = trimmed[..withIndex].Trim();
        var jsonPart = trimmed[(withIndex + " with ".Length)..].Trim();
        var context = ParseJsonContext(jsonPart);
        return (tuplePart, context);
    }

    private static IReadOnlyDictionary<string, object?> ParseJsonContext(string json)
    {
        using var doc = JsonDocument.Parse(json);
        if (doc.RootElement.ValueKind != JsonValueKind.Object)
        {
            throw new FormatException($"Assertion caveat context must be a JSON object, got: {json}");
        }

        var result = new Dictionary<string, object?>();
        foreach (var prop in doc.RootElement.EnumerateObject())
        {
            result[prop.Name] = ConvertJson(prop.Value);
        }

        return result;
    }

    private static object? ConvertJson(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.String => element.GetString(),
        JsonValueKind.Number => element.TryGetInt64(out var l) ? l : element.GetDouble(),
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.Null => null,
        JsonValueKind.Array => element.EnumerateArray().Select(ConvertJson).ToList(),
        JsonValueKind.Object => element.EnumerateObject().ToDictionary(p => p.Name, p => ConvertJson(p.Value)),
        _ => element.ToString(),
    };

    private sealed class RawValidationFile
    {
        public string? Schema { get; set; }
        public string? SchemaFile { get; set; }
        public string? Relationships { get; set; }
        public RawAssertions? Assertions { get; set; }
    }

    private sealed class RawAssertions
    {
        public List<string>? AssertTrue { get; set; }
        public List<string>? AssertFalse { get; set; }
        public List<string>? AssertCaveated { get; set; }
    }
}
