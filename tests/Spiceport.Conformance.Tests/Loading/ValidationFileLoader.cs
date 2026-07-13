using System.Collections.Immutable;
using System.Text.Json;
using System.Text.RegularExpressions;
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
        var validations = ParseValidations(raw.Validation);

        return new ValidationFile(schemaText, relationships, assertions, validations);
    }

    /// <summary>
    /// Loads a validation file, resolving a <c>schemaFile</c> reference relative to the file's own
    /// directory. Mirrors SpiceDB's validationfile loader: schema/schemaFile are mutually exclusive,
    /// a schemaFile must be local (no "../" escape), and the legacy <c>validation_tuples</c> key is rejected.
    /// </summary>
    public static ValidationFile LoadResolved(string path)
    {
        var yaml = File.ReadAllText(path);

        // Legacy key: SpiceDB rejects files declaring relationships via `validation_tuples`.
        if (yaml.Contains("validation_tuples", StringComparison.Ordinal))
        {
            throw new ValidationFileLoadException("relationships must be specified in `relationships`");
        }

        var raw = Deserializer.Deserialize<RawValidationFile>(yaml)
                  ?? throw new FormatException("Validation file is empty.");

        var hasSchema = !string.IsNullOrWhiteSpace(raw.Schema);
        var hasSchemaFile = !string.IsNullOrWhiteSpace(raw.SchemaFile);
        if (hasSchema && hasSchemaFile)
        {
            throw new ValidationFileLoadException("only one of schema or schemaFile can be specified");
        }

        string schemaText;
        if (hasSchemaFile)
        {
            var baseDir = Path.GetDirectoryName(Path.GetFullPath(path))!;
            var relative = raw.SchemaFile!.Trim();
            var full = Path.GetFullPath(Path.Combine(baseDir, relative));
            if (!full.StartsWith(baseDir + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            {
                throw new ValidationFileLoadException($"schema file \"{relative}\" is not local");
            }

            schemaText = File.ReadAllText(full).TrimEnd(); // FileNotFoundException -> missing-schemafile case
        }
        else
        {
            schemaText = (raw.Schema ?? string.Empty).TrimEnd();
        }

        var relationships = ParseRelationships(raw.Relationships ?? string.Empty);
        var assertions = ParseAssertions(raw.Assertions);
        var validations = ParseValidations(raw.Validation);
        return new ValidationFile(schemaText, relationships, assertions, validations);
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

    // ---- validation: block (SpiceDB's pkg/validationfile/blocks grammar) ----

    // Extracts the bracketed subject term: everything between the first '[' and the LAST
    // ']' in the entry (matching SpiceDB's `(.*?)\[(.*)](.*?)` — greedy inside the brackets),
    // so a caveat marker `[...]` nested inside the outer brackets doesn't truncate the match.
    // The `is <...>` resource-path suffix (if any) trails outside the brackets and is discarded.
    private static readonly Regex BracketedSubjectRegex =
        new(@"^.*?\[(.*)\].*$", RegexOptions.Singleline | RegexOptions.Compiled);

    // Subject term grammar: `SUBJECT_ONR ['[...]'] [' - {' EXCEPTIONS '}']`, e.g.
    // `user:tom`, `user:tom[...]`, `user:*[...] - {user:a, user:b[...]}`.
    private static readonly Regex SubjectWithExceptionsRegex = new(
        @"^(?<subject>[^\]\s]+)(?<caveat>\[\.\.\.\])?(\s*-\s*\{(?<exceptions>[^}]*)\})?$",
        RegexOptions.Compiled);

    private static ImmutableList<ValidationEntry> ParseValidations(Dictionary<string, List<string>>? raw)
    {
        var builder = ImmutableList.CreateBuilder<ValidationEntry>();
        if (raw is null)
        {
            return builder.ToImmutable();
        }

        foreach (var (key, values) in raw)
        {
            var onr = TupleStrings.ParseObjectAndRelation(key.Trim());
            var subjects = (values ?? []).Select(ParseExpectedSubject).ToImmutableList();
            builder.Add(new ValidationEntry(onr, subjects));
        }

        return builder.ToImmutable();
    }

    private static ExpectedSubject ParseExpectedSubject(string source)
    {
        var bracketMatch = BracketedSubjectRegex.Match(source.Trim());
        if (!bracketMatch.Success)
        {
            throw new FormatException($"invalid validation subject entry: '{source}'");
        }

        var userStr = bracketMatch.Groups[1].Value.Trim();
        var subjectMatch = SubjectWithExceptionsRegex.Match(userStr);
        if (!subjectMatch.Success)
        {
            throw new FormatException($"invalid validation subject: '{userStr}'");
        }

        var term = new ExpectedSubjectTerm(
            ParseSubjectOnr(subjectMatch.Groups["subject"].Value),
            subjectMatch.Groups["caveat"].Success);

        var exceptions = ImmutableList.CreateBuilder<ExpectedSubjectTerm>();
        var exceptionsGroup = subjectMatch.Groups["exceptions"];
        if (exceptionsGroup.Success && exceptionsGroup.Value.Trim().Length > 0)
        {
            foreach (var rawException in exceptionsGroup.Value.Split(','))
            {
                var trimmed = rawException.Trim();
                var isCaveated = trimmed.EndsWith("[...]", StringComparison.Ordinal);
                if (isCaveated)
                {
                    trimmed = trimmed[..^"[...]".Length];
                }

                exceptions.Add(new ExpectedSubjectTerm(ParseSubjectOnr(trimmed), isCaveated));
            }
        }

        return new ExpectedSubject(term, exceptions.ToImmutable());
    }

    /// <summary>
    /// Parses a bare subject ONR (no resource part): <c>type:id</c>, <c>type:id#relation</c>
    /// or the wildcard <c>type:*</c>. Absent relation normalises to the ellipsis, matching
    /// <see cref="TupleStrings"/>'s convention for a subject with no explicit sub-relation.
    /// </summary>
    private static ObjectAndRelation ParseSubjectOnr(string value)
    {
        var hashIndex = value.IndexOf('#');
        var typeAndId = hashIndex >= 0 ? value[..hashIndex] : value;
        var relation = hashIndex >= 0 ? value[(hashIndex + 1)..] : CoreConstants.Ellipsis;

        var colonIndex = typeAndId.IndexOf(':');
        if (colonIndex < 0)
        {
            throw new FormatException($"invalid subject ONR: '{value}'");
        }

        var type = typeAndId[..colonIndex];
        var id = typeAndId[(colonIndex + 1)..];
        return new ObjectAndRelation(type, id, relation);
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
        public Dictionary<string, List<string>>? Validation { get; set; }
    }

    private sealed class RawAssertions
    {
        public List<string>? AssertTrue { get; set; }
        public List<string>? AssertFalse { get; set; }
        public List<string>? AssertCaveated { get; set; }
    }
}

/// <summary>
/// Raised for loader-level (not schema-compile) validation failures: mutually-exclusive
/// schema/schemaFile, a non-local schemaFile, or the legacy <c>validation_tuples</c> key.
/// </summary>
public sealed class ValidationFileLoadException(string message) : Exception(message);
