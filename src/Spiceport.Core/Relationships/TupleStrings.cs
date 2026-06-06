using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Spiceport.Core;

/// <summary>
/// Parsing and formatting of SpiceDB ONR and relationship (tuple) strings.
/// </summary>
/// <remarks>
/// Grammar (matching the SpiceDB Go reference):
/// <code>
/// TUPLE      ::= ONR '@' SUBJECT [ CAVEAT ] [ EXPIRATION ]
/// ONR        ::= NAMESPACE ':' OBJECT_ID '#' RELATION
/// SUBJECT    ::= NAMESPACE ':' SUBJECT_ID [ '#' (RELATION | '...') ]
/// CAVEAT     ::= '[' CAVEAT_NAME [ ':' JSON_OBJECT ] ']'
/// EXPIRATION ::= '[expiration:' RFC3339 ']'
/// </code>
/// </remarks>
public static partial class TupleStrings
{
    private const string NamespaceNameExpr = "([a-z][a-z0-9_]{1,61}[a-z0-9]/)*[a-z][a-z0-9_]{1,62}[a-z0-9]";
    private const string ResourceIdExpr = "([a-zA-Z0-9/_|\\-=+]{1,})";
    private const string SubjectIdExpr = "([a-zA-Z0-9/_|\\-=+]{1,})|\\*";
    private const string RelationExpr = "[a-z][a-z0-9_]{1,62}[a-z0-9]";
    private const string CaveatNameExpr = "([a-z][a-z0-9_]{1,61}[a-z0-9]/)*[a-z][a-z0-9_]{1,62}[a-z0-9]";

    private const string OnrExpr =
        $"(?<resourceType>({NamespaceNameExpr})):(?<resourceID>{ResourceIdExpr})#(?<resourceRel>{RelationExpr})";

    private const string SubjectExpr =
        $"(?<subjectType>({NamespaceNameExpr})):(?<subjectID>{SubjectIdExpr})(#(?<subjectRel>{RelationExpr}|\\.\\.\\.))?";

    private const string CaveatExpr =
        $"\\[(?<caveatName>({CaveatNameExpr}))(:(?<caveatContext>(\\{{(.+)\\}})))?\\]";

    private const string ExpirationExpr =
        "\\[expiration:(?<expirationDateTime>([\\d\\-\\.:TZ]+))\\]";

    private const string TupleExpr = $"^{OnrExpr}@{SubjectExpr}({CaveatExpr})?({ExpirationExpr})?$";

    [GeneratedRegex($"^{OnrExpr}$", RegexOptions.CultureInvariant)]
    private static partial Regex OnrRegex();

    [GeneratedRegex(TupleExpr, RegexOptions.CultureInvariant)]
    private static partial Regex TupleRegex();

    [GeneratedRegex($"^{ResourceIdExpr}$", RegexOptions.CultureInvariant)]
    private static partial Regex ResourceIdRegex();

    [GeneratedRegex($"^({SubjectIdExpr})$", RegexOptions.CultureInvariant)]
    private static partial Regex SubjectIdRegex();

    /// <summary>The format used for expiration timestamps (RFC3339, UTC, "Z" suffix).</summary>
    private const string ExpirationFormat = "yyyy-MM-ddTHH:mm:ss.fffffff'Z'";

    // ---- Formatting ----

    /// <summary>
    /// Formats an ONR. An ellipsis relation is omitted (<c>namespace:object_id</c>);
    /// otherwise <c>namespace:object_id#relation</c>.
    /// </summary>
    public static string FormatObjectAndRelation(ObjectAndRelation onr) =>
        onr.Relation == CoreConstants.Ellipsis
            ? $"{onr.ObjectType}:{onr.ObjectId}"
            : $"{onr.ObjectType}:{onr.ObjectId}#{onr.Relation}";

    /// <summary>Formats a complete relationship as a canonical tuple string.</summary>
    public static string FormatRelationship(Relationship rel)
    {
        var sb = new StringBuilder();
        sb.Append(FormatObjectAndRelation(rel.Resource));
        sb.Append('@');
        sb.Append(FormatObjectAndRelation(rel.Subject));
        sb.Append(FormatCaveat(rel.OptionalCaveat));
        sb.Append(FormatExpiration(rel.OptionalExpiration));
        return sb.ToString();
    }

    private static string FormatCaveat(ContextualizedCaveat? caveat)
    {
        if (caveat is null || string.IsNullOrEmpty(caveat.CaveatName))
            return string.Empty;

        var contextString = string.Empty;
        if (caveat.Context is { Count: > 0 })
            contextString = ":" + JsonSerializer.Serialize(caveat.Context);

        return $"[{caveat.CaveatName}{contextString}]";
    }

    private static string FormatExpiration(DateTimeOffset? expiration) =>
        expiration is null
            ? string.Empty
            : $"[expiration:{expiration.Value.ToUniversalTime().ToString(ExpirationFormat, CultureInvariant)}]";

    private static readonly CultureInfo CultureInvariant = CultureInfo.InvariantCulture;

    // ---- Parsing ----

    /// <summary>Parses an ONR string (<c>namespace:object_id#relation</c>).</summary>
    /// <exception cref="FormatException">If the string is not a valid ONR.</exception>
    public static ObjectAndRelation ParseObjectAndRelation(string value)
    {
        if (!TryParseObjectAndRelation(value, out var onr))
            throw new FormatException($"invalid object and relation string: '{value}'");
        return onr;
    }

    /// <summary>Attempts to parse an ONR string.</summary>
    public static bool TryParseObjectAndRelation(string value, out ObjectAndRelation result)
    {
        var match = OnrRegex().Match(value);
        if (!match.Success)
        {
            result = null!;
            return false;
        }

        result = new ObjectAndRelation(
            match.Groups["resourceType"].Value,
            match.Groups["resourceID"].Value,
            match.Groups["resourceRel"].Value);
        return true;
    }

    /// <summary>Parses a relationship (tuple) string.</summary>
    /// <exception cref="FormatException">If the string is not a valid relationship.</exception>
    public static Relationship ParseRelationship(string value)
    {
        if (!TryParseRelationship(value, out var rel))
            throw new FormatException($"invalid relationship string: '{value}'");
        return rel;
    }

    /// <summary>Attempts to parse a relationship (tuple) string.</summary>
    public static bool TryParseRelationship(string value, out Relationship result)
    {
        var match = TupleRegex().Match(value);
        if (!match.Success)
        {
            result = null!;
            return false;
        }

        var resource = new ObjectAndRelation(
            match.Groups["resourceType"].Value,
            match.Groups["resourceID"].Value,
            match.Groups["resourceRel"].Value);

        var subjectRelGroup = match.Groups["subjectRel"];
        var subjectRelation = subjectRelGroup.Success && subjectRelGroup.Value.Length > 0
            ? subjectRelGroup.Value
            : CoreConstants.Ellipsis;

        var subject = new ObjectAndRelation(
            match.Groups["subjectType"].Value,
            match.Groups["subjectID"].Value,
            subjectRelation);

        ContextualizedCaveat? caveat = null;
        var caveatNameGroup = match.Groups["caveatName"];
        if (caveatNameGroup.Success && caveatNameGroup.Value.Length > 0)
        {
            IReadOnlyDictionary<string, object?>? context = null;
            var ctxGroup = match.Groups["caveatContext"];
            if (ctxGroup.Success && ctxGroup.Value.Length > 0)
            {
                try
                {
                    context = JsonSerializer.Deserialize<Dictionary<string, object?>>(ctxGroup.Value);
                }
                catch (JsonException)
                {
                    result = null!;
                    return false;
                }
            }

            caveat = new ContextualizedCaveat(caveatNameGroup.Value, context);
        }

        DateTimeOffset? expiration = null;
        var expGroup = match.Groups["expirationDateTime"];
        if (expGroup.Success && expGroup.Value.Length > 0)
        {
            if (!DateTimeOffset.TryParse(
                    expGroup.Value, CultureInvariant,
                    DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                    out var parsed))
            {
                result = null!;
                return false;
            }
            expiration = parsed;
        }

        result = new Relationship(new RelationshipReference(resource, subject), caveat, expiration);
        return true;
    }

    /// <summary>Validates a resource object id (1..1024 chars, allowed character set).</summary>
    public static bool IsValidResourceId(string objectId) =>
        objectId.Length is > 0 and <= 1024 && ResourceIdRegex().IsMatch(objectId);

    /// <summary>Validates a subject object id (1..1024 chars, allowed character set, or wildcard).</summary>
    public static bool IsValidSubjectId(string subjectId) =>
        subjectId.Length is > 0 and <= 1024 && SubjectIdRegex().IsMatch(subjectId);
}
