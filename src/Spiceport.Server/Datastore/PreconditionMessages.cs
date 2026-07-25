using System.Globalization;
using System.Text;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// The single copy of the precondition-failure message text: <c>DatastoreGrain.Commit</c> evaluates
/// preconditions and returns it as <c>CommitFailureWire.Detail</c>, and the client side
/// (<c>RelationshipsGrain</c>) reconstructs a byte-identical <c>PreconditionFailedException</c> from it
/// (via <see cref="TryParseFailure"/>). Do not duplicate the format — the zed-compat contract is that
/// the message a caller observes is unchanged from the historical inline evaluation.
/// </summary>
internal static class PreconditionMessages
{
    /// <summary>The failure text for a MUST_MATCH precondition whose filter matched no relationships.</summary>
    public static string MustMatchFailed(int index, RelationshipsFilter filter) =>
        $"precondition {index} failed: MUST_MATCH filter [{Describe(filter)}] matched no relationships";

    /// <summary>The failure text for a MUST_NOT_MATCH precondition whose filter matched at least one relationship.</summary>
    public static string MustNotMatchFailed(int index, RelationshipsFilter filter) =>
        $"precondition {index} failed: MUST_NOT_MATCH filter [{Describe(filter)}] matched at least one relationship";

    /// <summary>
    /// The inverse of the two factory methods above: recovers the failure kind and precondition index
    /// from a message THIS type produced, so the client side of the grain commit (which receives the
    /// message as <c>CommitFailureWire.Detail</c>) can reconstruct the exact
    /// <c>PreconditionFailedException(kind, index, message)</c> the inline evaluation used to throw —
    /// same type, same properties, byte-identical message. Returns false for any text this type did not
    /// produce (the caller then falls back rather than guessing).
    /// </summary>
    public static bool TryParseFailure(string message, out PreconditionFailureKind kind, out int index)
    {
        kind = default;
        index = 0;

        const string prefix = "precondition ";
        if (!message.StartsWith(prefix, StringComparison.Ordinal))
            return false;

        var rest = message.AsSpan(prefix.Length);
        var space = rest.IndexOf(' ');
        if (space <= 0 || !int.TryParse(rest[..space], NumberStyles.None, CultureInfo.InvariantCulture, out index))
            return false;

        rest = rest[space..];
        const string mustMatch = " failed: MUST_MATCH filter [";
        const string mustNotMatch = " failed: MUST_NOT_MATCH filter [";
        if (rest.StartsWith(mustNotMatch, StringComparison.Ordinal))
        {
            kind = PreconditionFailureKind.MustNotMatchFoundOne;
            return true;
        }

        if (rest.StartsWith(mustMatch, StringComparison.Ordinal))
        {
            kind = PreconditionFailureKind.MustMatchFoundNone;
            return true;
        }

        return false;
    }

    /// <summary>
    /// The human-readable filter description embedded in the failure messages (resource/subject
    /// constraints only — the caveat/expiration options play no part in the data-plane precondition
    /// surface and are deliberately not described).
    /// </summary>
    private static string Describe(RelationshipsFilter f)
    {
        var sb = new StringBuilder();
        if (f.OptionalResourceType is { } rt) sb.Append("resource_type=").Append(rt).Append(' ');
        if (f.OptionalResourceIds is { Count: > 0 } ids) sb.Append("resource_ids=").Append(string.Join(",", ids)).Append(' ');
        if (f.OptionalResourceIdPrefix is { Length: > 0 } p) sb.Append("resource_id_prefix=").Append(p).Append(' ');
        if (f.OptionalResourceRelation is { } rr) sb.Append("relation=").Append(rr).Append(' ');
        if (f.OptionalSubjectsSelectors is { Count: > 0 } sels)
        {
            foreach (var s in sels)
            {
                if (s.OptionalSubjectType is { } st) sb.Append("subject_type=").Append(st).Append(' ');
                if (s.OptionalSubjectIds is { Count: > 0 } sids) sb.Append("subject_ids=").Append(string.Join(",", sids)).Append(' ');
            }
        }
        return sb.ToString().TrimEnd();
    }
}
