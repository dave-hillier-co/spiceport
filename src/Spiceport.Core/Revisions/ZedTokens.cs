using System.Text;

namespace Spiceport.Core;

/// <summary>
/// Encoding and decoding helpers for <see cref="ZedToken"/>.
/// </summary>
/// <remarks>
/// The on-the-wire form is base64 of a newline-delimited record:
/// <code>v1\n{datastoreId}\n{revisionString}\n{schemaHash}</code>
/// where <c>datastoreId</c> and <c>schemaHash</c> may be empty. This is intentionally
/// simple (not protobuf) but stable and round-trippable.
/// </remarks>
public static class ZedTokens
{
    private const string Version = "v1";

    /// <summary>Mints a <see cref="ZedToken"/> from a revision, optional schema hash, and datastore id.</summary>
    public static ZedToken FromRevision(
        IRevision revision,
        string? schemaHash = null,
        string? datastoreUniqueId = null)
    {
        ArgumentNullException.ThrowIfNull(revision);
        var payload = string.Join('\n',
            Version,
            datastoreUniqueId ?? string.Empty,
            revision.ToString(),
            schemaHash ?? string.Empty);
        var encoded = Convert.ToBase64String(Encoding.UTF8.GetBytes(payload));
        return new ZedToken(encoded);
    }

    /// <summary>
    /// Decodes a <see cref="ZedToken"/> using the supplied parser to reconstruct the revision
    /// and to determine datastore-id match status.
    /// </summary>
    public static DecodedRevision DecodeRevision(ZedToken token, IRevisionParser parser)
    {
        ArgumentNullException.ThrowIfNull(token);
        ArgumentNullException.ThrowIfNull(parser);

        string payload;
        try
        {
            payload = Encoding.UTF8.GetString(Convert.FromBase64String(token.Token));
        }
        catch (FormatException)
        {
            return Unknown(parser);
        }

        var parts = payload.Split('\n');
        if (parts.Length != 4 || parts[0] != Version)
            return Unknown(parser);

        var datastoreId = parts[1];
        var revisionString = parts[2];
        var schemaHash = parts[3].Length == 0 ? null : parts[3];

        IRevision revision;
        try
        {
            revision = parser.ParseRevisionString(revisionString);
        }
        catch (Exception) when (IsParseException())
        {
            return Unknown(parser, schemaHash);
        }

        var status = datastoreId.Length == 0
            ? ZedTokenStatus.LegacyEmptyDatastoreId
            : datastoreId == parser.DatastoreUniqueId
                ? ZedTokenStatus.Valid
                : ZedTokenStatus.MismatchedDatastoreId;

        return new DecodedRevision(revision, schemaHash, status);

        static bool IsParseException() => true;
    }

    private static DecodedRevision Unknown(IRevisionParser parser, string? schemaHash = null) =>
        new(new TimestampRevision(0), schemaHash, ZedTokenStatus.Unknown);
}
