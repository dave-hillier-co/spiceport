using System.Text;
using Spiceport.Core;

namespace Spiceport.Grains;

/// <summary>
/// Opaque continuation cursor for bulk export. It pins the snapshot by carrying BOTH the export's
/// revision and the last tuple emitted, so a resumed export reads the exact same revision (never seeing
/// writes committed after it began) and continues strictly after the last tuple.
/// </summary>
/// <remarks>
/// Wire form is base64 of a newline-delimited record <c>v1\n{revisionString}\n{afterTuple}</c>, mirroring
/// the <see cref="ZedTokens"/> encoding style and SpiceDB's bulk-export cursor (revision + last tuple).
/// The revision string is the <see cref="TimestampRevision"/> integer form parsed by
/// <see cref="RevisionCodec"/>.
/// </remarks>
internal static class BulkExportCursor
{
    private const string Version = "v1";

    /// <summary>The decoded parts of a bulk-export cursor.</summary>
    public readonly record struct Decoded(IRevision Revision, string AfterTuple);

    /// <summary>Encodes the pinned revision and the last emitted tuple into an opaque cursor.</summary>
    public static string Encode(IRevision revision, string afterTuple)
    {
        ArgumentNullException.ThrowIfNull(revision);
        ArgumentNullException.ThrowIfNull(afterTuple);
        var payload = string.Join('\n', Version, revision.ToString(), afterTuple);
        return Convert.ToBase64String(Encoding.UTF8.GetBytes(payload));
    }

    /// <summary>
    /// Decodes a cursor. Returns false for a null/empty cursor (a first page) and throws
    /// <see cref="FormatException"/> for a malformed non-empty cursor.
    /// </summary>
    public static bool TryDecode(string? cursor, out Decoded decoded)
    {
        decoded = default;
        if (string.IsNullOrEmpty(cursor))
            return false;

        string payload;
        try
        {
            payload = Encoding.UTF8.GetString(Convert.FromBase64String(cursor));
        }
        catch (FormatException)
        {
            throw new FormatException("invalid bulk export cursor");
        }

        var parts = payload.Split('\n');
        if (parts.Length != 3 || parts[0] != Version)
            throw new FormatException("invalid bulk export cursor");

        IRevision revision;
        try
        {
            revision = RevisionCodec.Parse(parts[1]);
        }
        catch (Exception ex) when (ex is FormatException or OverflowException or ArgumentException)
        {
            throw new FormatException("invalid bulk export cursor");
        }

        decoded = new Decoded(revision, parts[2]);
        return true;
    }
}
