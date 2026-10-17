using System.Security.Cryptography;
using System.Text;
using Google.Protobuf;
using Grpc.Core;
using Spiceport.Core;
using V1 = Authzed.Api.V1;

namespace Spiceport.Api;

/// <summary>
/// The opaque v1 ReadRelationships cursor: standard base64 over a format tag, a hash of the request
/// that minted it, and the canonical tuple of the last relationship returned. Mirrors SpiceDB's
/// <c>cursor.EncodeFromDispatchCursor</c> / <c>DecodeToDispatchCursor</c> with
/// <c>computeReadRelationshipsRequestHash</c>: a cursor resumes only the call it came from (same
/// filter, consistency and limit), and anything else is rejected rather than read as a position.
/// </summary>
internal static class ReadRelationshipsCursor
{
    private const string FormatTag = "rr1";
    private const char Separator = '\n';

    /// <summary>SpiceDB's <c>ErrHashMismatch</c> text.</summary>
    internal const string HashMismatchMessage =
        "the cursor provided does not have the same arguments as the original API call; please ensure you "
        + "are making the same API call, with the exact same parameters (besides the cursor)";

    /// <summary>The hash binding a cursor to the request: everything but the cursor itself.</summary>
    public static string RequestHash(V1::ReadRelationshipsRequest request)
    {
        // As in SpiceDB's computeCallHash, an absent consistency hashes as minimize_latency.
        var consistency = request.Consistency ?? new V1::Consistency { MinimizeLatency = true };
        using var sha = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        sha.AppendData(Encoding.UTF8.GetBytes("v1.readrelationships"));
        AppendField(sha, request.RelationshipFilter?.ToByteArray() ?? []);
        AppendField(sha, consistency.ToByteArray());
        AppendField(sha, BitConverter.GetBytes(request.OptionalLimit));
        return Convert.ToHexString(sha.GetHashAndReset());
    }

    private static void AppendField(IncrementalHash sha, byte[] bytes)
    {
        sha.AppendData(BitConverter.GetBytes(bytes.Length));
        sha.AppendData(bytes);
    }

    /// <summary>Encodes the resume position after <paramref name="tuple"/> for the request hashed as <paramref name="requestHash"/>.</summary>
    public static string Encode(string requestHash, string tuple) =>
        Convert.ToBase64String(Encoding.UTF8.GetBytes(FormatTag + Separator + requestHash + Separator + tuple));

    /// <summary>
    /// Decodes a client cursor to the canonical tuple to resume after, or null for an absent/empty cursor.
    /// Throws SpiceDB's errors: INVALID_ARGUMENT for an undecodable cursor or one minted for another call,
    /// FAILED_PRECONDITION when the resume relationship cannot be parsed.
    /// </summary>
    public static string? Decode(V1::Cursor? cursor, string requestHash)
    {
        if (cursor is not { Token.Length: > 0 } c)
            return null;

        string raw;
        try
        {
            raw = new UTF8Encoding(false, throwOnInvalidBytes: true).GetString(Convert.FromBase64String(c.Token));
        }
        catch (Exception ex) when (ex is FormatException or ArgumentException)
        {
            throw InvalidCursor($"error decoding cursor: {ex.Message}");
        }

        var parts = raw.Split(Separator, 3);
        if (parts.Length != 3 || parts[0] != FormatTag || parts[1] != requestHash)
            throw InvalidCursor(HashMismatchMessage);

        if (!TupleStrings.TryParseRelationship(parts[2], out _))
        {
            // SpiceDB's v1 NewInvalidCursorErr never sets the error's reason field, so the metadata it
            // reports carries an empty "reason".
            throw RpcErrors.WithReason(
                StatusCode.FailedPrecondition,
                "the cursor provided is not valid: could not parse resume relationship",
                V1::ErrorReason.InvalidCursor,
                [new("reason", string.Empty)]);
        }

        return parts[2];
    }

    /// <summary>SpiceDB's <c>pkg/cursor</c> <c>InvalidCursorError</c>: INVALID_CURSOR with no metadata.</summary>
    private static RpcException InvalidCursor(string message) =>
        RpcErrors.WithReason(StatusCode.InvalidArgument, message, V1::ErrorReason.InvalidCursor);
}
