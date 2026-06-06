namespace Spiceport.Core;

/// <summary>
/// An opaque consistency cursor handed to clients. Encodes a revision plus optional
/// schema hash and datastore id. The <see cref="Token"/> string is opaque; decode it
/// via <see cref="ZedTokens"/>.
/// </summary>
/// <param name="Token">The opaque (base64-encoded) token string.</param>
public sealed record ZedToken(string Token);

/// <summary>Result status of decoding a <see cref="ZedToken"/>.</summary>
public enum ZedTokenStatus
{
    /// <summary>Decoding failed or the token is malformed.</summary>
    Unknown = 0,

    /// <summary>Legacy token with no datastore id.</summary>
    LegacyEmptyDatastoreId = 1,

    /// <summary>Token is valid and matches the current datastore.</summary>
    Valid = 2,

    /// <summary>Token came from a different datastore instance.</summary>
    MismatchedDatastoreId = 3,
}

/// <summary>The decoded contents of a <see cref="ZedToken"/>.</summary>
/// <param name="Revision">The revision carried by the token.</param>
/// <param name="SchemaHash">Optional schema hash captured when the token was minted.</param>
/// <param name="Status">The validation status relative to the current datastore.</param>
public sealed record DecodedRevision(
    IRevision Revision,
    string? SchemaHash = null,
    ZedTokenStatus Status = ZedTokenStatus.Unknown);

/// <summary>
/// Parses a datastore-specific revision string back into an <see cref="IRevision"/>.
/// Implemented by each datastore so ZedTokens can be decoded.
/// </summary>
public interface IRevisionParser
{
    /// <summary>A unique id for the datastore instance, embedded into minted tokens.</summary>
    string DatastoreUniqueId { get; }

    /// <summary>Parses a revision string produced by <see cref="IRevision.ToString"/>.</summary>
    IRevision ParseRevisionString(string revisionString);
}
