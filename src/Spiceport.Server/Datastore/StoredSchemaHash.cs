using System.Security.Cryptography;

namespace Spiceport.Grains;

/// <summary>
/// The single definition of the stored-schema hash: a lowercase-hex SHA-256 over the persisted UTF-8
/// schema bytes. Used when verifying fetched bytes on a <see cref="SchemaResolver"/>
/// cache miss, and by the tests' reference fold when minting a
/// <see cref="Spiceport.Grains.Abstractions.SchemaVersionWire"/> — one definition, so the write-side
/// and read-side hashes can never diverge.
/// </summary>
internal static class StoredSchemaHash
{
    /// <summary>Computes the lowercase-hex SHA-256 of the persisted schema bytes.</summary>
    public static string Compute(byte[] schemaBytes) => Convert.ToHexStringLower(SHA256.HashData(schemaBytes));
}
