namespace Spiceport.Core;

/// <summary>
/// Cryptographic integrity information used to verify a relationship was written by a trusted source.
/// </summary>
/// <param name="KeyId">Identifier of the key used to compute the hash.</param>
/// <param name="Hash">The integrity hash bytes.</param>
/// <param name="HashedAt">When the hash was computed.</param>
public sealed record RelationshipIntegrity(string KeyId, byte[] Hash, DateTimeOffset HashedAt);
