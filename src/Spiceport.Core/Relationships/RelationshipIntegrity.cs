namespace Spiceport.Core;

/// <summary>
/// Cryptographic integrity information used to verify a relationship was written by a trusted source.
/// </summary>
/// <param name="KeyId">Identifier of the key used to compute the hash.</param>
/// <param name="Hash">The integrity hash bytes.</param>
/// <param name="HashedAt">When the hash was computed.</param>
public sealed record RelationshipIntegrity(string KeyId, byte[] Hash, DateTimeOffset HashedAt)
{
    /// <summary>Value equality, comparing <see cref="Hash"/> by content rather than by array reference.</summary>
    public bool Equals(RelationshipIntegrity? other) =>
        other is not null
        && string.Equals(KeyId, other.KeyId, StringComparison.Ordinal)
        && StructuralEquality.BytesEqual(Hash, other.Hash)
        && HashedAt.Equals(other.HashedAt);

    /// <inheritdoc/>
    public override int GetHashCode() =>
        HashCode.Combine(StringComparer.Ordinal.GetHashCode(KeyId), StructuralEquality.BytesHash(Hash), HashedAt);
}
