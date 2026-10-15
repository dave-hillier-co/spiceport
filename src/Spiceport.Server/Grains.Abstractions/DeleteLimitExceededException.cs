namespace Spiceport.Grains.Abstractions;

/// <summary>
/// Thrown by a transactional DeleteRelationships (a limit without partial deletion) when more than
/// <see cref="Limit"/> relationships match its filter; nothing is deleted. Mirrors SpiceDB's
/// <c>CouldNotTransactionallyDeleteErr</c>, which the gRPC front door maps to <c>InvalidArgument</c>.
/// <c>[GenerateSerializer]</c> because it crosses the grain boundary.
/// </summary>
[GenerateSerializer]
public sealed class DeleteLimitExceededException : Exception
{
    /// <summary>The request's limit, which the matching relationships exceeded.</summary>
    [Id(0)]
    public ulong Limit { get; }

    /// <summary>Creates the exception for the exceeded <paramref name="limit"/>.</summary>
    public DeleteLimitExceededException(ulong limit)
        : base($"found more than {limit} relationships to be deleted and partial deletion was not requested")
    {
        Limit = limit;
    }
}
