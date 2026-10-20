using Grpc.Core;

namespace Spiceport.Api;

/// <summary>
/// A client-stream reader that presents each inbound message of one type as another, so a deprecated RPC
/// whose request type differs only in name can drive the implementation of its replacement.
/// </summary>
internal sealed class MappedAsyncStreamReader<TSource, TTarget>(
    IAsyncStreamReader<TSource> inner, Func<TSource, TTarget> map) : IAsyncStreamReader<TTarget>
{
    public TTarget Current => map(inner.Current);

    public Task<bool> MoveNext(CancellationToken cancellationToken) => inner.MoveNext(cancellationToken);
}
