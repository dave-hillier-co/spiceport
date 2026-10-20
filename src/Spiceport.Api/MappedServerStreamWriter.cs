using Grpc.Core;

namespace Spiceport.Api;

/// <summary>
/// A server-stream writer that converts each outbound message before writing it to the real stream, the
/// response-side counterpart of <see cref="MappedAsyncStreamReader{TSource, TTarget}"/>.
/// </summary>
internal sealed class MappedServerStreamWriter<TSource, TTarget>(
    IServerStreamWriter<TTarget> inner, Func<TSource, TTarget> map) : IServerStreamWriter<TSource>
{
    public WriteOptions? WriteOptions
    {
        get => inner.WriteOptions;
        set => inner.WriteOptions = value;
    }

    public Task WriteAsync(TSource message) => inner.WriteAsync(map(message));
}
