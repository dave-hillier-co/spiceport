using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Google.Rpc;
using Grpc.Core;
using GrpcStatus = Grpc.Core.Status;
using V1 = Authzed.Api.V1;

namespace Spiceport.Api;

/// <summary>
/// The structured error detail SpiceDB attaches to the errors it gives a reason: a
/// <c>google.rpc.ErrorInfo</c> with domain <c>authzed.com</c>, the <c>authzed.api.v1.ErrorReason</c> name as
/// its reason, and the error's metadata, carried in the status's details (the
/// <c>grpc-status-details-bin</c> trailer). Mirrors <c>pkg/spiceerrors</c>' <c>WithCodeAndDetails</c> and
/// <c>ForReason</c>.
/// </summary>
internal static class RpcErrors
{
    /// <summary>SpiceDB's <c>spiceerrors.Domain</c>.</summary>
    internal const string Domain = "authzed.com";

    /// <summary>The trailer gRPC carries a status's details in.</summary>
    internal const string StatusDetailsTrailer = "grpc-status-details-bin";

    /// <summary>
    /// An <see cref="RpcException"/> with <paramref name="code"/> and <paramref name="message"/> whose details
    /// carry an <c>ErrorInfo</c> for <paramref name="reason"/> and <paramref name="metadata"/>.
    /// </summary>
    internal static RpcException WithReason(
        StatusCode code,
        string message,
        V1::ErrorReason reason,
        IEnumerable<KeyValuePair<string, string>>? metadata = null)
    {
        var info = new ErrorInfo { Reason = ReasonName(reason), Domain = Domain };
        foreach (var (key, value) in metadata ?? [])
            info.Metadata[key] = value;

        var status = new Google.Rpc.Status { Code = (int)code, Message = message, Details = { Any.Pack(info) } };
        return new RpcException(
            new GrpcStatus(code, message),
            new Metadata { { StatusDetailsTrailer, status.ToByteArray() } });
    }

    /// <summary>
    /// The <c>google.rpc.Status</c> an <see cref="RpcException"/> stands for: its code and detail, with the
    /// details it carries in its <c>grpc-status-details-bin</c> trailer, as Go's
    /// <c>status.Convert(err).Proto()</c> gives them.
    /// </summary>
    internal static Google.Rpc.Status ToRpcStatus(RpcException ex)
    {
        var trailer = ex.Trailers.GetValueBytes(StatusDetailsTrailer);
        if (trailer is not null)
            return Google.Rpc.Status.Parser.ParseFrom(trailer);

        return new Google.Rpc.Status { Code = (int)ex.StatusCode, Message = ex.Status.Detail };
    }

    /// <summary>The <c>ErrorInfo</c> an <see cref="RpcException"/> carries, or <c>null</c> when it has none.</summary>
    internal static ErrorInfo? ErrorInfoOf(RpcException ex) =>
        ToRpcStatus(ex).Details.FirstOrDefault(d => d.Is(ErrorInfo.Descriptor))?.Unpack<ErrorInfo>();

    /// <summary>The proto name of <paramref name="reason"/> (<c>ERROR_REASON_...</c>), as Go's <c>ErrorReason_name</c>.</summary>
    internal static string ReasonName(V1::ErrorReason reason) =>
        V1::ErrorReasonReflection.Descriptor.EnumTypes[0].FindValueByNumber((int)reason).Name;
}
