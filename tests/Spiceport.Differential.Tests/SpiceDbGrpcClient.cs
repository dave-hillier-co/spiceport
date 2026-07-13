using System.Reflection;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using V1 = Authzed.Api.V1;

namespace Spiceport.Differential.Tests;

/// <summary>
/// A hand-rolled, minimal <c>authzed.api.v1</c> gRPC CLIENT for driving the real SpiceDB container.
/// </summary>
/// <remarks>
/// <para>
/// WHY NOT GENERATED CLIENT STUBS: <c>src/Spiceport.Protos</c> compiles the vendored
/// <c>authzed.api.v1</c> protos with <c>GrpcServices="Server"</c> (message classes + abstract server
/// bases only -- no <c>XxxServiceClient</c>). Compiling the SAME vendored <c>.proto</c> files a second
/// time in THIS project with <c>GrpcServices="Client"</c> would regenerate the message classes too
/// (every service proto defines its own request/response messages inline, not just the service), giving
/// two distinct CLR types named e.g. <c>Authzed.Api.V1.CheckPermissionRequest</c> -- one from
/// <c>Spiceport.Protos.dll</c> (needed to call the in-process <c>AuthzedPermissionsV1Service</c> whose
/// method signatures are fixed to that assembly's types) and one compiled locally into this assembly
/// (which would silently SHADOW the referenced one for any unqualified use in this project, per C#'s
/// current-compilation-wins resolution rule). Untangling that requires <c>extern alias</c> gymnastics on
/// every call site. Instead: reuse Spiceport.Protos' message types (the wire format is identical either
/// way -- it's the same proto) and hand-write the handful of <see cref="Method{TRequest,TResponse}"/>
/// descriptors this suite actually calls, invoked through a raw <see cref="CallInvoker"/>. This is the
/// least invasive mechanical option: no second proto compile, no aliasing, one source of truth for the
/// message types on both the Spiceport in-process side and the SpiceDB wire side.
/// </para>
/// <para>
/// The <c>Method&lt;,&gt;</c> descriptors the generated code itself would produce are private static
/// fields on the generated <c>XxxService</c> partial class (see <c>PermissionServiceGrpc.cs</c>), so they
/// cannot be reused by reflection either; constructing them by hand (service name, method name, and a
/// <see cref="Marshaller{T}"/> built from each message's public static <c>Parser</c>) is the same amount
/// of code the plugin would emit, just for five RPCs instead of all of them.
/// </para>
/// </remarks>
public sealed class SpiceDbGrpcClient : IDisposable
{
    private const string SchemaServiceName = "authzed.api.v1.SchemaService";
    private const string PermissionsServiceName = "authzed.api.v1.PermissionsService";

    private readonly GrpcChannel _channel;
    private readonly CallInvoker _invoker;
    private readonly string _preSharedKey;

    public SpiceDbGrpcClient(string address, string preSharedKey)
    {
        _channel = GrpcChannel.ForAddress(address);
        _invoker = _channel.CreateCallInvoker();
        _preSharedKey = preSharedKey;
    }

    private CallOptions Options() => new(headers: new Metadata
    {
        { "authorization", $"Bearer {_preSharedKey}" },
    });

    public async Task<V1::WriteSchemaResponse> WriteSchemaAsync(V1::WriteSchemaRequest request) =>
        await _invoker.AsyncUnaryCall(WriteSchemaMethod, null, Options(), request).ResponseAsync;

    public async Task<V1::WriteRelationshipsResponse> WriteRelationshipsAsync(V1::WriteRelationshipsRequest request) =>
        await _invoker.AsyncUnaryCall(WriteRelationshipsMethod, null, Options(), request).ResponseAsync;

    public async Task<V1::DeleteRelationshipsResponse> DeleteRelationshipsAsync(V1::DeleteRelationshipsRequest request) =>
        await _invoker.AsyncUnaryCall(DeleteRelationshipsMethod, null, Options(), request).ResponseAsync;

    public async Task<V1::CheckPermissionResponse> CheckPermissionAsync(V1::CheckPermissionRequest request) =>
        await _invoker.AsyncUnaryCall(CheckPermissionMethod, null, Options(), request).ResponseAsync;

    public async Task<List<V1::LookupResourcesResponse>> LookupResourcesAsync(V1::LookupResourcesRequest request)
    {
        using var call = _invoker.AsyncServerStreamingCall(LookupResourcesMethod, null, Options(), request);
        var results = new List<V1::LookupResourcesResponse>();
        while (await call.ResponseStream.MoveNext(CancellationToken.None))
            results.Add(call.ResponseStream.Current);
        return results;
    }

    public async Task<List<V1::LookupSubjectsResponse>> LookupSubjectsAsync(V1::LookupSubjectsRequest request)
    {
        using var call = _invoker.AsyncServerStreamingCall(LookupSubjectsMethod, null, Options(), request);
        var results = new List<V1::LookupSubjectsResponse>();
        while (await call.ResponseStream.MoveNext(CancellationToken.None))
            results.Add(call.ResponseStream.Current);
        return results;
    }

    public void Dispose() => _channel.Dispose();

    private static Marshaller<T> ProtoMarshaller<T>() where T : IMessage<T>
    {
        // Every Google.Protobuf-generated message exposes a public static `Parser` property of type
        // MessageParser<T>; IMessage<T> itself predates C# static-abstract-interface-members, so this is
        // resolved once via reflection at static-field init time rather than through a generic constraint.
        var parser = (MessageParser<T>)typeof(T).GetProperty("Parser", BindingFlags.Public | BindingFlags.Static)!
            .GetValue(null)!;
        return Marshallers.Create<T>(m => m.ToByteArray(), bytes => parser.ParseFrom(bytes));
    }

    private static readonly Method<V1::WriteSchemaRequest, V1::WriteSchemaResponse> WriteSchemaMethod = new(
        MethodType.Unary, SchemaServiceName, "WriteSchema",
        ProtoMarshaller<V1::WriteSchemaRequest>(), ProtoMarshaller<V1::WriteSchemaResponse>());

    private static readonly Method<V1::WriteRelationshipsRequest, V1::WriteRelationshipsResponse> WriteRelationshipsMethod = new(
        MethodType.Unary, PermissionsServiceName, "WriteRelationships",
        ProtoMarshaller<V1::WriteRelationshipsRequest>(), ProtoMarshaller<V1::WriteRelationshipsResponse>());

    private static readonly Method<V1::DeleteRelationshipsRequest, V1::DeleteRelationshipsResponse> DeleteRelationshipsMethod = new(
        MethodType.Unary, PermissionsServiceName, "DeleteRelationships",
        ProtoMarshaller<V1::DeleteRelationshipsRequest>(), ProtoMarshaller<V1::DeleteRelationshipsResponse>());

    private static readonly Method<V1::CheckPermissionRequest, V1::CheckPermissionResponse> CheckPermissionMethod = new(
        MethodType.Unary, PermissionsServiceName, "CheckPermission",
        ProtoMarshaller<V1::CheckPermissionRequest>(), ProtoMarshaller<V1::CheckPermissionResponse>());

    private static readonly Method<V1::LookupResourcesRequest, V1::LookupResourcesResponse> LookupResourcesMethod = new(
        MethodType.ServerStreaming, PermissionsServiceName, "LookupResources",
        ProtoMarshaller<V1::LookupResourcesRequest>(), ProtoMarshaller<V1::LookupResourcesResponse>());

    private static readonly Method<V1::LookupSubjectsRequest, V1::LookupSubjectsResponse> LookupSubjectsMethod = new(
        MethodType.ServerStreaming, PermissionsServiceName, "LookupSubjects",
        ProtoMarshaller<V1::LookupSubjectsRequest>(), ProtoMarshaller<V1::LookupSubjectsResponse>());
}
