using Grpc.Core;
using Spiceport.Grains.Abstractions;
using V1 = Authzed.Api.V1;

namespace Spiceport.Api;

/// <summary>
/// gRPC front door for the <c>authzed.api.v1.SchemaService</c>. Pure translation over the data-plane
/// <see cref="IRelationshipsGrain"/>: WriteSchema compiles-then-swaps; ReadSchema returns the current
/// schema text (NOT_FOUND if none has been written). The v1 responses in this snapshot carry no ZedToken.
/// </summary>
public sealed class AuthzedSchemaV1Service(IGrainFactory grains)
    : V1::SchemaService.SchemaServiceBase
{
    private IRelationshipsGrain Relationships => grains.GetGrain<IRelationshipsGrain>(IRelationshipsGrain.Key);

    public override async Task<V1::ReadSchemaResponse> ReadSchema(
        V1::ReadSchemaRequest request, ServerCallContext context)
    {
        var reply = await Relationships.ReadSchema();
        if (string.IsNullOrWhiteSpace(reply.SchemaText))
        {
            // SpiceDB's schema handler returns NOT_FOUND (ErrNoSchema) when nothing has been written.
            throw new RpcException(new Status(StatusCode.NotFound, "No schema has been defined"));
        }

        return new V1::ReadSchemaResponse { SchemaText = reply.SchemaText };
    }

    public override async Task<V1::WriteSchemaResponse> WriteSchema(
        V1::WriteSchemaRequest request, ServerCallContext context)
    {
        try
        {
            // v1 WriteSchemaResponse is empty in this snapshot; the written-at token is discarded.
            await Relationships.WriteSchema(new WriteSchemaArgs(request.Schema));
            return new V1::WriteSchemaResponse();
        }
        catch (Exception ex) when (ex is Spiceport.Schema.SchemaCompileException or ArgumentException)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (SchemaWriteValidationException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
    }
}
