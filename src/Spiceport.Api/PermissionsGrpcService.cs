using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Spiceport.Core;
using Spiceport.Grains.Abstractions;
using Spiceport.Protos;

namespace Spiceport.Api;

/// <summary>
/// gRPC front door: translates the proto <see cref="CheckPermissionRequest"/> into the grain
/// <see cref="CheckRequest"/>, invokes the single stateless-worker check grain, and maps the
/// verdict back to the proto permissionship.
/// </summary>
public sealed class PermissionsGrpcService(IGrainFactory grains)
    : PermissionsService.PermissionsServiceBase
{
    public override async Task<CheckPermissionResponse> CheckPermission(
        CheckPermissionRequest request, ServerCallContext context)
    {
        var grain = grains.GetGrain<ICheckGrain>(0);

        var subjectRelation = string.IsNullOrEmpty(request.Subject.OptionalRelation)
            ? CoreConstants.Ellipsis
            : request.Subject.OptionalRelation;

        var reply = await grain.Check(new CheckRequest(
            request.Resource.ObjectType,
            request.Resource.ObjectId,
            request.Permission,
            request.Subject.Object.ObjectType,
            request.Subject.Object.ObjectId,
            subjectRelation,
            StructToDict(request.Context)));

        var ship = reply.Verdict switch
        {
            CheckVerdict.Member => CheckPermissionResponse.Types.Permissionship.HasPermission,
            CheckVerdict.Caveated => CheckPermissionResponse.Types.Permissionship.ConditionalPermission,
            _ => CheckPermissionResponse.Types.Permissionship.NoPermission,
        };

        var resp = new CheckPermissionResponse { Permissionship = ship };
        resp.PartialCaveatMissingFields.AddRange(reply.MissingFields);
        return resp;
    }

    private static IReadOnlyDictionary<string, object?>? StructToDict(Struct? s)
    {
        if (s is null || s.Fields.Count == 0)
        {
            return null;
        }

        var d = new Dictionary<string, object?>();
        foreach (var (k, v) in s.Fields)
        {
            d[k] = ValueToObject(v);
        }

        return d;
    }

    private static object? ValueToObject(Value v) => v.KindCase switch
    {
        Value.KindOneofCase.NullValue => null,
        Value.KindOneofCase.NumberValue => v.NumberValue,
        Value.KindOneofCase.StringValue => v.StringValue,
        Value.KindOneofCase.BoolValue => v.BoolValue,
        Value.KindOneofCase.StructValue =>
            v.StructValue.Fields.ToDictionary(p => p.Key, p => ValueToObject(p.Value)),
        Value.KindOneofCase.ListValue =>
            v.ListValue.Values.Select(ValueToObject).ToList(),
        _ => null,
    };
}
