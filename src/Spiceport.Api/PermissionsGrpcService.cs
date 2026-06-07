using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Spiceport.Core;
using Spiceport.Engine;
using Spiceport.Grains;
using Spiceport.Protos;

namespace Spiceport.Api;

/// <summary>
/// gRPC front door: translates the proto <see cref="CheckPermissionRequest"/> into a top-level
/// permission check, dispatches it through the silo-wide Caching-over-Orleans root dispatcher (so the
/// recursion runs across the grain mesh), and maps the verdict back to the proto permissionship.
/// </summary>
public sealed class PermissionsGrpcService(IPermissionChecker checker)
    : PermissionsService.PermissionsServiceBase
{
    public override async Task<CheckPermissionResponse> CheckPermission(
        CheckPermissionRequest request, ServerCallContext context)
    {
        var subjectRelation = string.IsNullOrEmpty(request.Subject.OptionalRelation)
            ? CoreConstants.Ellipsis
            : request.Subject.OptionalRelation;

        var subject = new ObjectAndRelation(
            request.Subject.Object.ObjectType,
            request.Subject.Object.ObjectId,
            subjectRelation);

        var result = await checker.Check(
            request.Resource.ObjectType,
            request.Resource.ObjectId,
            request.Permission,
            subject,
            StructToDict(request.Context),
            context.CancellationToken);

        var ship = result.Verdict switch
        {
            Membership.Member => CheckPermissionResponse.Types.Permissionship.HasPermission,
            Membership.Caveated => CheckPermissionResponse.Types.Permissionship.ConditionalPermission,
            _ => CheckPermissionResponse.Types.Permissionship.NoPermission,
        };

        var resp = new CheckPermissionResponse { Permissionship = ship };
        resp.PartialCaveatMissingFields.AddRange(result.MissingFields);
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
