using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Spiceport.Core;
using Spiceport.Engine;
using Spiceport.Grains;
using Spiceport.Grains.Abstractions;
using Spiceport.Protos;
using WireFoundResource = Spiceport.Grains.Abstractions.FoundResourceWire;
using WireFoundSubject = Spiceport.Grains.Abstractions.FoundSubjectWire;
using WirePermissionship = Spiceport.Grains.Abstractions.Permissionship;
using ProtoPermissionship = Spiceport.Protos.Permissionship;
using ProtoFoundResource = Spiceport.Protos.FoundResource;
using ProtoFoundSubject = Spiceport.Protos.FoundSubject;
using ProtoTreeNode = Spiceport.Protos.PermissionTreeNode;

namespace Spiceport.Api;

/// <summary>
/// gRPC front door: translates the proto <see cref="CheckPermissionRequest"/> into a top-level
/// permission check, dispatches it through the silo-wide Caching-over-Orleans root dispatcher (so the
/// recursion runs across the grain mesh), and maps the verdict back to the proto permissionship.
/// The three reverse / tree ops are routed to the stateless-worker <see cref="IReverseOpsGrain"/>
/// (keyed by the constant <see cref="IReverseOpsGrain.Key"/>) and its replies mapped back to proto.
/// </summary>
public sealed class PermissionsGrpcService(IPermissionChecker checker, IGrainFactory grains)
    : PermissionsService.PermissionsServiceBase
{
    private IReverseOpsGrain ReverseOps => grains.GetGrain<IReverseOpsGrain>(IReverseOpsGrain.Key);
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

    public override async Task<ExpandPermissionTreeResponse> ExpandPermissionTree(
        ExpandPermissionTreeRequest request, ServerCallContext context)
    {
        var mode = request.Mode == ExpandPermissionTreeRequest.Types.ExpandMode.Recursive
            ? ExpandModeWire.Recursive
            : ExpandModeWire.Shallow;

        var reply = await ReverseOps.ExpandPermissionTree(new ExpandTreeArgs(
            request.Resource.ObjectType,
            request.Resource.ObjectId,
            request.Permission,
            mode));

        return new ExpandPermissionTreeResponse { TreeRoot = ToProto(reply.Root) };
    }

    public override async Task<LookupSubjectsResponse> LookupSubjects(
        LookupSubjectsRequest request, ServerCallContext context)
    {
        var subjectRelation = string.IsNullOrEmpty(request.OptionalSubjectRelation)
            ? CoreConstants.Ellipsis
            : request.OptionalSubjectRelation;

        var reply = await ReverseOps.LookupSubjects(new LookupSubjectsArgs(
            request.Resource.ObjectType,
            request.Resource.ObjectId,
            request.Permission,
            request.SubjectObjectType,
            subjectRelation,
            StructToDict(request.Context),
            (int)request.OptionalLimit,
            NullIfEmpty(request.OptionalCursor)));

        var resp = new LookupSubjectsResponse { AfterResultCursor = reply.Cursor ?? string.Empty };
        resp.Subjects.AddRange(reply.Subjects.Select(ToProto));
        return resp;
    }

    public override async Task<LookupResourcesResponse> LookupResources(
        LookupResourcesRequest request, ServerCallContext context)
    {
        var subjectRelation = string.IsNullOrEmpty(request.Subject.OptionalRelation)
            ? CoreConstants.Ellipsis
            : request.Subject.OptionalRelation;

        var reply = await ReverseOps.LookupResources(new LookupResourcesArgs(
            request.ResourceObjectType,
            request.Permission,
            request.Subject.Object.ObjectType,
            request.Subject.Object.ObjectId,
            subjectRelation,
            StructToDict(request.Context),
            (int)request.OptionalLimit,
            NullIfEmpty(request.OptionalCursor)));

        var resp = new LookupResourcesResponse { AfterResultCursor = reply.Cursor ?? string.Empty };
        resp.Resources.AddRange(reply.Resources.Select(ToProto));
        return resp;
    }

    private static ProtoPermissionship ToProto(WirePermissionship p)
    {
        var result = new ProtoPermissionship
        {
            Kind = p.IsCaveated
                ? ProtoPermissionship.Types.Kind.ConditionalPermission
                : ProtoPermissionship.Types.Kind.HasPermission,
        };
        result.PartialCaveatMissingFields.AddRange(p.MissingContextParams);
        return result;
    }

    private static ProtoFoundSubject ToProto(WireFoundSubject s) => new()
    {
        SubjectObjectId = s.SubjectId,
        IsWildcard = s.IsWildcard,
        Permissionship = ToProto(s.Permissionship),
    };

    private static ProtoFoundResource ToProto(WireFoundResource r) => new()
    {
        ResourceObjectId = r.ResourceId,
        Permissionship = ToProto(r.Permissionship),
    };

    private static ProtoTreeNode ToProto(ExpandTreeNodeWire node)
    {
        var result = new ProtoTreeNode
        {
            ExpandedObject = new ObjectReference
            {
                ObjectType = node.ExpandedType,
                ObjectId = node.ExpandedId,
            },
            ExpandedRelation = node.ExpandedRelation,
        };
        result.CaveatMissingFields.AddRange(node.CaveatMissingFields);

        if (node.IsLeaf)
        {
            var leaf = new ProtoTreeNode.Types.LeafNode();
            leaf.Subjects.AddRange(node.Subjects.Select(ToProto));
            result.Leaf = leaf;
        }
        else
        {
            var setOp = new ProtoTreeNode.Types.SetOpNode { Operation = ToProto(node.Operation) };
            setOp.Children.AddRange(node.Children.Select(ToProto));
            result.SetOp = setOp;
        }

        return result;
    }

    private static ProtoTreeNode.Types.DirectSubject ToProto(ExpandSubjectWire s)
    {
        var result = new ProtoTreeNode.Types.DirectSubject
        {
            Subject = new SubjectReference
            {
                Object = new ObjectReference { ObjectType = s.SubjectType, ObjectId = s.SubjectId },
                OptionalRelation = s.SubjectRelation == CoreConstants.Ellipsis ? string.Empty : s.SubjectRelation,
            },
            IsWildcard = s.IsWildcard,
        };
        result.CaveatMissingFields.AddRange(s.CaveatMissingFields);
        return result;
    }

    private static ProtoTreeNode.Types.SetOpNode.Types.Operation ToProto(SetOpWire op) => op switch
    {
        SetOpWire.Union => ProtoTreeNode.Types.SetOpNode.Types.Operation.Union,
        SetOpWire.Intersection => ProtoTreeNode.Types.SetOpNode.Types.Operation.Intersection,
        SetOpWire.Exclusion => ProtoTreeNode.Types.SetOpNode.Types.Operation.Exclusion,
        _ => ProtoTreeNode.Types.SetOpNode.Types.Operation.Unspecified,
    };

    private static string? NullIfEmpty(string s) => string.IsNullOrEmpty(s) ? null : s;

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
