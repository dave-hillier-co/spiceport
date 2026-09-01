using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Spiceport.Core;
using Spiceport.Datastore;
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
using Relationship = Spiceport.Protos.Relationship;
using RelationshipUpdate = Spiceport.Protos.RelationshipUpdate;
using ContextualizedCaveat = Spiceport.Protos.ContextualizedCaveat;
using RelationshipFilter = Spiceport.Protos.RelationshipFilter;
using ObjectReference = Spiceport.Protos.ObjectReference;
using SubjectReference = Spiceport.Protos.SubjectReference;
using ZedToken = Spiceport.Protos.ZedToken;
using ProtoConsistency = Spiceport.Protos.Consistency;
using WireConsistency = Spiceport.Grains.Abstractions.ConsistencyWire;
using WireConsistencyMode = Spiceport.Grains.Abstractions.ConsistencyModeWire;

namespace Spiceport.Api;

/// <summary>
/// gRPC front door: translates the proto <see cref="CheckPermissionRequest"/> into a top-level
/// permission check, dispatches it through the silo-wide Caching-over-Orleans root dispatcher (so the
/// recursion runs across the grain mesh), and maps the verdict back to the proto permissionship.
/// The reverse / tree ops run in-process via <see cref="Grains.ReverseOps"/>
/// and their replies are mapped back to proto.
/// </summary>
public sealed class PermissionsGrpcService(
    IPermissionChecker checker, IGrainFactory grains, Grains.ReverseOps reverseOps, Grains.RelationshipReads relationshipReads)
    : PermissionsService.PermissionsServiceBase
{
    private IRelationshipsGrain Relationships => grains.GetGrain<IRelationshipsGrain>(IRelationshipsGrain.Key);

    /// <summary>
    /// Drains an in-process stream up to <paramref name="limit"/> items. When a further item exists beyond
    /// the limit, the last kept item's cursor is returned as the continuation cursor (mirroring the prior
    /// grain's "one extra row detects more" paging); an unlimited (null) drain returns no cursor.
    /// </summary>
    private static async Task<(List<T> Items, string? Cursor)> Drain<T>(
        IAsyncEnumerable<T> stream, int? limit, Func<T, string?> cursorOf, CancellationToken ct)
    {
        var items = new List<T>();
        string? cursor = null;
        await foreach (var item in stream.WithCancellation(ct))
        {
            if (limit is { } cap && items.Count >= cap)
            {
                cursor = cursorOf(items[^1]);
                break;
            }
            items.Add(item);
        }
        return (items, cursor);
    }
    /// <summary>
    /// Maps a cross-silo dispatch failure surfaced by <c>OrleansDispatcher</c> onto its deliberately
    /// chosen gRPC status (cf. SpiceDB <c>rewriteError</c>): transient transport/silo-availability is
    /// retriable <c>Unavailable</c>; cancellation is <c>Cancelled</c>; a deadline is
    /// <c>DeadlineExceeded</c>; anything else is <c>Internal</c>.
    /// </summary>
    private static RpcException ToRpc(DispatchFailedException ex) =>
        new(new Status(
            ex.Code switch
            {
                DispatchErrorCode.Unavailable => StatusCode.Unavailable,
                DispatchErrorCode.Cancelled => StatusCode.Cancelled,
                DispatchErrorCode.DeadlineExceeded => StatusCode.DeadlineExceeded,
                _ => StatusCode.Internal,
            },
            ex.Message));

    private static RpcException ToRpc(CaveatEvaluationException ex) =>
        new(new Status(
            ex.Kind == CaveatEvaluationErrorKind.ParameterTypeMismatch
                ? StatusCode.InvalidArgument
                : StatusCode.FailedPrecondition,
            ex.Message));

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

        PermissionCheckResult result;
        try
        {
            result = await checker.Check(
                request.Resource.ObjectType,
                request.Resource.ObjectId,
                request.Permission,
                subject,
                StructToDict(request.Context),
                ToWire(request.Consistency).ToRequirement(),
                context.CancellationToken);
        }
        catch (InvalidConsistencyTokenException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (RevisionNotFoundException ex)
        {
            // The pinned revision has been garbage-collected (or never existed): same client-facing
            // contract as an invalid consistency token.
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (MaxDepthExceededException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (CaveatEvaluationException ex)
        {
            throw ToRpc(ex);
        }
        catch (DispatchFailedException ex)
        {
            throw ToRpc(ex);
        }

        var ship = result.Verdict switch
        {
            Membership.Member => CheckPermissionResponse.Types.Permissionship.HasPermission,
            Membership.Caveated => CheckPermissionResponse.Types.Permissionship.ConditionalPermission,
            _ => CheckPermissionResponse.Types.Permissionship.NoPermission,
        };

        var resp = new CheckPermissionResponse
        {
            Permissionship = ship,
            CheckedAt = new ZedToken { Token = result.EvaluatedToken },
        };
        resp.PartialCaveatMissingFields.AddRange(result.MissingFields);
        return resp;
    }

    public override async Task<BatchCheckPermissionResponse> BatchCheckPermission(
        BatchCheckPermissionRequest request, ServerCallContext context)
    {
        var items = request.Items.Select(it =>
        {
            var subjectRelation = string.IsNullOrEmpty(it.Subject.OptionalRelation)
                ? CoreConstants.Ellipsis
                : it.Subject.OptionalRelation;
            return new BatchCheckItem(
                it.Resource.ObjectType,
                it.Resource.ObjectId,
                it.Permission,
                new ObjectAndRelation(it.Subject.Object.ObjectType, it.Subject.Object.ObjectId, subjectRelation),
                StructToDict(it.Context));
        }).ToList();

        BatchCheckResult result;
        try
        {
            result = await checker.BatchCheck(
                items,
                ToWire(request.Consistency).ToRequirement(),
                context.CancellationToken);
        }
        catch (InvalidConsistencyTokenException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (RevisionNotFoundException ex)
        {
            // The pinned revision has been garbage-collected (or never existed): same client-facing
            // contract as an invalid consistency token.
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (DispatchFailedException ex)
        {
            throw ToRpc(ex);
        }

        var resp = new BatchCheckPermissionResponse
        {
            CheckedAt = new ZedToken { Token = result.EvaluatedToken },
        };
        for (var i = 0; i < result.Items.Count; i++)
        {
            var verdict = result.Items[i];
            var ship = verdict.Verdict switch
            {
                Membership.Member => CheckPermissionResponse.Types.Permissionship.HasPermission,
                Membership.Caveated => CheckPermissionResponse.Types.Permissionship.ConditionalPermission,
                _ => CheckPermissionResponse.Types.Permissionship.NoPermission,
            };
            var item = new BatchCheckPermissionResponseItem { Permissionship = ship };
            item.PartialCaveatMissingFields.AddRange(verdict.MissingFields);
            resp.Pairs.Add(new BatchCheckPermissionPair { Request = request.Items[i], Item = item });
        }

        return resp;
    }

    public override async Task<WriteSchemaResponse> WriteSchema(
        WriteSchemaRequest request, ServerCallContext context)
    {
        try
        {
            var reply = await Relationships.WriteSchema(new WriteSchemaArgs(request.Schema));
            return new WriteSchemaResponse { WrittenAt = new ZedToken { Token = reply.WrittenAtToken } };
        }
        catch (Exception ex) when (ex is Spiceport.Schema.SchemaCompileException or ArgumentException)
        {
            // The grain surfaces a compile failure as a serializable ArgumentException across the grain
            // boundary; in-process calls may still see SchemaCompileException directly.
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (SchemaWriteValidationException ex)
        {
            // The change would orphan existing relationships: reject and commit nothing.
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (SequencerOverloadedException ex)
        {
            // The per-silo admission gate shed this commit — the sequencer is saturated. A deliberate,
            // retryable overload signal (back off and retry), never an opaque timeout.
            throw new RpcException(new Status(StatusCode.ResourceExhausted, ex.Message));
        }
    }

    public override async Task<ReadSchemaResponse> ReadSchema(
        ReadSchemaRequest request, ServerCallContext context)
    {
        var reply = await Relationships.ReadSchema();
        return new ReadSchemaResponse
        {
            SchemaText = reply.SchemaText,
            ReadAt = new ZedToken { Token = reply.ReadAtToken },
        };
    }

    public override async Task<WriteRelationshipsResponse> WriteRelationships(
        WriteRelationshipsRequest request, ServerCallContext context)
    {
        var updates = request.Updates.Select(ToWire).ToList();
        var preconditions = ToWire(request.OptionalPreconditions);
        try
        {
            var reply = await Relationships.WriteRelationships(new WriteRelationshipsArgs(updates, preconditions));
            return new WriteRelationshipsResponse { WrittenAt = new ZedToken { Token = reply.WrittenAtToken } };
        }
        catch (PreconditionFailedException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (SequencerOverloadedException ex)
        {
            // Sequencer overload shed by the admission gate: retryable RESOURCE_EXHAUSTED.
            throw new RpcException(new Status(StatusCode.ResourceExhausted, ex.Message));
        }
    }

    public override async Task<ReadRelationshipsResponse> ReadRelationships(
        ReadRelationshipsRequest request, ServerCallContext context)
    {
        var limit = request.OptionalLimit == 0 ? (int?)null : (int)request.OptionalLimit;
        List<RelationshipStreamItem> items;
        string? cursor;
        try
        {
            (items, cursor) = await Drain(
                relationshipReads.ReadRelationships(
                    new ReadRelationshipsArgs(
                        ToWire(request.Filter),
                        limit,
                        NullIfEmpty(request.OptionalCursor),
                        ToWire(request.Consistency)),
                    context.CancellationToken),
                limit, i => i.ResumeCursor, context.CancellationToken);
        }
        catch (InvalidConsistencyTokenException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (RevisionNotFoundException ex)
        {
            // The pinned revision has been garbage-collected (or never existed): same client-facing
            // contract as an invalid consistency token.
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }

        var resp = new ReadRelationshipsResponse
        {
            AfterResultCursor = cursor ?? string.Empty,
            ReadAt = new ZedToken { Token = items.Count > 0 ? items[0].ReadAtToken : string.Empty },
        };
        resp.Relationships.AddRange(items.Select(i => ToProto(i.Relationship)));
        return resp;
    }

    public override async Task<DeleteRelationshipsResponse> DeleteRelationships(
        DeleteRelationshipsRequest request, ServerCallContext context)
    {
        try
        {
            var reply = await Relationships.DeleteRelationships(new DeleteRelationshipsArgs(
                ToWire(request.Filter),
                request.OptionalLimit == 0 ? null : request.OptionalLimit,
                ToWire(request.OptionalPreconditions)));

            return new DeleteRelationshipsResponse
            {
                DeletedCount = reply.DeletedCount,
                ReachedLimit = reply.ReachedLimit,
                DeletedAt = new ZedToken { Token = reply.DeletedAtToken },
            };
        }
        catch (PreconditionFailedException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (SequencerOverloadedException ex)
        {
            // Sequencer overload shed by the admission gate: retryable RESOURCE_EXHAUSTED.
            throw new RpcException(new Status(StatusCode.ResourceExhausted, ex.Message));
        }
    }

    private static IReadOnlyList<PreconditionWire>? ToWire(
        Google.Protobuf.Collections.RepeatedField<Spiceport.Protos.Precondition> preconditions)
    {
        if (preconditions.Count == 0)
            return null;

        return preconditions.Select(p => new PreconditionWire(
            p.Operation == Spiceport.Protos.Precondition.Types.Operation.MustNotMatch
                ? PreconditionOpWire.MustNotMatch
                : PreconditionOpWire.MustMatch,
            ToWire(p.Filter))).ToList();
    }

    private static RelationshipUpdateWire ToWire(RelationshipUpdate u)
    {
        var op = u.Operation switch
        {
            RelationshipUpdate.Types.Operation.Create => RelationshipUpdateOpWire.Create,
            RelationshipUpdate.Types.Operation.Delete => RelationshipUpdateOpWire.Delete,
            _ => RelationshipUpdateOpWire.Touch,
        };
        return new RelationshipUpdateWire(op, ToWire(u.Relationship));
    }

    private static RelationshipWire ToWire(Relationship r)
    {
        var subjectRelation = string.IsNullOrEmpty(r.Subject.OptionalRelation)
            ? CoreConstants.Ellipsis
            : r.Subject.OptionalRelation;
        DateTimeOffset? expiration = r.OptionalExpiresAtUnixSeconds == 0
            ? null
            : DateTimeOffset.FromUnixTimeSeconds(r.OptionalExpiresAtUnixSeconds);
        return new RelationshipWire(
            r.Resource.ObjectType, r.Resource.ObjectId, r.ResourceRelation,
            r.Subject.Object.ObjectType, r.Subject.Object.ObjectId, subjectRelation,
            r.OptionalCaveat is { CaveatName.Length: > 0 } c ? c.CaveatName : null,
            r.OptionalCaveat is { } cc ? StructToDict(cc.Context) : null,
            expiration);
    }

    private static Relationship ToProto(RelationshipWire w)
    {
        var rel = new Relationship
        {
            Resource = new ObjectReference { ObjectType = w.ResourceType, ObjectId = w.ResourceId },
            ResourceRelation = w.ResourceRelation,
            Subject = new SubjectReference
            {
                Object = new ObjectReference { ObjectType = w.SubjectType, ObjectId = w.SubjectId },
                OptionalRelation = w.SubjectRelation == CoreConstants.Ellipsis ? string.Empty : w.SubjectRelation,
            },
            OptionalExpiresAtUnixSeconds = w.Expiration is { } e ? e.ToUnixTimeSeconds() : 0,
        };
        if (w.CaveatName is { Length: > 0 })
        {
            rel.OptionalCaveat = new ContextualizedCaveat
            {
                CaveatName = w.CaveatName,
                Context = DictToStruct(w.CaveatContext),
            };
        }

        return rel;
    }

    private static RelationshipsFilterWire ToWire(RelationshipFilter f) => new(
        NullIfEmpty(f.ResourceType),
        NullIfEmpty(f.OptionalResourceIdPrefix),
        f.OptionalResourceIds.Count > 0 ? f.OptionalResourceIds.ToList() : null,
        NullIfEmpty(f.OptionalResourceRelation),
        NullIfEmpty(f.OptionalSubjectType),
        f.OptionalSubjectIds.Count > 0 ? f.OptionalSubjectIds.ToList() : null,
        NullIfEmpty(f.OptionalSubjectRelation));

    public override async Task<ExpandPermissionTreeResponse> ExpandPermissionTree(
        ExpandPermissionTreeRequest request, ServerCallContext context)
    {
        var mode = request.Mode == ExpandPermissionTreeRequest.Types.ExpandMode.Recursive
            ? ExpandModeWire.Recursive
            : ExpandModeWire.Shallow;

        ExpandTreeReply reply;
        try
        {
            reply = await reverseOps.ExpandPermissionTree(new ExpandTreeArgs(
                request.Resource.ObjectType,
                request.Resource.ObjectId,
                request.Permission,
                mode,
                ToWire(request.Consistency)));
        }
        catch (InvalidConsistencyTokenException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (RevisionNotFoundException ex)
        {
            // The pinned revision has been garbage-collected (or never existed): same client-facing
            // contract as an invalid consistency token.
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (DispatchFailedException ex)
        {
            throw ToRpc(ex);
        }

        return new ExpandPermissionTreeResponse
        {
            TreeRoot = ToProto(reply.Root),
            ExpandedAt = new ZedToken { Token = reply.ExpandedAtToken },
        };
    }

    public override async Task<LookupSubjectsResponse> LookupSubjects(
        LookupSubjectsRequest request, ServerCallContext context)
    {
        var subjectRelation = string.IsNullOrEmpty(request.OptionalSubjectRelation)
            ? CoreConstants.Ellipsis
            : request.OptionalSubjectRelation;

        var limit = request.OptionalLimit == 0 ? (int?)null : (int)request.OptionalLimit;
        List<FoundSubjectStreamItem> items;
        string? cursor;
        try
        {
            (items, cursor) = await Drain(
                reverseOps.StreamLookupSubjects(
                    new LookupSubjectsArgs(
                        request.Resource.ObjectType,
                        request.Resource.ObjectId,
                        request.Permission,
                        request.SubjectObjectType,
                        subjectRelation,
                        StructToDict(request.Context),
                        limit,
                        NullIfEmpty(request.OptionalCursor),
                        ToWire(request.Consistency)),
                    context.CancellationToken),
                limit, i => i.ResumeCursor, context.CancellationToken);
        }
        catch (InvalidConsistencyTokenException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (RevisionNotFoundException ex)
        {
            // The pinned revision has been garbage-collected (or never existed): same client-facing
            // contract as an invalid consistency token.
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (CaveatEvaluationException ex)
        {
            throw ToRpc(ex);
        }
        catch (DispatchFailedException ex)
        {
            throw ToRpc(ex);
        }

        var resp = new LookupSubjectsResponse
        {
            AfterResultCursor = cursor ?? string.Empty,
            LookedUpAt = new ZedToken { Token = items.Count > 0 ? items[0].LookedUpAtToken : string.Empty },
        };
        resp.Subjects.AddRange(items.Select(i => ToProto(i.Subject)));
        return resp;
    }

    public override async Task<LookupResourcesResponse> LookupResources(
        LookupResourcesRequest request, ServerCallContext context)
    {
        var subjectRelation = string.IsNullOrEmpty(request.Subject.OptionalRelation)
            ? CoreConstants.Ellipsis
            : request.Subject.OptionalRelation;

        var limit = request.OptionalLimit == 0 ? (int?)null : (int)request.OptionalLimit;
        List<WireFoundResource> items;
        string? cursor;
        try
        {
            (items, cursor) = await Drain(
                reverseOps.StreamLookupResources(
                    new LookupResourcesArgs(
                        request.ResourceObjectType,
                        request.Permission,
                        request.Subject.Object.ObjectType,
                        request.Subject.Object.ObjectId,
                        subjectRelation,
                        StructToDict(request.Context),
                        limit,
                        NullIfEmpty(request.OptionalCursor),
                        ToWire(request.Consistency)),
                    context.CancellationToken),
                limit, r => r.AfterResultCursor, context.CancellationToken);
        }
        catch (InvalidConsistencyTokenException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (RevisionNotFoundException ex)
        {
            // The pinned revision has been garbage-collected (or never existed): same client-facing
            // contract as an invalid consistency token.
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (CaveatEvaluationException ex)
        {
            throw ToRpc(ex);
        }
        catch (DispatchFailedException ex)
        {
            throw ToRpc(ex);
        }

        var resp = new LookupResourcesResponse
        {
            AfterResultCursor = cursor ?? string.Empty,
            LookedUpAt = new ZedToken { Token = items.Count > 0 ? items[0].LookedUpAtToken : string.Empty },
        };
        resp.Resources.AddRange(items.Select(ToProto));
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

    /// <summary>
    /// Maps the proto <see cref="ProtoConsistency"/> oneof to the cross-grain
    /// <see cref="WireConsistency"/>. An absent message (null) or an unset oneof is treated as
    /// minimize-latency — the server default — so existing clients are unchanged.
    /// </summary>
    private static WireConsistency ToWire(ProtoConsistency? consistency) => consistency?.RequirementCase switch
    {
        ProtoConsistency.RequirementOneofCase.AtLeastAsFresh =>
            new WireConsistency(WireConsistencyMode.AtLeastAsFresh, consistency.AtLeastAsFresh.Token),
        ProtoConsistency.RequirementOneofCase.FullyConsistent =>
            new WireConsistency(WireConsistencyMode.FullyConsistent),
        ProtoConsistency.RequirementOneofCase.AtExactSnapshot =>
            new WireConsistency(WireConsistencyMode.AtExactSnapshot, consistency.AtExactSnapshot.Token),
        // minimize_latency, or unset / absent.
        _ => WireConsistency.MinimizeLatency,
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

    private static Struct? DictToStruct(IReadOnlyDictionary<string, object?>? dict)
    {
        if (dict is null || dict.Count == 0)
        {
            return null;
        }

        var s = new Struct();
        foreach (var (k, v) in dict)
        {
            s.Fields[k] = ObjectToValue(v);
        }

        return s;
    }

    private static Value ObjectToValue(object? o) => o switch
    {
        null => Value.ForNull(),
        bool b => Value.ForBool(b),
        string str => Value.ForString(str),
        double d => Value.ForNumber(d),
        int i => Value.ForNumber(i),
        long l => Value.ForNumber(l),
        IReadOnlyDictionary<string, object?> map => Value.ForStruct(DictToStruct(map) ?? new Struct()),
        System.Collections.IEnumerable list => Value.ForList(
            list.Cast<object?>().Select(ObjectToValue).ToArray()),
        _ => Value.ForString(o.ToString() ?? string.Empty),
    };

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
