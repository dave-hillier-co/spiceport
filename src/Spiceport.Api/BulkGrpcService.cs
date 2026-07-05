using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;
using Spiceport.Protos;
using ProtoRelationship = Spiceport.Protos.Relationship;
using ProtoObjectReference = Spiceport.Protos.ObjectReference;
using ProtoSubjectReference = Spiceport.Protos.SubjectReference;
using ProtoContextualizedCaveat = Spiceport.Protos.ContextualizedCaveat;
using ProtoConsistency = Spiceport.Protos.Consistency;
using ProtoRelationshipFilter = Spiceport.Protos.RelationshipFilter;
using WireConsistency = Spiceport.Grains.Abstractions.ConsistencyWire;
using WireConsistencyMode = Spiceport.Grains.Abstractions.ConsistencyModeWire;
using ZedToken = Spiceport.Protos.ZedToken;

namespace Spiceport.Api;

/// <summary>
/// gRPC front door for streaming bulk import / export of relationships. Both RPCs are streaming but the
/// data-plane grain stays request/response: this service consumes the client import stream and calls the
/// grain ONCE PER inbound batch (one write transaction per batch), and drives the server export stream by
/// paging the grain over a single pinned snapshot. Mirrors authzed.api.v1 ImportBulk / ExportBulk.
/// </summary>
public sealed class BulkGrpcService(IGrainFactory grains)
    : Spiceport.Protos.BulkService.BulkServiceBase
{
    private IRelationshipsGrain Relationships => grains.GetGrain<IRelationshipsGrain>(IRelationshipsGrain.Key);

    public override async Task<ImportBulkRelationshipsResponse> ImportBulkRelationships(
        IAsyncStreamReader<ImportBulkRelationshipsRequest> requestStream,
        ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(requestStream);

        ulong total = 0;
        string? lastToken = null;

        // Consume the client stream batch by batch. Each batch is one all-or-nothing write transaction in
        // the grain; the load is additive across the whole stream. Empty batches are skipped so they do
        // not commit empty transactions.
        while (await requestStream.MoveNext(context.CancellationToken))
        {
            var batch = requestStream.Current;
            if (batch.Relationships.Count == 0)
                continue;

            var relationships = batch.Relationships.Select(ToWire).ToList();
            var reply = await Relationships.BulkImportRelationships(
                new BulkImportRelationshipsArgs(relationships));
            total += reply.NumLoaded;
            lastToken = reply.LoadedAtToken;
        }

        var response = new ImportBulkRelationshipsResponse { NumLoaded = total };
        if (lastToken is not null)
            response.LoadedAt = new ZedToken { Token = lastToken };
        return response;
    }

    public override async Task ExportBulkRelationships(
        ExportBulkRelationshipsRequest request,
        IServerStreamWriter<ExportBulkRelationshipsResponse> responseStream,
        ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(responseStream);

        var filter = request.OptionalFilter is { } f ? ToWire(f) : EmptyFilter;
        var limit = (int)request.OptionalLimit;
        var consistency = ToWire(request.Consistency);
        var cursor = string.IsNullOrEmpty(request.OptionalCursor) ? null : request.OptionalCursor;

        // Page the grain over a single pinned snapshot. The first page resolves and pins the revision and
        // returns a cursor encoding it; subsequent pages pass that cursor back so every page reads the
        // SAME snapshot. Keep going while the grain hands back a continuation cursor.
        while (!context.CancellationToken.IsCancellationRequested)
        {
            BulkExportRelationshipsReply reply;
            try
            {
                reply = await Relationships.BulkExportRelationships(
                    new BulkExportRelationshipsArgs(filter, limit, cursor, consistency));
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
            catch (FormatException ex)
            {
                throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
            }

            if (reply.Relationships.Count == 0)
                break;

            var response = new ExportBulkRelationshipsResponse
            {
                AfterCursor = reply.Cursor ?? string.Empty,
            };
            response.Relationships.AddRange(reply.Relationships.Select(ToProto));
            await responseStream.WriteAsync(response);

            if (reply.Cursor is null)
                break;
            cursor = reply.Cursor;
        }
    }

    private static readonly RelationshipsFilterWire EmptyFilter =
        new(null, null, null, null, null, null, null);

    private static RelationshipsFilterWire ToWire(ProtoRelationshipFilter f) => new(
        NullIfEmpty(f.ResourceType),
        NullIfEmpty(f.OptionalResourceIdPrefix),
        f.OptionalResourceIds.Count > 0 ? f.OptionalResourceIds.ToList() : null,
        NullIfEmpty(f.OptionalResourceRelation),
        NullIfEmpty(f.OptionalSubjectType),
        f.OptionalSubjectIds.Count > 0 ? f.OptionalSubjectIds.ToList() : null,
        NullIfEmpty(f.OptionalSubjectRelation));

    private static RelationshipWire ToWire(ProtoRelationship r)
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

    private static ProtoRelationship ToProto(RelationshipWire w)
    {
        var rel = new ProtoRelationship
        {
            Resource = new ProtoObjectReference { ObjectType = w.ResourceType, ObjectId = w.ResourceId },
            ResourceRelation = w.ResourceRelation,
            Subject = new ProtoSubjectReference
            {
                Object = new ProtoObjectReference { ObjectType = w.SubjectType, ObjectId = w.SubjectId },
                OptionalRelation = w.SubjectRelation == CoreConstants.Ellipsis ? string.Empty : w.SubjectRelation,
            },
            OptionalExpiresAtUnixSeconds = w.Expiration is { } e ? e.ToUnixTimeSeconds() : 0,
        };
        if (w.CaveatName is { Length: > 0 })
        {
            rel.OptionalCaveat = new ProtoContextualizedCaveat
            {
                CaveatName = w.CaveatName,
                Context = DictToStruct(w.CaveatContext),
            };
        }

        return rel;
    }

    private static WireConsistency ToWire(ProtoConsistency? consistency) => consistency?.RequirementCase switch
    {
        ProtoConsistency.RequirementOneofCase.AtLeastAsFresh =>
            new WireConsistency(WireConsistencyMode.AtLeastAsFresh, consistency.AtLeastAsFresh.Token),
        ProtoConsistency.RequirementOneofCase.FullyConsistent =>
            new WireConsistency(WireConsistencyMode.FullyConsistent),
        ProtoConsistency.RequirementOneofCase.AtExactSnapshot =>
            new WireConsistency(WireConsistencyMode.AtExactSnapshot, consistency.AtExactSnapshot.Token),
        _ => WireConsistency.MinimizeLatency,
    };

    private static string? NullIfEmpty(string s) => string.IsNullOrEmpty(s) ? null : s;

    private static IReadOnlyDictionary<string, object?>? StructToDict(Struct? s)
    {
        if (s is null || s.Fields.Count == 0)
            return null;

        var d = new Dictionary<string, object?>();
        foreach (var (k, v) in s.Fields)
            d[k] = ValueToObject(v);
        return d;
    }

    private static Struct? DictToStruct(IReadOnlyDictionary<string, object?>? dict)
    {
        if (dict is null || dict.Count == 0)
            return null;

        var s = new Struct();
        foreach (var (k, v) in dict)
            s.Fields[k] = ObjectToValue(v);
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
