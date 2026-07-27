using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains;
using Spiceport.Grains.Abstractions;
using V1 = Authzed.Api.V1;
using WirePermissionship = Spiceport.Grains.Abstractions.Permissionship;
using WireConsistency = Spiceport.Grains.Abstractions.ConsistencyWire;
using WireConsistencyMode = Spiceport.Grains.Abstractions.ConsistencyModeWire;

namespace Spiceport.Api;

/// <summary>
/// gRPC front door for the <c>authzed.api.v1.PermissionsService</c>. A pure translation layer over the
/// SAME grain mesh and in-process read helpers the internal <see cref="PermissionsGrpcService"/> uses:
/// unary check/write/delete dispatch through <see cref="IPermissionChecker"/> / <see cref="IRelationshipsGrain"/>;
/// expand and the read + lookup RPCs run in-process via
/// <see cref="Grains.ReverseOps"/> / <see cref="Grains.RelationshipReads"/> (mirroring <see cref="BulkGrpcService"/>).
/// </summary>
public sealed class AuthzedPermissionsV1Service(
    IPermissionChecker checker, IGrainFactory grains, Grains.ReverseOps reverseOps, Grains.RelationshipReads relationshipReads, ISchemaProvider schema)
    : V1::PermissionsService.PermissionsServiceBase
{
    private IRelationshipsGrain Relationships => grains.GetGrain<IRelationshipsGrain>(IRelationshipsGrain.Key);

    /// <summary>
    /// Maps a caveat-evaluation failure onto a gRPC status: a context value that does not match a
    /// declared parameter type is <c>InvalidArgument</c> (SpiceDB <c>ParameterTypeError</c>); a
    /// reference to a caveat absent from the schema is <c>FailedPrecondition</c> (SpiceDB
    /// <c>CaveatNameNotFoundErr</c>, a schema-skew condition).
    /// </summary>
    private static RpcException ToRpc(CaveatEvaluationException ex) =>
        new(new Status(
            ex.Kind == CaveatEvaluationErrorKind.ParameterTypeMismatch
                ? StatusCode.InvalidArgument
                : StatusCode.FailedPrecondition,
            ex.Message));

    /// <summary>
    /// Maps a cross-silo dispatch failure surfaced by <c>OrleansDispatcher</c> onto its deliberately
    /// chosen gRPC status (cf. SpiceDB <c>rewriteError</c>): a transient transport/silo-availability
    /// failure is retriable <c>Unavailable</c>; a cancellation is <c>Cancelled</c>; a deadline is
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

    public override async Task<V1::CheckPermissionResponse> CheckPermission(
        V1::CheckPermissionRequest request, ServerCallContext context)
    {
        var subjectRelation = string.IsNullOrEmpty(request.Subject.OptionalRelation)
            ? CoreConstants.Ellipsis
            : request.Subject.OptionalRelation;

        var subject = new ObjectAndRelation(
            request.Subject.Object.ObjectType,
            request.Subject.Object.ObjectId,
            subjectRelation);

        // A wildcard ("*") subject is not a valid thing to check (SpiceDB: checkInternal returns
        // NewWildcardNotAllowedErr -> InvalidArgument). It is only meaningful as a stored subject.
        SchemaValidation.RejectWildcardSubject(request.Subject.Object.ObjectId);

        // Validate the resource definition+permission and the subject definition+relation up front, as
        // SpiceDB does (namespace.CheckNamespaceAndRelations). An unknown definition/relation is a client
        // schema/typo bug surfaced as FailedPrecondition, NOT silently masked as a NO_PERMISSION verdict.
        SchemaValidation.CheckNamespaceAndRelations(
            schema.Current,
            new SchemaValidation.TypeAndRelation(request.Resource.ObjectType, request.Permission, AllowEllipsis: false),
            new SchemaValidation.TypeAndRelation(request.Subject.Object.ObjectType, subjectRelation, AllowEllipsis: true));

        // Reject an oversized request caveat context (SpiceDB: GetCaveatContext) -> InvalidArgument.
        RequestLimits.ValidateCaveatContextSize(request.Context);

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
            // The check exhausted its recursion budget (deep schema/data or a cycle). SpiceDB surfaces
            // this as FailedPrecondition, NOT a NoPermission verdict, so the client can tell "the server
            // gave up" apart from "not authorized".
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
            Membership.Member => V1::CheckPermissionResponse.Types.Permissionship.HasPermission,
            Membership.Caveated => V1::CheckPermissionResponse.Types.Permissionship.ConditionalPermission,
            _ => V1::CheckPermissionResponse.Types.Permissionship.NoPermission,
        };

        var resp = new V1::CheckPermissionResponse
        {
            Permissionship = ship,
            CheckedAt = new V1::ZedToken { Token = result.EvaluatedToken },
        };

        // partial_caveat_info carries a min_items=1 repeated field, so only populate it when the verdict is
        // caveated AND there is at least one missing field to report.
        if (result.Verdict == Membership.Caveated && result.MissingFields.Count > 0)
        {
            var partial = new V1::PartialCaveatInfo();
            partial.MissingRequiredContext.AddRange(result.MissingFields);
            resp.PartialCaveatInfo = partial;
        }

        return resp;
    }

    public override async Task<V1::WriteRelationshipsResponse> WriteRelationships(
        V1::WriteRelationshipsRequest request, ServerCallContext context)
    {
        // Reject over-limit/duplicate/oversized-context requests up front (SpiceDB validates the request
        // shape before applying it). All of these are InvalidArgument, not FailedPrecondition.
        RequestLimits.ValidateWriteRelationships(request);

        var updates = request.Updates.Select(ToWire).ToList();
        var preconditions = ToWire(request.OptionalPreconditions);
        try
        {
            var reply = await Relationships.WriteRelationships(new WriteRelationshipsArgs(updates, preconditions));
            return new V1::WriteRelationshipsResponse { WrittenAt = new V1::ZedToken { Token = reply.WrittenAtToken } };
        }
        catch (PreconditionFailedException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (WriteConflictException ex)
        {
            // CreateExisting -> AlreadyExists (SpiceDB CreateRelationshipExistsError): a permanent
            // duplicate-create the client must NOT retry as transient. Serialization -> Aborted (SpiceDB
            // SerializationError): a genuine write-write conflict the client retries as a whole tx.
            throw new RpcException(new Status(ToStatusCode(ex.Kind), ex.Message));
        }
        catch (SequencerOverloadedException ex)
        {
            // The per-silo admission gate shed this commit — the sequencer is saturated. A deliberate,
            // retryable overload signal (back off and retry), never an opaque timeout.
            throw new RpcException(new Status(StatusCode.ResourceExhausted, ex.Message));
        }
    }

    private static StatusCode ToStatusCode(WriteConflictKind kind) => kind switch
    {
        WriteConflictKind.CreateExisting => StatusCode.AlreadyExists,
        _ => StatusCode.Aborted,
    };

    public override async Task ReadRelationships(
        V1::ReadRelationshipsRequest request,
        IServerStreamWriter<V1::ReadRelationshipsResponse> responseStream,
        ServerCallContext context)
    {
        var filter = ToWire(request.RelationshipFilter);
        var consistency = ToWire(request.Consistency);

        // One in-process read over one pinned snapshot (the grain hop and page loop are both gone). Each
        // item carries the per-message read-at token, exactly as every page-message did before.
        try
        {
            await foreach (var item in relationshipReads.ReadRelationships(
                new ReadRelationshipsArgs(filter, null, null, consistency), context.CancellationToken))
            {
                await responseStream.WriteAsync(new V1::ReadRelationshipsResponse
                {
                    ReadAt = new V1::ZedToken { Token = item.ReadAtToken },
                    Relationship = ToProto(item.Relationship),
                });
            }
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
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            // A client that stops reading (or disconnects) cancels the request token; treat it as a
            // normal end of the stream, not an error (mirrors AuthzedWatchV1Service.Watch).
        }
    }

    public override async Task<V1::DeleteRelationshipsResponse> DeleteRelationships(
        V1::DeleteRelationshipsRequest request, ServerCallContext context)
    {
        try
        {
            var reply = await Relationships.DeleteRelationships(new DeleteRelationshipsArgs(
                ToWire(request.RelationshipFilter),
                null,
                ToWire(request.OptionalPreconditions)));

            // v1 DeleteRelationshipsResponse carries only deleted_at in this snapshot; count/limit discarded.
            return new V1::DeleteRelationshipsResponse
            {
                DeletedAt = new V1::ZedToken { Token = reply.DeletedAtToken },
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

    public override async Task<V1::ExpandPermissionTreeResponse> ExpandPermissionTree(
        V1::ExpandPermissionTreeRequest request, ServerCallContext context)
    {
        // Validate the resource definition+permission up front (SpiceDB: CheckNamespaceAndRelation).
        SchemaValidation.CheckNamespaceAndRelations(
            schema.Current,
            new SchemaValidation.TypeAndRelation(request.Resource.ObjectType, request.Permission, AllowEllipsis: false));

        // v1 ExpandPermissionTreeRequest has no mode field; authzed's expand is the recursive walk.
        ExpandTreeReply reply;
        try
        {
            reply = await reverseOps.ExpandPermissionTree(new ExpandTreeArgs(
                request.Resource.ObjectType,
                request.Resource.ObjectId,
                request.Permission,
                ExpandModeWire.Recursive,
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

        return new V1::ExpandPermissionTreeResponse
        {
            TreeRoot = ToProto(reply.Root),
            ExpandedAt = new V1::ZedToken { Token = reply.ExpandedAtToken },
        };
    }

    public override async Task LookupResources(
        V1::LookupResourcesRequest request,
        IServerStreamWriter<V1::LookupResourcesResponse> responseStream,
        ServerCallContext context)
    {
        var subjectRelation = string.IsNullOrEmpty(request.Subject.OptionalRelation)
            ? CoreConstants.Ellipsis
            : request.Subject.OptionalRelation;

        // Reject an oversized request caveat context (SpiceDB: GetCaveatContext) -> InvalidArgument.
        var context5 = StructToDict(RequestLimits.ValidateCaveatContextSize(request.Context));
        var consistency = ToWire(request.Consistency);

        // v1 contract: optional_limit (field 6) caps the resources returned; optional_cursor (field 7)
        // resumes after a prior page. SpiceDB disables cursors entirely when no limit is set, so we
        // only attach after_result_cursor when a limit was requested.
        var limit = request.OptionalLimit == 0 ? (int?)null : (int)request.OptionalLimit;
        var emitCursors = limit is not null;
        string? cursor = request.OptionalCursor is { } oc && !string.IsNullOrEmpty(oc.Token) ? oc.Token : null;

        // Validate the resource definition+permission and subject definition+relation up front
        // (SpiceDB: CheckNamespaceAndRelations) before streaming any results.
        SchemaValidation.CheckNamespaceAndRelations(
            schema.Current,
            new SchemaValidation.TypeAndRelation(request.ResourceObjectType, request.Permission, AllowEllipsis: false),
            new SchemaValidation.TypeAndRelation(request.Subject.Object.ObjectType, subjectRelation, AllowEllipsis: true));

        // One in-process stream. A limited request takes at most `limit` items then stops (the RPC stream
        // terminates; the client resumes via the last item's cursor). An unlimited request drains the whole
        // reachable set. `limit` also drives the engine path: a limited walk is cursor-bearing (so every item
        // has a resume cursor), an unlimited/cursorless walk may take the Leopard fast path.
        var emitted = 0;
        try
        {
            await foreach (var r in reverseOps.StreamLookupResources(
                new LookupResourcesArgs(
                    request.ResourceObjectType,
                    request.Permission,
                    request.Subject.Object.ObjectType,
                    request.Subject.Object.ObjectId,
                    subjectRelation,
                    context5,
                    limit,
                    cursor,
                    consistency),
                context.CancellationToken))
            {
                var resp = new V1::LookupResourcesResponse
                {
                    LookedUpAt = new V1::ZedToken { Token = r.LookedUpAtToken },
                    ResourceObjectId = r.ResourceId,
                    Permissionship = ToLookupPermissionship(r.Permissionship),
                };
                if (r.Permissionship.IsCaveated && r.Permissionship.MissingContextParams.Count > 0)
                {
                    var partial = new V1::PartialCaveatInfo();
                    partial.MissingRequiredContext.AddRange(r.Permissionship.MissingContextParams);
                    resp.PartialCaveatInfo = partial;
                }
                if (emitCursors && !string.IsNullOrEmpty(r.AfterResultCursor))
                    resp.AfterResultCursor = new V1::Cursor { Token = r.AfterResultCursor };
                await responseStream.WriteAsync(resp);

                emitted++;
                if (limit is { } cap && emitted >= cap)
                    break;
            }
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
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            // A client that stops reading (or disconnects) cancels the request token; treat it as a
            // normal end of the stream, not an error (mirrors AuthzedWatchV1Service.Watch).
        }
    }

    public override async Task LookupSubjects(
        V1::LookupSubjectsRequest request,
        IServerStreamWriter<V1::LookupSubjectsResponse> responseStream,
        ServerCallContext context)
    {
        var subjectRelation = string.IsNullOrEmpty(request.OptionalSubjectRelation)
            ? CoreConstants.Ellipsis
            : request.OptionalSubjectRelation;

        // Reject an oversized request caveat context (SpiceDB: GetCaveatContext) -> InvalidArgument.
        var context6 = StructToDict(RequestLimits.ValidateCaveatContextSize(request.Context));
        var consistency = ToWire(request.Consistency);

        // v1 contract: optional_concrete_limit (field 7) caps the *concrete* (non-wildcard) subjects
        // returned. optional_cursor is not supported for LookupSubjects (the proto documents it as
        // ignored), so we do not read it.
        var limit = request.OptionalConcreteLimit == 0 ? (int?)null : (int)request.OptionalConcreteLimit;

        // Validate the resource definition+permission and subject definition+relation up front
        // (SpiceDB: CheckNamespaceAndRelations) before streaming any results.
        SchemaValidation.CheckNamespaceAndRelations(
            schema.Current,
            new SchemaValidation.TypeAndRelation(request.Resource.ObjectType, request.Permission, AllowEllipsis: false),
            new SchemaValidation.TypeAndRelation(request.SubjectObjectType, subjectRelation, AllowEllipsis: true));

        // One in-process stream: count non-wildcard emissions and stop once the concrete limit is met.
        var emitted = 0;
        try
        {
            await foreach (var item in reverseOps.StreamLookupSubjects(
                new LookupSubjectsArgs(
                    request.Resource.ObjectType,
                    request.Resource.ObjectId,
                    request.Permission,
                    request.SubjectObjectType,
                    subjectRelation,
                    context6,
                    limit,
                    null,
                    consistency),
                context.CancellationToken))
            {
                var s = item.Subject;
                var ship = ToLookupPermissionship(s.Permissionship);
                V1::PartialCaveatInfo? partial = null;
                if (s.Permissionship.IsCaveated && s.Permissionship.MissingContextParams.Count > 0)
                {
                    partial = new V1::PartialCaveatInfo();
                    partial.MissingRequiredContext.AddRange(s.Permissionship.MissingContextParams);
                }

                var resp = new V1::LookupSubjectsResponse
                {
                    LookedUpAt = new V1::ZedToken { Token = item.LookedUpAtToken },
                    // Modern field.
                    Subject = new V1::ResolvedSubject
                    {
                        SubjectObjectId = s.SubjectId,
                        Permissionship = ship,
                    },
                    // Deprecated mirror fields for older clients.
                    SubjectObjectId = s.SubjectId,
                    Permissionship = ship,
                };
                if (partial is not null)
                {
                    resp.Subject.PartialCaveatInfo = partial;
                    resp.PartialCaveatInfo = partial.Clone();
                }
                await responseStream.WriteAsync(resp);

                // The cap is a *concrete* limit: wildcards do not count against it.
                if (!s.IsWildcard && ++emitted == limit)
                    break;
            }
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
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            // A client that stops reading (or disconnects) cancels the request token; treat it as a
            // normal end of the stream, not an error (mirrors AuthzedWatchV1Service.Watch).
        }
    }

    public override async Task<V1::CheckBulkPermissionsResponse> CheckBulkPermissions(
        V1::CheckBulkPermissionsRequest request, ServerCallContext context)
    {
        var snapshot = schema.Current;

        // Validate each pair against the schema UP FRONT and PER PAIR. An unknown resource definition,
        // unknown relation/permission, or a wildcard ("*") check subject is a client schema/typo bug for
        // that one pair; SpiceDB's CheckBulkPermissions surfaces it as that pair's google.rpc.Status error
        // (CheckBulkPermissionsPair_Error) rather than failing the whole RPC. Valid pairs are dispatched;
        // invalid pairs are short-circuited to their error and never sent to the grain.
        //
        // (Oversized request caveat context remains a WHOLE-request InvalidArgument, matching SpiceDB's
        // groupItems/GetCaveatContext which aborts the entire bulk request.)
        var pairErrors = new Google.Rpc.Status?[request.Items.Count];
        var validItems = new List<BatchCheckItem>();

        for (var i = 0; i < request.Items.Count; i++)
        {
            var it = request.Items[i];
            var subjectRelation = string.IsNullOrEmpty(it.Subject.OptionalRelation)
                ? CoreConstants.Ellipsis
                : it.Subject.OptionalRelation;

            var perPairError =
                SchemaValidation.WildcardSubjectError(it.Subject.Object.ObjectId)
                ?? SchemaValidation.TryCheckNamespaceAndRelations(
                    snapshot,
                    new SchemaValidation.TypeAndRelation(it.Resource.ObjectType, it.Permission, AllowEllipsis: false),
                    new SchemaValidation.TypeAndRelation(it.Subject.Object.ObjectType, subjectRelation, AllowEllipsis: true));

            if (perPairError is not null)
            {
                pairErrors[i] = SchemaValidation.ToRpcStatus(perPairError);
                continue;
            }

            validItems.Add(new BatchCheckItem(
                it.Resource.ObjectType,
                it.Resource.ObjectId,
                it.Permission,
                new ObjectAndRelation(it.Subject.Object.ObjectType, it.Subject.Object.ObjectId, subjectRelation),
                // Reject an oversized per-item caveat context (SpiceDB: GetCaveatContext) -> whole-request InvalidArgument.
                StructToDict(RequestLimits.ValidateCaveatContextSize(it.Context))));
        }

        BatchCheckResult? result = null;
        if (validItems.Count > 0)
        {
            try
            {
                result = await checker.BatchCheck(
                    validItems,
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
            catch (CaveatEvaluationException ex)
            {
                throw ToRpc(ex);
            }
            catch (DispatchFailedException ex)
            {
                throw ToRpc(ex);
            }
        }

        // One token for the whole batch (the single pinned revision every dispatched item was evaluated at).
        // When every pair failed validation, no revision was pinned, so no CheckedAt is emitted.
        var resp = new V1::CheckBulkPermissionsResponse();
        if (result is not null)
            resp.CheckedAt = new V1::ZedToken { Token = result.EvaluatedToken };

        // Re-interleave verdicts (from valid items, in dispatch order) and per-pair errors back into the
        // original request order. Each pair echoes its originating request item alongside either its verdict
        // or its google.rpc.Status error.
        var verdictPos = 0;
        for (var i = 0; i < request.Items.Count; i++)
        {
            var pair = new V1::CheckBulkPermissionsPair { Request = request.Items[i] };

            if (pairErrors[i] is { } error)
            {
                pair.Error = error;
            }
            else
            {
                var verdict = result!.Items[verdictPos++];
                var ship = verdict.Verdict switch
                {
                    Membership.Member => V1::CheckPermissionResponse.Types.Permissionship.HasPermission,
                    Membership.Caveated => V1::CheckPermissionResponse.Types.Permissionship.ConditionalPermission,
                    _ => V1::CheckPermissionResponse.Types.Permissionship.NoPermission,
                };

                var item = new V1::CheckBulkPermissionsResponseItem { Permissionship = ship };
                if (verdict.Verdict == Membership.Caveated && verdict.MissingFields.Count > 0)
                {
                    var partial = new V1::PartialCaveatInfo();
                    partial.MissingRequiredContext.AddRange(verdict.MissingFields);
                    item.PartialCaveatInfo = partial;
                }

                pair.Item = item;
            }

            resp.Pairs.Add(pair);
        }

        return resp;
    }

    public override async Task<V1::ImportBulkRelationshipsResponse> ImportBulkRelationships(
        IAsyncStreamReader<V1::ImportBulkRelationshipsRequest> requestStream,
        ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(requestStream);

        // Buffer the WHOLE client stream, then load it in ONE grain commit: real SpiceDB's
        // ImportBulkRelationships is atomic across the entire stream (observed v1.49.2 — a duplicate in
        // a later batch leaves NOTHING applied, including earlier batches), and one commit is the only
        // shape that reproduces that. Rows apply with CREATE semantics: a row already stored, or
        // repeated within the stream, rejects the import with AlreadyExists (SpiceDB's verbatim
        // "could not CREATE relationship ..." message) so the client does not retry the doomed import;
        // a genuine write-write serialization conflict maps to Aborted so the client retries.
        // Cost of the one-commit shape, accepted with eyes open: the buffered rows cross the grain
        // boundary as ONE Orleans message (subject to the runtime's message-size and response-timeout
        // limits) and the import executes as one commit turn on the sequencer's single non-reentrant
        // activation, serializing against every other cluster write for its duration. A truly huge
        // import that hits those limits should be split by the CALLER into idempotent-safe disjoint
        // streams; chunking here would silently forfeit the observed whole-stream atomicity.
        var relationships = new List<RelationshipWire>();
        while (await requestStream.MoveNext(context.CancellationToken))
        {
            foreach (var relationship in requestStream.Current.Relationships)
                relationships.Add(ToWire(relationship));
        }

        if (relationships.Count == 0)
            return new V1::ImportBulkRelationshipsResponse { NumLoaded = 0 };

        try
        {
            var reply = await Relationships.BulkImportRelationships(
                new BulkImportRelationshipsArgs(relationships));

            // v1 ImportBulkRelationshipsResponse carries only num_loaded (no token).
            return new V1::ImportBulkRelationshipsResponse { NumLoaded = reply.NumLoaded };
        }
        catch (WriteConflictException ex)
        {
            throw new RpcException(new Status(ToStatusCode(ex.Kind), ex.Message));
        }
        catch (SequencerOverloadedException ex)
        {
            // Sequencer overload shed by the admission gate: retryable RESOURCE_EXHAUSTED.
            throw new RpcException(new Status(StatusCode.ResourceExhausted, ex.Message));
        }
    }

    public override async Task ExportBulkRelationships(
        V1::ExportBulkRelationshipsRequest request,
        IServerStreamWriter<V1::ExportBulkRelationshipsResponse> responseStream,
        ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(request);
        ArgumentNullException.ThrowIfNull(responseStream);

        var filter = request.OptionalRelationshipFilter is { } f ? ToWire(f) : EmptyFilter;
        var limit = (int)request.OptionalLimit;
        var consistency = ToWire(request.Consistency);
        var cursor = request.OptionalCursor is { Token.Length: > 0 } c ? c.Token : null;

        // One in-process stream over a single pinned snapshot (pinned once from the cursor or the request
        // consistency). Batch up to `limit` relationships per response message — mirroring the prior per-page
        // reply shape — carrying the last item's cursor as that batch's continuation cursor.
        var batchSize = limit > 0 ? limit : DefaultExportBatchSize;
        var batch = new List<RelationshipStreamItem>(Math.Min(batchSize, 1024));
        try
        {
            await foreach (var item in relationshipReads.BulkExportRelationships(
                new BulkExportRelationshipsArgs(filter, limit, cursor, consistency), context.CancellationToken))
            {
                batch.Add(item);
                if (batch.Count >= batchSize)
                {
                    await WriteExportBatch(responseStream, batch);
                    batch.Clear();
                }
            }
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
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            // A client that stops reading (or disconnects) cancels the request token; treat it as a
            // normal end of the stream, not an error (mirrors AuthzedWatchV1Service.Watch). Any
            // already-written batches stand; nothing further is written below.
        }

        if (batch.Count > 0)
            await WriteExportBatch(responseStream, batch);
    }

    /// <summary>The default per-message bulk-export batch size when the request leaves the limit unset.</summary>
    private const int DefaultExportBatchSize = 1000;

    private static async Task WriteExportBatch(
        IServerStreamWriter<V1::ExportBulkRelationshipsResponse> responseStream,
        IReadOnlyList<RelationshipStreamItem> batch)
    {
        var response = new V1::ExportBulkRelationshipsResponse
        {
            AfterResultCursor = new V1::Cursor { Token = batch[^1].ResumeCursor },
        };
        response.Relationships.AddRange(batch.Select(i => ToProto(i.Relationship)));
        await responseStream.WriteAsync(response);
    }

    private static readonly RelationshipsFilterWire EmptyFilter =
        new(null, null, null, null, null, null, null);

    // ---- conversions ----

    private static V1::LookupPermissionship ToLookupPermissionship(WirePermissionship p) =>
        p.IsCaveated
            ? V1::LookupPermissionship.ConditionalPermission
            : V1::LookupPermissionship.HasPermission;

    private static IReadOnlyList<PreconditionWire>? ToWire(
        Google.Protobuf.Collections.RepeatedField<V1::Precondition> preconditions)
    {
        if (preconditions.Count == 0)
            return null;

        return preconditions.Select(p =>
        {
            var op = p.Operation switch
            {
                V1::Precondition.Types.Operation.MustMatch => PreconditionOpWire.MustMatch,
                V1::Precondition.Types.Operation.MustNotMatch => PreconditionOpWire.MustNotMatch,
                _ => throw new RpcException(new Status(
                    StatusCode.InvalidArgument, "precondition operation is unspecified")),
            };
            return new PreconditionWire(op, ToWire(p.Filter));
        }).ToList();
    }

    private static RelationshipUpdateWire ToWire(V1::RelationshipUpdate u)
    {
        var op = u.Operation switch
        {
            V1::RelationshipUpdate.Types.Operation.Create => RelationshipUpdateOpWire.Create,
            V1::RelationshipUpdate.Types.Operation.Touch => RelationshipUpdateOpWire.Touch,
            V1::RelationshipUpdate.Types.Operation.Delete => RelationshipUpdateOpWire.Delete,
            _ => throw new RpcException(new Status(
                StatusCode.InvalidArgument, "relationship update operation is unspecified")),
        };
        return new RelationshipUpdateWire(op, ToWire(u.Relationship));
    }

    private static RelationshipWire ToWire(V1::Relationship r)
    {
        var subjectRelation = string.IsNullOrEmpty(r.Subject.OptionalRelation)
            ? CoreConstants.Ellipsis
            : r.Subject.OptionalRelation;
        // v1 core.proto Relationship has no expiration field in this snapshot.
        return new RelationshipWire(
            r.Resource.ObjectType, r.Resource.ObjectId, r.Relation,
            r.Subject.Object.ObjectType, r.Subject.Object.ObjectId, subjectRelation,
            r.OptionalCaveat is { CaveatName.Length: > 0 } c ? c.CaveatName : null,
            r.OptionalCaveat is { } cc ? StructToDict(cc.Context) : null,
            null);
    }

    private static V1::Relationship ToProto(RelationshipWire w)
    {
        var rel = new V1::Relationship
        {
            Resource = new V1::ObjectReference { ObjectType = w.ResourceType, ObjectId = w.ResourceId },
            Relation = w.ResourceRelation,
            Subject = new V1::SubjectReference
            {
                Object = new V1::ObjectReference { ObjectType = w.SubjectType, ObjectId = w.SubjectId },
                OptionalRelation = w.SubjectRelation == CoreConstants.Ellipsis ? string.Empty : w.SubjectRelation,
            },
        };
        if (w.CaveatName is { Length: > 0 })
        {
            rel.OptionalCaveat = new V1::ContextualizedCaveat
            {
                CaveatName = w.CaveatName,
                Context = DictToStruct(w.CaveatContext),
            };
        }

        return rel;
    }

    private static RelationshipsFilterWire ToWire(V1::RelationshipFilter f)
    {
        var resourceIds = string.IsNullOrEmpty(f.OptionalResourceId)
            ? null
            : new List<string> { f.OptionalResourceId };

        string? subjectType = null;
        IReadOnlyList<string>? subjectIds = null;
        string? subjectRelation = null;
        if (f.OptionalSubjectFilter is { } sub)
        {
            subjectType = NullIfEmpty(sub.SubjectType);
            subjectIds = string.IsNullOrEmpty(sub.OptionalSubjectId)
                ? null
                : new List<string> { sub.OptionalSubjectId };
            subjectRelation = sub.OptionalRelation is { } rf ? NullIfEmpty(rf.Relation) : null;
        }

        return new RelationshipsFilterWire(
            NullIfEmpty(f.ResourceType),
            NullIfEmpty(f.OptionalResourceIdPrefix),
            resourceIds,
            NullIfEmpty(f.OptionalRelation),
            subjectType,
            subjectIds,
            subjectRelation);
    }

    private static V1::PermissionRelationshipTree ToProto(ExpandTreeNodeWire node)
    {
        var result = new V1::PermissionRelationshipTree
        {
            ExpandedObject = new V1::ObjectReference
            {
                ObjectType = node.ExpandedType,
                ObjectId = node.ExpandedId,
            },
            ExpandedRelation = node.ExpandedRelation,
        };

        if (node.IsLeaf)
        {
            // v1 DirectSubjectSet carries plain SubjectReferences with no per-subject caveat slots; a
            // wildcard is represented by object_id == "*".
            var leaf = new V1::DirectSubjectSet();
            leaf.Subjects.AddRange(node.Subjects.Select(ToProto));
            result.Leaf = leaf;
        }
        else
        {
            var intermediate = new V1::AlgebraicSubjectSet { Operation = ToProto(node.Operation) };
            intermediate.Children.AddRange(node.Children.Select(ToProto));
            result.Intermediate = intermediate;
        }

        return result;
    }

    private static V1::SubjectReference ToProto(ExpandSubjectWire s) => new()
    {
        Object = new V1::ObjectReference { ObjectType = s.SubjectType, ObjectId = s.SubjectId },
        OptionalRelation = s.SubjectRelation == CoreConstants.Ellipsis ? string.Empty : s.SubjectRelation,
    };

    private static V1::AlgebraicSubjectSet.Types.Operation ToProto(SetOpWire op) => op switch
    {
        SetOpWire.Union => V1::AlgebraicSubjectSet.Types.Operation.Union,
        SetOpWire.Intersection => V1::AlgebraicSubjectSet.Types.Operation.Intersection,
        SetOpWire.Exclusion => V1::AlgebraicSubjectSet.Types.Operation.Exclusion,
        _ => V1::AlgebraicSubjectSet.Types.Operation.Unspecified,
    };

    private static WireConsistency ToWire(V1::Consistency? consistency) => consistency?.RequirementCase switch
    {
        V1::Consistency.RequirementOneofCase.AtLeastAsFresh =>
            new WireConsistency(WireConsistencyMode.AtLeastAsFresh, consistency.AtLeastAsFresh.Token),
        V1::Consistency.RequirementOneofCase.FullyConsistent =>
            new WireConsistency(WireConsistencyMode.FullyConsistent),
        V1::Consistency.RequirementOneofCase.AtExactSnapshot =>
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
