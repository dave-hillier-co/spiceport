using Grpc.Core;
using Spiceport.Grains;
using Spiceport.Grains.Abstractions;
using V1 = Authzed.Api.V1;

namespace Spiceport.Api;

/// <summary>
/// gRPC front door for the <c>authzed.api.v1.ExperimentalService</c>. Only the relationship-counter RPCs
/// are implemented; they are pure data-plane operations delegated to the <see cref="IRelationshipsGrain"/>.
/// Counts are computed on demand (resolve a revision, look up the registered filter, count matching
/// relationships at that snapshot, mint a <c>read_at</c> ZedToken) — there is no async precomputation, so
/// the response always carries the <c>read_counter_value</c> arm and never <c>counter_still_calculating</c>.
/// The remaining ExperimentalService RPCs are left as the generated <c>Unimplemented</c> defaults.
/// </summary>
/// <remarks>
/// Error mapping follows the SpiceDB reference (<c>internal/services/shared/errors.go</c>), which maps both
/// <c>CounterAlreadyRegistered</c> and <c>CounterNotRegistered</c> to <see cref="StatusCode.FailedPrecondition"/>.
/// </remarks>
public sealed class AuthzedExperimentalV1Service(IGrainFactory grains, ISchemaProvider schema)
    : V1::ExperimentalService.ExperimentalServiceBase
{
    // schema is accepted to mirror the v1 service constructor pattern; counters need only the grain.
    private readonly ISchemaProvider _schema = schema;

    private IRelationshipsGrain Relationships => grains.GetGrain<IRelationshipsGrain>(IRelationshipsGrain.Key);

    public override async Task<V1::ExperimentalRegisterRelationshipCounterResponse> ExperimentalRegisterRelationshipCounter(
        V1::ExperimentalRegisterRelationshipCounterRequest request, ServerCallContext context)
    {
        RequestValidation.Validate(request);
        RequireName(request.Name);

        try
        {
            await Relationships.RegisterRelationshipCounter(
                new RegisterCounterArgs(request.Name, ToWire(request.RelationshipFilter)));
        }
        catch (CounterOperationException ex)
        {
            throw ToRpc(ex, request.Name, request.RelationshipFilter);
        }
        catch (SequencerOverloadedException ex)
        {
            // The per-silo admission gate shed this commit — the sequencer is saturated. A deliberate,
            // retryable overload signal (back off and retry), never an opaque timeout.
            throw new RpcException(new Status(StatusCode.ResourceExhausted, ex.Message));
        }

        return new V1::ExperimentalRegisterRelationshipCounterResponse();
    }

    public override async Task<V1::ExperimentalUnregisterRelationshipCounterResponse> ExperimentalUnregisterRelationshipCounter(
        V1::ExperimentalUnregisterRelationshipCounterRequest request, ServerCallContext context)
    {
        RequestValidation.Validate(request);
        RequireName(request.Name);

        try
        {
            await Relationships.UnregisterRelationshipCounter(new UnregisterCounterArgs(request.Name));
        }
        catch (CounterOperationException ex)
        {
            throw ToRpc(ex, request.Name, filter: null);
        }
        catch (SequencerOverloadedException ex)
        {
            // Sequencer overload shed by the admission gate: retryable RESOURCE_EXHAUSTED.
            throw new RpcException(new Status(StatusCode.ResourceExhausted, ex.Message));
        }

        return new V1::ExperimentalUnregisterRelationshipCounterResponse();
    }

    public override async Task<V1::ExperimentalCountRelationshipsResponse> ExperimentalCountRelationships(
        V1::ExperimentalCountRelationshipsRequest request, ServerCallContext context)
    {
        RequestValidation.Validate(request);
        RequireName(request.Name);

        CountRelationshipsReply reply;
        try
        {
            reply = await Relationships.CountRelationships(new CountRelationshipsArgs(request.Name));
        }
        catch (CounterOperationException ex)
        {
            throw ToRpc(ex, request.Name, filter: null);
        }

        // On-demand: the value is always available, so always emit the read_counter_value arm.
        return new V1::ExperimentalCountRelationshipsResponse
        {
            ReadCounterValue = new V1::ReadCounterValue
            {
                RelationshipCount = reply.Count,
                ReadAt = new V1::ZedToken { Token = reply.ReadAtToken },
            },
        };
    }

    /// <summary>
    /// Maps an authzed v1 <see cref="V1::RelationshipFilter"/> to the grain's filter wire. The v1 filter
    /// carries a single optional resource id (not a list) and a nested optional subject filter.
    /// </summary>
    private static RelationshipsFilterWire ToWire(V1::RelationshipFilter f)
    {
        IReadOnlyList<string>? resourceIds = string.IsNullOrEmpty(f.OptionalResourceId)
            ? null
            : [f.OptionalResourceId];

        string? subjectType = null;
        string? subjectId = null;
        string? subjectRelation = null;
        if (f.OptionalSubjectFilter is { } sf)
        {
            subjectType = NullIfEmpty(sf.SubjectType);
            subjectId = NullIfEmpty(sf.OptionalSubjectId);
            subjectRelation = sf.OptionalRelation is { } rf ? NullIfEmpty(rf.Relation) : null;
        }

        return new RelationshipsFilterWire(
            NullIfEmpty(f.ResourceType),
            NullIfEmpty(f.OptionalResourceIdPrefix),
            resourceIds,
            NullIfEmpty(f.OptionalRelation),
            subjectType,
            subjectId is null ? null : [subjectId],
            subjectRelation);
    }

    private static string? NullIfEmpty(string? value) => string.IsNullOrEmpty(value) ? null : value;

    /// <summary>
    /// Both counter failures map to FailedPrecondition, matching the SpiceDB reference, with the
    /// <c>ErrorInfo</c> <c>rewriteError</c> gives <c>CounterAlreadyRegisteredError</c> (the counter name and
    /// the NEW filter, every field present) and <c>CounterNotRegisteredError</c> (the counter name).
    /// </summary>
    private static RpcException ToRpc(CounterOperationException ex, string name, V1::RelationshipFilter? filter)
    {
        if (ex.Kind != CounterErrorKind.AlreadyRegistered || filter is null)
            return RpcErrors.WithReason(
                StatusCode.FailedPrecondition, ex.Message, V1::ErrorReason.CounterNotRegistered, [new("counter_name", name)]);

        var subjectFilter = filter.OptionalSubjectFilter;
        return RpcErrors.WithReason(
            StatusCode.FailedPrecondition,
            ex.Message,
            V1::ErrorReason.CounterAlreadyRegistered,
            [
                new("counter_name", name),
                new("new_filter_resource_type", filter.ResourceType),
                new("new_filter_resource_id", filter.OptionalResourceId),
                new("new_filter_resource_id_prefix", filter.OptionalResourceIdPrefix),
                new("new_filter_relation", filter.OptionalRelation),
                new("new_filter_subject_type", subjectFilter?.SubjectType ?? string.Empty),
                new("new_filter_subject_id", subjectFilter?.OptionalSubjectId ?? string.Empty),
                new("new_filter_subject_relation", subjectFilter?.OptionalRelation?.Relation ?? string.Empty),
            ]);
    }

    /// <summary>
    /// SpiceDB's counter handlers reject an empty name themselves (the name pattern admits the empty string):
    /// InvalidArgument, "name must be provided", reason UNSPECIFIED.
    /// </summary>
    private static void RequireName(string name)
    {
        if (string.IsNullOrEmpty(name))
            throw RpcErrors.WithReason(StatusCode.InvalidArgument, "name must be provided", V1::ErrorReason.Unspecified);
    }
}
