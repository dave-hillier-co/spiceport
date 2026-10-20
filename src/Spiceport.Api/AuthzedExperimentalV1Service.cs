using Grpc.Core;
using Spiceport.Grains;
using Spiceport.Grains.Abstractions;
using V1 = Authzed.Api.V1;

namespace Spiceport.Api;

// The deprecated aliases and Exp* RPCs (and BulkCheckPermissionRequest.items) are marked deprecated in the
// vendored proto, so their generated members are [Obsolete]. Upstream still serves them; so does this service.
#pragma warning disable CS0612, CS0672

/// <summary>
/// gRPC front door for the <c>authzed.api.v1.ExperimentalService</c>. The relationship-counter RPCs are pure
/// data-plane operations delegated to the <see cref="IRelationshipsGrain"/>. Counts are computed on demand
/// (resolve a revision, look up the registered filter, count matching relationships at that snapshot, mint a
/// <c>read_at</c> ZedToken) — there is no async precomputation, so the response always carries the
/// <c>read_counter_value</c> arm and never <c>counter_still_calculating</c>.
/// </summary>
/// <remarks>
/// <para>
/// The remaining RPCs are, upstream (<c>internal/services/v1/experimental.go</c>), the same code paths as
/// their v1 replacements under older message names, so each is translated onto the v1 service here:
/// BulkCheckPermission onto <see cref="AuthzedPermissionsV1Service.CheckBulkPermissions"/>
/// (<c>toCheckBulkPermissionsRequest</c>/<c>toBulkCheckPermissionResponse</c>);
/// BulkImportRelationships/BulkExportRelationships onto Import/ExportBulkRelationships, the export with the
/// experimental server's larger <c>optional_limit</c> ceiling; and the <c>Experimental*</c> schema-tooling RPCs
/// onto <see cref="AuthzedSchemaV1Service"/>, re-typing its <c>Reflection*</c> messages as the
/// wire-identical <c>Exp*</c> ones.
/// </para>
/// <para>
/// Counter error mapping follows the SpiceDB reference (<c>internal/services/shared/errors.go</c>), which maps
/// both <c>CounterAlreadyRegistered</c> and <c>CounterNotRegistered</c> to
/// <see cref="StatusCode.FailedPrecondition"/>.
/// </para>
/// </remarks>
public sealed class AuthzedExperimentalV1Service(
    IGrainFactory grains, AuthzedPermissionsV1Service permissions, AuthzedSchemaV1Service schema)
    : V1::ExperimentalService.ExperimentalServiceBase
{
    private IRelationshipsGrain Relationships => grains.GetGrain<IRelationshipsGrain>(IRelationshipsGrain.Key);

    public override async Task<V1::BulkCheckPermissionResponse> BulkCheckPermission(
        V1::BulkCheckPermissionRequest request, ServerCallContext context)
    {
        var converted = new V1::CheckBulkPermissionsRequest { Consistency = request.Consistency };
        foreach (var item in request.Items)
        {
            converted.Items.Add(new V1::CheckBulkPermissionsRequestItem
            {
                Resource = item.Resource,
                Permission = item.Permission,
                Subject = item.Subject,
                Context = item.Context,
            });
        }

        var result = await permissions.CheckBulkPermissions(converted, context);

        var response = new V1::BulkCheckPermissionResponse { CheckedAt = result.CheckedAt };
        foreach (var pair in result.Pairs)
        {
            var mapped = new V1::BulkCheckPermissionPair
            {
                Request = new V1::BulkCheckPermissionRequestItem
                {
                    Resource = pair.Request.Resource,
                    Permission = pair.Request.Permission,
                    Subject = pair.Request.Subject,
                    Context = pair.Request.Context,
                },
            };

            switch (pair.ResponseCase)
            {
                case V1::CheckBulkPermissionsPair.ResponseOneofCase.Item:
                    mapped.Item = new V1::BulkCheckPermissionResponseItem
                    {
                        Permissionship = pair.Item.Permissionship,
                        PartialCaveatInfo = pair.Item.PartialCaveatInfo,
                    };
                    break;
                case V1::CheckBulkPermissionsPair.ResponseOneofCase.Error:
                    mapped.Error = pair.Error;
                    break;
                default:
                    throw new InvalidOperationException($"unknown CheckBulkPermissionsPair response `{pair.ResponseCase}`");
            }

            response.Pairs.Add(mapped);
        }

        return response;
    }

    public override async Task<V1::BulkImportRelationshipsResponse> BulkImportRelationships(
        IAsyncStreamReader<V1::BulkImportRelationshipsRequest> requestStream, ServerCallContext context)
    {
        var reply = await permissions.ImportBulkRelationships(
            new MappedAsyncStreamReader<V1::BulkImportRelationshipsRequest, V1::ImportBulkRelationshipsRequest>(
                requestStream,
                batch => new V1::ImportBulkRelationshipsRequest { Relationships = { batch.Relationships } }),
            context);

        return new V1::BulkImportRelationshipsResponse { NumLoaded = reply.NumLoaded };
    }

    public override Task BulkExportRelationships(
        V1::BulkExportRelationshipsRequest request,
        IServerStreamWriter<V1::BulkExportRelationshipsResponse> responseStream,
        ServerCallContext context)
    {
        var converted = new V1::ExportBulkRelationshipsRequest
        {
            Consistency = request.Consistency,
            OptionalLimit = request.OptionalLimit,
            OptionalCursor = request.OptionalCursor,
            OptionalRelationshipFilter = request.OptionalRelationshipFilter,
        };

        return permissions.ExportBulk(
            converted,
            new MappedServerStreamWriter<V1::ExportBulkRelationshipsResponse, V1::BulkExportRelationshipsResponse>(
                responseStream,
                page => new V1::BulkExportRelationshipsResponse
                {
                    AfterResultCursor = page.AfterResultCursor,
                    Relationships = { page.Relationships },
                }),
            context,
            RequestCeilings.MaxExperimentalBulkExportRelationshipsLimit);
    }

    public override async Task<V1::ExperimentalReflectSchemaResponse> ExperimentalReflectSchema(
        V1::ExperimentalReflectSchemaRequest request, ServerCallContext context)
    {
        var converted = new V1::ReflectSchemaRequest { Consistency = request.Consistency };
        foreach (var filter in request.OptionalFilters)
            converted.OptionalFilters.Add(ProtoReshape.As<V1::ReflectionSchemaFilter>(filter));

        return ProtoReshape.As<V1::ExperimentalReflectSchemaResponse>(
            await schema.ReflectSchema(converted, context));
    }

    public override async Task<V1::ExperimentalDiffSchemaResponse> ExperimentalDiffSchema(
        V1::ExperimentalDiffSchemaRequest request, ServerCallContext context) =>
        ProtoReshape.As<V1::ExperimentalDiffSchemaResponse>(await schema.DiffSchema(
            new V1::DiffSchemaRequest { Consistency = request.Consistency, ComparisonSchema = request.ComparisonSchema },
            context));

    public override async Task<V1::ExperimentalComputablePermissionsResponse> ExperimentalComputablePermissions(
        V1::ExperimentalComputablePermissionsRequest request, ServerCallContext context) =>
        ProtoReshape.As<V1::ExperimentalComputablePermissionsResponse>(await schema.ComputablePermissions(
            new V1::ComputablePermissionsRequest
            {
                Consistency = request.Consistency,
                DefinitionName = request.DefinitionName,
                RelationName = request.RelationName,
                OptionalDefinitionNameFilter = request.OptionalDefinitionNameFilter,
            },
            context));

    public override async Task<V1::ExperimentalDependentRelationsResponse> ExperimentalDependentRelations(
        V1::ExperimentalDependentRelationsRequest request, ServerCallContext context) =>
        ProtoReshape.As<V1::ExperimentalDependentRelationsResponse>(await schema.DependentRelations(
            new V1::DependentRelationsRequest
            {
                Consistency = request.Consistency,
                DefinitionName = request.DefinitionName,
                PermissionName = request.PermissionName,
            },
            context));

    public override async Task<V1::ExperimentalRegisterRelationshipCounterResponse> ExperimentalRegisterRelationshipCounter(
        V1::ExperimentalRegisterRelationshipCounterRequest request, ServerCallContext context)
    {
        if (request.RelationshipFilter is null)
            throw new RpcException(new Status(StatusCode.InvalidArgument, "relationship_filter is required"));

        try
        {
            await Relationships.RegisterRelationshipCounter(
                new RegisterCounterArgs(request.Name, ToWire(request.RelationshipFilter)));
        }
        catch (CounterOperationException ex)
        {
            throw ToRpc(ex);
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
        try
        {
            await Relationships.UnregisterRelationshipCounter(new UnregisterCounterArgs(request.Name));
        }
        catch (CounterOperationException ex)
        {
            throw ToRpc(ex);
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
        CountRelationshipsReply reply;
        try
        {
            reply = await Relationships.CountRelationships(new CountRelationshipsArgs(request.Name));
        }
        catch (CounterOperationException ex)
        {
            throw ToRpc(ex);
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

    private static RpcException ToRpc(CounterOperationException ex) =>
        // Both counter failures map to FailedPrecondition, matching the SpiceDB reference.
        new(new Status(StatusCode.FailedPrecondition, ex.Message));
}
