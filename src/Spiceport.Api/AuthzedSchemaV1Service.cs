using System.Collections.Immutable;
using Grpc.Core;
using Spiceport.Core;
using Spiceport.Engine;
using Spiceport.Grains;
using Spiceport.Grains.Abstractions;
using V1 = Authzed.Api.V1;
using RelationReference = Spiceport.Engine.RelationReference;

namespace Spiceport.Api;

/// <summary>
/// gRPC front door for the <c>authzed.api.v1.SchemaService</c>. WriteSchema/ReadSchema are pure
/// translation over the data-plane <see cref="IRelationshipsGrain"/>. The schema-introspection RPCs
/// (ReflectSchema/DiffSchema/ComputablePermissions/DependentRelations) operate on the in-process compiled
/// schema via <see cref="ISchemaProvider.Current"/>, reusing the Core model, the shared
/// <see cref="SchemaDiff"/> diff core, the <see cref="ReachabilityGraph"/>, and <see cref="ReflectionMapper"/>.
/// The v1 responses in this snapshot carry no ZedToken (<c>read_at</c> is left unset).
/// </summary>
public sealed class AuthzedSchemaV1Service(IGrainFactory grains, ISchemaProvider schema)
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

        return new V1::ReadSchemaResponse
        {
            SchemaText = reply.SchemaText,
            // read_at must be populated: zed backup reads the schema, then uses this token for the
            // export's at_exact_snapshot consistency. A null token nil-derefs the zed client.
            ReadAt = new V1::ZedToken { Token = reply.ReadAtToken },
        };
    }

    public override async Task<V1::WriteSchemaResponse> WriteSchema(
        V1::WriteSchemaRequest request, ServerCallContext context)
    {
        try
        {
            var reply = await Relationships.WriteSchema(new WriteSchemaArgs(request.Schema));
            return new V1::WriteSchemaResponse { WrittenAt = new V1::ZedToken { Token = reply.WrittenAtToken } };
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

    public override Task<V1::ReflectSchemaResponse> ReflectSchema(
        V1::ReflectSchemaRequest request, ServerCallContext context)
    {
        var snapshot = schema.Current;
        var filters = SchemaFilters.FromRequest(request.OptionalFilters);

        var response = new V1::ReflectSchemaResponse();

        foreach (var def in snapshot.Namespaces)
        {
            if (!filters.MatchesDefinition(def.Name))
                continue;

            var reflected = new V1::ReflectionDefinition { Name = def.Name, Comment = string.Empty };
            foreach (var rel in def.Relations)
            {
                if (rel.IsPermission)
                {
                    if (filters.MatchesPermission(def.Name, rel.Name))
                        reflected.Permissions.Add(ReflectionMapper.Permission(def.Name, rel));
                }
                else
                {
                    if (filters.MatchesRelation(def.Name, rel.Name))
                        reflected.Relations.Add(ReflectionMapper.Relation(def.Name, rel));
                }
            }

            response.Definitions.Add(reflected);
        }

        foreach (var caveat in snapshot.Caveats)
        {
            if (filters.MatchesCaveat(caveat.Name))
                response.Caveats.Add(ReflectionMapper.Caveat(caveat));
        }

        return Task.FromResult(response);
    }

    public override Task<V1::DiffSchemaResponse> DiffSchema(
        V1::DiffSchemaRequest request, ServerCallContext context)
    {
        Spiceport.Schema.CompiledSchema comparison;
        try
        {
            comparison = Spiceport.Schema.SchemaCompiler.CompileSchema(request.ComparisonSchema);
            // Reject duplicate/reused names (and other type errors) up front: the diff core keys by name
            // and would otherwise throw a raw ArgumentException on a duplicate definition/caveat.
            SchemaTypeValidator.Validate(comparison);
        }
        catch (Exception ex) when (ex is Spiceport.Schema.SchemaCompileException or SchemaTypeException or ArgumentException)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }

        // Diff existing -> comparison so added/removed are relative to the live schema as the base.
        var deltas = SchemaDiff.Compute(schema.Current.Schema, comparison);

        var response = new V1::DiffSchemaResponse();
        foreach (var delta in deltas)
            response.Diffs.Add(MapDelta(delta));

        return Task.FromResult(response);
    }

    public override Task<V1::ComputablePermissionsResponse> ComputablePermissions(
        V1::ComputablePermissionsRequest request, ServerCallContext context)
    {
        var namespaces = schema.Current.Namespaces.ToImmutableDictionary(n => n.Name);

        IReadOnlyList<RelationReference> reachable;
        try
        {
            reachable = SchemaIntrospection.ComputablePermissions(
                namespaces,
                request.DefinitionName,
                request.RelationName,
                string.IsNullOrEmpty(request.OptionalDefinitionNameFilter) ? null : request.OptionalDefinitionNameFilter);
        }
        catch (SchemaIntrospectionException ex)
        {
            throw ToRpc(ex);
        }

        var response = new V1::ComputablePermissionsResponse();
        foreach (var r in reachable)
            response.Permissions.Add(RelationReferenceProto(namespaces, r));

        return Task.FromResult(response);
    }

    public override Task<V1::DependentRelationsResponse> DependentRelations(
        V1::DependentRelationsRequest request, ServerCallContext context)
    {
        var namespaces = schema.Current.Namespaces.ToImmutableDictionary(n => n.Name);

        IReadOnlyList<RelationReference> dependents;
        try
        {
            dependents = SchemaIntrospection.DependentRelations(
                namespaces, request.DefinitionName, request.PermissionName);
        }
        catch (SchemaIntrospectionException ex)
        {
            throw ToRpc(ex);
        }

        var response = new V1::DependentRelationsResponse();
        foreach (var r in dependents)
            response.Relations.Add(RelationReferenceProto(namespaces, r));

        return Task.FromResult(response);
    }

    private static V1::ReflectionRelationReference RelationReferenceProto(
        ImmutableDictionary<string, NamespaceDefinition> namespaces, RelationReference reference)
    {
        var isPermission = false;
        if (namespaces.TryGetValue(reference.Namespace, out var ns))
            isPermission = ns.Relations.FirstOrDefault(r => r.Name == reference.Relation)?.IsPermission ?? false;

        return new V1::ReflectionRelationReference
        {
            DefinitionName = reference.Namespace,
            RelationName = reference.Relation,
            IsPermission = isPermission,
        };
    }

    private static RpcException ToRpc(SchemaIntrospectionException ex) => ex.Kind switch
    {
        SchemaIntrospectionErrorKind.DefinitionNotFound =>
            new RpcException(new Status(StatusCode.NotFound, ex.Message)),
        SchemaIntrospectionErrorKind.RelationNotFound =>
            new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message)),
        SchemaIntrospectionErrorKind.NotAPermission =>
            new RpcException(new Status(StatusCode.InvalidArgument, ex.Message)),
        _ => new RpcException(new Status(StatusCode.Internal, ex.Message)),
    };

    private static V1::ReflectionSchemaDiff MapDelta(SchemaDelta delta) => delta switch
    {
        SchemaDelta.DefinitionAdded(var def) =>
            new() { DefinitionAdded = ReflectionMapper.Definition(def) },
        SchemaDelta.DefinitionRemoved(var def) =>
            new() { DefinitionRemoved = ReflectionMapper.Definition(def) },
        SchemaDelta.RelationAdded(var defName, var rel) =>
            new() { RelationAdded = ReflectionMapper.Relation(defName, rel) },
        SchemaDelta.RelationRemoved(var defName, var rel) =>
            new() { RelationRemoved = ReflectionMapper.Relation(defName, rel) },
        SchemaDelta.RelationSubjectTypeAdded(var defName, var rel, var subj) =>
            new()
            {
                RelationSubjectTypeAdded = new V1::ReflectionRelationSubjectTypeChange
                {
                    Relation = ReflectionMapper.Relation(defName, rel),
                    ChangedSubjectType = ReflectionMapper.TypeReference(subj),
                },
            },
        SchemaDelta.RelationSubjectTypeRemoved(var defName, var rel, var subj) =>
            new()
            {
                RelationSubjectTypeRemoved = new V1::ReflectionRelationSubjectTypeChange
                {
                    Relation = ReflectionMapper.Relation(defName, rel),
                    ChangedSubjectType = ReflectionMapper.TypeReference(subj),
                },
            },
        SchemaDelta.PermissionAdded(var defName, var perm) =>
            new() { PermissionAdded = ReflectionMapper.Permission(defName, perm) },
        SchemaDelta.PermissionRemoved(var defName, var perm) =>
            new() { PermissionRemoved = ReflectionMapper.Permission(defName, perm) },
        SchemaDelta.PermissionExprChanged(var defName, var perm) =>
            new() { PermissionExprChanged = ReflectionMapper.Permission(defName, perm) },
        SchemaDelta.CaveatAdded(var caveat) =>
            new() { CaveatAdded = ReflectionMapper.Caveat(caveat) },
        SchemaDelta.CaveatRemoved(var caveat) =>
            new() { CaveatRemoved = ReflectionMapper.Caveat(caveat) },
        SchemaDelta.CaveatExprChanged(var caveat) =>
            new() { CaveatExprChanged = ReflectionMapper.Caveat(caveat) },
        SchemaDelta.CaveatParameterAdded(var caveatName, var name, var type) =>
            new() { CaveatParameterAdded = ReflectionMapper.CaveatParameter(caveatName, name, type) },
        SchemaDelta.CaveatParameterRemoved(var caveatName, var name, var type) =>
            new() { CaveatParameterRemoved = ReflectionMapper.CaveatParameter(caveatName, name, type) },
        SchemaDelta.CaveatParameterTypeChanged(var caveatName, var name, var type, var previous) =>
            new()
            {
                CaveatParameterTypeChanged = new V1::ReflectionCaveatParameterTypeChange
                {
                    Parameter = ReflectionMapper.CaveatParameter(caveatName, name, type),
                    PreviousType = ReflectionMapper.TypeString(previous),
                },
            },
        _ => throw new InvalidOperationException($"unhandled schema delta `{delta.GetType().Name}`"),
    };
}
