using Grpc.Core;
using V1 = Authzed.Api.V1;

namespace Spiceport.Api;

/// <summary>
/// The set of <c>ReflectionSchemaFilter</c>s applied (OR'd) to a <c>ReflectSchema</c> response. Port of
/// SpiceDB's <c>newSchemaFilters</c> + the <c>HasNamespace</c>/<c>HasCaveat</c>/<c>HasRelation</c>/
/// <c>HasPermission</c> prefix-match predicates in <c>reflectionapi.go</c>.
/// </summary>
/// <remarks>
/// Within a single filter, a definition filter and a caveat filter are mutually exclusive, as are a
/// relation filter and a permission filter; a relation/permission filter requires a definition filter.
/// Violations throw <see cref="RpcException"/> with <see cref="StatusCode.InvalidArgument"/>.
/// </remarks>
public sealed class SchemaFilters
{
    private readonly IReadOnlyList<V1::ReflectionSchemaFilter> _filters;

    private SchemaFilters(IReadOnlyList<V1::ReflectionSchemaFilter> filters) => _filters = filters;

    /// <summary>Validates and wraps the request's filters.</summary>
    public static SchemaFilters FromRequest(IReadOnlyList<V1::ReflectionSchemaFilter> filters)
    {
        foreach (var f in filters)
        {
            var hasDef = !string.IsNullOrEmpty(f.OptionalDefinitionNameFilter);
            var hasCaveat = !string.IsNullOrEmpty(f.OptionalCaveatNameFilter);
            var hasRelation = !string.IsNullOrEmpty(f.OptionalRelationNameFilter);
            var hasPermission = !string.IsNullOrEmpty(f.OptionalPermissionNameFilter);

            if (hasDef && hasCaveat)
                throw Invalid("cannot filter by both definition and caveat name");
            if (hasRelation && hasPermission)
                throw Invalid("cannot filter by both relation and permission name");
            // A caveat + relation/permission combination is rejected by one of the rules above:
            // with a definition filter the def+caveat rule fires; without one this rule fires.
            // Matches SpiceDB, which has no dedicated caveat-vs-relation check.
            if ((hasRelation || hasPermission) && !hasDef)
                throw Invalid("relation/permission filter requires a definition filter");
        }

        return new SchemaFilters(filters);
    }

    /// <summary>True when no filters are present (everything passes) or some filter admits the definition.</summary>
    public bool MatchesDefinition(string definitionName)
    {
        // Caveat-only filters do not admit any definition; if every filter is caveat-only, no definition matches.
        if (_filters.Count == 0)
            return true;

        foreach (var f in _filters)
        {
            // A caveat-only filter (caveat name set, no definition name) never admits a definition.
            if (!string.IsNullOrEmpty(f.OptionalCaveatNameFilter) && string.IsNullOrEmpty(f.OptionalDefinitionNameFilter))
                continue;

            if (DefinitionMatches(f, definitionName))
                return true;
        }

        return false;
    }

    /// <summary>True when no filters are present, or some caveat-scoped filter admits the caveat.</summary>
    public bool MatchesCaveat(string caveatName)
    {
        if (_filters.Count == 0)
            return true;

        foreach (var f in _filters)
        {
            // A filter admits caveats only if it is caveat-scoped (or unscoped). Definition/relation/
            // permission-scoped filters never admit caveats.
            var defScoped = !string.IsNullOrEmpty(f.OptionalDefinitionNameFilter)
                || !string.IsNullOrEmpty(f.OptionalRelationNameFilter)
                || !string.IsNullOrEmpty(f.OptionalPermissionNameFilter);
            if (defScoped)
                continue;

            if (string.IsNullOrEmpty(f.OptionalCaveatNameFilter)
                || caveatName.StartsWith(f.OptionalCaveatNameFilter, StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    /// <summary>True when the relation passes for a definition that already matched.</summary>
    public bool MatchesRelation(string definitionName, string relationName)
    {
        if (_filters.Count == 0)
            return true;

        foreach (var f in _filters)
        {
            if (!string.IsNullOrEmpty(f.OptionalCaveatNameFilter) && string.IsNullOrEmpty(f.OptionalDefinitionNameFilter))
                continue;
            if (!string.IsNullOrEmpty(f.OptionalPermissionNameFilter))
                continue; // permission-only relation-side: excludes relations.
            if (!DefinitionMatches(f, definitionName))
                continue;
            if (string.IsNullOrEmpty(f.OptionalRelationNameFilter)
                || relationName.StartsWith(f.OptionalRelationNameFilter, StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    /// <summary>True when the permission passes for a definition that already matched.</summary>
    public bool MatchesPermission(string definitionName, string permissionName)
    {
        if (_filters.Count == 0)
            return true;

        foreach (var f in _filters)
        {
            if (!string.IsNullOrEmpty(f.OptionalCaveatNameFilter) && string.IsNullOrEmpty(f.OptionalDefinitionNameFilter))
                continue;
            if (!string.IsNullOrEmpty(f.OptionalRelationNameFilter))
                continue; // relation-only: excludes permissions.
            if (!DefinitionMatches(f, definitionName))
                continue;
            if (string.IsNullOrEmpty(f.OptionalPermissionNameFilter)
                || permissionName.StartsWith(f.OptionalPermissionNameFilter, StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    private static bool DefinitionMatches(V1::ReflectionSchemaFilter f, string definitionName) =>
        string.IsNullOrEmpty(f.OptionalDefinitionNameFilter)
        || definitionName.StartsWith(f.OptionalDefinitionNameFilter, StringComparison.Ordinal);

    private static RpcException Invalid(string message) =>
        new(new Status(StatusCode.InvalidArgument, message));
}
