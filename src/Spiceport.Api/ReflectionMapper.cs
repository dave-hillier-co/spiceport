using System.Text;
using Spiceport.Core;
using V1 = Authzed.Api.V1;

namespace Spiceport.Api;

/// <summary>
/// Translates the compiled Core schema model into the <c>authzed.api.v1</c> reflection proto messages.
/// Shared by <c>ReflectSchema</c> (whole-schema dump) and <c>DiffSchema</c> (per-delta builders). Port of
/// SpiceDB's <c>reflectionapi.go</c> (<c>namespaceAPIRepr</c>/<c>relationAPIRepr</c>/<c>permissionAPIRepr</c>/
/// <c>typeAPIRepr</c>/<c>caveatAPIRepr</c>).
/// </summary>
/// <remarks>
/// Honest gaps faithful to the compiled model: <c>comment</c> is always empty because the Core model
/// carries no doc-comment metadata (the lexer/AST drops comments). Caveat <c>expression</c> is recovered
/// from <see cref="CaveatDefinition.SerializedExpression"/>, which stores the verbatim CEL body text UTF-8.
/// </remarks>
public static class ReflectionMapper
{
    /// <summary>Builds the <c>ReflectionDefinition</c> for a namespace, splitting relations vs permissions.</summary>
    public static V1::ReflectionDefinition Definition(NamespaceDefinition def)
    {
        var result = new V1::ReflectionDefinition { Name = def.Name, Comment = string.Empty };
        foreach (var rel in def.Relations)
        {
            if (rel.IsPermission)
                result.Permissions.Add(Permission(def.Name, rel));
            else
                result.Relations.Add(Relation(def.Name, rel));
        }
        return result;
    }

    /// <summary>Builds the <c>ReflectionRelation</c> for a base relation (with its subject types).</summary>
    public static V1::ReflectionRelation Relation(string parentDefinitionName, Relation relation)
    {
        var result = new V1::ReflectionRelation
        {
            Name = relation.Name,
            Comment = string.Empty,
            ParentDefinitionName = parentDefinitionName,
        };

        var allowed = relation.TypeInformation?.AllowedDirectRelations;
        if (allowed is not null)
        {
            foreach (var a in allowed)
                result.SubjectTypes.Add(TypeReference(a));
        }
        return result;
    }

    /// <summary>Builds the <c>ReflectionTypeReference</c> for one allowed subject type.</summary>
    public static V1::ReflectionTypeReference TypeReference(AllowedRelation allowed)
    {
        var result = new V1::ReflectionTypeReference
        {
            SubjectDefinitionName = allowed.ObjectType,
        };

        if (allowed.RequiredCaveat is { } caveat)
            result.OptionalCaveatName = caveat.CaveatName;

        if (allowed.IsPublicWildcard)
            result.IsPublicWildcard = true;
        else if (allowed.RelationName is { } rel && rel != CoreConstants.Ellipsis)
            result.OptionalRelationName = rel;
        else
            result.IsTerminalSubject = true;

        return result;
    }

    /// <summary>Builds the <c>ReflectionPermission</c> for a permission.</summary>
    public static V1::ReflectionPermission Permission(string parentDefinitionName, Relation permission) =>
        new()
        {
            Name = permission.Name,
            Comment = string.Empty,
            ParentDefinitionName = parentDefinitionName,
        };

    /// <summary>Builds the <c>ReflectionCaveat</c> for a caveat (parameters sorted by name).</summary>
    public static V1::ReflectionCaveat Caveat(CaveatDefinition caveat)
    {
        var result = new V1::ReflectionCaveat
        {
            Name = caveat.Name,
            Comment = string.Empty,
            // SerializedExpression holds the verbatim CEL body text (UTF-8), captured at compile time.
            Expression = Encoding.UTF8.GetString(caveat.SerializedExpression),
        };

        foreach (var (name, type) in caveat.ParameterTypes.OrderBy(p => p.Key, StringComparer.Ordinal))
            result.Parameters.Add(CaveatParameter(caveat.Name, name, type));

        return result;
    }

    /// <summary>Builds the <c>ReflectionCaveatParameter</c> for one parameter.</summary>
    public static V1::ReflectionCaveatParameter CaveatParameter(
        string parentCaveatName, string name, CaveatTypeReference type) =>
        new()
        {
            Name = name,
            Type = TypeString(type),
            ParentCaveatName = parentCaveatName,
        };

    /// <summary>Renders a caveat parameter type as a string, e.g. <c>int</c>, <c>list&lt;string&gt;</c>.</summary>
    public static string TypeString(CaveatTypeReference type)
    {
        if (type.ChildTypes is not { Count: > 0 } children)
            return type.TypeName;
        var args = string.Join(", ", children.Select(TypeString));
        return $"{type.TypeName}<{args}>";
    }
}
