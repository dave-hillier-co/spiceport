using Spiceport.Core;
using Spiceport.Schema;

namespace Spiceport.Conformance.Tests;

/// <summary>
/// Mirrors SpiceDB's write-time <c>relationships.ValidateRelationshipsForCreateOrTouch</c>: a written
/// relationship's (subject type, subrelation, caveat) must match one of the relation's allowed types.
/// Spiceport's production write path does NOT perform this check (a known gap vs SpiceDB), so the
/// loader-robustness suite applies it explicitly before writing.
/// </summary>
public static class RelationshipSchemaValidator
{
    /// <summary>Raised when a written relationship is not permitted by the relation's allowed types.</summary>
    public sealed class RelationshipTypeException(string message) : Exception(message);

    public static void ValidateAll(CompiledSchema schema, IEnumerable<Relationship> relationships)
    {
        var byName = schema.Namespaces.ToDictionary(n => n.Name, StringComparer.Ordinal);
        foreach (var rel in relationships)
        {
            ValidateOne(byName, rel);
        }
    }

    private static void ValidateOne(IReadOnlyDictionary<string, NamespaceDefinition> byName, Relationship rel)
    {
        if (!byName.TryGetValue(rel.Resource.ObjectType, out var def))
        {
            throw new RelationshipTypeException(
                $"object definition `{rel.Resource.ObjectType}` not found");
        }

        var relation = def.Relations.FirstOrDefault(r => r.Name == rel.Resource.Relation && !r.IsPermission)
            ?? throw new RelationshipTypeException(
                $"relation/permission `{rel.Resource.Relation}` not found under definition `{rel.Resource.ObjectType}`");

        var allowed = relation.TypeInformation?.AllowedDirectRelations ?? [];
        var caveatName = rel.OptionalCaveat?.CaveatName;
        var subjectRel = rel.Subject.Relation;

        foreach (var a in allowed)
        {
            var typeOk = a.ObjectType == rel.Subject.ObjectType;
            var relOk = a.IsPublicWildcard
                ? rel.Subject.IsPublicWildcard
                : !rel.Subject.IsPublicWildcard && (a.RelationName ?? CoreConstants.Ellipsis) == subjectRel;
            var caveatOk = a.RequiredCaveat?.CaveatName == caveatName; // both null, or both same name
            if (typeOk && relOk && caveatOk)
            {
                return;
            }
        }

        // SpiceDB message: "subjects of type `user with some_caveat` are not allowed on relation `resource#reader`"
        var subjDesc = caveatName is null ? rel.Subject.ObjectType : $"{rel.Subject.ObjectType} with {caveatName}";
        throw new RelationshipTypeException(
            $"subjects of type `{subjDesc}` are not allowed on relation `{rel.Resource.ObjectType}#{rel.Resource.Relation}`");
    }
}
