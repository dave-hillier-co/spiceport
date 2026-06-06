namespace Spiceport.Core;

/// <summary>
/// A relation (or permission) qualified by a namespace, with no object id:
/// <c>namespace#relation</c> (e.g. "org#admin").
/// </summary>
/// <param name="ObjectType">The namespace / definition name.</param>
/// <param name="Relation">The relation or permission name.</param>
public sealed record RelationReference(string ObjectType, string Relation)
{
    /// <summary>Formats as <c>namespace#relation</c>.</summary>
    public override string ToString() => $"{ObjectType}#{Relation}";
}
