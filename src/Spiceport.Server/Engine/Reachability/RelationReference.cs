namespace Spiceport.Engine;

/// <summary>
/// A reference to a relation (or permission) within a namespace. Port of SpiceDB's
/// <c>core.RelationReference</c>.
/// </summary>
/// <param name="Namespace">The object type / definition name.</param>
/// <param name="Relation">The relation or permission name.</param>
public sealed record RelationReference(string Namespace, string Relation);
