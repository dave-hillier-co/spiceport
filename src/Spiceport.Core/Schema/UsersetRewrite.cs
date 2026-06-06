using System.Collections.Immutable;

namespace Spiceport.Core;

/// <summary>Where a <see cref="ComputedUserset"/> is computed.</summary>
public enum ComputedUsersetObject
{
    /// <summary>Compute on the resource (the same object).</summary>
    TupleObject = 0,

    /// <summary>Compute on the subject of a traversed tuple.</summary>
    TupleUsersetObject = 1,
}

/// <summary>
/// References another relation/permission, computed either on the resource itself
/// or on the subject reached via a tupleset traversal.
/// </summary>
/// <param name="Object">Which object the relation is computed on.</param>
/// <param name="Relation">The relation/permission name to compute.</param>
public sealed record ComputedUserset(ComputedUsersetObject Object, string Relation)
{
    /// <summary>Computes the relation on the resource itself (the common case).</summary>
    public static ComputedUserset OnResource(string relation) => new(ComputedUsersetObject.TupleObject, relation);
}

/// <summary>
/// Traverses a tupleset relation and computes a relation on each reached subject.
/// E.g. <c>parent-&gt;view</c>: walk "parent", then compute "view" on each parent.
/// </summary>
/// <param name="TuplesetRelation">The relation on the resource to traverse.</param>
/// <param name="ComputedUserset">The relation to compute on each traversed subject.</param>
public sealed record TupleToUserset(string TuplesetRelation, ComputedUserset ComputedUserset);

/// <summary>Aggregation function for a <see cref="FunctionedTupleToUserset"/>.</summary>
public enum TupleToUsersetFunction
{
    Unspecified = 0,

    /// <summary>The permission holds if ANY traversed subject grants it.</summary>
    Any = 1,

    /// <summary>The permission holds only if ALL traversed subjects grant it.</summary>
    All = 2,
}

/// <summary>
/// A <see cref="TupleToUserset"/> with an explicit aggregation function (<c>.any()</c> / <c>.all()</c>).
/// </summary>
/// <param name="Function">How results across traversed subjects are aggregated.</param>
/// <param name="TuplesetRelation">The relation on the resource to traverse.</param>
/// <param name="ComputedUserset">The relation to compute on each traversed subject.</param>
public sealed record FunctionedTupleToUserset(
    TupleToUsersetFunction Function,
    string TuplesetRelation,
    ComputedUserset ComputedUserset);

/// <summary>
/// A single operand within a <see cref="SetOperation"/>. This is a closed (sealed)
/// discriminated union of the possible child kinds.
/// </summary>
public abstract record SetOperationChild
{
    private SetOperationChild() { }

    /// <summary>Optional hierarchical position path within the rewrite tree.</summary>
    public ImmutableList<uint>? OperationPath { get; init; }

    /// <summary>Deprecated: the legacy "_this" set (all directly written subjects).</summary>
    public sealed record This : SetOperationChild;

    /// <summary>The empty set.</summary>
    public sealed record Nil : SetOperationChild;

    /// <summary>The resource itself treated as a subject (the "self" operand).</summary>
    public sealed record Self : SetOperationChild;

    /// <summary>A computed userset operand.</summary>
    public sealed record ComputedUsersetChild(ComputedUserset Value) : SetOperationChild;

    /// <summary>A tuple-to-userset traversal operand.</summary>
    public sealed record TupleToUsersetChild(TupleToUserset Value) : SetOperationChild;

    /// <summary>A functioned tuple-to-userset traversal operand.</summary>
    public sealed record FunctionedTupleToUsersetChild(FunctionedTupleToUserset Value) : SetOperationChild;

    /// <summary>A nested rewrite operand.</summary>
    public sealed record NestedRewrite(UsersetRewrite Value) : SetOperationChild;
}

/// <summary>How the children of a <see cref="SetOperation"/> are combined.</summary>
public enum SetOperationType
{
    /// <summary>OR.</summary>
    Union = 1,

    /// <summary>AND.</summary>
    Intersection = 2,

    /// <summary>AND-NOT (subtract subsequent children from the first).</summary>
    Exclusion = 3,
}

/// <summary>
/// A union / intersection / exclusion over one or more child operands.
/// </summary>
/// <param name="Type">The combination operator.</param>
/// <param name="Children">The operands (at least one).</param>
/// <param name="OperationPath">Optional hierarchical position path within the rewrite tree.</param>
public sealed record SetOperation(
    SetOperationType Type,
    ImmutableList<SetOperationChild> Children,
    ImmutableList<uint>? OperationPath = null)
{
    /// <summary>Creates a union over the given children.</summary>
    public static SetOperation Union(params SetOperationChild[] children) =>
        new(SetOperationType.Union, [.. children]);

    /// <summary>Creates an intersection over the given children.</summary>
    public static SetOperation Intersection(params SetOperationChild[] children) =>
        new(SetOperationType.Intersection, [.. children]);

    /// <summary>Creates an exclusion over the given children.</summary>
    public static SetOperation Exclusion(params SetOperationChild[] children) =>
        new(SetOperationType.Exclusion, [.. children]);
}

/// <summary>
/// The top-level computation for a permission. Always a single <see cref="SetOperation"/>.
/// </summary>
/// <param name="Operation">The root set operation.</param>
public sealed record UsersetRewrite(SetOperation Operation);
