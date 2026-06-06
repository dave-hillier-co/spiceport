namespace Spiceport.Core;

/// <summary>The kind of mutation to apply to a relationship.</summary>
public enum UpdateOperation
{
    /// <summary>Create the relationship if it does not exist, otherwise update it (upsert).</summary>
    Touch = 0,

    /// <summary>Create the relationship; fails if it already exists.</summary>
    Create = 1,

    /// <summary>Delete the relationship.</summary>
    Delete = 2,
}

/// <summary>A single relationship mutation.</summary>
/// <param name="Relationship">The relationship being mutated.</param>
/// <param name="Operation">The operation to apply.</param>
public sealed record RelationshipUpdate(Relationship Relationship, UpdateOperation Operation);
