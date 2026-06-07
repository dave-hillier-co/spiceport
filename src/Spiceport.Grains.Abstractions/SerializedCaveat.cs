using Spiceport.Core;

namespace Spiceport.Grains.Abstractions;

/// <summary>
/// An Orleans-serializable mirror of the engine's <c>CaveatExpression</c> tree, used to carry a
/// pre-context gating caveat across the grain boundary.
/// </summary>
/// <remarks>
/// The engine's <c>CaveatExpression</c> lives in Spiceport.Engine, which the abstractions assembly
/// deliberately does not reference. This tree is the wire form: a leaf names a caveat (with its
/// relationship-supplied context from <see cref="ContextualizedCaveat"/>, which is a Core type), and
/// composites combine children with OR / AND / NOT. The <see cref="Spiceport.Grains.OrleansDispatcher"/>
/// maps between this and the engine's tree.
/// </remarks>
[GenerateSerializer]
public abstract record SerializedCaveat
{
    /// <summary>A leaf caveat: a named caveat with its relationship-supplied context.</summary>
    [GenerateSerializer]
    public sealed record Leaf(
        [property: Id(0)] string CaveatName,
        [property: Id(1)] IReadOnlyDictionary<string, object?>? Context) : SerializedCaveat;

    /// <summary>A logical OR over child expressions.</summary>
    [GenerateSerializer]
    public sealed record Or([property: Id(0)] IReadOnlyList<SerializedCaveat> Children) : SerializedCaveat;

    /// <summary>A logical AND over child expressions.</summary>
    [GenerateSerializer]
    public sealed record And([property: Id(0)] IReadOnlyList<SerializedCaveat> Children) : SerializedCaveat;

    /// <summary>A logical NOT of a single child expression.</summary>
    [GenerateSerializer]
    public sealed record Not([property: Id(0)] SerializedCaveat Child) : SerializedCaveat;
}
