using Spiceport.Core;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// Maps between the engine's in-process <see cref="CaveatExpression"/> tree and the
/// Orleans-serializable <see cref="SerializedCaveat"/> wire form carried across the grain boundary.
/// </summary>
/// <remarks>
/// The abstractions assembly cannot reference the engine, so the wire form is a structural mirror of
/// the engine tree; this assembly (which references both) owns the round-trip.
/// </remarks>
internal static class CaveatWire
{
    public static SerializedCaveat? ToWire(CaveatExpression? expr) => expr switch
    {
        null => null,
        CaveatExpression.Leaf leaf => new SerializedCaveat.Leaf(leaf.Caveat.CaveatName, leaf.Caveat.Context),
        CaveatExpression.Or or => new SerializedCaveat.Or(or.Children.Select(ToWireNonNull).ToList()),
        CaveatExpression.And and => new SerializedCaveat.And(and.Children.Select(ToWireNonNull).ToList()),
        CaveatExpression.Not not => new SerializedCaveat.Not(ToWireNonNull(not.Child)),
        _ => throw new NotSupportedException($"Unknown caveat expression node: {expr.GetType().Name}"),
    };

    public static CaveatExpression? FromWire(SerializedCaveat? wire) => wire switch
    {
        null => null,
        SerializedCaveat.Leaf leaf => new CaveatExpression.Leaf(
            new ContextualizedCaveat(leaf.CaveatName, leaf.Context)),
        SerializedCaveat.Or or => new CaveatExpression.Or(or.Children.Select(FromWireNonNull).ToList()),
        SerializedCaveat.And and => new CaveatExpression.And(and.Children.Select(FromWireNonNull).ToList()),
        SerializedCaveat.Not not => new CaveatExpression.Not(FromWireNonNull(not.Child)),
        _ => throw new NotSupportedException($"Unknown serialized caveat node: {wire.GetType().Name}"),
    };

    private static SerializedCaveat ToWireNonNull(CaveatExpression expr) =>
        ToWire(expr) ?? throw new InvalidOperationException("Composite caveat child must not be null.");

    private static CaveatExpression FromWireNonNull(SerializedCaveat wire) =>
        FromWire(wire) ?? throw new InvalidOperationException("Composite caveat child must not be null.");
}
