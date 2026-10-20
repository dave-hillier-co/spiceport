using Google.Protobuf;

namespace Spiceport.Api;

/// <summary>
/// Re-types a protobuf message as another message type with the SAME wire schema (identical field numbers,
/// types and nesting) by round-tripping its encoding. The ExperimentalService <c>Exp*</c> reflection messages
/// are field-for-field copies of the SchemaService <c>Reflection*</c> messages, and upstream SpiceDB builds
/// them from duplicated converters (<c>internal/services/v1/expreflection.go</c> is
/// <c>reflectionapi.go</c> with the types renamed); re-typing the v1 response reproduces that output without a
/// second converter. The schema equivalence this relies on is pinned by a descriptor test.
/// </summary>
internal static class ProtoReshape
{
    internal static TTarget As<TTarget>(IMessage source)
        where TTarget : IMessage<TTarget>, new()
    {
        var target = new TTarget();
        target.MergeFrom(source.ToByteString());
        return target;
    }
}
