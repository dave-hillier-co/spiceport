using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// Encodes and decodes the <c>IGraphShardGrain</c> string key, which IS the shard's identity — one
/// adjacency slice of the graph: <c>direction/objectType/objectId</c>, where direction is <c>"f"</c>
/// (forward: rows whose RESOURCE is the object) or <c>"r"</c> (reverse: rows whose SUBJECT is the
/// object).
/// </summary>
/// <remarks>
/// Mirrors <see cref="GrainKey"/>/<see cref="MembershipWalkKey"/>'s escaping/parsing conventions via
/// <see cref="GrainKeyCodec"/>: components are URL-style escaped so a literal separator in any field
/// cannot corrupt the key. UNLIKE those keys this one carries NO revision or schema-hash segment: the
/// shard holds the key's whole MVCC history within the GC window and serves any covered revision, so
/// the revision is a call argument (<c>RowsAt</c>), not part of the identity — one activation per slice,
/// not one per (slice, revision).
/// </remarks>
internal static class GraphShardGrainKey
{
    private const string ForwardSegment = "f";
    private const string ReverseSegment = "r";

    public static string Build(GraphShardKeyWire key) =>
        GrainKeyCodec.Join(
            key.Direction == GraphShardDirection.Forward ? ForwardSegment : ReverseSegment,
            key.ObjectType,
            key.ObjectId);

    public static GraphShardKeyWire Parse(string key)
    {
        var parts = GrainKeyCodec.Split(key, 3);
        var direction = parts[0] switch
        {
            ForwardSegment => GraphShardDirection.Forward,
            ReverseSegment => GraphShardDirection.Reverse,
            _ => throw new FormatException($"Malformed graph-shard key (unknown direction '{parts[0]}'): '{key}'."),
        };
        return new GraphShardKeyWire(direction, parts[1], parts[2]);
    }
}
