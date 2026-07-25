using Orleans.Runtime;
using Orleans.Runtime.Placement;

namespace Spiceport.Grains;

/// <summary>
/// The placement director behind <see cref="GraphLocalityPlacement"/>: chooses the silo for the FIRST
/// activation of a graph grain by a stable hash of the grain key's locality key — the (type, id) of
/// the object whose <see cref="GraphShardGrain"/> the grain reads — so compute lands on the silo that
/// (under the same rule) hosts its data shard. Every silo sorts the compatible-silo list by
/// <see cref="SiloAddress"/> before indexing, so every silo computes the same answer for the same key.
/// Note the "same answer" holds PER GRAIN TYPE: co-location additionally assumes the attributed grain
/// families share one compatible-silo set (true here — all four are hosted on every silo). Under
/// heterogeneous hosting (placement filters, families absent from some silos) the per-type lists
/// differ and the modulus names different silos for the same key — correctness is unaffected (this is
/// only a hint) but the locality benefit silently disappears; revisit the indexing rule before ever
/// splitting the families across silo subsets.
/// </summary>
/// <remarks>
/// A pure locality hint, default OFF (<see cref="GraphPlacementOptions.CoLocateWithShards"/>): when
/// the option is off — or a key shape is unrecognized — the director mirrors Orleans' random
/// placement (uniform pick from the compatible silos, honoring an explicit placement hint first,
/// exactly as <c>RandomPlacementDirector</c> does). See <see cref="GraphLocalityPlacement"/> for why
/// this is not the deleted hash ring: the directory, not this director, owns identity and dedup.
/// <para>
/// Locality-key extraction dispatches on the key's segment count, which is unique across the four
/// attributed grain classes (the closed set this director can ever see — only classes bearing
/// <see cref="GraphLocalityPlacementAttribute"/> route here). Segments are compared in their ESCAPED
/// form: all four key codecs escape via the same <see cref="GrainKeyCodec"/>
/// (<see cref="Uri.EscapeDataString(string)"/> is deterministic), so the same object yields the same
/// locality key from every key shape without unescaping.
/// </para>
/// <list type="bullet">
/// <item><see cref="GrainKey"/> (CheckGrain, 8 segments): the (resourceType, resourceId) prefix —
/// the check's first data read is the resource's forward shard.</item>
/// <item><see cref="GraphShardGrainKey"/> (GraphShardGrain, 3 segments): the (type, id) after the
/// direction segment. Direction is deliberately ignored: the forward and reverse shards of the same
/// object co-locate, which is fine and simple.</item>
/// <item><see cref="MembershipWalkKey"/> (MembershipWalkGrain, 5 segments): the (subjectType,
/// subjectId) prefix — the walk's one data read is the subject's REVERSE shard, whose locality key
/// is that same (type, id).</item>
/// <item><see cref="SubjectFrontierKey"/> (SubjectFrontierGrain, 7 segments): the (resourceType,
/// resourceId) prefix. The frontier key names its subject side by TYPE only (no subject id exists in
/// the key), and the frontier's whole LookupSubjects walk starts by reading the RESOURCE root's
/// forward shard — so the resource is the honest locality key, co-locating the frontier with that
/// shard and the CheckGrains of the same resource.</item>
/// </list>
/// </remarks>
internal sealed class GraphLocalityPlacementDirector(GraphPlacementOptions? options = null) : IPlacementDirector
{
    private readonly GraphPlacementOptions _options = options ?? new GraphPlacementOptions();

    /// <inheritdoc />
    public Task<SiloAddress> OnAddActivation(
        PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
    {
        var silos = context.GetCompatibleSilos(target);

        // An explicit placement hint (e.g. placed-by-caller scenarios) wins in both modes, exactly as
        // Orleans' own RandomPlacementDirector honors it.
        if (IPlacementDirector.GetPlacementHint(target.RequestContextData, silos) is { } hint)
            return Task.FromResult(hint);

        if (!_options.CoLocateWithShards || silos.Length == 1)
            return Task.FromResult(silos[Random.Shared.Next(silos.Length)]);

        var localityKey = LocalityKey(target.GrainIdentity.Key.ToString());
        if (localityKey is null)
            return Task.FromResult(silos[Random.Shared.Next(silos.Length)]);

        // Sort by SiloAddress so every silo indexes an identically-ordered list (GetCompatibleSilos
        // makes no ordering promise); the modulus then names one silo cluster-wide for this key.
        var sorted = (SiloAddress[])silos.Clone();
        Array.Sort(sorted);
        return Task.FromResult(sorted[(int)(StableHash.Fnv1a64(localityKey) % (ulong)sorted.Length)]);
    }

    /// <summary>
    /// Extracts the locality key — the escaped <c>type/id</c> of the object whose shard the grain
    /// reads — from a graph grain's string key, dispatching on segment count (see the class remarks);
    /// null for an unrecognized shape (the caller falls back to a random pick).
    /// </summary>
    internal static string? LocalityKey(string grainKey)
    {
        var parts = grainKey.Split('/');
        return parts.Length switch
        {
            8 => $"{parts[0]}/{parts[1]}", // GrainKey (CheckGrain): resourceType/resourceId
            3 => $"{parts[1]}/{parts[2]}", // GraphShardGrainKey: type/id (direction ignored)
            5 => $"{parts[0]}/{parts[1]}", // MembershipWalkKey: subjectType/subjectId
            7 => $"{parts[0]}/{parts[1]}", // SubjectFrontierKey: resourceType/resourceId
            _ => null,
        };
    }

}
