using System.Collections.Immutable;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// An <see cref="IDispatcher"/> that turns each sub-problem into a grain call: it derives the
/// canonical grain key from the request, resolves the keyed <see cref="ICheckGrain"/> via
/// <see cref="IGrainFactory"/>, and invokes <see cref="ICheckGrain.DispatchCheck"/>.
/// </summary>
/// <remarks>
/// This is how recursion crosses grain boundaries: a grain computing one sub-problem dispatches each
/// of its children through an <see cref="OrleansDispatcher"/>, so every child becomes a (potentially
/// remote) call to a different grain keyed by that child's identity. The dispatcher maps the engine's
/// <see cref="DispatchCheckRequest"/> / <see cref="DispatchCheckResult"/> to and from the serializable
/// grain <see cref="DispatchCheckArgs"/> / <see cref="DispatchCheckReply"/>.
/// </remarks>
public sealed class OrleansDispatcher : IDispatcher
{
    private readonly IGrainFactory _grains;
    private readonly ISchemaHashSource _schemaHash;

    /// <summary>Creates an Orleans dispatcher.</summary>
    /// <param name="grains">The grain factory used to resolve keyed check grains.</param>
    /// <param name="schemaHash">Supplies the live schema hash embedded in every grain key (scopes identity to the current schema).</param>
    public OrleansDispatcher(IGrainFactory grains, ISchemaHashSource schemaHash)
    {
        ArgumentNullException.ThrowIfNull(grains);
        ArgumentNullException.ThrowIfNull(schemaHash);
        _grains = grains;
        _schemaHash = schemaHash;
    }

    /// <inheritdoc/>
    public async Task<DispatchCheckResult> DispatchCheck(DispatchCheckRequest request, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(request);
        ct.ThrowIfCancellationRequested();

        var key = GrainKey.Build(
            request.Resource,
            request.Subject,
            request.Meta.Revision.ToString(),
            _schemaHash.CurrentSchemaHash,
            request.Meta.Mode);

        var grain = _grains.GetGrain<ICheckGrain>(key);

        var args = new DispatchCheckArgs(
            request.Meta.DepthRemaining,
            request.Meta.Visited
                .Select(v => new VisitKeyParts(
                    v.ResourceType, v.ResourceId, v.ResourceRelation,
                    v.SubjectType, v.SubjectId, v.SubjectRelation))
                .ToImmutableHashSet(),
            request.Meta.Mode);

        var reply = await grain.DispatchCheck(args).ConfigureAwait(false);

        return new DispatchCheckResult(reply.Member, CaveatWire.FromWire(reply.Caveat), reply.CycleCut);
    }

    /// <summary>Rebuilds the cycle-guard visited set carried in args into the engine's typed set.</summary>
    internal static ImmutableHashSet<VisitKey> ToVisitKeys(IReadOnlySet<VisitKeyParts> parts) =>
        parts
            .Select(v => new VisitKey(
                v.ResourceType, v.ResourceId, v.ResourceRelation,
                v.SubjectType, v.SubjectId, v.SubjectRelation))
            .ToImmutableHashSet();
}
