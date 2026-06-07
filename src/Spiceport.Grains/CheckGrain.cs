using System.Collections.Immutable;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>
/// A grain keyed by a single canonical sub-problem. It computes exactly ONE expansion step of that
/// sub-problem and dispatches every deeper sub-problem back out through the (Orleans) dispatcher, so
/// recursion crosses grain boundaries and the mesh is real.
/// </summary>
/// <remarks>
/// On <see cref="DispatchCheck"/> the grain decodes its identity from its string key (resource,
/// subject, revision, schema hash), resolves a snapshot reader at that revision from the injected
/// <see cref="IDatastore"/> singleton, and runs a <see cref="LocalDispatcher"/> whose onward
/// <see cref="LocalDispatcher.Dispatcher"/> is the silo-wide onward dispatcher
/// (<see cref="ISiloDispatcher"/> = Caching over Orleans). The local dispatcher performs the one step;
/// children flow through Orleans as further grain calls.
/// <para>
/// CACHING: there is no per-activation result cache here. Caching lives in the silo-wide
/// <see cref="CachingDispatcher"/> (the shared branch cache), through which both the API entry and the
/// grain's onward sub-dispatch flow; the grain identity itself additionally provides activation-level
/// locality per sub-problem key.
/// </para>
/// </remarks>
public sealed class CheckGrain(
    IDatastore datastore,
    ISchemaProvider schemaProvider,
    ISiloDispatcher onward) : Grain, ICheckGrain
{
    /// <inheritdoc />
    public async Task<DispatchCheckReply> DispatchCheck(DispatchCheckArgs args)
    {
        ArgumentNullException.ThrowIfNull(args);

        var parts = GrainKey.Parse(this.GetPrimaryKeyString());
        var revision = RevisionCodec.Parse(parts.Revision);
        var now = DateTimeOffset.UtcNow;

        // A LocalDispatcher does ONE expansion step; its onward Dispatcher (the silo-wide
        // Caching-over-Orleans dispatcher) turns each child sub-problem into a further grain call.
        var namespaces = schemaProvider.Current.Namespaces.ToImmutableDictionary(ns => ns.Name);
        var state = new CheckState();
        var local = new LocalDispatcher(
            namespaces,
            datastore.SnapshotReader,
            now,
            state)
        {
            Dispatcher = onward.Dispatcher,
        };

        var meta = new ResolverMeta(
            revision,
            args.DepthRemaining,
            OrleansDispatcher.ToVisitKeys(args.Visited));
        var request = new DispatchCheckRequest(parts.Resource, parts.Subject, meta);

        var result = await local.DispatchCheck(request, CancellationToken.None).ConfigureAwait(false);

        return new DispatchCheckReply(result.Member, CaveatWire.ToWire(result.Caveat), result.CycleCut);
    }
}
