using Orleans.Concurrency;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains;

/// <summary>Toggle for the Leopard membership-walk accelerator. Default ON (opt-out).</summary>
public sealed class MembershipIndexOptions
{
    /// <summary>When false the accelerator is never consulted; lookups run the live traversal.</summary>
    public bool Enabled { get; init; } = true;

    /// <summary>
    /// The <see cref="MembershipWalkGrain"/> activation's idle-collection age. The grain key already embeds
    /// the exact revision and schema hash, so the keyspace rotates on its own as revisions/schema advance;
    /// idle activation collection at this age IS the memo's eviction policy — no separate cache/TTL
    /// bookkeeping is needed. Default 2 minutes (mirrors <see cref="ActivationMemoOptions.CollectionAge"/>).
    /// </summary>
    public TimeSpan CollectionAge { get; init; } = TimeSpan.FromMinutes(2);
}

/// <summary>
/// A grain keyed by "the membership-walk closure rooted at subject key <c>subjType:subjId#subjRelation</c>
/// at (revision, schemaHash)" (see <see cref="IMembershipWalkGrain"/>). It computes ONE reverse-adjacency
/// hop (<see cref="MembershipWalk.DirectParents"/>) and dispatches every parent onward to the SIBLING
/// <see cref="IMembershipWalkGrain"/> keyed by that parent, so the walk is a real sharded mesh — the
/// addressable replacement for the retired per-silo <c>MembershipIndexCache</c> replica.
/// </summary>
/// <remarks>
/// Flow: memo check (serve <see cref="_memo"/> when set and enabled) — parse the key — resolve a reader
/// pinned at the key's exact revision — resolve the schema at that revision — read its lazily-built
/// <see cref="Spiceport.Grains.SchemaSnapshot.MembershipCoverage"/> — compute this subject's direct parents
/// — if <see cref="MembershipWalkArgs.DepthRemaining"/> is exhausted, return an <see cref="MembershipClosureReply.Incomplete"/>
/// reply with no recursion — otherwise, for each parent whose OWN subject key (its (type, id, relation) as
/// a subject) is NOT already on the caller's path, dispatch to the sibling grain for that parent with the
/// path extended by this grain's own key and the depth budget decremented; union every child's nodes with
/// this hop's direct parents, and OR every child's <see cref="MembershipClosureReply.CycleCut"/> /
/// <see cref="MembershipClosureReply.Incomplete"/> into this reply. A parent already on the path is a
/// genuine back-edge: it is included as a node (it IS a direct parent) but skipped for recursion and marks
/// <see cref="MembershipClosureReply.CycleCut"/> — no further work is needed because that ancestor's own
/// closure is already being accumulated by the call that put it on the path.
/// <para>
/// TWO DIVERGENCES from <see cref="CheckGrain"/>'s dispatch pattern, both because this walk's job is a
/// COMPLETE candidate superset rather than a probabilistic-cycle-tolerant verdict:
/// <list type="bullet">
/// <item>
/// <b>Exact path list, not a bounded traversal bloom.</b> A false-positive skip in a probabilistic filter
/// would silently drop a whole subtree of candidates here — an incomplete result a caller could not detect
/// — whereas Check's bloom only risks an extra (harmless) re-expansion. So the path travels as an exact
/// list of canonical subject-key strings.
/// </item>
/// <item>
/// <b>Skip-on-path-hit, not call-anyway.</b> Check always makes the reentrant call and lets the callee's own
/// exact visited-set decide (because a callee-side check is needed for OTHER reasons); here a true back-edge
/// is unconditionally complete — the ancestor's closure is already being walked by the call that first put it
/// on the path — so no reentrant call is needed at all, only the cheap membership test against the path list.
/// </item>
/// </list>
/// </para>
/// <para>
/// CACHING mirrors <see cref="CheckGrain"/>'s stage-(a) activation memo exactly: the freshly computed reply
/// is stored ONLY when it is neither <see cref="MembershipClosureReply.CycleCut"/> nor
/// <see cref="MembershipClosureReply.Incomplete"/> (a cut/incomplete result is path- or budget-dependent, not
/// a pure function of this grain's identity alone, so caching it would be unsound for another caller). The
/// activation's idle-collection age (<see cref="MembershipIndexOptions.CollectionAge"/>, applied via
/// <see cref="SiloBuilderExtensions.AddActivationMemoCollectionAge"/>) IS the memo's eviction policy.
/// </para>
/// </remarks>
[Reentrant] // A genuine data cycle (group A contains group B contains group A) re-enters the activation that
            // started the walk; without reentrancy that would deadlock exactly as CheckGrain's remarks explain.
public sealed class MembershipWalkGrain(
    IDatastore datastore,
    ISchemaProvider schemaProvider,
    SchemaResolver schemaResolver,
    IGrainFactory grainFactory,
    MembershipIndexOptions? options = null) : Grain, IMembershipWalkGrain
{
    private readonly MembershipIndexOptions _options = options ?? new MembershipIndexOptions();

    /// <summary>The most-recently computed complete (non-cut, non-incomplete) reply, or null before one exists.</summary>
    private MembershipClosureReply? _memo;

    /// <inheritdoc />
    public async Task<MembershipClosureReply> GetContainingSet(MembershipWalkArgs args, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(args);

        if (_options.Enabled && _memo is { } cached)
            return cached;

        var ownKey = this.GetPrimaryKeyString();
        var parts = MembershipWalkKey.Parse(ownKey);
        var revision = RevisionCodec.Parse(parts.Revision);
        var reader = datastore.SnapshotReader(revision);

        var schema = await schemaResolver.ResolveAsync(
            parts.SchemaHash, reader, schemaProvider.Current, cancellationToken);
        var coverage = schema.MembershipCoverage;

        var subject = new MembershipWalk.SubjectKey(parts.SubjectType, parts.SubjectId, parts.SubjectRelation);
        var directParents = await MembershipWalk
            .DirectParents(reader, coverage, subject, cancellationToken)
            .ConfigureAwait(true);

        var nodes = new List<ResourceNodeWire>(directParents.Count);
        foreach (var p in directParents)
            nodes.Add(new ResourceNodeWire(p.Type, p.Id, p.Relation));

        if (args.DepthRemaining <= 0)
        {
            // Budget exhausted before this hop could recurse into any parent: the direct parents themselves
            // are still valid candidates, but nothing beyond them was explored — the whole reply is partial.
            return new MembershipClosureReply(nodes, CycleCut: false, Incomplete: directParents.Count > 0);
        }

        var cycleCut = false;
        var incomplete = false;
        // The path carries CANONICAL SUBJECT KEYS (type:id#relation), not grain keys: the revision and
        // schema hash are constant along one walk, so the subject key is the whole cycle identity — and it
        // is the same string shape the parent-side containment test below compares against.
        var childPath = new List<string>(args.Path.Count + 1);
        childPath.AddRange(args.Path);
        childPath.Add(subject.ToString());

        foreach (var parent in directParents)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var parentSubjectKey = new MembershipWalk.SubjectKey(parent.Type, parent.Id, parent.Relation).ToString();
            if (args.Path.Contains(parentSubjectKey))
            {
                // A genuine back-edge: the ancestor is already being walked by the call that first put it on
                // the path, so its closure is already accounted for there — complete without recursing.
                cycleCut = true;
                continue;
            }

            var parentGrainKey = MembershipWalkKey.Build(parent.Type, parent.Id, parent.Relation, parts.Revision, parts.SchemaHash);
            var siblingGrain = grainFactory.GetGrain<IMembershipWalkGrain>(parentGrainKey);
            var childArgs = new MembershipWalkArgs(childPath, args.DepthRemaining - 1);

            // The caller's own token drives the sibling call directly: Orleans' native cancellation-token
            // support propagates it to that activation, so there is nothing left to bridge here.
            var childReply = await siblingGrain
                .GetContainingSet(childArgs, cancellationToken)
                .ConfigureAwait(true);

            nodes.AddRange(childReply.Nodes);
            cycleCut |= childReply.CycleCut;
            incomplete |= childReply.Incomplete;
        }

        var reply = new MembershipClosureReply(nodes, cycleCut, incomplete);

        // Never memoize a cut/incomplete result — it is path- or budget-dependent, not a pure function of
        // this grain's identity alone (mirrors CheckGrain's memo-eligibility rule verbatim).
        if (_options.Enabled && !reply.CycleCut && !reply.Incomplete)
            _memo = reply;

        return reply;
    }
}
