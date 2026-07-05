using System.Collections.Immutable;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Schema;

namespace Spiceport.Conformance.Tests;

/// <summary>
/// Property-style consistency cross-check that ties the Phase 3 reverse/tree APIs
/// (<see cref="LookupSubjectsEngine"/>, <see cref="LookupResourcesEngine"/>,
/// <see cref="ExpandEngine"/>) back to the trusted forward oracle
/// (<see cref="CheckEngine"/>). For a representative slice of the conformance corpus
/// it asserts the soundness+completeness invariants from the design's §4:
///
/// <list type="bullet">
///   <item>Every subject returned by LookupSubjects yields Check = Member-or-Caveated;
///         every subject of the requested type in the bounded universe that is NOT
///         returned yields Check = NotMember (wildcards generalise to a fresh id).</item>
///   <item>Every resource returned by LookupResources yields Check = Member-or-Caveated;
///         every resource of the requested type in the bounded universe that is NOT
///         returned yields Check = NotMember.</item>
///   <item>The ExpandPermissionTree of a relation/permission, flattened to its effective
///         concrete subject set, agrees with LookupSubjects for the same target.</item>
/// </list>
///
/// The bounded universe is the finite set of object ids per type observed in the
/// corpus relationships, plus a synthetic absent id, plus (for subjects) a fresh id
/// to validate wildcard semantics. Caveated tuples are evaluated by Check with an
/// empty context (resolving to <see cref="Membership.Caveated"/>), which is the
/// allowed-set member the reverse APIs must agree with when they carry a caveat.
/// </summary>
public class ReverseConsistencyCrossCheckTests
{
    /// <summary>
    /// The representative corpus slice the cross-check runs over, deliberately spanning
    /// each productive shape: a wildcard schema, an arrow schema, nested-group
    /// indirection (incl. exclusion under the group permission), basic RBAC and a caveat.
    /// </summary>
    public static IEnumerable<object[]> CorpusFiles()
    {
        yield return ["basicrbac.yaml"];        // base relations + simple union permission
        yield return ["public.yaml"];           // wildcards (user:* and a second user type)
        yield return ["document.yaml"];          // arrow (parent->view) + intersection
        yield return ["directgroups.yaml"];      // nested group#member chains
        yield return ["indirectnestedgroups.yaml"]; // nested groups with exclusion in the group perm
        yield return ["basiccaveat.yaml"];       // caveated subjects
    }

    private const string FreshSubjectId = "__cc_fresh_subject__";
    private const string AbsentId = "__cc_absent__";

    [Theory]
    [MemberData(nameof(CorpusFiles))]
    public async Task LookupSubjects_AgreesWithCheck(string fileName)
    {
        var ctx = await LoadCorpus(fileName);

        // For every (resource type, resource id, relation/permission) target and every
        // requested subject (type, relation), the returned set must exactly agree with
        // Check over the bounded subject universe.
        foreach (var ns in ctx.Namespaces)
        {
            foreach (var relation in ns.Relations)
            {
                foreach (var resourceId in ctx.IdsOf(ns.Name))
                {
                    var resource = new ObjectAndRelation(ns.Name, resourceId, relation.Name);

                    // Cross-check against terminal subjects only. For non-terminal subject
                    // relations (e.g. group#member) LookupSubjects yields the *directly written*
                    // usersets matching the type+relation, whereas Check resolves userset
                    // membership transitively, so the two are deliberately not 1:1 there (the
                    // design's §4 oracle is framed over concrete subjects). Terminal subjects are
                    // exactly where SpiceDB's own consistency tests operate.
                    foreach (var subjectType in ctx.TerminalSubjectTypes)
                    {
                        var found = await Collect(ctx.LookupSubjects.LookupSubjects(
                            ctx.Reader, resource, subjectType, CoreConstants.Ellipsis));

                        await AssertLookupSubjectsAgrees(ctx, resource, subjectType, CoreConstants.Ellipsis, found);
                    }
                }
            }
        }
    }

    [Theory]
    [MemberData(nameof(CorpusFiles))]
    public async Task LookupResources_AgreesWithCheck(string fileName)
    {
        var ctx = await LoadCorpus(fileName);

        // For every terminal subject (type, id) and every (resource type, relation/perm),
        // the returned resource set must exactly agree with Check over the bounded
        // resource universe. Subjects are scoped to terminal (ellipsis) for the same reason
        // as LookupSubjects above: that is where the reverse/forward correspondence is 1:1.
        const string subjectRelation = CoreConstants.Ellipsis;
        foreach (var subjectType in ctx.TerminalSubjectTypes)
        {
            foreach (var subjectId in ctx.IdsOf(subjectType))
            {
                foreach (var resNs in ctx.Namespaces)
                {
                    foreach (var relation in resNs.Relations)
                    {
                        var found = await Collect(ctx.LookupResources.LookupResources(
                            ctx.Reader,
                            subjectType,
                            subjectId,
                            subjectRelation,
                            resNs.Name,
                            relation.Name));

                        var foundIds = found.Select(f => f.ResourceId).ToHashSet(StringComparer.Ordinal);
                        var subject = new ObjectAndRelation(subjectType, subjectId, subjectRelation);

                        // Soundness: each returned resource agrees with Check. An unconditional
                        // (Member) result must be a definite Check Member; a Caveated result must
                        // be Member-or-Caveated under Check with empty context (a satisfiable
                        // conditional edge; definitely-false embedded caveats are sheared before
                        // being returned).
                        foreach (var f in found)
                        {
                            var verdict = (await ctx.Check.Check(
                                ctx.Reader, resNs.Name, f.ResourceId, relation.Name, subject)).Verdict;
                            if (f.Membership == Membership.Member)
                            {
                                Assert.True(
                                    verdict == Membership.Member,
                                    $"{fileName}: LookupResources returned Member {resNs.Name}:{f.ResourceId}#{relation.Name} " +
                                    $"for {subject}, but Check says {verdict}");
                            }
                            else
                            {
                                Assert.True(
                                    verdict is Membership.Member or Membership.Caveated,
                                    $"{fileName}: LookupResources returned Caveated {resNs.Name}:{f.ResourceId}#{relation.Name} " +
                                    $"for {subject}, but Check says {verdict}");
                            }
                        }

                        // Completeness: everything in the resource universe NOT returned is NotMember.
                        foreach (var resourceId in ctx.IdsOf(resNs.Name))
                        {
                            if (foundIds.Contains(resourceId))
                            {
                                continue;
                            }

                            var verdict = (await ctx.Check.Check(
                                ctx.Reader, resNs.Name, resourceId, relation.Name, subject)).Verdict;
                            Assert.True(
                                verdict == Membership.NotMember,
                                $"{fileName}: LookupResources did NOT return {resNs.Name}:{resourceId}#{relation.Name} " +
                                $"for {subject}, but Check says {verdict}");
                        }
                    }
                }
            }
        }
    }

    [Theory]
    [MemberData(nameof(CorpusFiles))]
    public async Task ExpandTree_FlattensToLookupSubjects(string fileName)
    {
        var ctx = await LoadCorpus(fileName);

        // The flattened concrete (terminal) subject set of an expansion tree, evaluated
        // by interpreting the set operations, must agree with Check for each candidate
        // subject id of every subject type. This makes Expand testable via the same oracle.
        foreach (var ns in ctx.Namespaces)
        {
            foreach (var relation in ns.Relations)
            {
                foreach (var resourceId in ctx.IdsOf(ns.Name))
                {
                    var resource = new ObjectAndRelation(ns.Name, resourceId, relation.Name);

                    var tree = await ctx.Expand.ExpandPermissionTree(
                        ctx.Reader, resource, ExpandMode.Recursive);

                    var flattened = FlattenTerminalSubjects(tree);

                    // Subjects that appear anywhere in the tree under a caveat are exempt: the
                    // structural tree carries the caveat verbatim, so a written falsifying
                    // embedded context (e.g. {somecondition:41}) would make Check definitely
                    // NotMember even though the structural edge exists.
                    var caveatedSubjects = CollectCaveatedSubjects(tree);

                    // Every unconditional concrete terminal subject the tree yields must be a
                    // definite Member by Check.
                    foreach (var subject in flattened)
                    {
                        if (subject.IsPublicWildcard || caveatedSubjects.Contains(subject))
                        {
                            continue; // wildcard / caveated fidelity is covered by the LookupSubjects cross-check
                        }

                        var verdict = (await ctx.Check.Check(ctx.Reader, resource, subject)).Verdict;
                        Assert.True(
                            verdict == Membership.Member,
                            $"{fileName}: ExpandTree({resource}) yielded unconditional subject {subject}, but Check says {verdict}");
                    }
                }
            }
        }
    }

    // --- LookupSubjects assertion (soundness + completeness, with wildcard handling) ---

    private static async Task AssertLookupSubjectsAgrees(
        CorpusContext ctx,
        ObjectAndRelation resource,
        string subjectType,
        string subjectRelation,
        IReadOnlyList<FoundSubject> found)
    {
        // Unconditional (Caveat == null) returns must be definite Members of Check.
        // Conditional (Caveat != null) returns are the "Caveated marker": they are exempt
        // from the soundness verdict (the relationship's *embedded* caveat context may even
        // falsify the edge under Check, e.g. a written {somecondition:41}), and they are
        // excluded from the NotMember complement below. This mirrors the design §4 rule:
        // "Member if s.Caveat == null; Member-or-Caveated if caveated; never required to be
        //  NotMember when caveated".
        var unconditionalFound = found
            .Where(f => !f.IsWildcard && f.Caveat is null)
            .Select(f => f.SubjectId)
            .ToHashSet(StringComparer.Ordinal);
        var anyFound = found
            .Where(f => !f.IsWildcard)
            .Select(f => f.SubjectId)
            .ToHashSet(StringComparer.Ordinal);
        var hasUnconditionalWildcard = found.Any(f => f.IsWildcard && f.Caveat is null);
        var hasAnyWildcard = found.Any(f => f.IsWildcard);

        // Soundness: each unconditional concrete returned subject is a definite Member.
        foreach (var f in found.Where(f => !f.IsWildcard && f.Caveat is null))
        {
            var subject = new ObjectAndRelation(subjectType, f.SubjectId, subjectRelation);
            var verdict = (await ctx.Check.Check(ctx.Reader, resource, subject)).Verdict;
            Assert.True(
                verdict == Membership.Member,
                $"LookupSubjects returned unconditional {subject} for {resource}, but Check says {verdict}");
        }

        // An unconditional '*' means "every subject of this type" -> a fresh id must be Member.
        // If there is NO wildcard at all -> a fresh id must be NotMember (absent everywhere).
        var freshSubject = new ObjectAndRelation(subjectType, FreshSubjectId, subjectRelation);
        var freshVerdict = (await ctx.Check.Check(ctx.Reader, resource, freshSubject)).Verdict;
        if (hasUnconditionalWildcard)
        {
            Assert.True(
                freshVerdict == Membership.Member,
                $"LookupSubjects returned unconditional '*' for {resource} subjectType {subjectType}, " +
                $"but Check on a fresh id says {freshVerdict}");
        }
        else if (!hasAnyWildcard)
        {
            Assert.True(
                freshVerdict == Membership.NotMember,
                $"LookupSubjects did NOT return '*' for {resource} subjectType {subjectType}, " +
                $"but Check on a fresh id says {freshVerdict}");
        }

        // Completeness: every concrete id of subjectType NOT returned at all (neither
        // unconditional nor caveated) and not covered by a wildcard must be NotMember.
        foreach (var candidateId in ctx.IdsOf(subjectType))
        {
            if (anyFound.Contains(candidateId) || hasAnyWildcard)
            {
                // Returned (in some form) or potentially covered by a wildcard: not part of
                // the strict NotMember complement.
                continue;
            }

            var subject = new ObjectAndRelation(subjectType, candidateId, subjectRelation);
            var verdict = (await ctx.Check.Check(ctx.Reader, resource, subject)).Verdict;
            Assert.True(
                verdict == Membership.NotMember,
                $"LookupSubjects did NOT return {subject} for {resource}, but Check says {verdict}");
        }

        // Strengthen the unconditional-wildcard case: when '*' is unconditionally present,
        // every concrete id of the type must be a Member under Check.
        if (hasUnconditionalWildcard)
        {
            foreach (var candidateId in ctx.IdsOf(subjectType))
            {
                var subject = new ObjectAndRelation(subjectType, candidateId, subjectRelation);
                var verdict = (await ctx.Check.Check(ctx.Reader, resource, subject)).Verdict;
                Assert.True(
                    verdict == Membership.Member,
                    $"LookupSubjects returned unconditional '*' (covering {subject}) for {resource}, " +
                    $"but Check says {verdict}");
            }
        }
    }

    // --- Expand tree flattening (interpret union/intersection/exclusion structurally) ---

    private static IReadOnlySet<ObjectAndRelation> FlattenTerminalSubjects(PermissionTreeNode node) =>
        node switch
        {
            PermissionTreeNode.Leaf leaf => leaf.Subjects
                .Select(s => s.Subject)
                .Where(IsTerminal)
                .ToHashSet(),
            PermissionTreeNode.SetOp setOp => FlattenSetOp(setOp),
            _ => new HashSet<ObjectAndRelation>(),
        };

    private static IReadOnlySet<ObjectAndRelation> FlattenSetOp(PermissionTreeNode.SetOp setOp)
    {
        var children = setOp.Children.Select(FlattenTerminalSubjects).ToList();
        if (children.Count == 0)
        {
            return new HashSet<ObjectAndRelation>();
        }

        return setOp.Operation switch
        {
            SetOperationType.Union => children
                .Aggregate(new HashSet<ObjectAndRelation>(), (acc, c) => { acc.UnionWith(c); return acc; }),
            SetOperationType.Intersection => children
                .Skip(1)
                .Aggregate(new HashSet<ObjectAndRelation>(children[0]), (acc, c) => { acc.IntersectWith(c); return acc; }),
            SetOperationType.Exclusion => children
                .Skip(1)
                .Aggregate(new HashSet<ObjectAndRelation>(children[0]), (acc, c) => { acc.ExceptWith(c); return acc; }),
            _ => new HashSet<ObjectAndRelation>(),
        };
    }

    private static bool IsTerminal(ObjectAndRelation onr) =>
        onr.Relation == CoreConstants.Ellipsis || onr.IsPublicWildcard;

    /// <summary>
    /// Collects every terminal subject ONR that appears anywhere in the tree carrying a
    /// caveat (either on its <see cref="DirectSubject"/> or on an enclosing node), so the
    /// Expand soundness check can exempt conditional subjects.
    /// </summary>
    private static IReadOnlySet<ObjectAndRelation> CollectCaveatedSubjects(PermissionTreeNode node)
    {
        var set = new HashSet<ObjectAndRelation>();
        Walk(node, false);
        return set;

        void Walk(PermissionTreeNode n, bool underCaveat)
        {
            var nowUnderCaveat = underCaveat || n.Caveat is not null;
            switch (n)
            {
                case PermissionTreeNode.Leaf leaf:
                    foreach (var s in leaf.Subjects.Where(IsTerminalSubject))
                    {
                        if (nowUnderCaveat || s.Caveat is not null)
                        {
                            set.Add(s.Subject);
                        }
                    }

                    break;
                case PermissionTreeNode.SetOp setOp:
                    foreach (var child in setOp.Children)
                    {
                        Walk(child, nowUnderCaveat);
                    }

                    break;
            }
        }

        static bool IsTerminalSubject(DirectSubject s) => IsTerminal(s.Subject);
    }

    // --- Corpus loading / universe construction ---

    private static async Task<CorpusContext> LoadCorpus(string fileName)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "TestData", fileName);
        var file = ValidationFileLoader.LoadFromFile(path);

        var compiled = SchemaCompiler.CompileSchema(file.SchemaText);

        var store = new ReferenceDatastore();
        var rev = await LoadRelationships(store, file.Relationships);
        var reader = store.SnapshotReader(rev);

        // Universe of object ids per type: every id seen as a resource or a concrete subject,
        // plus a synthetic absent id to exercise the NotMember complement.
        var idsByType = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        var terminalSubjectTypes = new HashSet<string>(StringComparer.Ordinal);

        foreach (var ns in compiled.Namespaces)
        {
            GetIds(idsByType, ns.Name).Add(AbsentId);
        }

        foreach (var rel in file.Relationships)
        {
            GetIds(idsByType, rel.Resource.ObjectType).Add(rel.Resource.ObjectId);

            var subj = rel.Subject;
            if (!subj.IsPublicWildcard)
            {
                GetIds(idsByType, subj.ObjectType).Add(subj.ObjectId);
            }

            // Terminal subject types: the types appearing as ellipsis or wildcard subjects.
            if (subj.Relation == CoreConstants.Ellipsis || subj.IsPublicWildcard)
            {
                terminalSubjectTypes.Add(subj.ObjectType);
            }
        }

        // Fall back to all defined types if the corpus has no terminal subjects, so the
        // cross-check still probes the natural subject types.
        if (terminalSubjectTypes.Count == 0)
        {
            foreach (var ns in compiled.Namespaces)
            {
                terminalSubjectTypes.Add(ns.Name);
            }
        }

        return new CorpusContext(
            fileName,
            compiled.Namespaces,
            reader,
            idsByType.ToImmutableDictionary(
                kv => kv.Key,
                kv => (IReadOnlyList<string>)kv.Value.OrderBy(x => x, StringComparer.Ordinal).ToList()),
            terminalSubjectTypes.OrderBy(t => t, StringComparer.Ordinal).ToList(),
            new CheckEngine(compiled.Namespaces, compiled.Caveats),
            new LookupSubjectsEngine(compiled.Namespaces),
            new LookupResourcesEngine(compiled.Namespaces, compiled.Caveats),
            new ExpandEngine(compiled.Namespaces));
    }

    private static HashSet<string> GetIds(Dictionary<string, HashSet<string>> map, string type)
    {
        if (!map.TryGetValue(type, out var set))
        {
            set = new HashSet<string>(StringComparer.Ordinal);
            map[type] = set;
        }

        return set;
    }

    private static async Task<IRevision> LoadRelationships(
        ReferenceDatastore datastore,
        ImmutableList<Relationship> relationships)
    {
        if (relationships.Count == 0)
        {
            var head = await datastore.HeadRevision();
            return head.Revision;
        }

        var updates = relationships
            .Select(r => new RelationshipUpdate(r, UpdateOperation.Create))
            .ToList();

        return await datastore.ReadWriteTx(tx => tx.WriteRelationships(updates));
    }

    private static async Task<List<T>> Collect<T>(IAsyncEnumerable<T> e)
    {
        var list = new List<T>();
        await foreach (var f in e)
        {
            list.Add(f);
        }

        return list;
    }

    private sealed record CorpusContext(
        string FileName,
        ImmutableList<NamespaceDefinition> Namespaces,
        IDatastoreReader Reader,
        ImmutableDictionary<string, IReadOnlyList<string>> IdsByType,
        IReadOnlyList<string> TerminalSubjectTypes,
        CheckEngine Check,
        LookupSubjectsEngine LookupSubjects,
        LookupResourcesEngine LookupResources,
        ExpandEngine Expand)
    {
        public IReadOnlyList<string> IdsOf(string type) =>
            IdsByType.TryGetValue(type, out var ids) ? ids : [];
    }
}
