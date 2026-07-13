using System.Text.Json;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Spiceport.Api;
using Spiceport.Conformance.Tests;
using Spiceport.Core;
using Spiceport.Engine;
using Spiceport.Grains;
using Spiceport.Grains.Tests;
using Xunit.Abstractions;
using V1 = Authzed.Api.V1;
using CoreRelationship = Spiceport.Core.Relationship;
using CoreRelationshipUpdate = Spiceport.Core.RelationshipUpdate;

namespace Spiceport.Differential.Tests;

/// <summary>
/// Replays the vendored SpiceDB conformance corpus (<c>tests/Spiceport.Conformance.Tests/TestData/*.yaml</c>,
/// excluding the <c>Quarantine/</c> subfolder) through the SAME real-SpiceDB-vs-Spiceport differential
/// harness <see cref="DifferentialConformanceTests"/> established for <see cref="RandomAuthzWorlds"/>: one
/// [Theory] case per corpus file, writing the file's schema+relationships to a real SpiceDB container AND
/// to an in-process Orleans mesh, then cross-checking CheckPermission / LookupResources / LookupSubjects.
/// </summary>
/// <remarks>
/// <para>
/// WHY THIS EXISTS ALONGSIDE <see cref="DifferentialConformanceTests"/>: that suite proves Spiceport agrees
/// with real SpiceDB over RANDOMLY GENERATED worlds built from a small template set. This suite instead
/// replays the CURATED corpus SpiceDB's own maintainers wrote to pin down tricky semantics (caveats,
/// expiration, wildcard exclusion, arrow composition, etc — see <c>tests/Spiceport.Conformance.Tests</c>'s
/// own remarks). Running that exact corpus against a real SpiceDB binary catches two distinct things a
/// same-process oracle never can: (1) a shared misunderstanding of the Zanzibar spec baked into
/// <c>Spiceport.Engine</c>, and (2) a vendored fixture whose recorded expected outcome has drifted from
/// what the pinned upstream SpiceDB version actually returns today (see the "expectation notes" collected
/// below).
/// </para>
/// <para>
/// RESET STRATEGY: each corpus file declares its own, usually self-contained, schema. The SpiceDB
/// container is a class-shared fixture (<see cref="SpiceDbContainerFixture"/>, amortizing container
/// start-up across every file), so a later file's <c>WriteSchema</c> would otherwise collide with an
/// earlier file's still-live relationships (SpiceDB refuses to drop/narrow a relation that still has
/// data). This suite deletes every resource type that appeared in THIS file's relationships in a
/// <c>finally</c> block after the file's comparisons run — empirically, this is sufficient for every file
/// in the corpus (no per-file container fallback was needed; see the class-level test run for confirmation).
/// The in-process Spiceport side needs no reset: <see cref="MeshTestCluster.CreateAsync"/> stands up a
/// brand-new cluster (and datastore) per file.
/// </para>
/// <para>
/// WILDCARD SUBJECTS: no existing suite in this repo drives the real V1 <c>LookupSubjects</c>/
/// <c>CheckPermission</c> RPCs with a subject whose object id IS the wildcard (<c>*</c>) itself (that is
/// never a valid Check/Lookup subject — <c>*</c> only ever appears as a wildcard being matched, not as the
/// thing being checked). This suite defensively excludes any assertion whose subject id is <c>"*"</c> from
/// driving a Lookup query (there should be none in a well-formed corpus file, but the exclusion documents
/// the invariant rather than silently relying on it).
/// </para>
/// </remarks>
[Collection(SpiceDbCollection.Name)]
public sealed class CorpusDifferentialTests
{
    /// <summary>
    /// Corpus files this suite cannot run faithfully against real SpiceDB, and the concrete reason
    /// observed. Kept empty unless empirical verification turns up a genuine SpiceDB-rejects-what-we-accept
    /// (or vice versa) case; a file only lands here with the exact failure recorded, never silently.
    /// </summary>
    // HISTORY: against v1.44.2 this list carried arrowsublr.yaml — a genuine LookupResources divergence
    // (spicedb=[] while its own Check and Spiceport both said Member for the arrow-over-computed-userset
    // shape). Root-caused upstream: SpiceDB's reachability graph skipped entrypoints for a relation reused
    // by an arrow in the same permission; fixed in spicedb 8c2edbe1 ("fix entrypoints over relations that
    // are reused for arrows", first released in v1.47.0), the very commit that ADDED arrowsublr.yaml as its
    // regression fixture. Bumping the pinned image past the fix emptied this list.
    private static readonly IReadOnlyDictionary<string, string> SkippedFiles =
        new Dictionary<string, string>(StringComparer.Ordinal);

    private readonly SpiceDbContainerFixture _spiceDb;
    private readonly ITestOutputHelper _output;

    public CorpusDifferentialTests(SpiceDbContainerFixture spiceDb, ITestOutputHelper output)
    {
        _spiceDb = spiceDb;
        _output = output;
    }

    /// <summary>
    /// The MAIN corpus only: non-recursive enumeration of <c>TestData/*.yaml</c>, exactly like
    /// <c>ConformanceTests.AllYamlFiles</c> -- the <c>Quarantine/</c> subfolder is a subdirectory and is
    /// therefore never matched by this non-recursive glob.
    /// </summary>
    public static IEnumerable<object[]> CorpusFiles()
    {
        var dir = Path.Combine(AppContext.BaseDirectory, "TestData");
        foreach (var path in Directory.EnumerateFiles(dir, "*.yaml").OrderBy(p => p, StringComparer.Ordinal))
        {
            yield return [Path.GetFileName(path)];
        }
    }

    [SkippableTheory]
    [MemberData(nameof(CorpusFiles))]
    public async Task Corpus_file_agrees_with_real_SpiceDB(string fileName)
    {
        Skip.IfNot(_spiceDb.Available, _spiceDb.SkipReason ?? "Docker/SpiceDB container unavailable");
        Skip.If(SkippedFiles.TryGetValue(fileName, out var skipReason), $"{fileName}: {skipReason}");

        var path = Path.Combine(AppContext.BaseDirectory, "TestData", fileName);
        var file = ValidationFileLoader.LoadResolved(path);

        var resourceTypesToReset = file.Relationships
            .Select(r => r.Resource.ObjectType)
            .Distinct(StringComparer.Ordinal)
            .ToList();

        using var spiceDbClient = new SpiceDbGrpcClient(_spiceDb.Address, _spiceDb.PreSharedKey);
        await using var cluster = await MeshTestCluster.CreateAsync(file.SchemaText);

        try
        {
            // Defensive pre-write reset: the SpiceDB container is a class-shared fixture across BOTH this
            // class's theory cases AND DifferentialConformanceTests's seeds (same [Collection]), and xUnit
            // gives no ordering guarantee between the two test classes. DifferentialConformanceTests only
            // resets ITS fixed type set ("group", "folder", "document") at the START of its own next seed,
            // so if it runs immediately before this file (in either interleaving), its last seed's data can
            // still be live here. Clear this file's own types plus that fixed set before WriteSchema, so a
            // leftover relationship under a type this file's schema no longer defines can never block the
            // schema transition. Each delete is best-effort: a type not defined in whatever schema is
            // CURRENTLY active (e.g. never touched by the prior test) is not an error here.
            foreach (var resourceType in resourceTypesToReset.Concat(["group", "folder", "document"]).Distinct(StringComparer.Ordinal))
            {
                try
                {
                    await spiceDbClient.DeleteRelationshipsAsync(new V1::DeleteRelationshipsRequest
                    {
                        RelationshipFilter = new V1::RelationshipFilter { ResourceType = resourceType },
                    });
                }
                catch (RpcException)
                {
                    // Not currently defined under whatever schema is active; nothing to reset.
                }
            }

            await spiceDbClient.WriteSchemaAsync(new V1::WriteSchemaRequest { Schema = file.SchemaText });
            await WriteRelationshipsAsync(spiceDbClient, cluster, file.Relationships);

            var permissionsService = new AuthzedPermissionsV1Service(
                cluster.Services.GetRequiredService<IPermissionChecker>(), cluster.GrainFactory,
                cluster.ReverseOps, cluster.RelationshipReads,
                cluster.Services.GetRequiredService<ISchemaProvider>());

            var failures = new List<string>();
            var expectationNotes = new List<string>();
            var skippedQueries = new List<string>();

            // --- Check comparisons: every assertion (assertTrue/assertFalse/assertCaveated). ---
            foreach (var assertion in file.Assertions)
            {
                await CompareCheck(spiceDbClient, permissionsService, fileName, assertion, failures, expectationNotes);
            }

            // --- Lookup comparisons: every distinct (resourceType, permission, subjectType, subjectRelation)
            // shape appearing among the file's assertions, driven by the CONCRETE ids that shape's
            // assertions actually reference (the corpus is small by construction). ---
            var shapes = file.Assertions.GroupBy(a => (
                ResourceType: a.Resource.ObjectType,
                Permission: a.Resource.Relation,
                SubjectType: a.Subject.ObjectType,
                SubjectRelation: a.Subject.Relation));

            foreach (var shape in shapes)
            {
                var (resourceType, permission, subjectType, subjectRelation) = shape.Key;

                var subjectIds = shape
                    .Select(a => a.Subject.ObjectId)
                    .Where(id => id != CoreConstants.PublicWildcard) // never a valid Lookup/Check subject id
                    .Distinct(StringComparer.Ordinal)
                    .ToList();
                var resourceIds = shape.Select(a => a.Resource.ObjectId).Distinct(StringComparer.Ordinal).ToList();

                foreach (var subjectId in subjectIds)
                {
                    await CompareLookupResources(
                        spiceDbClient, permissionsService, fileName,
                        resourceType, permission, subjectType, subjectRelation, subjectId, failures, skippedQueries);
                }

                foreach (var resourceId in resourceIds)
                {
                    await CompareLookupSubjects(
                        spiceDbClient, permissionsService, fileName,
                        resourceType, resourceId, permission, subjectType, failures, skippedQueries);
                }
            }

            if (skippedQueries.Count > 0)
            {
                _output.WriteLine(
                    $"{fileName}: {skippedQueries.Count} Lookup query/queries could not be compared " +
                    "(real SpiceDB rejected the request at the RPC level -- not counted as a failure):");
                foreach (var note in skippedQueries)
                {
                    _output.WriteLine("  " + note);
                }
            }

            if (expectationNotes.Count > 0)
            {
                _output.WriteLine(
                    $"{fileName}: {expectationNotes.Count} yaml-expectation-vs-real-SpiceDB disagreement(s) " +
                    "(not a test failure -- the vendored fixture's expectation appears stale relative to " +
                    "the pinned upstream SpiceDB version):");
                foreach (var note in expectationNotes)
                {
                    _output.WriteLine("  " + note);
                }
            }

            Assert.True(
                failures.Count == 0,
                $"Divergence(s) between real SpiceDB and Spiceport in {fileName}:\n" + string.Join("\n", failures));
        }
        finally
        {
            // Best-effort cleanup: if this file's own WriteSchema/WriteRelationships never completed (e.g.
            // it threw), the resource types below may not exist in whatever schema is still active on the
            // shared container. Swallow that case rather than let a cleanup-time RpcException mask the
            // real failure that got the test here in the first place.
            foreach (var resourceType in resourceTypesToReset)
            {
                try
                {
                    await spiceDbClient.DeleteRelationshipsAsync(new V1::DeleteRelationshipsRequest
                    {
                        RelationshipFilter = new V1::RelationshipFilter { ResourceType = resourceType },
                    });
                }
                catch (RpcException)
                {
                    // Nothing to clean up under this schema; see remarks above.
                }
            }
        }
    }

    private static async Task CompareCheck(
        SpiceDbGrpcClient spiceDbClient,
        AuthzedPermissionsV1Service permissionsService,
        string fileName,
        ParsedAssertion assertion,
        List<string> failures,
        List<string> expectationNotes)
    {
        var resource = new V1::ObjectReference
        {
            ObjectType = assertion.Resource.ObjectType,
            ObjectId = assertion.Resource.ObjectId,
        };
        var subject = new V1::SubjectReference
        {
            Object = new V1::ObjectReference
            {
                ObjectType = assertion.Subject.ObjectType,
                ObjectId = assertion.Subject.ObjectId,
            },
            OptionalRelation = assertion.Subject.Relation == CoreConstants.Ellipsis ? string.Empty : assertion.Subject.Relation,
        };
        var contextStruct = DictToStruct(assertion.CaveatContext);

        V1::CheckPermissionRequest BuildRequest()
        {
            var req = new V1::CheckPermissionRequest
            {
                Resource = resource,
                Permission = assertion.Resource.Relation,
                Subject = subject,
                Consistency = new V1::Consistency { FullyConsistent = true },
            };
            if (contextStruct is not null)
            {
                req.Context = contextStruct;
            }

            return req;
        }

        var spiceDbResp = await spiceDbClient.CheckPermissionAsync(BuildRequest());
        var spiceportResp = await permissionsService.CheckPermission(BuildRequest(), FakeServerCallContext.Default);

        var spiceDbVerdict = Normalize(spiceDbResp.Permissionship);
        var spiceportVerdict = Normalize(spiceportResp.Permissionship);

        if (spiceDbVerdict != spiceportVerdict)
        {
            failures.Add(
                $"{fileName}: CheckPermission \"{assertion.SourceText}\" " +
                $"spicedb={spiceDbVerdict} spiceport={spiceportVerdict}");
        }

        var expected = NormalizeExpectation(assertion.Expectation);
        if (spiceDbVerdict != expected)
        {
            expectationNotes.Add(
                $"\"{assertion.SourceText}\" => yaml expects {expected}, real SpiceDB returned {spiceDbVerdict}");
        }
    }

    private static async Task CompareLookupResources(
        SpiceDbGrpcClient spiceDbClient,
        AuthzedPermissionsV1Service permissionsService,
        string fileName,
        string resourceType,
        string permission,
        string subjectType,
        string subjectRelation,
        string subjectId,
        List<string> failures,
        List<string> skippedQueries)
    {
        var subject = new V1::SubjectReference
        {
            Object = new V1::ObjectReference { ObjectType = subjectType, ObjectId = subjectId },
            OptionalRelation = subjectRelation == CoreConstants.Ellipsis ? string.Empty : subjectRelation,
        };

        List<V1::LookupResourcesResponse> spiceDbResults;
        try
        {
            spiceDbResults = await spiceDbClient.LookupResourcesAsync(new V1::LookupResourcesRequest
            {
                ResourceObjectType = resourceType,
                Permission = permission,
                Subject = subject,
                Consistency = new V1::Consistency { FullyConsistent = true },
            });
        }
        catch (RpcException ex)
        {
            // Real SpiceDB rejects some subject shapes (e.g. a non-terminal subject relation) that this
            // harness still enumerates from the assertion set; not a Spiceport defect to chase, so it's
            // surfaced as a skipped comparison rather than a hard failure.
            skippedQueries.Add(
                $"LookupResources {resourceType}#{permission}@{subjectType}:{subjectId} " +
                $"-- real SpiceDB rejected the request: {ex.Status}");
            return;
        }

        var spiceDbIds = spiceDbResults.Select(r => r.ResourceObjectId).ToHashSet(StringComparer.Ordinal);

        var writer = new CollectingStreamWriter<V1::LookupResourcesResponse>();
        await permissionsService.LookupResources(new V1::LookupResourcesRequest
        {
            ResourceObjectType = resourceType,
            Permission = permission,
            Subject = subject,
            Consistency = new V1::Consistency { FullyConsistent = true },
        }, writer, FakeServerCallContext.Default);
        var spiceportIds = writer.Collected.Select(r => r.ResourceObjectId).ToHashSet(StringComparer.Ordinal);

        if (!spiceDbIds.SetEquals(spiceportIds))
        {
            failures.Add(
                $"{fileName}: LookupResources {resourceType}#{permission}@{subjectType}:{subjectId} " +
                $"spicedb=[{string.Join(",", spiceDbIds.OrderBy(x => x, StringComparer.Ordinal))}] " +
                $"spiceport=[{string.Join(",", spiceportIds.OrderBy(x => x, StringComparer.Ordinal))}]");
        }
    }

    private static async Task CompareLookupSubjects(
        SpiceDbGrpcClient spiceDbClient,
        AuthzedPermissionsV1Service permissionsService,
        string fileName,
        string resourceType,
        string resourceId,
        string permission,
        string subjectType,
        List<string> failures,
        List<string> skippedQueries)
    {
        var resource = new V1::ObjectReference { ObjectType = resourceType, ObjectId = resourceId };

        List<V1::LookupSubjectsResponse> spiceDbResults;
        try
        {
            spiceDbResults = await spiceDbClient.LookupSubjectsAsync(new V1::LookupSubjectsRequest
            {
                Resource = resource,
                Permission = permission,
                SubjectObjectType = subjectType,
                Consistency = new V1::Consistency { FullyConsistent = true },
            });
        }
        catch (RpcException ex)
        {
            // See the class remarks on wildcard subjects: no established in-repo precedent covers real
            // SpiceDB's behavior here for every corpus shape, so a genuine RPC-level rejection is treated
            // as a documented skip for this one (resource, permission, subjectType) query rather than a
            // hard failure.
            skippedQueries.Add(
                $"LookupSubjects {resourceType}:{resourceId}#{permission}@{subjectType} " +
                $"-- real SpiceDB rejected the request: {ex.Status}");
            return;
        }

        var spiceDbIds = spiceDbResults.Select(r => r.Subject.SubjectObjectId).ToHashSet(StringComparer.Ordinal);

        var writer = new CollectingStreamWriter<V1::LookupSubjectsResponse>();
        await permissionsService.LookupSubjects(new V1::LookupSubjectsRequest
        {
            Resource = resource,
            Permission = permission,
            SubjectObjectType = subjectType,
            Consistency = new V1::Consistency { FullyConsistent = true },
        }, writer, FakeServerCallContext.Default);
        var spiceportIds = writer.Collected.Select(r => r.Subject.SubjectObjectId).ToHashSet(StringComparer.Ordinal);

        if (!spiceDbIds.SetEquals(spiceportIds))
        {
            failures.Add(
                $"{fileName}: LookupSubjects {resourceType}:{resourceId}#{permission}@{subjectType} " +
                $"spicedb=[{string.Join(",", spiceDbIds.OrderBy(x => x, StringComparer.Ordinal))}] " +
                $"spiceport=[{string.Join(",", spiceportIds.OrderBy(x => x, StringComparer.Ordinal))}]");
        }
    }

    /// <summary>
    /// Maps <c>Permissionship</c> onto a small closed set so the two systems' verdicts (and the yaml's
    /// own recorded expectation) compare structurally. Matches <see cref="DifferentialConformanceTests"/>'s
    /// normalization exactly.
    /// </summary>
    private static string Normalize(V1::CheckPermissionResponse.Types.Permissionship p) => p switch
    {
        V1::CheckPermissionResponse.Types.Permissionship.HasPermission => "Member",
        V1::CheckPermissionResponse.Types.Permissionship.ConditionalPermission => "Caveated",
        _ => "NotMember",
    };

    private static string NormalizeExpectation(AssertionExpectation expectation) => expectation switch
    {
        AssertionExpectation.True => "Member",
        AssertionExpectation.False => "NotMember",
        AssertionExpectation.Caveated => "Caveated",
        _ => "NotMember",
    };

    private static async Task WriteRelationshipsAsync(
        SpiceDbGrpcClient spiceDbClient, MeshTestCluster cluster, IReadOnlyList<CoreRelationship> relationships)
    {
        if (relationships.Count == 0)
        {
            return;
        }

        // SpiceDB side: WriteRelationships in bounded batches over the real gRPC wire, preserving caveat
        // context and expiration (see ToUpdate).
        const int batchSize = 50;
        for (var i = 0; i < relationships.Count; i += batchSize)
        {
            var req = new V1::WriteRelationshipsRequest();
            foreach (var rel in relationships.Skip(i).Take(batchSize))
            {
                req.Updates.Add(ToUpdate(rel));
            }

            await spiceDbClient.WriteRelationshipsAsync(req);
        }

        // Spiceport side: straight into the datastore transaction (bypassing the gRPC proto round-trip
        // entirely), so caveat context and expiration ride through as first-class Core.Relationship
        // fields with no lossy proto conversion in between -- exactly the fidelity WriteRelationshipsAsync
        // in DifferentialConformanceTests already relies on for its (caveat/expiration-free) worlds.
        var updates = relationships
            .Select(rel => new CoreRelationshipUpdate(rel, UpdateOperation.Touch))
            .ToList();
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(updates));
    }

    private static V1::RelationshipUpdate ToUpdate(CoreRelationship rel)
    {
        var relationship = new V1::Relationship
        {
            Resource = new V1::ObjectReference { ObjectType = rel.Resource.ObjectType, ObjectId = rel.Resource.ObjectId },
            Relation = rel.Resource.Relation,
            Subject = new V1::SubjectReference
            {
                Object = new V1::ObjectReference { ObjectType = rel.Subject.ObjectType, ObjectId = rel.Subject.ObjectId },
                OptionalRelation = rel.Subject.Relation == CoreConstants.Ellipsis ? string.Empty : rel.Subject.Relation,
            },
        };

        if (rel.OptionalCaveat is { } caveat)
        {
            relationship.OptionalCaveat = new V1::ContextualizedCaveat
            {
                CaveatName = caveat.CaveatName,
                Context = DictToStruct(caveat.Context) ?? new Struct(),
            };
        }

        if (rel.OptionalExpiration is { } expiration)
        {
            relationship.OptionalExpiresAt = Timestamp.FromDateTimeOffset(expiration);
        }

        return new V1::RelationshipUpdate
        {
            Operation = V1::RelationshipUpdate.Types.Operation.Touch,
            Relationship = relationship,
        };
    }

    // --- Dictionary<string, object?> <-> google.protobuf.Struct, mirroring AuthzedPermissionsV1Service's
    // own (private) DictToStruct/ObjectToValue -- duplicated here rather than exposed, matching this
    // repo's established convention of each gRPC-adjacent test file keeping its own small copy (see e.g.
    // PermissionsGrpcService and AuthzedPermissionsV1Service, which each already keep their own). Extended
    // to accept JsonElement values too: ValidationFileLoader's `with {json}` assertion-context parsing
    // yields plain CLR scalars, but TupleStrings' relationship-tuple caveat parsing
    // (`[caveat:{"k":1}]`) round-trips through System.Text.Json's `Dictionary<string, object?>` and
    // therefore yields boxed JsonElement values -- both shapes must convert cleanly. ---

    private static Struct? DictToStruct(IReadOnlyDictionary<string, object?>? dict)
    {
        if (dict is null || dict.Count == 0)
        {
            return null;
        }

        var s = new Struct();
        foreach (var (k, v) in dict)
        {
            s.Fields[k] = ObjectToValue(v);
        }

        return s;
    }

    private static Value ObjectToValue(object? o) => o switch
    {
        null => Value.ForNull(),
        JsonElement je => JsonElementToValue(je),
        bool b => Value.ForBool(b),
        string str => Value.ForString(str),
        double d => Value.ForNumber(d),
        int i => Value.ForNumber(i),
        long l => Value.ForNumber(l),
        IReadOnlyDictionary<string, object?> map => Value.ForStruct(DictToStruct(map) ?? new Struct()),
        System.Collections.IEnumerable list => Value.ForList(list.Cast<object?>().Select(ObjectToValue).ToArray()),
        _ => Value.ForString(o.ToString() ?? string.Empty),
    };

    private static Value JsonElementToValue(JsonElement e) => e.ValueKind switch
    {
        JsonValueKind.Null => Value.ForNull(),
        JsonValueKind.String => Value.ForString(e.GetString() ?? string.Empty),
        JsonValueKind.Number => Value.ForNumber(e.GetDouble()),
        JsonValueKind.True => Value.ForBool(true),
        JsonValueKind.False => Value.ForBool(false),
        JsonValueKind.Array => Value.ForList(e.EnumerateArray().Select(JsonElementToValue).ToArray()),
        JsonValueKind.Object => Value.ForStruct(JsonElementToStruct(e)),
        _ => Value.ForString(e.GetRawText()),
    };

    private static Struct JsonElementToStruct(JsonElement e)
    {
        var s = new Struct();
        foreach (var prop in e.EnumerateObject())
        {
            s.Fields[prop.Name] = JsonElementToValue(prop.Value);
        }

        return s;
    }

    /// <summary>A server stream writer that records every response (matches the pattern in the gRPC service tests).</summary>
    private sealed class CollectingStreamWriter<T> : IServerStreamWriter<T>
    {
        public List<T> Collected { get; } = [];
        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(T message)
        {
            Collected.Add(message);
            return Task.CompletedTask;
        }
    }

    private sealed class FakeServerCallContext(CancellationToken cancellationToken) : ServerCallContext
    {
        public static FakeServerCallContext Default { get; } = new(CancellationToken.None);

        protected override string MethodCore => string.Empty;
        protected override string HostCore => string.Empty;
        protected override string PeerCore => string.Empty;
        protected override DateTime DeadlineCore => DateTime.MaxValue;
        protected override Metadata RequestHeadersCore => [];
        protected override CancellationToken CancellationTokenCore => cancellationToken;
        protected override Metadata ResponseTrailersCore => [];
        protected override Status StatusCore { get; set; }
        protected override WriteOptions? WriteOptionsCore { get; set; }
        protected override AuthContext AuthContextCore => new(string.Empty, new Dictionary<string, List<AuthProperty>>());

        protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options) =>
            throw new NotSupportedException();

        protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders) => Task.CompletedTask;
    }
}
