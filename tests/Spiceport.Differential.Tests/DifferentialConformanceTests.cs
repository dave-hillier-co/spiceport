using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Spiceport.Api;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine.Tests;
using Spiceport.Grains;
using Spiceport.Grains.Tests;
using V1 = Authzed.Api.V1;
using CoreRelationship = Spiceport.Core.Relationship;
using CoreRelationshipUpdate = Spiceport.Core.RelationshipUpdate;

namespace Spiceport.Differential.Tests;

/// <summary>
/// THE external correctness gate: for each seeded random world from
/// <see cref="RandomAuthzWorlds"/>, writes the SAME schema and relationships to a REAL, independent
/// SpiceDB (over gRPC, in a Testcontainers container) and to Spiceport (in-process, through the real
/// grain mesh -- <see cref="MeshTestCluster"/>), then asserts every Check/LookupResources/LookupSubjects
/// verdict over the world's full query universe agrees between the two.
/// </summary>
/// <remarks>
/// <para>
/// WHY THIS SUITE EXISTS: every other "two-way" / "cross-API" / "metamorphic" gate in this repo (see
/// <c>tests/Spiceport.Engine.Tests</c>) compares Spiceport's engine against Spiceport's OWN engine, over
/// a different code path (<c>ReferenceDatastore</c> oracle vs. the grain mesh, or Check vs. the
/// membership-walk accelerator). Those gates catch REGRESSIONS and cross-path DISAGREEMENTS, but they
/// share the same understanding of the SpiceDB semantics baked into <c>Spiceport.Engine</c> -- a bug in
/// that shared understanding (e.g. misreading the Zanzibar spec for exclusion-under-intersection, or an
/// arrow-composition edge case) would pass every one of them. This suite is the only one that can catch
/// that class of bug, because the oracle (a real <c>authzed/spicedb</c> binary) shares NO code with
/// Spiceport.
/// </para>
/// <para>
/// CI NOTE: the dominant cost is the SpiceDB image pull on a cold Docker cache (one-time) and the
/// container starting (a few seconds, amortized once per test class via <see cref="SpiceDbContainerFixture"/>,
/// an <c>ICollectionFixture</c> shared across every seed's [Theory] iteration). After that, each seed's
/// wall time is dominated by the query-universe enumeration (see <see cref="EnumerateQueries"/>) -- one
/// CheckPermission RPC pair (SpiceDB + in-process Spiceport) per (resource, permission, subject) point,
/// plus one LookupResources/LookupSubjects RPC pair per subject/resource. Seeds run sequentially within
/// the class (xUnit's default collection behavior), so total suite wall time is roughly linear in seed
/// count; see the class's own test bodies for per-seed guidance on trimming <see cref="SeedCount"/> if the
/// suite becomes too slow to run routinely.
/// </para>
/// <para>
/// DIALECT: <see cref="RandomAuthzWorlds"/>'s schema DSL (definition/relation/permission, union `+`,
/// intersection `&amp;`, exclusion `-`, arrow `-&gt;`, userset `group#member`, wildcard `user:*`) is
/// exactly the SpiceDB zed schema language -- no translation step is needed between the two systems. If a
/// future template addition to that generator turns out NOT to parse on real SpiceDB, that divergence
/// belongs in this file's remarks as a dialect finding, and the fix belongs in a differential-specific
/// template subset here, NOT in a weakening of <see cref="RandomAuthzWorlds"/> itself (which is shared by
/// gates that don't need SpiceDB compatibility).
/// </para>
/// </remarks>
[Collection(SpiceDbCollection.Name)]
public sealed class DifferentialConformanceTests
{
    /// <summary>
    /// Seed count for the differential gate. Deliberately smaller than <see cref="RandomAuthzWorlds.SeedCount"/>
    /// (24, used by the in-process metamorphic gates) -- each seed here pays for a real network round trip
    /// per query point against a real SpiceDB process, so this trades exhaustiveness for keeping the suite
    /// fast enough to run routinely. 10 seeds span every <c>DocumentViewTemplates</c> shape (6 templates)
    /// at least once with room to spare.
    /// </summary>
    private const int SeedCount = 10;

    public static IEnumerable<object[]> Seeds => Enumerable.Range(0, SeedCount).Select(s => new object[] { s });

    private readonly SpiceDbContainerFixture _spiceDb;

    public DifferentialConformanceTests(SpiceDbContainerFixture spiceDb) => _spiceDb = spiceDb;

    [SkippableTheory]
    [MemberData(nameof(Seeds))]
    public async Task Check_LookupResources_LookupSubjects_agree_with_real_SpiceDB(int seed)
    {
        Skip.IfNot(_spiceDb.Available, _spiceDb.SkipReason ?? "Docker/SpiceDB container unavailable");

        var world = RandomAuthzWorlds.Build(seed);

        using var spiceDbClient = new SpiceDbGrpcClient(_spiceDb.Address, _spiceDb.PreSharedKey);
        await using var cluster = await MeshTestCluster.CreateAsync(world.SchemaText);

        // --- Write the SAME schema + relationships to both systems. ---
        await spiceDbClient.WriteSchemaAsync(new V1::WriteSchemaRequest { Schema = world.SchemaText });
        // Spiceport's cluster is already compiled with this schema at startup (MeshTestCluster.CreateAsync),
        // so there is nothing further to write on that side -- it starts from the same schema text.

        // The SpiceDB container is shared across this class's seeds (start-up cost amortization), and the
        // seeds reuse one small id alphabet, so every previous seed's rows would contaminate this one's
        // verdicts (supersets under union templates; FLIPPED verdicts under exclusion templates, where a
        // stale `banned` row turns a member into a non-member). Reset every relation-bearing type before
        // writing this seed's world; Spiceport's cluster is fresh per seed and needs no reset.
        foreach (var resourceType in new[] { "group", "folder", "document" })
        {
            await spiceDbClient.DeleteRelationshipsAsync(new V1::DeleteRelationshipsRequest
            {
                RelationshipFilter = new V1::RelationshipFilter { ResourceType = resourceType },
            });
        }

        await WriteRelationshipsAsync(spiceDbClient, cluster, world.Relationships);

        var permissionsService = new AuthzedPermissionsV1Service(
            cluster.Services.GetRequiredService<IPermissionChecker>(), cluster.GrainFactory,
            cluster.ReverseOps, cluster.RelationshipReads,
            cluster.Services.GetRequiredService<ISchemaProvider>());

        var failures = new List<string>();

        // --- CheckPermission over the full query universe: both `view` (mixed set-op shapes across
        // seeds) and `view_mono` (always union/arrow-only) so every generator template is exercised. ---
        foreach (var permission in new[] { "view", "view_mono" })
        {
            foreach (var (resourceId, subjectId) in EnumerateCheckQueries(world))
            {
                var resource = new V1::ObjectReference { ObjectType = "document", ObjectId = resourceId };
                var subject = new V1::SubjectReference
                {
                    Object = new V1::ObjectReference { ObjectType = "user", ObjectId = subjectId },
                };

                var spiceDbResp = await spiceDbClient.CheckPermissionAsync(new V1::CheckPermissionRequest
                {
                    Resource = resource,
                    Permission = permission,
                    Subject = subject,
                    Consistency = new V1::Consistency { FullyConsistent = true },
                });

                var spiceportResp = await permissionsService.CheckPermission(new V1::CheckPermissionRequest
                {
                    Resource = resource,
                    Permission = permission,
                    Subject = subject,
                    Consistency = new V1::Consistency { FullyConsistent = true },
                }, FakeServerCallContext.Default);

                if (Normalize(spiceDbResp.Permissionship) != Normalize(spiceportResp.Permissionship))
                {
                    failures.Add(
                        $"seed={seed} CheckPermission document:{resourceId}#{permission}@user:{subjectId} " +
                        $"spicedb={spiceDbResp.Permissionship} spiceport={spiceportResp.Permissionship}");
                }
            }
        }

        // --- LookupResources: the accessible-resource SET must match, per subject, per permission. ---
        foreach (var permission in new[] { "view", "view_mono" })
        {
            foreach (var subjectId in world.Users)
            {
                var subject = new V1::SubjectReference
                {
                    Object = new V1::ObjectReference { ObjectType = "user", ObjectId = subjectId },
                };

                var spiceDbIds = (await spiceDbClient.LookupResourcesAsync(new V1::LookupResourcesRequest
                {
                    ResourceObjectType = "document",
                    Permission = permission,
                    Subject = subject,
                    Consistency = new V1::Consistency { FullyConsistent = true },
                })).Select(r => r.ResourceObjectId).ToHashSet(StringComparer.Ordinal);

                var writer = new CollectingStreamWriter<V1::LookupResourcesResponse>();
                await permissionsService.LookupResources(new V1::LookupResourcesRequest
                {
                    ResourceObjectType = "document",
                    Permission = permission,
                    Subject = subject,
                    Consistency = new V1::Consistency { FullyConsistent = true },
                }, writer, FakeServerCallContext.Default);
                var spiceportIds = writer.Collected.Select(r => r.ResourceObjectId).ToHashSet(StringComparer.Ordinal);

                if (!spiceDbIds.SetEquals(spiceportIds))
                {
                    failures.Add(
                        $"seed={seed} LookupResources document#{permission}@user:{subjectId} " +
                        $"spicedb=[{string.Join(",", spiceDbIds.OrderBy(x => x, StringComparer.Ordinal))}] " +
                        $"spiceport=[{string.Join(",", spiceportIds.OrderBy(x => x, StringComparer.Ordinal))}]");
                }
            }
        }

        // --- LookupSubjects: the resolved-subject SET must match, per resource, per permission. ---
        foreach (var permission in new[] { "view", "view_mono" })
        {
            foreach (var resourceId in world.Documents)
            {
                var resource = new V1::ObjectReference { ObjectType = "document", ObjectId = resourceId };

                var spiceDbIds = (await spiceDbClient.LookupSubjectsAsync(new V1::LookupSubjectsRequest
                {
                    Resource = resource,
                    Permission = permission,
                    SubjectObjectType = "user",
                    Consistency = new V1::Consistency { FullyConsistent = true },
                })).Select(r => r.Subject.SubjectObjectId).ToHashSet(StringComparer.Ordinal);

                var writer = new CollectingStreamWriter<V1::LookupSubjectsResponse>();
                await permissionsService.LookupSubjects(new V1::LookupSubjectsRequest
                {
                    Resource = resource,
                    Permission = permission,
                    SubjectObjectType = "user",
                    Consistency = new V1::Consistency { FullyConsistent = true },
                }, writer, FakeServerCallContext.Default);
                var spiceportIds = writer.Collected.Select(r => r.Subject.SubjectObjectId).ToHashSet(StringComparer.Ordinal);

                if (!spiceDbIds.SetEquals(spiceportIds))
                {
                    failures.Add(
                        $"seed={seed} LookupSubjects document:{resourceId}#{permission} " +
                        $"spicedb=[{string.Join(",", spiceDbIds.OrderBy(x => x, StringComparer.Ordinal))}] " +
                        $"spiceport=[{string.Join(",", spiceportIds.OrderBy(x => x, StringComparer.Ordinal))}]");
                }
            }
        }

        Assert.True(failures.Count == 0, "Divergence(s) between real SpiceDB and Spiceport:\n" + string.Join("\n", failures));
    }

    /// <summary>
    /// The full (resource, subject) query universe for this world's documents x users -- small by
    /// construction (<see cref="RandomAuthzWorlds"/>'s alphabet is 6 documents x 5 users), so enumeration
    /// rather than sampling stays fast.
    /// </summary>
    private static IEnumerable<(string ResourceId, string SubjectId)> EnumerateCheckQueries(RandomAuthzWorlds.World world)
    {
        foreach (var doc in world.Documents)
            foreach (var user in world.Users)
                yield return (doc, user);
    }

    /// <summary>
    /// Maps SpiceDB/Spiceport's <c>CheckPermissionResponse.Permissionship</c> onto a small closed set
    /// (Member/NotMember/Caveated) so the two systems' verdicts compare structurally rather than by raw
    /// enum identity (both sides use the SAME generated enum type here, but normalizing keeps the
    /// UNSPECIFIED/NO_PERMISSION distinction from ever being load-bearing for this gate).
    /// </summary>
    private static string Normalize(V1::CheckPermissionResponse.Types.Permissionship p) => p switch
    {
        V1::CheckPermissionResponse.Types.Permissionship.HasPermission => "Member",
        V1::CheckPermissionResponse.Types.Permissionship.ConditionalPermission => "Caveated",
        _ => "NotMember", // NO_PERMISSION and UNSPECIFIED both normalize to "not a member"
    };

    private static async Task WriteRelationshipsAsync(
        SpiceDbGrpcClient spiceDbClient, MeshTestCluster cluster, IReadOnlyList<CoreRelationship> relationships)
    {
        // SpiceDB side: WriteRelationships in bounded batches over the real gRPC wire.
        const int batchSize = 50;
        for (var i = 0; i < relationships.Count; i += batchSize)
        {
            var req = new V1::WriteRelationshipsRequest();
            foreach (var rel in relationships.Skip(i).Take(batchSize))
                req.Updates.Add(ToUpdate(rel));
            await spiceDbClient.WriteRelationshipsAsync(req);
        }

        // Spiceport side: straight to the datastore transaction (the same path WriteRelationships's gRPC
        // handler uses under the hood), batched identically for parity of write order.
        var updates = relationships
            .Select(rel => new CoreRelationshipUpdate(rel, UpdateOperation.Touch))
            .ToList();
        await cluster.Datastore.ReadWriteTx(tx => tx.WriteRelationships(updates));
    }

    private static V1::RelationshipUpdate ToUpdate(CoreRelationship rel) => new()
    {
        Operation = V1::RelationshipUpdate.Types.Operation.Touch,
        Relationship = new V1::Relationship
        {
            Resource = new V1::ObjectReference { ObjectType = rel.Resource.ObjectType, ObjectId = rel.Resource.ObjectId },
            Relation = rel.Resource.Relation,
            Subject = new V1::SubjectReference
            {
                Object = new V1::ObjectReference { ObjectType = rel.Subject.ObjectType, ObjectId = rel.Subject.ObjectId },
                OptionalRelation = rel.Subject.Relation == CoreConstants.Ellipsis ? string.Empty : rel.Subject.Relation,
            },
        },
    };

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

[CollectionDefinition(Name)]
public sealed class SpiceDbCollection : ICollectionFixture<SpiceDbContainerFixture>
{
    public const string Name = "spicedb-differential";
}
