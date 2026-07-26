using System.Text.RegularExpressions;
using Grpc.Core;
using Spiceport.Api;
using Spiceport.Grains.Tests;
using V1 = Authzed.Api.V1;

namespace Spiceport.Differential.Tests;

/// <summary>
/// Directed (non-random) differential coverage for issue #33: SpiceDB rejects <c>WriteSchema</c> when a
/// wildcard is reachable through a userset reference ("wildcard relations cannot be transitively
/// included"), and accepts a handful of adjacent shapes that must NOT be over-rejected. Unlike
/// <see cref="DifferentialConformanceTests"/> (randomly generated worlds) and
/// <see cref="CorpusDifferentialTests"/> (the vendored corpus), this suite targets specific, hand-written
/// schema shapes chosen to pin the accept/reject boundary the type validator has to reproduce exactly.
/// </summary>
/// <remarks>
/// Each case writes the SAME schema text to a real SpiceDB container and to Spiceport's in-process
/// <see cref="AuthzedSchemaV1Service"/> (over the real grain mesh, via <see cref="MeshTestCluster"/>), and
/// asserts both sides reach the same accept/reject verdict -- and, when rejecting, the same gRPC status
/// code (<c>FailedPrecondition</c>).
/// </remarks>
[Collection(SpiceDbCollection.Name)]
public sealed class WriteSchemaWildcardTransitivityTests
{
    private readonly SpiceDbContainerFixture _spiceDb;

    public WriteSchemaWildcardTransitivityTests(SpiceDbContainerFixture spiceDb) => _spiceDb = spiceDb;

    // Pre-write container reset shared with the other directed suites: see SpiceDbReset for why it is
    // load-bearing (residual relationships from sibling classes make a schema write's verdict reflect
    // data-orphan rejection, not the rule under test).
    private static Task ResetSpiceDbAsync(SpiceDbGrpcClient client) => SpiceDbReset.ResetAsync(client);

    public static IEnumerable<object[]> RejectedShapes()
    {
        yield return new object[]
        {
            "wildcard-one-level-via-userset",
            """
            definition user {}
            definition group {
                relation member: user:*
            }
            definition document {
                relation viewer: group#member
            }
            """,
        };
        yield return new object[]
        {
            "wildcard-two-levels-via-userset",
            """
            definition user {}
            definition group {
                relation member: user:*
            }
            definition team {
                relation groupmember: group#member
            }
            definition document {
                relation viewer: team#groupmember
            }
            """,
        };
        yield return new object[]
        {
            "wildcard-on-left-of-arrow",
            """
            definition user {}
            definition document {
                relation parent: user:*
                permission view = parent->view
            }
            """,
        };
    }

    public static IEnumerable<object[]> AcceptedShapes()
    {
        yield return new object[]
        {
            "direct-wildcard-on-defining-relation",
            """
            definition user {}
            definition document {
                relation viewer: user:*
            }
            """,
        };
        yield return new object[]
        {
            "wildcard-reachable-only-through-a-permission",
            """
            definition user {}
            definition group {
                relation member: user:*
            }
            definition team {
                relation groupmember: group
                permission allmembers = groupmember->member
            }
            definition document {
                relation viewer: team#allmembers
            }
            """,
        };
        yield return new object[]
        {
            "wildcard-reachable-through-same-definition-cross-relation",
            """
            definition user {}
            definition group {
                relation adminwildcard: user:*
                relation member: group#adminwildcard
            }
            """,
        };
    }

    [SkippableTheory]
    [MemberData(nameof(RejectedShapes))]
    public async Task Rejected_by_real_SpiceDB_is_also_rejected_by_Spiceport(string label, string schema)
    {
        Skip.IfNot(_spiceDb.Available, _spiceDb.SkipReason ?? "Docker/SpiceDB container unavailable");

        using var spiceDbClient = new SpiceDbGrpcClient(_spiceDb.Address, _spiceDb.PreSharedKey);
        await ResetSpiceDbAsync(spiceDbClient);
        var spiceDbEx = await Assert.ThrowsAsync<RpcException>(() =>
            spiceDbClient.WriteSchemaAsync(new V1::WriteSchemaRequest { Schema = schema }));
        Assert.Equal(StatusCode.FailedPrecondition, spiceDbEx.StatusCode);

        await using var cluster = await MeshTestCluster.CreateAsync("definition user {}");
        var service = new AuthzedSchemaV1Service(cluster.GrainFactory, cluster.SchemaProvider);

        var spiceportEx = await Assert.ThrowsAsync<RpcException>(() => service.WriteSchema(
            new V1::WriteSchemaRequest { Schema = schema }, FakeServerCallContext.Default));

        Assert.True(
            StatusCode.FailedPrecondition == spiceportEx.StatusCode,
            $"[{label}] expected FailedPrecondition, got {spiceportEx.StatusCode}: {spiceportEx.Status.Detail}");
    }

    [SkippableTheory]
    [MemberData(nameof(AcceptedShapes))]
    public async Task Accepted_by_real_SpiceDB_is_also_accepted_by_Spiceport(string label, string schema)
    {
        Skip.IfNot(_spiceDb.Available, _spiceDb.SkipReason ?? "Docker/SpiceDB container unavailable");

        using var spiceDbClient = new SpiceDbGrpcClient(_spiceDb.Address, _spiceDb.PreSharedKey);
        await ResetSpiceDbAsync(spiceDbClient);
        // Must not throw -- real SpiceDB accepts this shape.
        await spiceDbClient.WriteSchemaAsync(new V1::WriteSchemaRequest { Schema = schema });

        await using var cluster = await MeshTestCluster.CreateAsync("definition user {}");
        var service = new AuthzedSchemaV1Service(cluster.GrainFactory, cluster.SchemaProvider);

        var ex = await Record.ExceptionAsync(() => service.WriteSchema(
            new V1::WriteSchemaRequest { Schema = schema }, FakeServerCallContext.Default));

        Assert.True(ex is null, $"[{label}] expected Spiceport to accept, got: {ex}");
    }

    /// <summary>
    /// Deterministic regression for the reset itself: plants residue the way another class in this
    /// collection could leave it (a data-bearing relation under a type the next schema does not define),
    /// then asserts the reset-then-write path succeeds anyway. Without the reset, real SpiceDB rejects
    /// the write outright ("cannot delete object definition ... as at least one relationship exists under
    /// it"), which is exactly the order-dependent failure the theories above must be immune to.
    /// </summary>
    [SkippableFact]
    public async Task Reset_makes_schema_write_verdict_independent_of_residual_relationships()
    {
        Skip.IfNot(_spiceDb.Available, _spiceDb.SkipReason ?? "Docker/SpiceDB container unavailable");

        using var spiceDbClient = new SpiceDbGrpcClient(_spiceDb.Address, _spiceDb.PreSharedKey);
        await ResetSpiceDbAsync(spiceDbClient);

        await spiceDbClient.WriteSchemaAsync(new V1::WriteSchemaRequest
        {
            Schema = """
                definition user {}
                definition residuewidget {
                    relation owner: user
                }
                """,
        });
        await spiceDbClient.WriteRelationshipsAsync(new V1::WriteRelationshipsRequest
        {
            Updates =
            {
                new V1::RelationshipUpdate
                {
                    Operation = V1::RelationshipUpdate.Types.Operation.Touch,
                    Relationship = new V1::Relationship
                    {
                        Resource = new V1::ObjectReference { ObjectType = "residuewidget", ObjectId = "w1" },
                        Relation = "owner",
                        Subject = new V1::SubjectReference
                        {
                            Object = new V1::ObjectReference { ObjectType = "user", ObjectId = "u1" },
                        },
                    },
                },
            },
        });

        await ResetSpiceDbAsync(spiceDbClient);
        // Must not throw: with the residue cleared, dropping residuewidget is a pure schema transition.
        await spiceDbClient.WriteSchemaAsync(new V1::WriteSchemaRequest { Schema = "definition user {}" });
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
