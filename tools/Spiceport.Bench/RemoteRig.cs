using System.Net.Http.Json;
using System.Reflection;
using System.Text.Json;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Spiceport.Core;
using Spiceport.Grains;
using Spiceport.Grains.Abstractions;
using V1 = Authzed.Api.V1;

namespace Spiceport.Bench;

/// <summary>
/// The driver half of the real-network rig (docs/scalability-program.md section 2): a hand-rolled
/// <c>authzed.api.v1</c> gRPC client fanned out over one channel per <c>--endpoints</c> silo, plus an
/// <see cref="HttpClient"/> talking to each silo's rig HTTP surface (<c>--rig</c>) for
/// <c>/healthz</c>/<c>/rig/reset</c>/<c>/rig/metrics</c>. Same client-construction pattern as
/// <c>tests/Spiceport.Differential.Tests/SpiceDbGrpcClient.cs</c> — Spiceport.Protos compiles the
/// vendored protos message/server-only, so there are no generated <c>XxxServiceClient</c> stubs;
/// recompiling the protos client-side in THIS project would produce duplicate CLR message types
/// that shadow the ones the rest of the solution uses (see that file's remarks for the full
/// reasoning). The rig has no auth (RigSilo serves it unauthenticated on localhost), so unlike the
/// differential client, calls carry no bearer header.
/// </summary>
public sealed class RemoteRig : IDisposable
{
    private const string PermissionsServiceName = "authzed.api.v1.PermissionsService";
    private const string SchemaServiceName = "authzed.api.v1.SchemaService";

    private readonly GrpcChannel[] _channels;
    private readonly CallInvoker[] _invokers;
    private readonly string[] _rigHosts;
    private readonly HttpClient _http = new();

    public RemoteRig(string[] endpoints, string[] rigHosts)
    {
        if (endpoints.Length == 0)
            throw new BenchUsageException("--endpoints requires at least one gRPC address, e.g. --endpoints=127.0.0.1:50051.");
        if (rigHosts.Length == 0)
            throw new BenchUsageException("--rig requires at least one metrics-HTTP address, e.g. --rig=127.0.0.1:8081.");
        if (rigHosts.Length != endpoints.Length)
            throw new BenchUsageException(
                $"--endpoints ({endpoints.Length}) and --rig ({rigHosts.Length}) must list the same number of silos, in the same order.");

        _channels = endpoints.Select(e => GrpcChannel.ForAddress(NormalizeGrpc(e))).ToArray();
        _invokers = _channels.Select(c => c.CreateCallInvoker()).ToArray();
        _rigHosts = rigHosts.Select(NormalizeHttp).ToArray();
    }

    /// <summary>Silo count (channel/rig-host pair count).</summary>
    public int SiloCount => _channels.Length;

    /// <summary>
    /// Round-robins gRPC channels ACROSS WORKERS (one fixed channel per worker for the run, not
    /// per call): a closed-loop worker issues many calls back-to-back, so pinning its channel
    /// avoids the per-call channel-selection overhead while still spreading load evenly when
    /// workers outnumber silos.
    /// </summary>
    public CallInvoker InvokerForWorker(int workerIndex) => _invokers[workerIndex % _invokers.Length];

    public async Task WriteSchemaAsync(string schemaText)
    {
        var request = new V1::WriteSchemaRequest { Schema = schemaText };
        await _invokers[0].AsyncUnaryCall(WriteSchemaMethod, null, new CallOptions(), request).ResponseAsync;
    }

    /// <summary>
    /// Loads the world over the wire via batched <c>WriteRelationships</c> TOUCH updates, each batch
    /// capped at <paramref name="batchSize"/> (default: <c>RequestLimits.MaxUpdatesPerWrite</c> from
    /// <c>src/Spiceport.Api/RequestLimits.cs</c>, which is <c>internal</c> to that project — the value
    /// is copied here, not referenced, since Bench's scope is deliberately outside src/). Returns the
    /// last batch's <c>ZedToken</c>, the freshness floor for at-least-as-fresh checks. Progress goes
    /// to stderr so stdout stays clean for result tables.
    /// </summary>
    public async Task<string> LoadWorldAsync(BenchWorld world, int batchSize = 1000)
    {
        var relationships = world.Relationships;
        var token = "";
        for (var offset = 0; offset < relationships.Count; offset += batchSize)
        {
            var batch = relationships.Skip(offset).Take(batchSize);
            var request = new V1::WriteRelationshipsRequest();
            request.Updates.AddRange(batch.Select(ToUpdate));
            var reply = await _invokers[0].AsyncUnaryCall(WriteRelationshipsMethod, null, new CallOptions(), request).ResponseAsync;
            token = reply.WrittenAt.Token;
            var loaded = Math.Min(offset + batchSize, relationships.Count);
            if (relationships.Count > 2 * batchSize)
                Console.Error.WriteLine($"  loaded {loaded}/{relationships.Count} relationships");
        }
        return token;
    }

    /// <summary>
    /// Touches one <c>document#viewer@user</c> relationship on the worker's channel (the remote analog
    /// of <see cref="BenchCommon.MakeWriteOp"/>). Returns the commit's <c>ZedToken</c>.
    /// </summary>
    public async Task<string> TouchAsync(CallInvoker invoker, string doc, string user)
    {
        var request = new V1::WriteRelationshipsRequest();
        request.Updates.Add(new V1::RelationshipUpdate
        {
            Operation = V1::RelationshipUpdate.Types.Operation.Touch,
            Relationship = ToRelationship(new RelationshipWire(
                "document", doc, "viewer", "user", user, CoreConstants.Ellipsis, null, null, null)),
        });
        var reply = await invoker.AsyncUnaryCall(WriteRelationshipsMethod, null, new CallOptions(), request).ResponseAsync;
        return reply.WrittenAt.Token;
    }

    /// <summary>
    /// Issues <c>document.view</c> for a uniform user, mapping <paramref name="consistency"/> onto the
    /// <c>authzed.api.v1.Consistency</c> oneof (<c>ConsistencyRequirement.MinimizeLatency</c> -&gt;
    /// <c>minimize_latency</c>, <c>FullyConsistent</c> -&gt; <c>fully_consistent</c>,
    /// <c>AtLeastAsFresh(token)</c> -&gt; <c>at_least_as_fresh</c>; <c>AtExactSnapshot</c> is unused by
    /// the bench mixes but mapped for completeness).
    /// </summary>
    public async Task CheckAsync(CallInvoker invoker, string doc, string user, ConsistencyRequirement consistency)
    {
        var request = new V1::CheckPermissionRequest
        {
            Consistency = ToWireConsistency(consistency),
            Resource = new V1::ObjectReference { ObjectType = "document", ObjectId = doc },
            Permission = "view",
            Subject = new V1::SubjectReference { Object = new V1::ObjectReference { ObjectType = "user", ObjectId = user } },
        };
        await invoker.AsyncUnaryCall(CheckPermissionMethod, null, new CallOptions(), request).ResponseAsync;
    }

    /// <summary>POSTs <c>/rig/reset</c> to every silo, resetting both metric seams cluster-wide.</summary>
    public async Task ResetAllAsync()
    {
        await Task.WhenAll(_rigHosts.Select(async host =>
        {
            using var response = await _http.PostAsync($"{host}/rig/reset", content: null);
            response.EnsureSuccessStatusCode();
        }));
    }

    /// <summary>
    /// Waits for every silo's <c>/healthz</c> to return 200, polling at <paramref name="pollInterval"/>
    /// until <paramref name="timeout"/> elapses. Throws <see cref="BenchUsageException"/> on timeout —
    /// this is the driver-side half of the orchestrator's readiness gate (tools/rig/rig.sh boots and
    /// polls the silos; the driver re-checks so a scenario fails fast with a clear message instead of
    /// timing out obscurely inside the first gRPC call).
    /// </summary>
    public async Task WaitUntilReadyAsync(TimeSpan timeout, TimeSpan? pollInterval = null)
    {
        var interval = pollInterval ?? TimeSpan.FromMilliseconds(200);
        var deadline = DateTime.UtcNow + timeout;
        foreach (var host in _rigHosts)
        {
            while (true)
            {
                try
                {
                    using var response = await _http.GetAsync($"{host}/healthz");
                    if (response.IsSuccessStatusCode)
                        break;
                }
                catch (HttpRequestException)
                {
                    // Not up yet; retried below until the deadline.
                }
                if (DateTime.UtcNow > deadline)
                    throw new BenchUsageException($"rig silo at {host} did not become healthy within {timeout}.");
                await Task.Delay(interval);
            }
        }
    }

    /// <summary>
    /// Fetches every silo's <c>/rig/metrics</c> and sums the dispatch and sequencer snapshots — the
    /// remote analog of <c>MeshTestCluster.MetricsSnapshot</c>/<c>SequencerMetricsSnapshot</c>. Only the
    /// silo hosting the singleton datastore grain reports nonzero sequencer counters, so summing across
    /// silos is the correct cluster total (the other silos legitimately contribute zero).
    /// </summary>
    public async Task<(DispatchMetricsSnapshot Dispatch, SequencerMetricsSnapshot Sequencer)> SummedMetricsAsync()
    {
        var snapshots = await Task.WhenAll(_rigHosts.Select(async host =>
        {
            using var response = await _http.GetAsync($"{host}/rig/metrics");
            response.EnsureSuccessStatusCode();
            var payload = await response.Content.ReadFromJsonAsync<RigMetricsPayload>(JsonOptions)
                ?? throw new BenchUsageException($"rig silo at {host} returned an empty /rig/metrics body.");
            return payload;
        }));

        var dispatch = default(DispatchMetricsSnapshot);
        var sequencer = default(SequencerMetricsSnapshot);
        foreach (var snapshot in snapshots)
        {
            dispatch += snapshot.Dispatch;
            // CommitMicrosMax combines via Math.Max, not +, so the record's own operator+ (which
            // already encodes that) is used rather than a field-by-field sum here.
            sequencer += snapshot.Sequencer;
        }
        return (dispatch, sequencer);
    }

    public void Dispose()
    {
        foreach (var channel in _channels)
            channel.Dispose();
        _http.Dispose();
    }

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private sealed record RigMetricsPayload(DispatchMetricsSnapshot Dispatch, SequencerMetricsSnapshot Sequencer);

    private static string NormalizeGrpc(string endpoint) =>
        endpoint.Contains("://", StringComparison.Ordinal) ? endpoint : $"http://{endpoint}";

    private static string NormalizeHttp(string host) =>
        (host.Contains("://", StringComparison.Ordinal) ? host : $"http://{host}").TrimEnd('/');

    private static V1::Consistency ToWireConsistency(ConsistencyRequirement consistency) => consistency switch
    {
        ConsistencyRequirement.MinimizeLatencyRequirement => new V1::Consistency { MinimizeLatency = true },
        ConsistencyRequirement.FullyConsistentRequirement => new V1::Consistency { FullyConsistent = true },
        ConsistencyRequirement.AtLeastAsFreshRequirement fresh =>
            new V1::Consistency { AtLeastAsFresh = new V1::ZedToken { Token = fresh.Token.Token } },
        ConsistencyRequirement.AtExactSnapshotRequirement exact =>
            new V1::Consistency { AtExactSnapshot = new V1::ZedToken { Token = exact.Token.Token } },
        _ => throw new ArgumentOutOfRangeException(nameof(consistency), consistency, "Unknown consistency requirement."),
    };

    private static V1::RelationshipUpdate ToUpdate(RelationshipWire wire) => new()
    {
        Operation = V1::RelationshipUpdate.Types.Operation.Touch,
        Relationship = ToRelationship(wire),
    };

    private static V1::Relationship ToRelationship(RelationshipWire wire)
    {
        var relationship = new V1::Relationship
        {
            Resource = new V1::ObjectReference { ObjectType = wire.ResourceType, ObjectId = wire.ResourceId },
            Relation = wire.ResourceRelation,
            Subject = new V1::SubjectReference
            {
                Object = new V1::ObjectReference { ObjectType = wire.SubjectType, ObjectId = wire.SubjectId },
                OptionalRelation = wire.SubjectRelation == CoreConstants.Ellipsis ? "" : wire.SubjectRelation,
            },
        };
        return relationship;
    }

    private static Marshaller<T> ProtoMarshaller<T>() where T : IMessage<T>
    {
        var parser = (MessageParser<T>)typeof(T).GetProperty("Parser", BindingFlags.Public | BindingFlags.Static)!
            .GetValue(null)!;
        return Marshallers.Create<T>(m => m.ToByteArray(), bytes => parser.ParseFrom(bytes));
    }

    private static readonly Method<V1::WriteSchemaRequest, V1::WriteSchemaResponse> WriteSchemaMethod = new(
        MethodType.Unary, SchemaServiceName, "WriteSchema",
        ProtoMarshaller<V1::WriteSchemaRequest>(), ProtoMarshaller<V1::WriteSchemaResponse>());

    private static readonly Method<V1::WriteRelationshipsRequest, V1::WriteRelationshipsResponse> WriteRelationshipsMethod = new(
        MethodType.Unary, PermissionsServiceName, "WriteRelationships",
        ProtoMarshaller<V1::WriteRelationshipsRequest>(), ProtoMarshaller<V1::WriteRelationshipsResponse>());

    private static readonly Method<V1::CheckPermissionRequest, V1::CheckPermissionResponse> CheckPermissionMethod = new(
        MethodType.Unary, PermissionsServiceName, "CheckPermission",
        ProtoMarshaller<V1::CheckPermissionRequest>(), ProtoMarshaller<V1::CheckPermissionResponse>());
}
