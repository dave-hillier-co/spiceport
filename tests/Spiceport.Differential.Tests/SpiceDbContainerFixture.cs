using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace Spiceport.Differential.Tests;

/// <summary>
/// A disposable, real SpiceDB container (Testcontainers), for the differential conformance gate: the
/// only test suite in this repo that checks Spiceport's engine against a genuine, independent SpiceDB
/// implementation rather than another view of Spiceport's own engine.
/// </summary>
/// <remarks>
/// <para>
/// One container per test class, shared across that class's [Theory] seeds (see <see cref="DifferentialConformanceTests"/>):
/// starting SpiceDB dominates the runtime (~seconds), so amortizing it across seeds keeps the suite fast.
/// </para>
/// <para>
/// DOCKER SKIP SEMANTICS: mirrors <c>AdoNetDatastoreFixture</c>'s established pattern in
/// <c>tests/Spiceport.Grains.Tests/Durability</c> -- construction of the Testcontainers builder AND
/// <c>StartAsync</c> both happen inside <see cref="InitializeAsync"/>'s try/catch (never in a field
/// initializer, which would run before any try/catch and escape it), so a Docker-absent environment sets
/// <see cref="Available"/> false and <see cref="SkipReason"/> rather than throwing out of the fixture
/// constructor. Tests gate on it via <c>Skip.IfNot(_fixture.Available, ...)</c> from Xunit.SkippableFact
/// (already a project dependency here, and the same package the durability suite uses) -- SKIPPED, not
/// FAILED, when Docker is unavailable.
/// </para>
/// </remarks>
public sealed class SpiceDbContainerFixture : IAsyncLifetime
{
    // Pinned (not `latest`) so the suite doesn't flake when upstream publishes a breaking image; bump
    // deliberately. Reported in the class header alongside the differential suite's other CI notes.
    private const string Image = "authzed/spicedb:v1.49.2";
    private const int GrpcPort = 50051;
    private const string PresharedKey = "testkey";

    private IContainer? _container;

    /// <summary>True once the container started and is accepting gRPC connections.</summary>
    public bool Available { get; private set; }

    /// <summary>Why the fixture is unavailable (e.g. Docker not running), for the test skip message.</summary>
    public string? SkipReason { get; private set; }

    /// <summary>The gRPC pre-shared key configured on the container (also the client's auth header).</summary>
    public string PreSharedKey => PresharedKey;

    /// <summary>The gRPC endpoint address for <see cref="SpiceDbGrpcClient"/>.</summary>
    public string Address { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        try
        {
            // The authzed/spicedb image is distroless (no shell), so any INTERNAL probe wait strategy
            // (UntilInternalTcpPortIsAvailable executes inside the container) can never succeed and
            // StartAsync hangs forever. Wait on the startup log line instead — emitted once the gRPC
            // server is accepting connections — which needs nothing inside the container.
            var container = new ContainerBuilder(Image)
                .WithCommand(
                    "serve", "--grpc-preshared-key", PresharedKey,
                    // The corpus's expiration fixtures (`use expiration` + `with expiration`) are gated
                    // behind this experimental flag on older real SpiceDB releases (pre-graduation) -- without it, WriteSchema
                    // rejects any schema using the trait with "expiration trait is not allowed". Spiceport
                    // supports expiration unconditionally (no server-side feature flag), so enabling it
                    // here is what makes the corpus's expiration files an apples-to-apples comparison.
                    "--enable-experimental-relationship-expiration")
                .WithPortBinding(GrpcPort, true)
                .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("grpc server started serving"))
                .Build();

            await container.StartAsync();
            _container = container;
        }
        catch (Exception ex)
        {
            // ONLY container start (Docker absent / image pull failure / daemon unreachable) is
            // skip-worthy; anything after a successful start is a real defect and must fail loudly, so
            // it is deliberately outside this catch (mirrors AdoNetDatastoreFixture's documented split).
            Available = false;
            SkipReason = $"Docker/Testcontainers unavailable: {ex.GetType().Name}: {ex.Message}";
            return;
        }

        var mappedPort = _container.GetMappedPublicPort(GrpcPort);
        Address = $"http://{_container.Hostname}:{mappedPort}";
        Available = true;
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
            await _container.DisposeAsync();
    }
}
