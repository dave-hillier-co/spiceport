using Orleans.Runtime;
using Spiceport.Core;
using Spiceport.Grains;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Pure-function unit tests for <see cref="DispatchErrorMapper"/> — the port's <c>rewriteError</c>
/// equivalent at the cross-silo dispatch boundary. Each taxonomy branch is asserted WITHOUT provoking a
/// real silo failure: a true Orleans transport failure cannot be injected into the in-process
/// <c>TestCluster</c>, so the classification logic is exercised directly against representative
/// exception instances.
/// </summary>
public class DispatchErrorMapperTests
{
    private static SiloAddress Silo => SiloAddress.New(System.Net.IPAddress.Loopback, 11111, 1);

    public static IEnumerable<object[]> DomainExceptions()
    {
        yield return [new MaxDepthExceededException()];
        yield return [new InvalidConsistencyTokenException("bad token")];
        yield return [new CaveatEvaluationException(CaveatEvaluationErrorKind.ParameterTypeMismatch, "x")];
        yield return [new PreconditionFailedException(PreconditionFailureKind.MustMatchFoundNone, 0, "p")];
        yield return [new WriteConflictException(WriteConflictKind.Serialization, "conflict")];
        yield return [new SchemaWriteValidationException("dangling")];
    }

    [Theory]
    [MemberData(nameof(DomainExceptions))]
    public void Domain_exceptions_pass_through_unchanged(Exception ex)
    {
        var c = DispatchErrorMapper.Classify(ex);

        Assert.True(c.PassThrough);
    }

    [Fact]
    public void An_already_classified_dispatch_failure_passes_through_so_its_code_survives_further_hops()
    {
        var ex = new DispatchFailedException(DispatchErrorCode.Unavailable, "transient");

        var c = DispatchErrorMapper.Classify(ex);

        Assert.True(c.PassThrough);
    }

    public static IEnumerable<object[]> TransportFailures()
    {
        yield return [new SiloUnavailableException("silo gone")];
        // OrleansMessageRejectionException has no public constructor; construct it uninitialised so the
        // mapper's type-based classification can still be asserted against it.
        yield return [(Exception)System.Runtime.CompilerServices.RuntimeHelpers
            .GetUninitializedObject(typeof(OrleansMessageRejectionException))];
        yield return [new TimeoutException("response timed out")];
        yield return [new OrleansException("no compatible silo")];
    }

    [Theory]
    [MemberData(nameof(TransportFailures))]
    public void Transport_and_availability_failures_map_to_retriable_Unavailable(Exception ex)
    {
        var c = DispatchErrorMapper.Classify(ex);

        Assert.False(c.PassThrough);
        Assert.Equal(DispatchErrorCode.Unavailable, c.Code);
    }

    [Fact]
    public void Cancellation_maps_to_Cancelled()
    {
        var c = DispatchErrorMapper.Classify(new OperationCanceledException());

        Assert.False(c.PassThrough);
        Assert.Equal(DispatchErrorCode.Cancelled, c.Code);
    }

    [Fact]
    public void TaskCanceledException_maps_to_Cancelled()
    {
        var c = DispatchErrorMapper.Classify(new TaskCanceledException());

        Assert.False(c.PassThrough);
        Assert.Equal(DispatchErrorCode.Cancelled, c.Code);
    }

    [Fact]
    public void An_unrecognised_exception_maps_to_Internal()
    {
        var c = DispatchErrorMapper.Classify(new InvalidOperationException("boom"));

        Assert.False(c.PassThrough);
        Assert.Equal(DispatchErrorCode.Internal, c.Code);
    }

    [Fact]
    public void Classify_rejects_null()
    {
        Assert.Throws<ArgumentNullException>(() => DispatchErrorMapper.Classify(null!));
    }
}
