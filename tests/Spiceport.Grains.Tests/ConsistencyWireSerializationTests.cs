using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;
using Spiceport.Core;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Verifies the [GenerateSerializer] consistency DTO survives the Orleans serializer (i.e. it can be
/// carried across grain calls) and that it maps losslessly to and from the domain requirement.
/// </summary>
public class ConsistencyWireSerializationTests
{
    private static Serializer NewSerializer() =>
        new ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider()
            .GetRequiredService<Serializer>();

    public static IEnumerable<object[]> Cases()
    {
        yield return [ConsistencyWire.MinimizeLatency];
        yield return [new ConsistencyWire(ConsistencyModeWire.FullyConsistent)];
        yield return [new ConsistencyWire(ConsistencyModeWire.AtLeastAsFresh, "tok-fresh")];
        yield return [new ConsistencyWire(ConsistencyModeWire.AtExactSnapshot, "tok-exact")];
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void ConsistencyWire_OrleansRoundTrip_PreservesValue(ConsistencyWire wire)
    {
        var serializer = NewSerializer();
        var bytes = serializer.SerializeToArray(wire);
        var back = serializer.Deserialize<ConsistencyWire>(bytes);
        Assert.Equal(wire, back);
    }

    [Fact]
    public void ConsistencyWire_MapsToAndFromDomainRequirement()
    {
        var token = new ZedToken("the-token");

        Assert.IsType<ConsistencyRequirement.MinimizeLatencyRequirement>(
            ConsistencyWire.MinimizeLatency.ToRequirement());
        Assert.IsType<ConsistencyRequirement.FullyConsistentRequirement>(
            new ConsistencyWire(ConsistencyModeWire.FullyConsistent).ToRequirement());

        var atLeast = (ConsistencyRequirement.AtLeastAsFreshRequirement)
            new ConsistencyWire(ConsistencyModeWire.AtLeastAsFresh, token.Token).ToRequirement();
        Assert.Equal(token, atLeast.Token);

        var atExact = (ConsistencyRequirement.AtExactSnapshotRequirement)
            new ConsistencyWire(ConsistencyModeWire.AtExactSnapshot, token.Token).ToRequirement();
        Assert.Equal(token, atExact.Token);

        // Round trip through FromRequirement.
        Assert.Equal(
            new ConsistencyWire(ConsistencyModeWire.AtLeastAsFresh, token.Token),
            ConsistencyWire.FromRequirement(ConsistencyRequirement.AtLeastAsFresh(token)));
    }
}
