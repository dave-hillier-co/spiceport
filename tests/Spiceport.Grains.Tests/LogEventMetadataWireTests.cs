using System.Collections.Immutable;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// The transaction metadata a <see cref="LogEvent"/> carries is journaled with the sequencer's state, so
/// it must survive the Orleans serializer inside the journaled pending tail, and an event journaled before
/// the field existed must still read back (with no metadata). Pure serializer tests: no cluster, no host.
/// </summary>
public sealed class LogEventMetadataWireTests
{
    private static Serializer NewSerializer() =>
        new ServiceCollection()
            .AddSerializer(b =>
            {
                b.AddAssembly(typeof(LogEvent).Assembly);
                b.AddAssembly(typeof(LogEventMetadataWireTests).Assembly);
            })
            .BuildServiceProvider()
            .GetRequiredService<Serializer>();

    private static readonly RelationshipUpdateWire Touch = new(
        RelationshipUpdateOpWire.Touch,
        new RelationshipWire("doc", "a", "viewer", "user", "alice", "...", null, null, null));

    [Fact]
    public void The_journaled_pending_tail_round_trips_each_events_metadata()
    {
        var state = DatastoreMetaState.Empty(1) with
        {
            HeadRevision = 3,
            Pending = ImmutableList.Create(
                new LogEvent(2, new[] { Touch }, null, Array.Empty<CounterDeltaWire>(), null, TransactionMetadata: [10, 20, 30]),
                new LogEvent(3, new[] { Touch }, null, Array.Empty<CounterDeltaWire>(), null)),
        };

        var serializer = NewSerializer();
        var back = serializer.Deserialize<DatastoreMetaState>(serializer.SerializeToArray(state));

        Assert.Equal([10, 20, 30], back.Pending[0].TransactionMetadata!);
        Assert.Null(back.Pending[1].TransactionMetadata);

        var entry = serializer.Deserialize<DatastoreJournalEntry>(serializer.SerializeToArray<DatastoreJournalEntry>(
            new DatastoreJournalEntry.Committed(state.Pending[0])));
        Assert.Equal([10, 20, 30], ((DatastoreJournalEntry.Committed)entry).Event.TransactionMetadata!);
    }

    [Fact]
    public void An_event_journaled_before_the_metadata_field_reads_back_with_none()
    {
        var serializer = NewSerializer();
        var old = new PreMetadataLogEvent(7, new[] { Touch }, null, Array.Empty<CounterDeltaWire>(), null);

        var back = serializer.Deserialize<LogEvent>(serializer.SerializeToArray(old));

        Assert.Equal(7, back.Revision);
        Assert.Equal(Touch, Assert.Single(back.RelationshipChanges));
        Assert.Null(back.TransactionMetadata);
    }
}

/// <summary>The <see cref="LogEvent"/> wire shape as journaled before transaction metadata was added.</summary>
[GenerateSerializer]
public sealed record PreMetadataLogEvent(
    [property: Id(0)] long Revision,
    [property: Id(1)] IReadOnlyList<RelationshipUpdateWire> RelationshipChanges,
    [property: Id(2)] SchemaVersionWire? SchemaChange,
    [property: Id(3)] IReadOnlyList<CounterDeltaWire> CounterChanges,
    [property: Id(4)] long? GcFloor);
