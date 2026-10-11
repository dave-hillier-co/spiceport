using Spiceport.Grains;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// The label rule <see cref="ShardFold.Overlay"/> rests on: a key's current state is its projected
/// state with the pending events ABOVE the projected label folded over. Events at or below the label
/// are already projected and must not fold twice; events above it must; and the result is relabeled to
/// the head with the current floor applied. This is what makes a projection that lands between
/// reading the pending tail and reading the projected state harmless.
/// </summary>
public sealed class ShardFoldOverlayTests
{
    private static readonly GraphShardKeyWire Key = GraphShardKeyWire.ForResource("doc", "d");

    private static RelationshipWire Rel(string subject) =>
        new("doc", "d", "viewer", "user", subject, "...", null, null, null);

    private static LogEvent Touch(long revision, string subject) => new(
        revision,
        new List<RelationshipUpdateWire> { new(RelationshipUpdateOpWire.Touch, Rel(subject)) },
        SchemaChange: null, Array.Empty<CounterDeltaWire>(), GcFloor: null);

    private static LogEvent Delete(long revision, string subject) => new(
        revision,
        new List<RelationshipUpdateWire> { new(RelationshipUpdateOpWire.Delete, Rel(subject)) },
        SchemaChange: null, Array.Empty<CounterDeltaWire>(), GcFloor: null);

    private static GraphShardState Fold(params LogEvent[] events) =>
        events.Aggregate(GraphShardState.Empty, (state, ev) => ShardFold.ApplyEvent(state, ev, Key));

    private static List<string> Rows(GraphShardState state) => state.Rows
        .Select(r => $"{r.Relationship.SubjectId}|{r.CreatedRevision}|{r.DeletedRevision?.ToString() ?? "live"}")
        .OrderBy(s => s, StringComparer.Ordinal)
        .ToList();

    [Fact]
    public void Folds_pending_events_above_the_label_and_relabels_to_the_head()
    {
        var projected = Fold(Touch(10, "alice")) with { AppliedRevision = 10 };
        var pending = new[] { Touch(20, "bob"), Delete(30, "alice") };

        var current = ShardFold.Overlay(projected, pending, Key, head: 40, floor: 0);

        Assert.Equal(Rows(Fold(Touch(10, "alice"), Touch(20, "bob"), Delete(30, "alice"))), Rows(current));
        Assert.Equal(40, current.AppliedRevision);
    }

    [Fact]
    public void Skips_pending_events_the_projection_already_holds()
    {
        // The projection landed after the tail was captured: it already holds revision 20.
        var projected = Fold(Touch(10, "alice"), Touch(20, "bob")) with { AppliedRevision = 25 };
        var pending = new[] { Touch(20, "bob"), Touch(30, "carol") };

        var current = ShardFold.Overlay(projected, pending, Key, head: 30, floor: 0);

        Assert.Equal(Rows(Fold(Touch(10, "alice"), Touch(20, "bob"), Touch(30, "carol"))), Rows(current));
    }

    [Fact]
    public void Ignores_pending_events_for_other_keys()
    {
        var other = new LogEvent(
            20,
            new List<RelationshipUpdateWire>
            {
                new(RelationshipUpdateOpWire.Touch, new RelationshipWire("doc", "other", "viewer", "user", "x", "...", null, null, null)),
            },
            SchemaChange: null, Array.Empty<CounterDeltaWire>(), GcFloor: null);

        var current = ShardFold.Overlay(GraphShardState.Empty, [other], Key, head: 20, floor: 0);

        Assert.Empty(current.Rows);
        Assert.Equal(20, current.AppliedRevision);
    }

    [Fact]
    public void Applies_the_current_floor_to_rows_dead_below_it()
    {
        var projected = Fold(Touch(10, "alice"), Delete(20, "alice"), Touch(30, "bob")) with { AppliedRevision = 30 };

        var current = ShardFold.Overlay(projected, [], Key, head: 50, floor: 25);

        Assert.Equal(["bob|30|live"], Rows(current));
        Assert.Equal(25, current.GcFloor);
        Assert.Equal(50, current.AppliedRevision);
    }
}
