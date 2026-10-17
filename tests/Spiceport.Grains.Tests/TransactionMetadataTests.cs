using Orleans.Runtime;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Transaction metadata (SpiceDB <c>optional_transaction_metadata</c> on WriteRelationships and
/// DeleteRelationships, stored with the transaction by <c>options.WithMetadata</c> and returned by Watch
/// for that revision) is part of the log: it rides the declarative <see cref="CommitRequest"/> into the
/// journaled <see cref="LogEvent"/> of the revision the commit mints, so it survives journal replay and
/// projection exactly as the event does, and the Watch feed reads it from there. These gates drive the
/// real sequencer through a <see cref="MeshTestCluster"/>; the metadata is opaque bytes to everything below
/// the gRPC service.
/// </summary>
[Collection(MeshClusterCollection.Name)]
public sealed class TransactionMetadataTests
{
    private const string Schema = """
        definition user {}
        definition doc {
            relation viewer: user
        }
        """;

    private static IDatastoreGrain Sequencer(MeshTestCluster cluster) =>
        cluster.GrainFactory.GetGrain<IDatastoreGrain>(IDatastoreGrain.Key);

    private static RelationshipWire Viewer(string doc, string user) =>
        WireConvert.ToWire(Relationship.Create(
            new ObjectAndRelation("doc", doc, "viewer"),
            new ObjectAndRelation("user", user, CoreConstants.Ellipsis)));

    private static CommitRequest Touch(string doc, string user, byte[]? metadata) =>
        new(Array.Empty<CommitPreconditionWire>(),
            new[] { new RelationshipUpdateWire(RelationshipUpdateOpWire.Touch, Viewer(doc, user)) },
            DeleteByFilter: null, SchemaBytes: null, ExpectedSchemaHash: null,
            Array.Empty<CounterDeltaWire>(), ExpectedHead: null, TransactionMetadata: metadata);

    // Collection expressions build compiler-synthesized list types the Orleans serializer has no codec
    // for, so every list crossing a grain call here is an array.

    private static byte[] Metadata(string text) => System.Text.Encoding.UTF8.GetBytes(text);

    private static async Task<List<RevisionChange>> WatchUntil(
        IDatastore datastore, IRevision after, int count, WatchContent content = WatchContent.Relationships)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var changes = new List<RevisionChange>();
        await foreach (var change in datastore.Watch(after, new WatchOptions(content), cts.Token))
        {
            changes.Add(change);
            if (changes.Count == count)
                break;
        }
        return changes;
    }

    [Fact]
    public async Task A_declarative_commit_journals_its_metadata_on_its_own_event_and_watch_returns_it()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var grain = Sequencer(cluster);
        var from = await cluster.Datastore.HeadRevision();

        var tagged = await grain.Commit(Touch("a", "alice", Metadata("""{"who":"alice"}""")));
        var untagged = await grain.Commit(Touch("b", "bob", metadata: null));
        // SpiceDB stores no metadata for an empty struct (AddRevisionMetadata skips Len() == 0); an empty
        // struct serializes to zero bytes.
        var empty = await grain.Commit(Touch("c", "carol", metadata: []));

        var events = (await grain.ReadFrom(((TimestampRevision)from.Revision).TimestampNanosSinceEpoch, -1)).Events;
        Assert.Equal([tagged.Revision!.Value, untagged.Revision!.Value, empty.Revision!.Value], events.Select(e => e.Revision));
        Assert.Equal(Metadata("""{"who":"alice"}"""), events[0].TransactionMetadata);
        Assert.Null(events[1].TransactionMetadata);
        Assert.Null(events[2].TransactionMetadata);

        var changes = await WatchUntil(cluster.Datastore, from.Revision, 3);
        Assert.Equal(Metadata("""{"who":"alice"}"""), changes[0].TransactionMetadata);
        Assert.Null(changes[1].TransactionMetadata);
        Assert.Null(changes[2].TransactionMetadata);
    }

    [Fact]
    public async Task A_rejected_commit_appends_nothing_so_its_metadata_never_reaches_the_log()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var grain = Sequencer(cluster);
        var from = (await grain.GetHead()).Head;

        var create = new CommitRequest(
            Array.Empty<CommitPreconditionWire>(),
            new[] { new RelationshipUpdateWire(RelationshipUpdateOpWire.Create, Viewer("a", "alice")) },
            DeleteByFilter: null, SchemaBytes: null, ExpectedSchemaHash: null,
            Array.Empty<CounterDeltaWire>(), ExpectedHead: null, TransactionMetadata: Metadata("first"));
        Assert.Null((await grain.Commit(create)).Failure);
        var rejected = await grain.Commit(create with { TransactionMetadata = Metadata("second") });
        Assert.Equal(CommitFailureKind.CreateAlreadyExists, rejected.Failure!.Kind);

        var ev = Assert.Single((await grain.ReadFrom(from, -1)).Events);
        Assert.Equal(Metadata("first"), ev.TransactionMetadata);
    }

    [Fact]
    public async Task Relationship_writes_and_deletes_carry_their_metadata_into_the_log()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var grain = Sequencer(cluster);
        var from = await cluster.Datastore.HeadRevision();

        await cluster.Relationships.WriteRelationships(new WriteRelationshipsArgs(
            new[] { new RelationshipUpdateWire(RelationshipUpdateOpWire.Touch, Viewer("a", "alice")) },
            TransactionMetadata: Metadata("write")));
        await cluster.Relationships.DeleteRelationships(new DeleteRelationshipsArgs(
            new RelationshipsFilterWire("doc", null, new[] { "a" }, null, null, null, null),
            OptionalLimit: null,
            TransactionMetadata: Metadata("delete")));

        var changes = await WatchUntil(cluster.Datastore, from.Revision, 2);
        Assert.Equal(UpdateOperation.Touch, Assert.Single(changes[0].RelationshipChanges).Operation);
        Assert.Equal(Metadata("write"), changes[0].TransactionMetadata);
        // A delete-by-filter's deletions all belong to the one revision, and so to its metadata.
        Assert.Equal(UpdateOperation.Delete, Assert.Single(changes[1].RelationshipChanges).Operation);
        Assert.Equal(Metadata("delete"), changes[1].TransactionMetadata);
    }

    [Fact]
    public async Task The_compatibility_path_carries_metadata_through_the_expected_head_commit()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var from = await cluster.Datastore.HeadRevision();

        var revision = await cluster.Datastore.ReadWriteTx(
            tx => tx.WriteRelationships([new RelationshipUpdate(WireConvert.ToRelationship(Viewer("a", "alice")), UpdateOperation.Touch)]),
            Metadata("compat"));

        var change = Assert.Single(await WatchUntil(cluster.Datastore, from.Revision, 1));
        Assert.Equal(revision, change.Revision);
        Assert.Equal(Metadata("compat"), change.TransactionMetadata);
    }

    /// <summary>
    /// The metadata is part of the journaled event, so it survives the sequencer's reactivation (replay
    /// of the journaled state) and is untouched by a projection flush: events still pending after a
    /// flush keep their own metadata, and a Watch resumed across the reactivation returns it.
    /// </summary>
    [Fact]
    public async Task Metadata_survives_projection_and_journal_replay()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var grain = Sequencer(cluster);

        // Cross the 64-event flush boundary so the auto-flush projects most events; the tail after it
        // stays pending in the journaled state.
        for (var i = 0; i < 66; i++)
            Assert.Null((await grain.Commit(Touch($"p{i}", "alice", Metadata($"pre-{i}")))).Failure);

        var cursor = await cluster.Datastore.HeadRevision();
        var replies = new List<CommitReply>();
        for (var i = 0; i < 3; i++)
            replies.Add(await grain.Commit(Touch($"t{i}", "bob", i == 1 ? null : Metadata($"tail-{i}"))));

        var management = cluster.GrainFactory.GetGrain<IManagementGrain>(0);
        await management.ForceActivationCollection(TimeSpan.Zero);
        var stats = await management.GetDetailedGrainStatistics();
        Assert.DoesNotContain(stats, s =>
            s.GrainType.ToString()!.Contains("datastoregrain", StringComparison.OrdinalIgnoreCase));

        var changes = await WatchUntil(cluster.Datastore, cursor.Revision, 3);
        Assert.Equal(replies.Select(r => r.Revision!.Value), changes.Select(c => ((TimestampRevision)c.Revision).TimestampNanosSinceEpoch));
        Assert.Equal(Metadata("tail-0"), changes[0].TransactionMetadata);
        Assert.Null(changes[1].TransactionMetadata);
        Assert.Equal(Metadata("tail-2"), changes[2].TransactionMetadata);
    }

    /// <summary>
    /// A revision whose only content is metadata (a delete that matched nothing) is not a Watch change:
    /// SpiceDB's Watch service sends a response only for relationship updates, schema changes or a
    /// checkpoint, so the metadata alone emits nothing.
    /// </summary>
    [Fact]
    public async Task A_revision_with_metadata_but_no_changes_emits_no_watch_change()
    {
        await using var cluster = await MeshTestCluster.CreateAsync(Schema);
        var from = await cluster.Datastore.HeadRevision();

        await cluster.Relationships.DeleteRelationships(new DeleteRelationshipsArgs(
            new RelationshipsFilterWire("doc", null, new[] { "nothing" }, null, null, null, null),
            OptionalLimit: null,
            TransactionMetadata: Metadata("empty-delete")));
        await cluster.Relationships.WriteRelationships(new WriteRelationshipsArgs(
            new[] { new RelationshipUpdateWire(RelationshipUpdateOpWire.Touch, Viewer("a", "alice")) }));

        var change = Assert.Single(await WatchUntil(cluster.Datastore, from.Revision, 1));
        Assert.Equal("a", Assert.Single(change.RelationshipChanges).Relationship.Resource.ObjectId);
        Assert.Null(change.TransactionMetadata);
    }
}
