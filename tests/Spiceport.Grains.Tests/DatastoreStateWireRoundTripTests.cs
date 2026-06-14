using System.Collections.Immutable;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;
using Spiceport.Grains;
using Spiceport.Grains.Abstractions;

namespace Spiceport.Grains.Tests;

/// <summary>
/// Proves the grain's persisted state (<see cref="DatastoreGrainState"/>) survives an Orleans serializer
/// round-trip losslessly: revisions, created/deleted MVCC stamps, multiple schema versions, a fully
/// featured counter filter, and boxed caveat-context <see cref="JsonElement"/> values. This is a pure
/// serializer test — it builds a minimal <see cref="ServiceCollection"/>, NOT a <c>TestCluster</c>, so it
/// does not need the non-parallel cluster collection and does not boot a host.
/// </summary>
public sealed class DatastoreStateWireRoundTripTests
{
    private static Serializer<DatastoreGrainState> BuildSerializer()
    {
        var services = new ServiceCollection();
        services.AddSerializer(b =>
        {
            // The wire records (Grains.Abstractions assembly) AND the JsonElement surrogate/copier
            // (Grains assembly) so boxed caveat-context JsonElement values round-trip.
            b.AddAssembly(typeof(DatastoreGrainState).Assembly);
            b.AddAssembly(typeof(JsonElementSurrogate).Assembly);
        });
        return services.BuildServiceProvider().GetRequiredService<Serializer<DatastoreGrainState>>();
    }

    private static IReadOnlyDictionary<string, object?> CaveatContext()
    {
        // Boxed JsonElement values, exactly as caveat context is parsed in production.
        const string json = """{ "level": 7, "name": "alice", "active": true }""";
        return JsonSerializer.Deserialize<Dictionary<string, object?>>(json)!;
    }

    [Fact]
    public void CommittedState_RoundTrips_ByteForByte_AtLiveSetLevel()
    {
        var caveatCtx = CaveatContext();

        var relA = new RelationshipWire("doc", "a", "viewer", "user", "alice", "...", null, null, null);
        var relB = new RelationshipWire(
            "doc", "b", "viewer", "user", "bob", "...",
            "is_active", caveatCtx, new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero));

        var counterFilter = new FullRelationshipsFilterWire(
            OptionalResourceType: "doc",
            OptionalResourceIds: new[] { "a", "b" },
            OptionalResourceIdPrefix: null,
            OptionalResourceRelation: "viewer",
            OptionalSubjectsSelectors: new[]
            {
                new SubjectsSelectorWire("user", new[] { "alice" },
                    new SubjectRelationFilterWire(null, true, false)),
                new SubjectsSelectorWire("group", null,
                    new SubjectRelationFilterWire("member", false, false)),
            },
            OptionalCaveatNameFilter: new CaveatNameFilterWire(1, "is_active"),
            OptionalExpirationOption: 1);

        // rev1: create A and B (B with caveat context). rev2: delete A (closes its row), touch B.
        var rev1 = 1000L;
        var rev2 = 2000L;
        var schemaBytes1 = Encoding.UTF8.GetBytes("definition doc {}");
        var schemaBytes2 = Encoding.UTF8.GetBytes("definition doc { relation viewer: user }");

        var state = new DatastoreGrainState
        {
            HeadRevision = rev2,
            Relationships = ImmutableList.Create(
                // A: created at rev1, deleted at rev2.
                new StoredRelationshipWire(relA, rev1, rev2),
                // B original: created at rev1, closed by touch at rev2.
                new StoredRelationshipWire(relB, rev1, rev2),
                // B re-created by the touch at rev2, still live.
                new StoredRelationshipWire(relB, rev2, null)),
            Schemas = ImmutableList.Create(
                new SchemaVersionWire(rev1, schemaBytes1, "hash1"),
                new SchemaVersionWire(rev2, schemaBytes2, "hash2")),
            Counters = ImmutableList.Create(
                new CounterVersionWire(rev1, "c1", counterFilter)),
        };

        var serializer = BuildSerializer();
        var bytes = serializer.SerializeToArray(state);
        var restored = serializer.Deserialize(bytes);

        Assert.Equal(state.HeadRevision, restored.HeadRevision);

        // Relationships: structural element-wise (record equality on RelationshipWire compares the
        // caveat-context dictionary by reference of boxed JsonElement, which is not value-equal — so
        // compare identity + stamps here and assert the caveat context explicitly below).
        Assert.Equal(state.Relationships.Count, restored.Relationships.Count);
        for (var i = 0; i < state.Relationships.Count; i++)
        {
            var a = state.Relationships[i];
            var r = restored.Relationships[i];
            Assert.Equal(a.CreatedRevision, r.CreatedRevision);
            Assert.Equal(a.DeletedRevision, r.DeletedRevision);
            AssertRelEqual(a.Relationship, r.Relationship);
        }

        // Schemas: byte[] is reference-equal under record equality, so compare bytes via SequenceEqual.
        Assert.Equal(state.Schemas.Count, restored.Schemas.Count);
        for (var i = 0; i < state.Schemas.Count; i++)
        {
            Assert.Equal(state.Schemas[i].Revision, restored.Schemas[i].Revision);
            Assert.Equal(state.Schemas[i].Hash, restored.Schemas[i].Hash);
            Assert.True(state.Schemas[i].Bytes.SequenceEqual(restored.Schemas[i].Bytes));
        }

        // Counters: FullRelationshipsFilterWire embeds IReadOnlyList collections, which use reference
        // equality under record value-equality (a deserialized list is a fresh instance). Compare the
        // filter structurally.
        Assert.Equal(state.Counters.Count, restored.Counters.Count);
        for (var i = 0; i < state.Counters.Count; i++)
        {
            Assert.Equal(state.Counters[i].Revision, restored.Counters[i].Revision);
            Assert.Equal(state.Counters[i].Name, restored.Counters[i].Name);
            AssertFilterEqual(state.Counters[i].Filter, restored.Counters[i].Filter);
        }

        // Idempotent re-serialize: bytes are stable.
        Assert.Equal(bytes, serializer.SerializeToArray(restored));

        // Explicit proof the JsonElement surrogate is registered and caveat context survives.
        var restoredB = restored.Relationships.Single(r => r.Relationship.ResourceId == "b" && r.DeletedRevision is null).Relationship;
        Assert.NotNull(restoredB.CaveatContext);
        Assert.Equal("is_active", restoredB.CaveatName);
        var rawAlice = ((JsonElement)restoredB.CaveatContext!["name"]!).GetRawText();
        Assert.Equal("\"alice\"", rawAlice);
        Assert.Equal("7", ((JsonElement)restoredB.CaveatContext!["level"]!).GetRawText());
        Assert.Equal("true", ((JsonElement)restoredB.CaveatContext!["active"]!).GetRawText());
    }

    private static void AssertFilterEqual(FullRelationshipsFilterWire? a, FullRelationshipsFilterWire? b)
    {
        if (a is null)
        {
            Assert.Null(b);
            return;
        }
        Assert.NotNull(b);
        Assert.Equal(a.OptionalResourceType, b!.OptionalResourceType);
        Assert.Equal(a.OptionalResourceIds, b.OptionalResourceIds);
        Assert.Equal(a.OptionalResourceIdPrefix, b.OptionalResourceIdPrefix);
        Assert.Equal(a.OptionalResourceRelation, b.OptionalResourceRelation);
        Assert.Equal(a.OptionalCaveatNameFilter, b.OptionalCaveatNameFilter);
        Assert.Equal(a.OptionalExpirationOption, b.OptionalExpirationOption);

        if (a.OptionalSubjectsSelectors is null)
        {
            Assert.Null(b.OptionalSubjectsSelectors);
            return;
        }
        Assert.NotNull(b.OptionalSubjectsSelectors);
        Assert.Equal(a.OptionalSubjectsSelectors.Count, b.OptionalSubjectsSelectors!.Count);
        for (var i = 0; i < a.OptionalSubjectsSelectors.Count; i++)
        {
            var sa = a.OptionalSubjectsSelectors[i];
            var sb = b.OptionalSubjectsSelectors[i];
            Assert.Equal(sa.OptionalSubjectType, sb.OptionalSubjectType);
            Assert.Equal(sa.OptionalSubjectIds, sb.OptionalSubjectIds);
            Assert.Equal(sa.RelationFilter, sb.RelationFilter);
        }
    }

    private static void AssertRelEqual(RelationshipWire a, RelationshipWire b)
    {
        Assert.Equal(a.ResourceType, b.ResourceType);
        Assert.Equal(a.ResourceId, b.ResourceId);
        Assert.Equal(a.ResourceRelation, b.ResourceRelation);
        Assert.Equal(a.SubjectType, b.SubjectType);
        Assert.Equal(a.SubjectId, b.SubjectId);
        Assert.Equal(a.SubjectRelation, b.SubjectRelation);
        Assert.Equal(a.CaveatName, b.CaveatName);
        Assert.Equal(a.Expiration, b.Expiration);

        if (a.CaveatContext is null)
        {
            Assert.Null(b.CaveatContext);
            return;
        }
        Assert.NotNull(b.CaveatContext);
        Assert.Equal(a.CaveatContext.Keys.OrderBy(k => k), b.CaveatContext!.Keys.OrderBy(k => k));
        foreach (var key in a.CaveatContext.Keys)
        {
            var expected = ((JsonElement)a.CaveatContext[key]!).GetRawText();
            var actual = ((JsonElement)b.CaveatContext[key]!).GetRawText();
            Assert.Equal(expected, actual);
        }
    }
}
