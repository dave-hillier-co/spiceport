using System.Collections.Immutable;
using System.Text.Json;

namespace Spiceport.Core.Tests;

/// <summary>
/// Records advertise value equality. The ones that hold a byte array, a dictionary or an immutable list
/// must compare those members by content, or two equal values read as different the moment they meet a
/// set, a Distinct or a diff.
/// </summary>
public class RecordValueEqualityTests
{
    [Fact]
    public void RelationshipIntegrity_compares_its_hash_by_content()
    {
        var at = DateTimeOffset.UnixEpoch;
        var a = new RelationshipIntegrity("key1", [1, 2, 3], at);
        var b = new RelationshipIntegrity("key1", [1, 2, 3], at);
        var c = new RelationshipIntegrity("key1", [1, 2, 4], at);

        Assert.Equal(a, b);
        Assert.True(a == b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.NotEqual(a, c);
        Assert.NotEqual(a, a with { KeyId = "key2" });
    }

    [Fact]
    public void ContextualizedCaveat_compares_its_context_by_content()
    {
        var a = new ContextualizedCaveat("ip_allowed", new Dictionary<string, object?>
        {
            ["cidr"] = "10.0.0.0/8",
            ["limit"] = 3.0,
            ["tags"] = new List<object?> { "a", "b" },
            ["nested"] = new Dictionary<string, object?> { ["x"] = true },
        });
        var b = new ContextualizedCaveat("ip_allowed", new Dictionary<string, object?>
        {
            ["nested"] = new Dictionary<string, object?> { ["x"] = true },
            ["tags"] = new List<object?> { "a", "b" },
            ["limit"] = 3.0,
            ["cidr"] = "10.0.0.0/8",
        });

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.NotEqual(a, a with { Context = new Dictionary<string, object?> { ["cidr"] = "10.0.0.0/8" } });
        Assert.NotEqual(a, b with { CaveatName = "other" });
    }

    [Fact]
    public void ContextualizedCaveat_compares_parsed_json_context_by_content()
    {
        var a = new ContextualizedCaveat("c", JsonSerializer.Deserialize<Dictionary<string, object?>>("""{"n":1,"l":[1,2]}"""));
        var b = new ContextualizedCaveat("c", JsonSerializer.Deserialize<Dictionary<string, object?>>("""{"l":[1,2],"n":1}"""));
        var c = new ContextualizedCaveat("c", JsonSerializer.Deserialize<Dictionary<string, object?>>("""{"l":[2,1],"n":1}"""));

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.NotEqual(a, c);
    }

    [Fact]
    public void ContextualizedCaveat_without_context_still_equals_itself()
    {
        Assert.Equal(new ContextualizedCaveat("c"), new ContextualizedCaveat("c"));
        // The record documents null and empty alike as "no context".
        Assert.Equal(new ContextualizedCaveat("c"), new ContextualizedCaveat("c", new Dictionary<string, object?>()));
        Assert.Equal(
            new ContextualizedCaveat("c").GetHashCode(),
            new ContextualizedCaveat("c", new Dictionary<string, object?>()).GetHashCode());
        Assert.NotEqual(new ContextualizedCaveat("c"), new ContextualizedCaveat("c", new Dictionary<string, object?> { ["k"] = 1 }));
    }

    [Fact]
    public void CaveatTypeReference_compares_its_child_types_by_content()
    {
        var a = new CaveatTypeReference("map", [new CaveatTypeReference("list", [new CaveatTypeReference("int")])]);
        var b = new CaveatTypeReference("map", [new CaveatTypeReference("list", [new CaveatTypeReference("int")])]);
        var c = new CaveatTypeReference("map", [new CaveatTypeReference("list", [new CaveatTypeReference("string")])]);

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.NotEqual(a, c);
        Assert.NotEqual(new CaveatTypeReference("list"), new CaveatTypeReference("list", []));
    }

    [Fact]
    public void CaveatDefinition_compares_its_expression_and_parameters_by_content()
    {
        CaveatDefinition Make(byte[] expr, string paramType) => new(
            "ip_allowed",
            expr,
            new Dictionary<string, CaveatTypeReference>
            {
                ["cidr"] = new(paramType),
                ["tags"] = new("list", ImmutableList.Create(new CaveatTypeReference("string"))),
            });

        var a = Make([7, 8], "string");
        var b = Make([7, 8], "string");

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.NotEqual(a, Make([7, 9], "string"));
        Assert.NotEqual(a, Make([7, 8], "ipaddress"));
    }

    [Fact]
    public void Relationship_equality_follows_its_caveat_and_integrity_content()
    {
        var resource = new ObjectAndRelation("document", "readme", "viewer");
        var subject = new ObjectAndRelation("user", "alice", CoreConstants.Ellipsis);
        Relationship Make() => Relationship.Create(
            resource,
            subject,
            new ContextualizedCaveat("c", new Dictionary<string, object?> { ["k"] = "v" }),
            integrity: new RelationshipIntegrity("key", [1], DateTimeOffset.UnixEpoch));

        Assert.Equal(Make(), Make());
        Assert.Single(new HashSet<Relationship> { Make(), Make() });
    }

    private static Dictionary<string, object?> NestedContext(object? innerFlag, bool reverseInsertion)
    {
        var nested = new Dictionary<string, object?>
        {
            ["regions"] = new List<object?> { "eu", "us", 7L, null },
            ["flag"] = innerFlag,
        };
        var entries = new List<KeyValuePair<string, object?>>
        {
            new("level", 7L),
            new("name", "alice"),
            new("missing", null),
            new("nested", nested),
            new("tags", new List<object?> { new Dictionary<string, object?> { ["k"] = "v" }, 1L }),
        };
        if (reverseInsertion)
            entries.Reverse();
        return new Dictionary<string, object?>(entries);
    }

    [Fact]
    public void ContextualizedCaveat_compares_null_members_and_maps_inside_lists_by_content()
    {
        var a = new ContextualizedCaveat("c", NestedContext(innerFlag: true, reverseInsertion: false));
        var b = new ContextualizedCaveat("c", NestedContext(innerFlag: true, reverseInsertion: true));

        Assert.Equal(a, b);
        Assert.True(a == b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void ContextualizedCaveat_differing_only_in_a_nested_value_is_not_equal()
    {
        var a = new ContextualizedCaveat("c", NestedContext(innerFlag: true, reverseInsertion: false));
        var b = new ContextualizedCaveat("c", NestedContext(innerFlag: false, reverseInsertion: false));
        var c = new ContextualizedCaveat("c", NestedContext(innerFlag: null, reverseInsertion: false));

        Assert.NotEqual(a, b);
        Assert.False(a == b);
        Assert.NotEqual(a, c);
    }

    [Fact]
    public void CaveatDefinition_equality_ignores_parameter_insertion_order_and_sees_nested_child_types()
    {
        static CaveatDefinition Make(string listElement, bool reverseInsertion)
        {
            var entries = new List<KeyValuePair<string, CaveatTypeReference>>
            {
                new("ip", new CaveatTypeReference("ipaddress")),
                new("allowed", new CaveatTypeReference("list", [new CaveatTypeReference(listElement)])),
                new("limits", new CaveatTypeReference("map", [new CaveatTypeReference("int")])),
            };
            if (reverseInsertion)
                entries.Reverse();
            return new CaveatDefinition("org/ip_allowed", [1, 2, 3, 4], new Dictionary<string, CaveatTypeReference>(entries));
        }

        var a = Make("string", reverseInsertion: false);
        var b = Make("string", reverseInsertion: true);

        Assert.Equal(a, b);
        Assert.True(a == b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.NotEqual(a, Make("int", reverseInsertion: false));
    }
}
