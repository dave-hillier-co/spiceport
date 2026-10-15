namespace Spiceport.Core.Tests;

public class RecordEqualityTests
{
    private static CaveatDefinition Caveat(byte[] expression, bool reverseInsertion)
    {
        var entries = new List<KeyValuePair<string, CaveatTypeReference>>
        {
            new("ip", new CaveatTypeReference("ipaddress")),
            new("allowed", new CaveatTypeReference("list", [new CaveatTypeReference("string")])),
            new("limits", new CaveatTypeReference("map", [new CaveatTypeReference("int")])),
        };
        if (reverseInsertion)
        {
            entries.Reverse();
        }

        var parameters = new Dictionary<string, CaveatTypeReference>();
        foreach (var (key, value) in entries)
        {
            parameters.Add(key, value);
        }

        return new CaveatDefinition("org/ip_allowed", expression, parameters);
    }

    [Fact]
    public void CaveatDefinition_with_equal_content_in_distinct_arrays_and_dictionaries_is_equal()
    {
        var a = Caveat([1, 2, 3, 4], reverseInsertion: false);
        var b = Caveat([1, 2, 3, 4], reverseInsertion: true);

        Assert.Equal(a, b);
        Assert.True(a == b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void CaveatDefinition_differing_in_one_expression_byte_is_not_equal()
    {
        var a = Caveat([1, 2, 3, 4], reverseInsertion: false);
        var b = Caveat([1, 2, 3, 5], reverseInsertion: false);

        Assert.NotEqual(a, b);
        Assert.False(a == b);
    }

    [Fact]
    public void CaveatDefinition_differing_in_a_parameter_type_is_not_equal()
    {
        var a = Caveat([1, 2, 3, 4], reverseInsertion: false);
        var parameters = new Dictionary<string, CaveatTypeReference>(a.ParameterTypes)
        {
            ["allowed"] = new CaveatTypeReference("list", [new CaveatTypeReference("int")]),
        };
        var b = a with { ParameterTypes = parameters };

        Assert.NotEqual(a, b);
    }

    [Fact]
    public void CaveatTypeReference_with_equal_child_types_in_distinct_lists_is_equal()
    {
        var a = new CaveatTypeReference("map", [new CaveatTypeReference("list", [new CaveatTypeReference("string")])]);
        var b = new CaveatTypeReference("map", [new CaveatTypeReference("list", [new CaveatTypeReference("string")])]);
        var c = new CaveatTypeReference("map", [new CaveatTypeReference("list", [new CaveatTypeReference("int")])]);

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.NotEqual(a, c);
    }

    [Fact]
    public void RelationshipIntegrity_with_equal_hash_bytes_is_equal()
    {
        var at = new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero);
        var a = new RelationshipIntegrity("key1", [0xde, 0xad, 0xbe, 0xef], at);
        var b = new RelationshipIntegrity("key1", [0xde, 0xad, 0xbe, 0xef], at);
        var c = new RelationshipIntegrity("key1", [0xde, 0xad, 0xbe, 0xee], at);

        Assert.Equal(a, b);
        Assert.True(a == b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.NotEqual(a, c);
    }

    private static Dictionary<string, object?> Context(object? innerFlag, bool reverseInsertion)
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
            new("active", true),
            new("missing", null),
            new("nested", nested),
            new("tags", new List<object?> { new Dictionary<string, object?> { ["k"] = "v" }, 1L }),
        };
        if (reverseInsertion)
        {
            entries.Reverse();
        }

        var context = new Dictionary<string, object?>();
        foreach (var (key, value) in entries)
        {
            context.Add(key, value);
        }

        return context;
    }

    [Fact]
    public void ContextualizedCaveat_with_equal_nested_context_is_equal()
    {
        var a = new ContextualizedCaveat("is_active", Context(innerFlag: true, reverseInsertion: false));
        var b = new ContextualizedCaveat("is_active", Context(innerFlag: true, reverseInsertion: true));

        Assert.Equal(a, b);
        Assert.True(a == b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    [Fact]
    public void ContextualizedCaveat_differing_in_a_nested_value_is_not_equal()
    {
        var a = new ContextualizedCaveat("is_active", Context(innerFlag: true, reverseInsertion: false));
        var b = new ContextualizedCaveat("is_active", Context(innerFlag: false, reverseInsertion: false));

        Assert.NotEqual(a, b);
        Assert.False(a == b);
    }

    [Fact]
    public void ContextualizedCaveat_with_null_and_empty_context_is_equal()
    {
        var a = new ContextualizedCaveat("is_active");
        var b = new ContextualizedCaveat("is_active", new Dictionary<string, object?>());
        var c = new ContextualizedCaveat("is_active", new Dictionary<string, object?> { ["level"] = 1L });

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.NotEqual(a, c);
        Assert.NotEqual(new ContextualizedCaveat("other"), a);
    }
}
