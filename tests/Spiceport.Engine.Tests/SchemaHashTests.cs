using System.Text;
using Spiceport.Core;
using Spiceport.Schema;

namespace Spiceport.Engine.Tests;

public class SchemaHashTests
{
    private static string SchemaWithCaveat(string parameters, string body) => $$"""
        caveat c({{parameters}}) {
          {{body}}
        }

        definition user {}

        definition document {
          relation viewer: user with c
          permission view = viewer
        }
        """;

    private static string HashOf(string schemaText)
    {
        var compiled = SchemaCompiler.CompileSchema(schemaText);
        return SchemaHash.Compute(compiled.Namespaces, compiled.Caveats);
    }

    [Fact]
    public void Caveat_body_edit_changes_hash()
    {
        var before = HashOf(SchemaWithCaveat("x int", "x > 1"));
        var after = HashOf(SchemaWithCaveat("x int", "x > 100"));

        Assert.NotEqual(before, after);
    }

    [Fact]
    public void Caveat_parameter_type_edit_changes_hash()
    {
        var before = HashOf(SchemaWithCaveat("x int", "x > 1"));
        var after = HashOf(SchemaWithCaveat("x uint", "x > 1"));

        Assert.NotEqual(before, after);
    }

    [Fact]
    public void Caveat_generic_parameter_type_edit_changes_hash()
    {
        var before = HashOf(SchemaWithCaveat("x list<int>", "1 in x"));
        var after = HashOf(SchemaWithCaveat("x list<uint>", "1 in x"));

        Assert.NotEqual(before, after);
    }

    [Fact]
    public void Identical_schema_hashes_identically()
    {
        var text = SchemaWithCaveat("x int, y map<string>", "x > 1");

        Assert.Equal(HashOf(text), HashOf(text));
    }

    [Fact]
    public void Caveat_parameter_order_does_not_change_hash()
    {
        var body = Encoding.UTF8.GetBytes("x > y");
        var forward = new Dictionary<string, CaveatTypeReference>
        {
            ["x"] = new("int"),
            ["y"] = new("list", [new CaveatTypeReference("int")]),
        };
        var reversed = new Dictionary<string, CaveatTypeReference>
        {
            ["y"] = new("list", [new CaveatTypeReference("int")]),
            ["x"] = new("int"),
        };

        var a = SchemaHash.Compute(Array.Empty<NamespaceDefinition>(), [new CaveatDefinition("c", body, forward)]);
        var b = SchemaHash.Compute(Array.Empty<NamespaceDefinition>(), [new CaveatDefinition("c", (byte[])body.Clone(), reversed)]);

        Assert.Equal(a, b);
    }
}
