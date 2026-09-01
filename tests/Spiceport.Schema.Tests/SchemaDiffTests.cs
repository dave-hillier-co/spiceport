using Spiceport.Grains;
using Spiceport.Schema;

namespace Spiceport.Schema.Tests;

public class SchemaDiffTests
{
    [Fact]
    public void ReapplyingUnchangedCaveatSchemaProducesNoParameterTypeChangeDelta()
    {
        const string schema = """
            caveat has_level(levels list<int>) {
              levels.exists(l, l > 0)
            }
            """;

        // Two independent compiles mirror WriteSchema-ing identical text twice: each pass
        // synthesizes its own CaveatTypeReference tree, so the ImmutableList<CaveatTypeReference>
        // child nodes are separate objects with the same shape but different references.
        var first = SchemaCompiler.CompileSchema(schema);
        var second = SchemaCompiler.CompileSchema(schema);

        var deltas = SchemaDiff.Compute(first, second);

        Assert.DoesNotContain(deltas, d => d is SchemaDelta.CaveatParameterTypeChanged);
    }

    [Fact]
    public void GenericCaveatParameterTypeChangeIsStillDetected()
    {
        const string existingSchema = """
            caveat has_level(levels list<int>) {
              levels.exists(l, l > 0)
            }
            """;
        const string nextSchema = """
            caveat has_level(levels list<string>) {
              levels.exists(l, l != "")
            }
            """;

        var existing = SchemaCompiler.CompileSchema(existingSchema);
        var next = SchemaCompiler.CompileSchema(nextSchema);

        var deltas = SchemaDiff.Compute(existing, next);

        Assert.Contains(deltas, d => d is SchemaDelta.CaveatParameterTypeChanged);
    }
}
