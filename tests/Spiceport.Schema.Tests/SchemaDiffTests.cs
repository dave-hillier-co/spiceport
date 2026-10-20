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

    private const string UncommentedDocSchema = """
        caveat ip_match(allowed string) {
          allowed == "x"
        }
        definition user {}
        definition document {
          relation viewer: user
          permission view = viewer
        }
        """;

    private const string CommentedDocSchema = """
        // caveat doc
        caveat ip_match(allowed string) {
          allowed == "x"
        }
        definition user {}
        // document doc
        definition document {
          // viewer doc
          relation viewer: user
          // view doc
          permission view = viewer
        }
        """;

    [Fact]
    public void DocCommentChangesAreReportedPerDeclarationAgainstTheComparisonSchema()
    {
        // SpiceDB's pkg/diff/namespace and pkg/diff/caveats compare GetComments on kept definitions,
        // relations, permissions and caveats, and DiffSchema surfaces each as a *DocCommentChanged case.
        var existing = SchemaCompiler.CompileSchema(UncommentedDocSchema);
        var next = SchemaCompiler.CompileSchema(CommentedDocSchema);

        var deltas = SchemaDiff.Compute(existing, next);

        Assert.Equal(4, deltas.Count);
        var def = Assert.Single(deltas.OfType<SchemaDelta.DefinitionDocCommentChanged>());
        Assert.Equal(["// document doc"], def.Definition.Comments);
        var rel = Assert.Single(deltas.OfType<SchemaDelta.RelationDocCommentChanged>());
        Assert.Equal(("document", "viewer"), (rel.DefinitionName, rel.Relation.Name));
        Assert.Equal(["// viewer doc"], rel.Relation.Comments);
        var perm = Assert.Single(deltas.OfType<SchemaDelta.PermissionDocCommentChanged>());
        Assert.Equal(("document", "view"), (perm.DefinitionName, perm.Permission.Name));
        Assert.Equal(["// view doc"], perm.Permission.Comments);
        var caveat = Assert.Single(deltas.OfType<SchemaDelta.CaveatDocCommentChanged>());
        Assert.Equal(["// caveat doc"], caveat.Caveat.Comments);
    }

    [Fact]
    public void UnchangedDocCommentsYieldNoDeltas()
    {
        var first = SchemaCompiler.CompileSchema(CommentedDocSchema);
        var second = SchemaCompiler.CompileSchema(CommentedDocSchema);

        Assert.Empty(SchemaDiff.Compute(first, second));
    }

    [Fact]
    public void DocCommentChangesNeverReachTheOrphanCheckRemovalSubset()
    {
        var existing = SchemaCompiler.CompileSchema(CommentedDocSchema);
        var next = SchemaCompiler.CompileSchema(UncommentedDocSchema);

        var deltas = SchemaDiff.Compute(existing, next);

        Assert.All(deltas, d => Assert.True(
            d is SchemaDelta.DefinitionDocCommentChanged or SchemaDelta.RelationDocCommentChanged
                or SchemaDelta.PermissionDocCommentChanged or SchemaDelta.CaveatDocCommentChanged,
            $"unexpected delta {d}"));
    }
}
