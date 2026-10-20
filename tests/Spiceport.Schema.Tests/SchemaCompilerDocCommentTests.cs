using Spiceport.Core;
using Spiceport.Schema;

namespace Spiceport.Schema.Tests;

/// <summary>
/// Doc comments: SpiceDB's parser attaches every comment lexed since the previous significant token to
/// the next token (pkg/schemadsl/parser/parser_impl.go consumeToken), and the translator turns the ones on
/// a definition, relation, permission or caveat node into DocComment metadata, each comment verbatim with
/// its markers and every line whitespace-trimmed (pkg/schemadsl/compiler/translator.go addComments /
/// normalizeComment). A newline after an identifier, keyword, <c>}</c>, <c>)</c> or <c>*</c> lexes as a
/// synthetic semicolon, which takes any comments before it with it, so a trailing comment on a statement
/// line never reaches the next statement.
/// </summary>
public class SchemaCompilerDocCommentTests
{
    private static CompiledSchema Compile(string schema) => SchemaCompiler.CompileSchema(schema);

    private static NamespaceDefinition Def(CompiledSchema compiled, string name) =>
        compiled.Namespaces.Single(n => n.Name == name);

    private static Relation Rel(NamespaceDefinition def, string name) => def.Relations.Single(r => r.Name == name);

    [Fact]
    public void Leading_line_comments_attach_to_the_definition_in_order_across_blank_lines()
    {
        var compiled = Compile("// def doc\n\n// second\ndefinition user {}\n");

        Assert.Equal(["// def doc", "// second"], Def(compiled, "user").Comments);
    }

    [Fact]
    public void A_block_comment_keeps_its_markers_and_has_every_line_trimmed()
    {
        var compiled = Compile("/**\n   * Line one\n   *   indented\n   */\ndefinition user {}\n");

        Assert.Equal(["/**\n* Line one\n*   indented\n*/"], Def(compiled, "user").Comments);
    }

    [Fact]
    public void Line_comment_padding_is_trimmed_and_blank_block_lines_survive()
    {
        var compiled = Compile("\t//   padded   \n\t/*  a  \n\n   b */\ndefinition user {}\n");

        Assert.Equal(["//   padded", "/*  a\n\nb */"], Def(compiled, "user").Comments);
    }

    [Fact]
    public void Relation_and_permission_comments_attach_to_their_own_declarations()
    {
        var compiled = Compile("""
            /* d */ definition user {}
            definition document { // after brace
              // viewer doc
              relation viewer: user // trailing
              relation editor: user; // after semi
              relation owner: user
              /* perm */
              permission view = viewer + /* inner */ editor
              // dangling
            }
            // after def
            definition other {} // trailing brace
            definition third {}
            """);

        Assert.Equal(["/* d */"], Def(compiled, "user").Comments);

        var document = Def(compiled, "document");
        Assert.Empty(document.Comments);
        Assert.Equal(["// after brace", "// viewer doc"], Rel(document, "viewer").Comments);
        Assert.Empty(Rel(document, "editor").Comments);
        Assert.Equal(["// after semi"], Rel(document, "owner").Comments);
        Assert.Equal(["/* perm */"], Rel(document, "view").Comments);

        Assert.Equal(["// after def"], Def(compiled, "other").Comments);
        Assert.Empty(Def(compiled, "third").Comments);
    }

    [Fact]
    public void A_trailing_comment_after_a_wildcard_or_paren_is_dropped()
    {
        var compiled = Compile("""
            definition user {}
            definition document {
              relation viewer: user:* // star trailing
              relation editor: user
              permission view = (viewer) // paren trailing
              permission edit = editor
            }
            """);

        var document = Def(compiled, "document");
        Assert.Empty(Rel(document, "editor").Comments);
        Assert.Empty(Rel(document, "edit").Comments);
    }

    [Fact]
    public void Caveat_comments_attach_to_the_caveat_and_body_comments_do_not_leak()
    {
        var compiled = Compile("// cav doc\ncaveat c(x int) {\n  x > 1 // in body\n}\ndefinition user {}\n");

        Assert.Equal(["// cav doc"], Assert.Single(compiled.Caveats).Comments);
        Assert.Empty(Def(compiled, "user").Comments);
    }

    [Fact]
    public void A_line_comment_stops_before_a_carriage_return()
    {
        var compiled = Compile("// crlf doc  \r\ndefinition user {}\r\n");

        Assert.Equal(["// crlf doc"], Def(compiled, "user").Comments);
    }

    [Fact]
    public void A_schema_without_comments_compiles_to_empty_comment_lists()
    {
        var compiled = Compile("caveat c(x int) { x > 1 }\ndefinition user { relation self_rel: user }\n");

        Assert.Empty(Def(compiled, "user").Comments);
        Assert.Empty(Rel(Def(compiled, "user"), "self_rel").Comments);
        Assert.Empty(Assert.Single(compiled.Caveats).Comments);
    }

    [Fact]
    public void Comments_take_no_part_in_caveat_equality()
    {
        var plain = Assert.Single(Compile("caveat c(x int) { x > 1 }").Caveats);
        var commented = Assert.Single(Compile("// doc\ncaveat c(x int) { x > 1 }").Caveats);

        Assert.Equal(plain, commented);
        Assert.Equal(plain.GetHashCode(), commented.GetHashCode());
    }
}
