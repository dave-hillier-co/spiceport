using System.Collections.Immutable;
using System.Text;
using Spiceport.Core;
using Spiceport.Schema;

namespace Spiceport.Schema.Tests;

public class SchemaCompilerTests
{
    [Fact]
    public void CompilesBasicDocumentSchema()
    {
        const string schema = """
            definition user {}

            definition document {
              relation owner: user
              relation editor: user
              permission read = owner + editor
              permission write = owner
            }
            """;

        var defs = SchemaCompiler.Compile(schema);

        Assert.Equal(2, defs.Count);
        Assert.Equal("user", defs[0].Name);
        Assert.Empty(defs[0].Relations);

        var doc = defs[1];
        Assert.Equal("document", doc.Name);
        Assert.Equal(4, doc.Relations.Count);

        var owner = doc.Relations.Single(r => r.Name == "owner");
        Assert.False(owner.IsPermission);
        AllowedRelation allowed = Assert.Single(owner.TypeInformation!.AllowedDirectRelations);
        Assert.Equal("user", allowed.ObjectType);
        Assert.Equal(CoreConstants.Ellipsis, allowed.RelationName);
        Assert.Equal(AllowedRelationKind.Relation, allowed.Kind);

        var read = doc.Relations.Single(r => r.Name == "read");
        Assert.True(read.IsPermission);
        var op = read.UsersetRewrite!.Operation;
        Assert.Equal(SetOperationType.Union, op.Type);
        Assert.Equal(2, op.Children.Count);
        Assert.Equal(
            new[] { "owner", "editor" },
            op.Children.Select(c => ((SetOperationChild.ComputedUsersetChild)c).Value.Relation).ToArray());

        var write = doc.Relations.Single(r => r.Name == "write");
        var writeOp = write.UsersetRewrite!.Operation;
        Assert.Equal(SetOperationType.Union, writeOp.Type);
        var child = Assert.Single(writeOp.Children);
        Assert.Equal("owner", ((SetOperationChild.ComputedUsersetChild)child).Value.Relation);
    }

    [Fact]
    public void CompilesWildcardAndSubrelationTypeRefs()
    {
        const string schema = """
            definition resource {
              relation viewer: user:* | group#member | user
            }
            """;

        var defs = SchemaCompiler.Compile(schema);
        var viewer = defs.Single().Relations.Single(r => r.Name == "viewer");
        var types = viewer.TypeInformation!.AllowedDirectRelations;

        Assert.Equal(3, types.Count);

        Assert.True(types[0].IsPublicWildcard);
        Assert.Equal("user", types[0].ObjectType);

        Assert.Equal(AllowedRelationKind.Relation, types[1].Kind);
        Assert.Equal("group", types[1].ObjectType);
        Assert.Equal("member", types[1].RelationName);

        Assert.Equal("user", types[2].ObjectType);
        Assert.Equal(CoreConstants.Ellipsis, types[2].RelationName);
    }

    [Fact]
    public void CompilesArrowExpressionToTupleToUserset()
    {
        const string schema = """
            definition folder {
              relation parent: folder
              relation viewer: user
              permission view = viewer + parent->view
            }
            """;

        var defs = SchemaCompiler.Compile(schema);
        var view = defs.Single().Relations.Single(r => r.Name == "view");
        var op = view.UsersetRewrite!.Operation;

        Assert.Equal(SetOperationType.Union, op.Type);
        Assert.Equal(2, op.Children.Count);

        var arrow = Assert.IsType<SetOperationChild.TupleToUsersetChild>(op.Children[1]);
        Assert.Equal("parent", arrow.Value.TuplesetRelation);
        Assert.Equal("view", arrow.Value.ComputedUserset.Relation);
        Assert.Equal(ComputedUsersetObject.TupleUsersetObject, arrow.Value.ComputedUserset.Object);
    }

    [Fact]
    public void RespectsPrecedenceUnionAboveIntersectionAboveExclusion()
    {
        // a + b & c - d  ==>  (((a + b) & c) - d)
        const string schema = """
            definition t {
              permission p = a + b & c - nil
            }
            """;

        var defs = SchemaCompiler.Compile(schema);
        var op = defs.Single().Relations.Single(r => r.Name == "p").UsersetRewrite!.Operation;

        // Top level is exclusion.
        Assert.Equal(SetOperationType.Exclusion, op.Type);
        Assert.Equal(2, op.Children.Count);
        Assert.IsType<SetOperationChild.Nil>(op.Children[1]);

        // Left of exclusion is intersection.
        var intersect = Assert.IsType<SetOperationChild.NestedRewrite>(op.Children[0]).Value.Operation;
        Assert.Equal(SetOperationType.Intersection, intersect.Type);

        // Left of intersection is union(a, b).
        var union = Assert.IsType<SetOperationChild.NestedRewrite>(intersect.Children[0]).Value.Operation;
        Assert.Equal(SetOperationType.Union, union.Type);
        Assert.Equal(
            new[] { "a", "b" },
            union.Children.Select(c => ((SetOperationChild.ComputedUsersetChild)c).Value.Relation).ToArray());
    }

    [Fact]
    public void FlattensAssociativeUnion()
    {
        const string schema = """
            definition t {
              permission p = a + b + c
            }
            """;

        var op = SchemaCompiler.Compile(schema).Single()
            .Relations.Single(r => r.Name == "p").UsersetRewrite!.Operation;

        Assert.Equal(SetOperationType.Union, op.Type);
        Assert.Equal(
            new[] { "a", "b", "c" },
            op.Children.Select(c => ((SetOperationChild.ComputedUsersetChild)c).Value.Relation).ToArray());
    }

    [Fact]
    public void ParsesCaveatBlockWithoutError()
    {
        const string schema = """
            caveat ip_allowlist(user_ip ipaddress, cidr string) {
              user_ip.in_cidr(cidr)
            }

            definition resource {
              relation viewer: user with ip_allowlist
            }
            """;

        var defs = SchemaCompiler.Compile(schema);
        var viewer = defs.Single().Relations.Single(r => r.Name == "viewer");
        var allowed = Assert.Single(viewer.TypeInformation!.AllowedDirectRelations);
        Assert.Equal("ip_allowlist", allowed.RequiredCaveat!.CaveatName);
    }

    [Fact]
    public void CompilesCaveatBlockIntoCaveatDefinition()
    {
        const string schema = """
            caveat ip_allowlist(user_ip ipaddress, allowed_ips list<string>) {
              allowed_ips.exists(cidr, user_ip.in_cidr(cidr))
            }

            definition resource {
              relation viewer: user with ip_allowlist
            }
            """;

        var compiled = SchemaCompiler.CompileSchema(schema);

        Assert.Single(compiled.Namespaces);
        var caveat = Assert.Single(compiled.Caveats);

        Assert.Equal("ip_allowlist", caveat.Name);

        // The raw CEL expression is captured verbatim (UTF-8), not evaluated.
        Assert.Equal(
            "allowed_ips.exists(cidr, user_ip.in_cidr(cidr))", Encoding.UTF8.GetString(caveat.SerializedExpression));

        Assert.Equal(2, caveat.ParameterTypes.Count);
        Assert.Equal("ipaddress", caveat.ParameterTypes["user_ip"].TypeName);
        Assert.Null(caveat.ParameterTypes["user_ip"].ChildTypes);

        var listType = caveat.ParameterTypes["allowed_ips"];
        Assert.Equal("list", listType.TypeName);
        var child = Assert.Single(listType.ChildTypes!);
        Assert.Equal("string", child.TypeName);

        // The caveat name is also attached to the referencing relation.
        var viewer = compiled.Namespaces.Single().Relations.Single(r => r.Name == "viewer");
        var allowed = Assert.Single(viewer.TypeInformation!.AllowedDirectRelations);
        Assert.Equal("ip_allowlist", allowed.RequiredCaveat!.CaveatName);
    }

    [Fact]
    public void CompilesWithExpirationRelation()
    {
        const string schema = """
            use expiration

            definition document {
              relation viewer: user with expiration
              relation editor: user with some_caveat and expiration
            }
            """;

        var compiled = SchemaCompiler.CompileSchema(schema);
        var doc = Assert.Single(compiled.Namespaces);
        Assert.Empty(compiled.Caveats);

        var viewer = doc.Relations.Single(r => r.Name == "viewer");
        var viewerAllowed = Assert.Single(viewer.TypeInformation!.AllowedDirectRelations);
        Assert.True(viewerAllowed.RequiresExpiration);
        Assert.Null(viewerAllowed.RequiredCaveat);

        var editor = doc.Relations.Single(r => r.Name == "editor");
        var editorAllowed = Assert.Single(editor.TypeInformation!.AllowedDirectRelations);
        Assert.True(editorAllowed.RequiresExpiration);
        Assert.Equal("some_caveat", editorAllowed.RequiredCaveat!.CaveatName);
    }

    [Fact]
    public void CompileStillReturnsNamespacesOnly()
    {
        const string schema = """
            caveat c(x int) { x > 0 }
            definition user {}
            """;

        var namespaces = SchemaCompiler.Compile(schema);
        Assert.Equal("user", Assert.Single(namespaces).Name);
    }

    [Fact]
    public void ThrowsOnMalformedSchema()
    {
        const string schema = "definition document { relation owner }";
        var ex = Assert.Throws<SchemaCompileException>(() => SchemaCompiler.Compile(schema));
        Assert.True(ex.Line > 0);
    }

    [Fact]
    public void CompilesSelfOperandUnderUseSelf()
    {
        const string schema = """
            use self

            definition user {}

            definition document {
              relation viewer: user
              permission view = self + viewer
            }
            """;

        var doc = SchemaCompiler.Compile(schema).Single(d => d.Name == "document");
        var view = doc.Relations.Single(r => r.Name == "view");
        var op = view.UsersetRewrite!.Operation;
        Assert.Equal(SetOperationType.Union, op.Type);
        Assert.Contains(op.Children, c => c is SetOperationChild.Self);
    }

    [Fact]
    public void SelfWithoutFlagIsAnOrdinaryReference()
    {
        const string schema = """
            definition document {
              permission view = self
            }
            """;

        var doc = Assert.Single(SchemaCompiler.Compile(schema));
        var view = doc.Relations.Single(r => r.Name == "view");
        var child = Assert.Single(view.UsersetRewrite!.Operation.Children);
        var computed = Assert.IsType<SetOperationChild.ComputedUsersetChild>(child);
        Assert.Equal("self", computed.Value.Relation);
    }

    [Fact]
    public void RejectsWithExpirationWithoutUseFlag()
    {
        const string schema = """
            definition user {}

            definition document {
              relation viewer: user with expiration
            }
            """;

        // Without `use expiration`, `expiration` stays an identifier and is parsed as a caveat
        // name, which fails caveat-existence validation rather than silently enabling expiration.
        var doc = SchemaCompiler.Compile(schema).Single(d => d.Name == "document");
        var viewer = doc.Relations.Single(r => r.Name == "viewer");
        var allowed = Assert.Single(viewer.TypeInformation!.AllowedDirectRelations);
        Assert.False(allowed.RequiresExpiration);
        Assert.Equal("expiration", allowed.RequiredCaveat!.CaveatName);
    }

    [Fact]
    public void RejectsUseFlagAfterDefinition()
    {
        const string schema = """
            definition user {}
            use expiration
            """;

        Assert.Throws<SchemaCompileException>(() => SchemaCompiler.Compile(schema));
    }

    [Fact]
    public void RejectsRepeatedWith()
    {
        const string schema = """
            definition document {
              relation viewer: user with cav1 with cav2
            }
            """;

        Assert.Throws<SchemaCompileException>(() => SchemaCompiler.Compile(schema));
    }

    [Fact]
    public void RejectsNestedArrows()
    {
        const string schema = """
            definition document {
              permission view = a->b->c
            }
            """;

        var ex = Assert.Throws<SchemaCompileException>(() => SchemaCompiler.Compile(schema));
        Assert.Contains("Nested arrows not yet supported", ex.Message);
    }

    [Fact]
    public void RejectsUnknownUseFlag()
    {
        const string schema = "use bogus\ndefinition user {}";
        Assert.Throws<SchemaCompileException>(() => SchemaCompiler.Compile(schema));
    }

    [Fact]
    public void RejectsACaveatWhoseExpressionDoesNotTypeCheck()
    {
        // SpiceDB's checker rejects a non-boolean operand of `||` at WriteSchema. The CEL part of the
        // message is upstream's verbatim, positioned in the schema as upstream positions it.
        var ex = Assert.Throws<SchemaCompileException>(() =>
            SchemaCompiler.CompileSchema("caveat c(x int, y int) { x == 1 || y }"));
        Assert.Equal(
            "schema error at line 1, column 26: invalid expression for caveat `c`: "
            + "ERROR: c:1:36: expected type 'bool' but found 'int'\n | x == 1 || y\n | ...........^",
            ex.Message);
    }

    [Fact]
    public void RejectsACaveatWithANonBooleanExpression()
    {
        var ex = Assert.Throws<SchemaCompileException>(() =>
            SchemaCompiler.CompileSchema("caveat c(x int) {\n  x == 1 ? 'a' : 'b'\n}"));
        Assert.Equal(
            "schema error at line 2, column 3: invalid expression for caveat `c`: "
            + "caveat expression must result in a boolean value: found `string`",
            ex.Message);
    }

    [Theory]
    // SpiceDB slices the body from its first token to its last; a newline after an identifier, `)`, `}`
    // or `*` is a token (a synthetic semicolon), so the CEL source then has a second, empty line and the
    // relocated error quotes that empty line.
    [InlineData("caveat c(x int) {\n  x.y == 1\n}",
        "schema error at line 2, column 3: invalid expression for caveat `c`: "
        + "ERROR: c:2:4: type 'int' does not support field selection\n | \n | ^")]
    [InlineData("caveat c(x int) {\n  x.y == \"a\"\n}",
        "schema error at line 2, column 3: invalid expression for caveat `c`: "
        + "ERROR: c:2:4: type 'int' does not support field selection")]
    [InlineData("caveat c(x int) { /* lead */ x.y == 1 /* trail */ }",
        "schema error at line 1, column 30: invalid expression for caveat `c`: "
        + "ERROR: c:1:31: type 'int' does not support field selection\n | x.y == 1\n | ........^")]
    [InlineData("caveat c(x int) {\n  x.y\n  == 1\n}",
        "schema error at line 2, column 3: invalid expression for caveat `c`: "
        + "ERROR: c:2:4: type 'int' does not support field selection\n |   == 1\n | ...^")]
    public void PositionsCaveatErrorsAsSpiceDbDoes(string schema, string expected) =>
        Assert.Equal(expected, Assert.Throws<SchemaCompileException>(() => SchemaCompiler.CompileSchema(schema)).Message);

    [Theory]
    [InlineData("caveat c(x foo) { x }",
        "schema error at line 1, column 10: invalid type for caveat parameter `x` on caveat `c`: unknown type `foo`")]
    [InlineData("caveat c(x int, y list) { x == 1 }",
        "schema error at line 1, column 17: invalid type for caveat parameter `y` on caveat `c`: type `list` requires 1 generic types; found 0")]
    [InlineData("caveat c(x int, x int) { x == 1 }",
        "schema error at line 1, column 17: duplicate parameter `x` defined on caveat `c`")]
    public void RejectsInvalidCaveatParameters(string schema, string expected) =>
        Assert.Equal(expected, Assert.Throws<SchemaCompileException>(() => SchemaCompiler.CompileSchema(schema)).Message);

    [Fact]
    public void CompilesAStoredSchemaWithoutRecompilingItsCaveats()
    {
        // A stored schema was compiled when it was written; reloading it must not fail because the
        // caveat checks have since grown stricter.
        var compiled = SchemaCompiler.CompileStoredSchema("caveat c(x int, y int) { x == 1 || y }");

        var caveat = Assert.Single(compiled.Caveats);
        Assert.Equal("x == 1 || y", Encoding.UTF8.GetString(caveat.SerializedExpression));
    }
}
