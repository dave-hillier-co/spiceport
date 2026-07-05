using System.Collections.Immutable;
using Spiceport.Core;
using Spiceport.Datastore;
using Spiceport.Engine;
using Spiceport.Schema;

namespace Spiceport.Conformance.Tests;

/// <summary>
/// SpiceDB validationfile loader/server-validation robustness suite, ported from
/// pkg/validationfile/{loader_test.go,fileformat_test.go}. These files carry null validation blocks
/// and empty assertions — they exercise the LOADER and write-time SCHEMA VALIDATION, not Check semantics.
/// Each positive case asserts the loaded, sorted relationship strings; each negative case asserts that
/// load/compile/validate/write fails in the expected category.
/// </summary>
public class ValidationLoaderSuiteTests
{
    private static string Dir => Path.Combine(AppContext.BaseDirectory, "TestData", "LoaderSuite");

    private static string Path_(string name) => Path.Combine(Dir, name);

    // ---------- POSITIVE ----------

    public static IEnumerable<object[]> PositiveSingleFile() =>
    [
        ["loader_no_comment.yaml", new[]
        {
            "example/project:pied_piper#owner@example/user:milburga",
            "example/project:pied_piper#reader@example/user:tarben",
            "example/project:pied_piper#writer@example/user:freyja",
        }],
        ["loader_with_comment.yaml", new[]
        {
            "example/project:pied_piper#owner@example/user:milburga",
            "example/project:pied_piper#reader@example/user:tarben",
            "example/project:pied_piper#writer@example/user:freyja",
        }],
        ["loader_using_schemafile.yaml", new[]
        {
            "example/project:pied_piper#owner@example/user:milburga",
            "example/project:pied_piper#reader@example/user:tarben",
            "example/project:pied_piper#writer@example/user:freyja",
        }],
        ["basic_caveats.yaml", new[]
        {
            "resource:first#reader@user:sarah[some_caveat:{\"somecondition\":42}]",
            "resource:first#reader@user:tom[some_caveat]",
        }],
        ["caveat_order.yaml", new[]
        {
            "resource:first#reader@user:sarah[some_caveat:{\"somecondition\":42}]",
            "resource:first#reader@user:tom[some_caveat]",
        }],
    ];

    [Theory]
    [MemberData(nameof(PositiveSingleFile))]
    public async Task Loads_single_file_with_expected_relationships(string fileName, string[] expected)
    {
        var file = ValidationFileLoader.LoadResolved(Path_(fileName));
        var compiled = SchemaCompiler.CompileSchema(file.SchemaText);
        SchemaTypeValidator.Validate(compiled);
        RelationshipSchemaValidator.ValidateAll(compiled, file.Relationships);

        await WriteAll(file.Relationships);

        var got = file.Relationships
            .Select(TupleStrings.FormatRelationship)
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToArray();
        Assert.Equal(expected.OrderBy(s => s, StringComparer.Ordinal).ToArray(), got);
    }

    [Fact]
    public async Task Loads_multiple_files_merging_relationships()
    {
        var schemaFile = ValidationFileLoader.LoadResolved(Path_("initial_schema_and_rels.yaml"));
        var relsOnly = ValidationFileLoader.LoadResolved(Path_("just_rels.yaml"));

        var merged = schemaFile.Relationships.AddRange(relsOnly.Relationships);
        var compiled = SchemaCompiler.CompileSchema(schemaFile.SchemaText);
        SchemaTypeValidator.Validate(compiled);
        RelationshipSchemaValidator.ValidateAll(compiled, merged);
        await WriteAll(merged);

        string[] expected =
        [
            "example/project:pied_piper#owner@example/user:milburga",
            "example/project:pied_piper#reader@example/user:tarben",
            "example/project:pied_piper#writer@example/user:freyja",
            "example/project:pied_piper#owner@example/user:fred",
            "example/project:pied_piper#reader@example/user:tom",
            "example/project:pied_piper#writer@example/user:sarah",
        ];
        var got = merged.Select(TupleStrings.FormatRelationship)
            .OrderBy(s => s, StringComparer.Ordinal).ToArray();
        Assert.Equal(expected.OrderBy(s => s, StringComparer.Ordinal).ToArray(), got);
    }

    [Fact]
    public async Task Requires_chunking_loads_all_relationships()
    {
        var file = ValidationFileLoader.LoadResolved(Path_("requires_chunking.yaml"));
        Assert.Equal(501, file.Relationships.Count); // user:0 .. user:500 inclusive
        var compiled = SchemaCompiler.CompileSchema(file.SchemaText);
        SchemaTypeValidator.Validate(compiled);
        RelationshipSchemaValidator.ValidateAll(compiled, file.Relationships);
        var rev = await WriteAll(file.Relationships);
        Assert.NotNull(rev);
    }

    // ---------- NEGATIVE ----------

    [Fact]
    public void Just_rels_alone_rejected_missing_definition()
    {
        var file = ValidationFileLoader.LoadResolved(Path_("just_rels.yaml"));
        var compiled = SchemaCompiler.CompileSchema(file.SchemaText); // empty schema compiles
        var ex = Assert.Throws<RelationshipSchemaValidator.RelationshipTypeException>(
            () => RelationshipSchemaValidator.ValidateAll(compiled, file.Relationships));
        Assert.Contains("object definition `example/project` not found", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Schema_and_schemafile_rejected_mutually_exclusive()
    {
        var ex = Assert.Throws<ValidationFileLoadException>(
            () => ValidationFileLoader.LoadResolved(Path_("schema_and_schemafile.yaml")));
        Assert.Contains("only one of schema or schemaFile can be specified", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Invalid_schemafile_rejected_with_schema_parse_error()
    {
        var file = ValidationFileLoader.LoadResolved(Path_("loader_using_invalid_schemafile.yaml"));
        Assert.Throws<SchemaCompileException>(() => SchemaCompiler.CompileSchema(file.SchemaText));
    }

    [Fact]
    public void Non_local_schemafile_rejected()
    {
        var ex = Assert.Throws<ValidationFileLoadException>(
            () => ValidationFileLoader.LoadResolved(Path_("loader_using_non-local_schemafile.yaml")));
        Assert.Contains("is not local", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Missing_schemafile_rejected_file_not_found()
    {
        Assert.Throws<FileNotFoundException>(
            () => ValidationFileLoader.LoadResolved(Path_("loader_using_missing_schemafile.yaml")));
    }

    [Fact]
    public void Legacy_validation_tuples_key_rejected()
    {
        var ex = Assert.Throws<ValidationFileLoadException>(
            () => ValidationFileLoader.LoadResolved(Path_("legacy.yaml")));
        Assert.Contains("relationships must be specified in `relationships`", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Invalid_caveat_rejected_caveat_not_found()
    {
        var file = ValidationFileLoader.LoadResolved(Path_("invalid_caveat.yaml"));
        var compiled = SchemaCompiler.CompileSchema(file.SchemaText);
        var ex = Assert.Throws<SchemaTypeException>(() => SchemaTypeValidator.Validate(compiled));
        Assert.Contains("caveat with name `some_caveat` not found", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Invalid_caveated_rel_rejected_subject_type_not_allowed()
    {
        var file = ValidationFileLoader.LoadResolved(Path_("invalid_caveated_rel.yaml"));
        var compiled = SchemaCompiler.CompileSchema(file.SchemaText);
        SchemaTypeValidator.Validate(compiled);
        var ex = Assert.Throws<RelationshipSchemaValidator.RelationshipTypeException>(
            () => RelationshipSchemaValidator.ValidateAll(compiled, file.Relationships));
        Assert.Contains("are not allowed on relation `resource#reader`", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Invalid_caveated_rel_syntax_rejected_parse_error()
    {
        Assert.Throws<FormatException>(
            () => ValidationFileLoader.LoadResolved(Path_("invalid_caveated_rel_syntax.yaml")));
    }

    [Fact]
    public async Task Repeated_relationship_rejected_on_create()
    {
        var file = ValidationFileLoader.LoadResolved(Path_("repeated_relationship.yaml"));
        var compiled = SchemaCompiler.CompileSchema(file.SchemaText);
        SchemaTypeValidator.Validate(compiled);
        await Assert.ThrowsAsync<CreateRelationshipExistsException>(() => WriteAll(file.Relationships));
    }

    // ---------- shared write helper ----------

    private static async Task<IRevision> WriteAll(ImmutableList<Relationship> relationships)
    {
        var datastore = new ReferenceDatastore();
        if (relationships.Count == 0)
        {
            return (await datastore.HeadRevision()).Revision;
        }

        var updates = relationships
            .Select(r => new RelationshipUpdate(r, UpdateOperation.Create))
            .ToList();
        return await datastore.ReadWriteTx(tx => tx.WriteRelationships(updates));
    }
}
