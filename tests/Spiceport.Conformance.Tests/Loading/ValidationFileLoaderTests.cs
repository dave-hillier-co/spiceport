using Spiceport.Core;

namespace Spiceport.Conformance.Tests;

public class ValidationFileLoaderTests
{
    private static string DataPath(string fileName) =>
        Path.Combine(AppContext.BaseDirectory, "TestData", fileName);

    [Theory]
    [InlineData("basicrbac.yaml")]
    [InlineData("indirectgroups.yaml")]
    [InlineData("simplewildcard.yaml")]
    public void Parses_sample_file_without_error(string fileName)
    {
        var file = ValidationFileLoader.LoadFromFile(DataPath(fileName));

        Assert.NotEmpty(file.SchemaText);
        Assert.NotEmpty(file.Relationships);
        Assert.NotEmpty(file.Assertions);
    }

    [Fact]
    public void Parses_basicrbac_relationships_and_assertions()
    {
        var file = ValidationFileLoader.LoadFromFile(DataPath("basicrbac.yaml"));

        Assert.Contains("definition example/document", file.SchemaText);
        Assert.Equal(3, file.Relationships.Count);

        var rel = file.Relationships[0];
        Assert.Equal("example/document", rel.Resource.ObjectType);
        Assert.Equal("firstdoc", rel.Resource.ObjectId);
        Assert.Equal("writer", rel.Resource.Relation);
        Assert.Equal("example/user", rel.Subject.ObjectType);
        Assert.Equal("tom", rel.Subject.ObjectId);

        Assert.Equal(6, file.Assertions.Count);
        Assert.Equal(4, file.Assertions.Count(a => a.Expected));
        Assert.Equal(2, file.Assertions.Count(a => !a.Expected));
    }

    [Fact]
    public void Parses_typed_assertion_resource_relation_subject()
    {
        var file = ValidationFileLoader.LoadFromFile(DataPath("basicrbac.yaml"));

        var trueAssertion = file.Assertions.First(a => a.Expected);
        Assert.Equal("example/document", trueAssertion.Resource.ObjectType);
        Assert.Equal("write", trueAssertion.Resource.Relation);
        Assert.Equal("example/user", trueAssertion.Subject.ObjectType);
        Assert.Equal("tom", trueAssertion.Subject.ObjectId);
        Assert.Equal(AssertionExpectation.True, trueAssertion.Expectation);
    }

    [Fact]
    public void Ellipsis_subject_relation_normalised()
    {
        var file = ValidationFileLoader.LoadFromFile(DataPath("indirectgroups.yaml"));

        var assertion = file.Assertions[0];
        Assert.Equal(CoreConstants.Ellipsis, assertion.Subject.Relation);
    }

    [Fact]
    public void Wildcard_subject_parsed()
    {
        var file = ValidationFileLoader.LoadFromFile(DataPath("simplewildcard.yaml"));

        var wildcard = file.Relationships.First(r => r.Subject.IsPublicWildcard);
        Assert.Equal(CoreConstants.PublicWildcard, wildcard.Subject.ObjectId);
        Assert.Equal("test/user", wildcard.Subject.ObjectType);
    }

    [Fact]
    public void Parses_assertion_with_caveat_context()
    {
        const string yaml = """
            schema: |
              definition user {}
            relationships: |
              doc:d#viewer@user:tom
            assertions:
              assertTrue:
                - 'doc:d#view@user:tom with {"now": "2023-01-01T00:00:00Z", "count": 42}'
            """;

        var file = ValidationFileLoader.Parse(yaml);

        var assertion = Assert.Single(file.Assertions);
        Assert.NotNull(assertion.CaveatContext);
        Assert.Equal("2023-01-01T00:00:00Z", assertion.CaveatContext!["now"]);
        Assert.Equal(42L, Convert.ToInt64(assertion.CaveatContext!["count"]));
        Assert.Equal("doc", assertion.Resource.ObjectType);
        Assert.Equal("view", assertion.Resource.Relation);
    }

    [Fact]
    public void Parses_caveat_relationship_and_caveated_assertion()
    {
        const string yaml = """
            schema: |+
              use expiration
              caveat somecaveat(somecondition int) {
                somecondition == 42
              }
              definition user {}
              definition document {
                relation viewer: user with somecaveat and expiration
                permission view = viewer
              }
            relationships: |
              document:firstdoc#viewer@user:sarah[somecaveat:{"somecondition":42}][expiration:2300-12-01T00:00:00Z]
              document:firstdoc#viewer@user:fred[somecaveat][expiration:2300-12-01T00:00:00Z]
            assertions:
              assertTrue:
                - 'document:firstdoc#view@user:fred with {"somecondition": 42}'
              assertCaveated:
                - "document:firstdoc#view@user:fred"
              assertFalse:
                - "document:firstdoc#view@user:tom"
            """;

        var file = ValidationFileLoader.Parse(yaml);

        // Relationship: caveat name + JSON context + expiration all populated.
        var sarah = file.Relationships.First(r => r.Subject.ObjectId == "sarah");
        Assert.NotNull(sarah.OptionalCaveat);
        Assert.Equal("somecaveat", sarah.OptionalCaveat!.CaveatName);
        Assert.NotNull(sarah.OptionalCaveat.Context);
        // Relationship caveat context is deserialised lazily as JsonElement by the core parser.
        Assert.Equal("42", sarah.OptionalCaveat.Context!["somecondition"]!.ToString());
        Assert.NotNull(sarah.OptionalExpiration);
        Assert.Equal(2300, sarah.OptionalExpiration!.Value.Year);

        // Relationship: caveat name, no context (context provided at check time).
        var fred = file.Relationships.First(r => r.Subject.ObjectId == "fred");
        Assert.NotNull(fred.OptionalCaveat);
        Assert.Equal("somecaveat", fred.OptionalCaveat!.CaveatName);
        Assert.Null(fred.OptionalCaveat.Context);

        // assertTrue with " with {json}" context.
        var trueAssertion = file.Assertions.First(a => a.Expectation == AssertionExpectation.True);
        Assert.NotNull(trueAssertion.CaveatContext);
        Assert.Equal(42L, Convert.ToInt64(trueAssertion.CaveatContext!["somecondition"]));

        // assertCaveated maps to the Caveated outcome / Membership.Caveated.
        var caveated = file.Assertions.First(a => a.Expectation == AssertionExpectation.Caveated);
        Assert.Equal(Spiceport.Engine.Membership.Caveated, caveated.ExpectedMembership);
        Assert.Equal("fred", caveated.Subject.ObjectId);

        // assertFalse maps to NotMember.
        var falseAssertion = file.Assertions.First(a => a.Expectation == AssertionExpectation.False);
        Assert.Equal(Spiceport.Engine.Membership.NotMember, falseAssertion.ExpectedMembership);
    }

    // ---- validation: block parsing (mirrors SpiceDB's pkg/validationfile/blocks grammar) ----

    private static ValidationFile ParseWithValidation(string validationYaml)
    {
        var yaml = $$"""
            schema: |
              definition user {}
              definition document {
                relation viewer: user
                permission view = viewer
              }
            relationships: |
              document:firstdoc#viewer@user:tom
            validation:
            {{validationYaml}}
            """;
        return ValidationFileLoader.Parse(yaml);
    }

    [Fact]
    public void Validation_block_absent_yields_no_entries()
    {
        var file = ValidationFileLoader.Parse("""
            schema: |
              definition user {}
            relationships: ""
            """);

        Assert.Empty(file.Validations);
    }

    [Fact]
    public void Validation_entry_key_parses_as_object_and_relation()
    {
        var file = ParseWithValidation("""
              document:firstdoc#view:
              - "[user:tom] is <document:firstdoc#viewer>"
            """);

        var entry = Assert.Single(file.Validations);
        Assert.Equal("document", entry.ObjectAndRelation.ObjectType);
        Assert.Equal("firstdoc", entry.ObjectAndRelation.ObjectId);
        Assert.Equal("view", entry.ObjectAndRelation.Relation);
    }

    [Fact]
    public void Terminal_subject_without_ellipsis_normalises_to_ellipsis()
    {
        var file = ParseWithValidation("""
              document:firstdoc#view:
              - "[user:tom] is <document:firstdoc#viewer>"
            """);

        var subject = Assert.Single(Assert.Single(file.Validations).ExpectedSubjects);
        Assert.Equal("user", subject.Subject.ObjectType);
        Assert.Equal("tom", subject.Subject.ObjectId);
        Assert.Equal(CoreConstants.Ellipsis, subject.Subject.Relation);
        Assert.False(subject.IsCaveated);
        Assert.Empty(subject.Exceptions);
    }

    [Fact]
    public void Subject_with_explicit_ellipsis_parses_same_as_bare_id()
    {
        var file = ParseWithValidation("""
              document:firstdoc#view:
              - "[user:tom#...] is <document:firstdoc#viewer>"
            """);

        var subject = Assert.Single(Assert.Single(file.Validations).ExpectedSubjects);
        Assert.Equal("tom", subject.Subject.ObjectId);
        Assert.Equal(CoreConstants.Ellipsis, subject.Subject.Relation);
    }

    [Fact]
    public void Subject_with_relation_parses_as_subject_relation_not_ellipsis()
    {
        var file = ParseWithValidation("""
              document:firstdoc#view:
              - "[group:eng#member] is <document:firstdoc#viewer>"
            """);

        var subject = Assert.Single(Assert.Single(file.Validations).ExpectedSubjects);
        Assert.Equal("group", subject.Subject.ObjectType);
        Assert.Equal("eng", subject.Subject.ObjectId);
        Assert.Equal("member", subject.Subject.Relation);
    }

    [Fact]
    public void Wildcard_subject_parses_with_public_wildcard_id()
    {
        var file = ParseWithValidation("""
              document:firstdoc#view:
              - "[user:*] is <document:firstdoc#viewer>"
            """);

        var subject = Assert.Single(Assert.Single(file.Validations).ExpectedSubjects);
        Assert.True(subject.Subject.IsPublicWildcard);
        Assert.False(subject.IsCaveated);
    }

    [Fact]
    public void Caveated_subject_marked_via_bracket_ellipsis_suffix()
    {
        var file = ParseWithValidation("""
              document:firstdoc#view:
              - "[user:tom[...]] is <document:firstdoc#viewer>"
            """);

        var subject = Assert.Single(Assert.Single(file.Validations).ExpectedSubjects);
        Assert.Equal("tom", subject.Subject.ObjectId);
        Assert.True(subject.IsCaveated);
    }

    [Fact]
    public void Wildcard_with_exceptions_parses_excluded_subjects_with_own_caveat_flags()
    {
        var file = ParseWithValidation("""
              document:firstdoc#view:
              - "[user:*[...] - {user:a, user:b[...]}] is <document:firstdoc#viewer>"
            """);

        var subject = Assert.Single(Assert.Single(file.Validations).ExpectedSubjects);
        Assert.True(subject.Subject.IsPublicWildcard);
        Assert.True(subject.IsCaveated);
        Assert.Equal(2, subject.Exceptions.Count);

        var a = subject.Exceptions.Single(e => e.Subject.ObjectId == "a");
        Assert.False(a.IsCaveated);
        var b = subject.Exceptions.Single(e => e.Subject.ObjectId == "b");
        Assert.True(b.IsCaveated);
    }

    [Fact]
    public void Multiple_resource_path_suffix_is_ignored_only_subject_is_asserted()
    {
        var file = ParseWithValidation("""
              document:firstdoc#view:
              - "[user:tom] is <document:firstdoc#viewer>/<document:firstdoc#builder>"
            """);

        var subject = Assert.Single(Assert.Single(file.Validations).ExpectedSubjects);
        Assert.Equal("tom", subject.Subject.ObjectId);
    }

    [Fact]
    public void Multiple_expected_subjects_for_same_entry_all_parsed()
    {
        var file = ParseWithValidation("""
              document:firstdoc#view:
              - "[user:tom] is <document:firstdoc#viewer>"
              - "[user:fred] is <document:firstdoc#viewer>"
            """);

        var entry = Assert.Single(file.Validations);
        Assert.Equal(2, entry.ExpectedSubjects.Count);
        Assert.Contains(entry.ExpectedSubjects, s => s.Subject.ObjectId == "tom");
        Assert.Contains(entry.ExpectedSubjects, s => s.Subject.ObjectId == "fred");
    }
}
