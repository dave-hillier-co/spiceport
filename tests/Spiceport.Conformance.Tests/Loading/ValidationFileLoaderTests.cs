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
}
