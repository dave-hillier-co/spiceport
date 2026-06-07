using Spiceport.Schema;

namespace Spiceport.Engine.Tests;

/// <summary>
/// Unit tests for <see cref="SchemaTypeValidator"/>, mirroring SpiceDB's <c>TypeSystem.Validate</c>
/// (<c>pkg/schema/typesystem_validation.go</c>) and <c>ValidateCaveatDefinition</c>
/// (<c>internal/namespace/caveats.go</c>): undefined references, permission-on-left-of-arrow, wildcard
/// in arrow, self-reference, missing allowed types, undefined caveat, duplicate/reused names, and the
/// caveat-definition rules (≥1 parameter, parseable CEL, every declared parameter referenced).
/// </summary>
public class SchemaTypeValidatorTests
{
    private static void Validate(string schemaText) =>
        SchemaTypeValidator.Validate(SchemaCompiler.CompileSchema(schemaText));

    private static SchemaTypeException ValidateThrows(string schemaText) =>
        Assert.Throws<SchemaTypeException>(() => Validate(schemaText));

    [Fact]
    public void ValidSchema_Passes()
    {
        Validate("""
            definition user {}
            definition group {
                relation member: user
            }
            definition document {
                relation viewer: user | group#member
                relation parent: document
                permission view = viewer + parent->view
            }
            """);
    }

    [Fact]
    public void Permission_ReferencingUndefinedRelation_IsRejected()
    {
        var ex = ValidateThrows("""
            definition user {}
            definition document {
                permission view = nonexistent
            }
            """);
        Assert.Contains("nonexistent", ex.Message);
    }

    [Fact]
    public void Permission_OnLeftOfArrow_IsRejected()
    {
        var ex = ValidateThrows("""
            definition user {}
            definition document {
                relation viewer: user
                permission edit = viewer
                permission view = edit->something
            }
            """);
        Assert.Contains("left hand side of an arrow", ex.Message);
    }

    [Fact]
    public void Wildcard_OnLeftOfArrow_IsRejected()
    {
        var ex = ValidateThrows("""
            definition user {}
            definition document {
                relation parent: user:*
                permission view = parent->view
            }
            """);
        Assert.Contains("wildcard", ex.Message);
    }

    [Fact]
    public void Relation_AllowingItself_IsRejected()
    {
        var ex = ValidateThrows("""
            definition document {
                relation viewer: document#viewer
            }
            """);
        Assert.Contains("itself", ex.Message);
    }

    [Fact]
    public void AllowedSubjectType_WithUndefinedNamespace_IsRejected()
    {
        var ex = ValidateThrows("""
            definition document {
                relation viewer: missingtype
            }
            """);
        Assert.Contains("missingtype", ex.Message);
    }

    [Fact]
    public void AllowedSubjectSubrelation_Undefined_IsRejected()
    {
        var ex = ValidateThrows("""
            definition user {}
            definition group {
                relation member: user
            }
            definition document {
                relation viewer: group#nosuchrel
            }
            """);
        Assert.Contains("nosuchrel", ex.Message);
    }

    [Fact]
    public void WithUndefinedCaveat_IsRejected()
    {
        var ex = ValidateThrows("""
            definition user {}
            definition document {
                relation viewer: user with nosuchcaveat
            }
            """);
        Assert.Contains("nosuchcaveat", ex.Message);
    }

    [Fact]
    public void DuplicateAllowedType_IsRejected()
    {
        var ex = ValidateThrows("""
            definition user {}
            definition document {
                relation viewer: user | user
            }
            """);
        Assert.Contains("duplicate", ex.Message);
    }

    [Fact]
    public void DuplicateDefinitionName_IsRejected()
    {
        var ex = ValidateThrows("""
            definition user {}
            definition user {}
            """);
        Assert.Contains("reused", ex.Message);
    }

    [Fact]
    public void NameReusedBetweenDefinitionAndCaveat_IsRejected()
    {
        var ex = ValidateThrows("""
            definition thing {}
            caveat thing(x int) { x > 0 }
            """);
        Assert.Contains("reused", ex.Message);
    }

    [Fact]
    public void Caveat_WithNoParameters_IsRejected()
    {
        var ex = ValidateThrows("""
            definition user {}
            caveat always() { true }
            """);
        Assert.Contains("at least one parameter", ex.Message);
    }

    [Fact]
    public void Caveat_WithUnusedParameter_IsRejected()
    {
        var ex = ValidateThrows("""
            definition user {}
            caveat c(used int, unused string) { used > 0 }
            """);
        Assert.Contains("unused", ex.Message);
    }

    [Fact]
    public void Caveat_WithUnparseableCel_IsRejected()
    {
        var ex = ValidateThrows("""
            definition user {}
            caveat c(x int) { x > > 0 }
            """);
        Assert.Contains("c", ex.Message);
    }

    [Fact]
    public void Caveat_AllParametersReferenced_Passes()
    {
        Validate("""
            definition user {}
            definition document {
                relation viewer: user with ip_match
            }
            caveat ip_match(allowed string, user_ip string) { user_ip == allowed }
            """);
    }
}
