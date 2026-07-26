using Spiceport.Schema;

namespace Spiceport.Engine.Tests;

/// <summary>
/// Unit tests for <see cref="SchemaTypeValidator"/>, mirroring SpiceDB's <c>TypeSystem.Validate</c>
/// (<c>pkg/schema/typesystem_validation.go</c>) and <c>ValidateCaveatDefinition</c>
/// (<c>internal/namespace/caveats.go</c>): undefined references, permission-on-left-of-arrow, wildcard
/// in arrow, missing allowed types, undefined caveat, duplicate/reused names, and the
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

    // --- Wildcard reachable through a userset reference ("wildcard relations cannot be transitively
    // included") -- issue #33. Real SpiceDB v1.49.2 rejects every shape below at WriteSchema with
    // FailedPrecondition; verified empirically against the container (see
    // tests/Spiceport.Differential.Tests/WriteSchemaWildcardTransitivityTests.cs). A relation is validated
    // wherever it is DEFINED, so a chain of cross-definition userset references is still caught: the link
    // that directly names a wildcard-bearing relation/subrelation fails on its own, regardless of how
    // deeply it is nested under the schema's other definitions.

    [Fact]
    public void Wildcard_ReachableOneLevel_ThroughUserset_IsRejected()
    {
        var ex = ValidateThrows("""
            definition user {}
            definition group {
                relation member: user:*
            }
            definition document {
                relation viewer: group#member
            }
            """);
        Assert.Contains("wildcard", ex.Message);
        Assert.Contains("group#member", ex.Message);
    }

    [Fact]
    public void Wildcard_ReachableTwoLevels_ThroughUserset_IsRejected()
    {
        // document.viewer -> team#groupmember -> group#member (user:*). The offending link is
        // team.groupmember itself (validated independently of who references `team#groupmember`), so the
        // rejection fires regardless of chain depth.
        var ex = ValidateThrows("""
            definition user {}
            definition group {
                relation member: user:*
            }
            definition team {
                relation groupmember: group#member
            }
            definition document {
                relation viewer: team#groupmember
            }
            """);
        Assert.Contains("wildcard", ex.Message);
    }

    [Fact]
    public void Wildcard_ReachableThroughUserset_ViaUnionMember_IsRejected()
    {
        // The wildcard-bearing relation is one union arm of an otherwise-fine relation; still rejected,
        // because `team.groupmember` itself directly names the wildcard-bearing `group#member`.
        var ex = ValidateThrows("""
            definition user {}
            definition group {
                relation member: user:*
            }
            definition team {
                relation directmember: user
                relation groupmember: group#member
                permission member = directmember + groupmember
            }
            definition document {
                relation viewer: team#member
            }
            """);
        Assert.Contains("wildcard", ex.Message);
    }

    [Fact]
    public void DirectWildcard_OnBaseRelation_IsAccepted()
    {
        // A wildcard on the relation where it is DEFINED is legal SpiceDB -- only a userset reference TO
        // a wildcard-bearing relation from elsewhere is rejected.
        Validate("""
            definition user {}
            definition document {
                relation viewer: user:*
            }
            """);
    }

    [Fact]
    public void Wildcard_ReachableOnlyThroughAPermission_IsAccepted()
    {
        // Real SpiceDB does NOT walk into a permission's rewrite tree for the transitive-wildcard check --
        // only base-relation userset references are checked. A permission computed via an arrow over a
        // wildcard-bearing relation is a legal userset type.
        Validate("""
            definition user {}
            definition group {
                relation member: user:*
            }
            definition team {
                relation groupmember: group
                permission allmembers = groupmember->member
            }
            definition document {
                relation viewer: team#allmembers
            }
            """);
    }

    [Fact]
    public void Wildcard_ReachableThroughSameDefinition_CrossRelation_IsAccepted()
    {
        // Real SpiceDB only rejects a CROSS-definition userset reference to a wildcard-bearing relation;
        // referencing a wildcard-bearing relation of the SAME definition is accepted (mirrors the
        // recursive-group self-reference exception below).
        Validate("""
            definition user {}
            definition group {
                relation adminwildcard: user:*
                relation member: group#adminwildcard
            }
            """);
    }

    [Fact]
    public void Relation_AllowingItself_IsAccepted()
    {
        // Canonical recursive-group SpiceDB shape (the conformance corpus' directgroups.yaml);
        // real SpiceDB's WriteSchema accepts it, so ours must too.
        Validate("""
            definition user {}
            definition group {
                relation member: user | group#member
            }
            """);
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
