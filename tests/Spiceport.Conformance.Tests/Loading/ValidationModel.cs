using System.Collections.Immutable;
using Spiceport.Core;
using Spiceport.Engine;

namespace Spiceport.Conformance.Tests;

/// <summary>
/// A fully parsed SpiceDB validation/consistency file: schema DSL text, the
/// relationships that make up the datastore, and the boolean assertions to run.
/// </summary>
public sealed record ValidationFile(
    string SchemaText,
    ImmutableList<Relationship> Relationships,
    ImmutableList<ParsedAssertion> Assertions);

/// <summary>
/// The expected outcome of a single Check assertion. Maps directly onto the
/// engine <see cref="Membership"/> verdict the harness should observe.
/// </summary>
public enum AssertionExpectation
{
    /// <summary>assertTrue: the Check is expected to return a definite allow (<see cref="Membership.Member"/>).</summary>
    True = 0,

    /// <summary>assertFalse: the Check is expected to return a definite deny (<see cref="Membership.NotMember"/>).</summary>
    False = 1,

    /// <summary>
    /// assertCaveated: the Check is expected to be conditional on a caveat
    /// (<see cref="Membership.Caveated"/>), i.e. it needs context to resolve.
    /// </summary>
    Caveated = 2,
}

/// <summary>
/// A single parsed assertion, ready to be translated into a Check call.
/// </summary>
/// <param name="Resource">The resource ONR (object type, id and relation/permission).</param>
/// <param name="Subject">The subject ONR.</param>
/// <param name="Expectation">The expected membership outcome.</param>
/// <param name="CaveatContext">
/// Optional caveat context supplied via the <c>... with {json}</c> suffix.
/// This context overrides any context embedded in the relationship tuple.
/// </param>
/// <param name="SourceText">The original assertion line, retained for diagnostics.</param>
public sealed record ParsedAssertion(
    ObjectAndRelation Resource,
    ObjectAndRelation Subject,
    AssertionExpectation Expectation,
    IReadOnlyDictionary<string, object?>? CaveatContext,
    string SourceText)
{
    /// <summary>
    /// True for assertTrue assertions. Retained for callers that only distinguish
    /// allow/deny; prefer <see cref="Expectation"/> or <see cref="ExpectedMembership"/>.
    /// </summary>
    public bool Expected => Expectation == AssertionExpectation.True;

    /// <summary>The expected engine <see cref="Membership"/> verdict for this assertion.</summary>
    public Membership ExpectedMembership => Expectation switch
    {
        AssertionExpectation.True => Membership.Member,
        AssertionExpectation.False => Membership.NotMember,
        AssertionExpectation.Caveated => Membership.Caveated,
        _ => Membership.NotMember,
    };
}
