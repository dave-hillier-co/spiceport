using System.Collections.Immutable;
using Spiceport.Core;

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
/// The expected outcome of a single Check assertion.
/// </summary>
public enum AssertionExpectation
{
    /// <summary>assertTrue: the Check is expected to return a definite allow.</summary>
    True = 0,

    /// <summary>assertFalse: the Check is expected to return a definite deny.</summary>
    False = 1,
}

/// <summary>
/// A single parsed assertion, ready to be translated into a Check call.
/// </summary>
public sealed record ParsedAssertion(
    ObjectAndRelation Resource,
    ObjectAndRelation Subject,
    bool Expected,
    IReadOnlyDictionary<string, object?>? CaveatContext,
    string SourceText)
{
    /// <summary>The expected membership outcome (typed convenience accessor).</summary>
    public AssertionExpectation Expectation => Expected ? AssertionExpectation.True : AssertionExpectation.False;
}
