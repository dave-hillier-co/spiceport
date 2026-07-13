using System.Collections.Immutable;
using Spiceport.Core;
using Spiceport.Engine;

namespace Spiceport.Conformance.Tests;

/// <summary>
/// A fully parsed SpiceDB validation/consistency file: schema DSL text, the
/// relationships that make up the datastore, the boolean assertions to run, and the
/// (optional, usually absent in this corpus) `validation:` expected-subjects block.
/// </summary>
public sealed record ValidationFile(
    string SchemaText,
    ImmutableList<Relationship> Relationships,
    ImmutableList<ParsedAssertion> Assertions,
    ImmutableList<ValidationEntry> Validations);

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

/// <summary>
/// A single subject term parsed out of a validation-block expected-subjects string, e.g.
/// <c>user:tom</c> (terminal), <c>group:eng#member</c> (subject-with-relation), or
/// <c>user:*</c> (wildcard). <see cref="IsCaveated"/> mirrors SpiceDB's bracket-ellipsis
/// marker (<c>[...]</c>) directly suffixed to the subject: "this subject reaches the
/// permission only via a caveat expression", not a fully-evaluated caveat context.
/// </summary>
public sealed record ExpectedSubjectTerm(ObjectAndRelation Subject, bool IsCaveated);

/// <summary>
/// One <c>[subject...]</c> entry of a validation-block expected-subjects list, e.g.
/// <c>"[user:tom] is &lt;document:first#viewer&gt;"</c>. The <c>is &lt;...&gt;</c>
/// resource-path suffix is a human-readable breadcrumb only (per SpiceDB's own
/// <c>pkg/validationfile/blocks</c> grammar) and is intentionally NOT modeled here — only
/// <see cref="Term"/> (and, for a wildcard term, its <see cref="Exceptions"/>) matters for
/// assertion purposes.
/// </summary>
/// <param name="Term">The subject (or wildcard) this entry asserts.</param>
/// <param name="Exceptions">
/// For a wildcard <see cref="Term"/>, the concrete subjects excluded from it (SpiceDB's
/// <c>excluded_subjects</c>): the wildcard means "every subject of the type <em>except</em>
/// these". Each exception carries its own optional caveat marker. Empty for non-wildcard terms.
/// </param>
public sealed record ExpectedSubject(
    ExpectedSubjectTerm Term,
    ImmutableList<ExpectedSubjectTerm> Exceptions)
{
    public ObjectAndRelation Subject => Term.Subject;

    public bool IsCaveated => Term.IsCaveated;
}

/// <summary>
/// One <c>resource#permission: [...]</c> entry of a validation block: the resource ONR key
/// and the full set of subjects it is expected to resolve to.
/// </summary>
public sealed record ValidationEntry(
    ObjectAndRelation ObjectAndRelation,
    ImmutableList<ExpectedSubject> ExpectedSubjects);
